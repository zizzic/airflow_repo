from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import slack

import re
import requests
import json


def connect_to_mysql():
    db_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    conn = db_hook.get_conn()
    conn.autocommit = True

    return conn.cursor()


# Game info 테이블에 있는 Game들의 app_id를 이용해 게임 정량 평가를 가져오는 함수
def get_rating(app_id):
    url = f"https://store.steampowered.com/app/{app_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    reviewdesc_short = soup.find_all("span", {"class": "responsive_reviewdesc_short"})

    if reviewdesc_short == []:
        return {
            "game_id": app_id,
            "ALL_POSITIVE_NUM": 0,
            "ALL_POSITIVE_PERCENT": 0,
            "RECENT_POSITIVE_NUM": 0,
            "RECENT_POSITIVE_PERCENT": 0,
        }

    recent_reviews = None
    all_reviews = None

    for review in reviewdesc_short:
        if "Recent" in review.text:
            recent_reviews = review
        elif "All Time" in review.text:
            all_reviews = review

    recent_reviews_text = (
        recent_reviews.text.strip().replace("\xa0", " ")
        if recent_reviews
        else "No recent reviews"
    )
    all_reviews_text = (
        all_reviews.text.strip().replace("\xa0", " ") if all_reviews else "No reviews"
    )

    # 최근, 모든 평가에서 추출한 숫자들을 리스트로 저장
    recent_reviews_numbers = [
        int(num) for num in re.findall(r"\d+", recent_reviews_text.replace(",", ""))
    ][::-1]
    all_reviews_numbers = [
        int(num) for num in re.findall(r"\d+", all_reviews_text.replace(",", ""))
    ][::-1]

    data = {
        "game_id": app_id,
        "all_positive_num": all_reviews_numbers[0],
        "all_positive_percent": all_reviews_numbers[1],
    }

    if len(recent_reviews_numbers) == 0:
        data["recent_positive_num"] = 0
        data["recent_positive_percent"] = 0
    else:
        data["recent_positive_num"] = recent_reviews_numbers[0]
        data["recent_positive_percent"] = recent_reviews_numbers[1]

    return data


@task
def get_ratings_task():
    # 1. GCE의 RDS의 GAME_INFO 테이블에서 게임 리스트 가져오기
    cur = connect_to_mysql()
    sql = """
            SELECT game_id, game_nm
            FROM GAME_INFO
            WHERE IS_TOP300 IN ("T", "F");
            """
    cur.execute(sql)
    records = cur.fetchall()

    data = []
    # 2. Steam API를 통해 게임별 정량 평가 가져오기
    for app_id, game in records:
        temp = get_rating(app_id)
        data.append(temp)
        print(game, temp)
    return data


# 3. API 응답들이 담긴 리스트를 JSON으로 저장
@task
def save_to_json(data):
    data = {"raw_game_rating": data}
    result = json.dumps(
        data, ensure_ascii=False
    )  # API 응답들이 담긴 리스트를 JSON으로 저장

    return result


with DAG(
    dag_id="game_rating_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="0 0 * * *",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    data = get_ratings_task()
    data_json = save_to_json(data)

    bucket_name = "de-2-1-bucket"

    current_time = "{{ data_interval_end.in_timezone('Asia/Seoul') }}"
    year = "{{ data_interval_end.in_timezone('Asia/Seoul').year }}"
    month = "{{ data_interval_end.in_timezone('Asia/Seoul').month }}"
    day = "{{ data_interval_end.in_timezone('Asia/Seoul').day }}"
    hour = "{{ data_interval_end.in_timezone('Asia/Seoul').hour }}"
    table_name = "raw_game_rating"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
