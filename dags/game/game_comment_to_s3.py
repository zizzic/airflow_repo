from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import slack

import requests
import json


def connect_to_mysql():
    db_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    conn = db_hook.get_conn()
    conn.autocommit = True

    return conn.cursor()


def get_game_reviews(app_id):
    api_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    api_key = Variable.get("steam_api_key")
    params = {
        "key": api_key,
        "json": "1",
        "language": "korean",  # 한국어 리뷰들만 가져오기
        "num_per_page": 100,  # 페이지당 100개의 리뷰 가져오기
    }

    # API 요청 보내기
    response = requests.get(api_url, params=params)

    # 응답 확인
    if response.status_code == 200:
        data = response.json()
        data["game_id"] = app_id
        return data
    #         reviews = data.get("reviews", [])
    #         for review in reviews:
    #             print(f"Review ID: {review["recommendationid"]}")
    #             print(f"Author: {review["author"]["steamid"]}")
    #             print(f"Review: {review["review"]}")
    #             print(f"Recommended: {review["voted_up"]}")
    #             print(f"----------------------------------")
    else:
        print(f"Error: Unable to fetch reviews. Status Code: {response.status_code}")


@task
def get_ratings_task():
    # 1. GCE의 RDS의 GAME_INFO 테이블에서 게임 리스트 가져오기
    cur = connect_to_mysql()
    sql = """
            SELECT game_id
            FROM GAME_INFO
            WHERE IS_TOP300 IN ("T", "F");
            """
    cur.execute(sql)
    records = cur.fetchall()

    data = []
    # 2. Steam API를 통해 게임별 커멘트 가져오기
    for app_id in records:
        temp = get_game_reviews(app_id)
        data.append(temp)

    return data


# 3. API 응답들이 담긴 리스트를 JSON으로 저장
@task
def save_to_json(data):
    result = json.dumps(data, ensure_ascii=False)

    return result


with DAG(
    dag_id="game_comment_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="0 0 1 * *",
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
    table_name = "raw_game_review"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/hour={hour}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
