from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.models import Variable


# from plugins import slack
from top_300_games import games


# Game info 테이블에 있는 Game들의 app_id를 이용해 게임 정량 평가를 가져오는 함수
def get_game_reviews(app_id):
    # API URL
    api_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    # API 키를 설정
    api_key = Variable.get("steam_api_key")

    # API 요청 매개변수 설정
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
        return data
    #         reviews = data.get('reviews', [])
    #         for review in reviews:
    #             print(f"Review ID: {review['recommendationid']}")
    #             print(f"Author: {review['author']['steamid']}")
    #             print(f"Review: {review['review']}")
    #             print(f"Recommended: {review['voted_up']}")
    #             print(f"----------------------------------")
    else:
        print(f"Error: Unable to fetch reviews. Status Code: {response.status_code}")


with DAG(
    dag_id="game_comment_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="@once",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        # 'on_failure_callback': slack.on_failure_callback,
    },
) as dag:

    @task
    def get_ratings_task():
        data = list()
        for app_id, game in games.items():
            temp = dict()
            temp[app_id] = get_game_reviews(app_id)
            print(game, temp)
            data.append(temp)
            print()
        return data

    @task
    def save_to_json(data):
        result = json.dumps(data)  # API 응답들이 담긴 리스트를 JSON으로 저장

        return result

    data = get_ratings_task()
    data_json = save_to_json(data)

    bucket_name = "de-2-1-bucket"

    current_time = "{{ data_interval_end }}"
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = "RAW_GAME_REVIEW"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
