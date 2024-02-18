from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
# from plugins import slack
from top_300_games import games

import re
import pandas as pd
import requests
import json


# Game info 테이블에 있는 Game들의 app_id를 이용해 게임 정량 평가를 가져오는 함수
def get_rating(app_id):
    url = f"https://store.steampowered.com/app/{app_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    reviewdesc_short = soup.find_all('span', {'class': 'responsive_reviewdesc_short'})
    
    if reviewdesc_short == []:
        return {'ALL_POSITIVE_NUM': 0, 'ALL_POSITIVE_PERCENT': 0, 'RECENT_POSITIVE_NUM': 0, 'RECENT_POSITIVE_PERCENT': 0}

    recent_reviews = None
    all_reviews = None

    for review in reviewdesc_short:
        if "Recent" in review.text:
            recent_reviews = review
        elif "All Time" in review.text:
            all_reviews = review

    recent_reviews_text = (recent_reviews.text.strip().replace('\xa0', ' ') if recent_reviews else 'No recent reviews')
    all_reviews_text = (all_reviews.text.strip().replace('\xa0', ' ') if all_reviews else 'No reviews')

    # 최근, 모든 평가에서 추출한 숫자들을 리스트로 저장
    recent_reviews_numbers = [int(num) for num in re.findall(r'\d+', recent_reviews_text.replace(',', ''))][::-1]
    all_reviews_numbers = [int(num) for num in re.findall(r'\d+', all_reviews_text.replace(',', ''))][::-1]
    
    print('r', recent_reviews_numbers)
    print('a', all_reviews_numbers)
    
    data = {'ALL_POSITIVE_NUM': all_reviews_numbers[0],
            'ALL_POSITIVE_PERCENT': all_reviews_numbers[1]}
    
    if len(recent_reviews_numbers) == 0:
        data['RECENT_POSITIVE_NUM'] = 0
        data['RECENT_POSITIVE_PERCENT'] = 0
    else:
        data['RECENT_POSITIVE_NUM'] = recent_reviews_numbers[0]
        data['RECENT_POSITIVE_PERCENT'] = recent_reviews_numbers[1]
    
    return data


with DAG(
    dag_id = 'game_rating_to_s3',
    start_date = datetime(2024,1,1),
    catchup=False,
    tags=['Steam_API'],
    schedule_interval = '@once',
    default_args = {
        'retries': 3,
        'retry_delay': timedelta(minutes=1),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    @task
    def get_ratings_task():
        data = list()
        for app_id, game in games.items():
            temp = dict()
            temp[app_id] = get_rating(app_id)
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
    table_name = 'RAW_GAME_RATING'

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
