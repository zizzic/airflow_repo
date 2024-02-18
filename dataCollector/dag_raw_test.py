from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time
import json
import os
from datetime import datetime,timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs


# get streamer_list in rds
def get_s_list():
    # RDS 연결 설정
    ti = [["fe558c6d1b8ef3206ac0bc0419f3f564","0b33823ac81de48d5b78a38cdbc0ab94"],["devil0108"]]
    return ti

def chzzk_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='get_s_list_task')
    chzzk, afreeca = lists
    chzzk_ids = chzzk
    live_stream_data = {}

    for id in chzzk_ids:
        res = requests.get(f"https://api.chzzk.naver.com/polling/v2/channels/{id}/live-status")

        if res.status_code == 200:
            live_data = res.json()
            live = live_data["content"]["status"]
            if live == "OPEN":
                live_stream_data[id] = live_data
        else:
            pass
    answer = {'chzzk': live_stream_data}
    print(answer)
    with open('./live_stream_data.json', 'r+') as f:
        json.dump(answer, f, indent=4)


def afreeca_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='get_s_list_task')
    chzzk, afreeca = lists

    afreeca_ids = afreeca
    live_stream_data = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    def get_live_status(bjid, headers):
        # BJ의 생방송 상태와 game category, broadcast title 등의 정보를 조회하는 함수
        live_status_url = (
            f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bjid}"
        )
        live_res = requests.post(
            live_status_url, headers=headers, data={"bid": bjid, "type": "live"}
        ).json()["CHANNEL"]
        return live_res

    def get_broad_info(bjid, headers):
        # BJ의 생방송 상태, 실시간 시청자 수, timestamp를 조회하는 함수
        broad_url = f"https://bjapi.afreecatv.com/api/{bjid}/station"
        broad_res = requests.get(broad_url, headers=headers).json()
        return broad_res

    if afreeca_ids != []:
        for bjid in afreeca_ids:
            live_res = get_live_status(bjid, headers)
            broad_res = get_broad_info(bjid, headers)
            broad_info = broad_res.get("broad")

            if live_res.get("RESULT") and broad_info:
                combined_res = {
                    "live_status":live_res,
                    "broad_info":broad_res
                }
                live_stream_data[bjid] = combined_res

            else:
                pass

    answer = {'afreeca':live_stream_data}

    # 파일 읽기
    try:
        with open('./live_stream_data.json', 'r') as f:
            data = json.load(f)  # 파일에서 기존 데이터 로드
    except FileNotFoundError:
        data = {}  # 파일이 없으면 빈 딕셔너리 사용
    print(data)
    # answer 딕셔너리를 기존 data 딕셔너리에 추가/업데이트
    data.update(answer)

    # 'stream_data'라는 최상위 키로 전체 데이터를 감싸는 새로운 딕셔너리 생성
    stream_data = {'stream_data': data}

    # 변경된 전체 데이터를 JSON 파일에 저장
    with open('./live_stream_data.json', 'w') as f:
        json.dump(stream_data, f, indent=4)

## test data
#
# data_dict = {"apple": 0.5, "milk": 2.5, "bread": 4.0}
# data_json = json.dumps(data_dict)

bucket_name = "de-2-1-bucket"

# dag codes
with DAG(
    'get_raw_data',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024,1,17),
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval="*/5 * * * *",
    catchup=False,
)as dag:
    
    task_get_s_list = PythonOperator(
        task_id='get_s_list_task',
        python_callable=get_s_list
    )
    task_raw_chzzk = PythonOperator(
        task_id='chzzk_raw_task',
        python_callable=chzzk_raw
    )
    task_raw_afreeca = PythonOperator(
        task_id='afreeca_raw_task',
        python_callable=afreeca_raw
    )

    # load
    current_time = '{{ data_interval_end}}'
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = 'raw_live_viewer'

    filename = './live_stream_data.json'

    # 파일이 존재하는지 먼저 확인
    if not os.path.exists(filename):
        # 파일이 존재하지 않으면 빈 JSON 객체로 새 파일 생성
        with open(filename, 'w') as f:
            json.dump({}, f)

    # 파일을 읽고 쓰기 모드로 열기
    with open(filename, 'r+') as f:
        # 파일 내용 읽기
        try:
            data_json = json.load(f)
        except json.JSONDecodeError:
            # 파일이 비어있거나 JSON 형식이 아닐 경우 빈 객체 사용
            data_json = {}

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
task_get_s_list >> task_raw_chzzk >> task_raw_afreeca >> task_load_raw_data

