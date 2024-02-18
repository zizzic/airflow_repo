from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time
import json
from datetime import datetime,timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs


# get streamer_list in rds
def get_s_list():
    # RDS 연결 설정
    return [["0d027498b18371674fac3ed17247e6b8", "fe558c6d1b8ef3206ac0bc0419f3f564",
            "0b33823ac81de48d5b78a38cdbc0ab94"],[""]]
    # pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres')
    #
    # # 데이터베이스에서 데이터 가져오기
    # sql = "SELECT * FROM STREAMER_INFO;"  # 적절한 SQL 쿼리로 대체
    # connection = pg_hook.get_conn()
    # cursor = connection.cursor()
    # cursor.execute(sql)
    # results = cursor.fetchall()
    #
    #
    # # streamer 분리 및 저장
    # chzzk = []
    # afreeca = []
    # for row in results:
    #     if row[1]:
    #         chzzk.append(row[0], row[1]) #streamer_nm,chz_id
    #     if row[2]:
    #         afreeca.append(row[0], row[2]) #streamer_nm,af_id
    #
    # cursor.close()
    # connection.close()

    #

def chzzk_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='get_s_list')
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
    with open('./live_stream_data.json', 'w') as f:
        json.dump(answer, f, indent=4)


def afreeca_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='get_s_list')
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
    try:
        with open('/live_stream_data.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}

    data.update(answer)

    with open('/live_stream_data.json', 'w') as f:
        data = json.load(f)
        json.dump(answer, f, indent=4)



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
        'start_date': datetime(2024,2,18),
        'retries':1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval="*/5 * * * *",
    catchup=False,
)as dag:
    
    task_get_s_list = PythonOperator(
        task_id='get_s_list',
        python_callable=get_s_list
    )
    task_raw_chzzk = PythonOperator(
        task_id='chzzk_raw',
        python_callable=chzzk_raw
    )
    task_raw_afreeca = PythonOperator(
        task_id='afreeca_raw',
        python_callable=afreeca_raw
    )

    # load
    current_time = '{{ data_interval_end}}'
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = 'raw_live_viewer'

    try:
        with open('/live_stream_data.json', 'r') as f:
            data_json = json.load(f)
    except FileNotFoundError:
        data_json = {}

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
task_get_s_list >> [task_raw_chzzk,task_raw_afreeca] >> task_load_raw_data

