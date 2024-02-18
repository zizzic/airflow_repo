from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import time
from datetime import datetime,timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs

# get streamer_list in elasticcache
def get_s_list():
    # test streamer_list
    return ["0d027498b18371674fac3ed17247e6b8", "fe558c6d1b8ef3206ac0bc0419f3f564",
            "0b33823ac81de48d5b78a38cdbc0ab94"]

def chzzk_raw(chzzk_ids):
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
    return live_stream_data

def afreeca_raw(afreeca_ids):
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

    return live_stream_data


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
    task_load_raw_data = DummyOperator(task_id='load')

task_get_s_list > [task_raw_chzzk,task_raw_afreeca] > task_load_raw_data

