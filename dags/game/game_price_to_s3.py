from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import slack

import requests
import json
import time


def connect_to_mysql():
    db_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    conn = db_hook.get_conn()
    conn.autocommit = True

    return conn.cursor()


@task
def get_price():
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
    # 2. Steam API를 통해 게임별 가격 가져오기
    for appid, name in records:
        appid = str(appid)

        url = f"http://store.steampowered.com/api/appdetails?appids={appid}&cc=kr"
        api_key = Variable.get("steam_api_key")
        params = {
            "key": api_key,
            "json": "1",
        }

        response = requests.get(url, params=params)
        temp = response.json()

        # 실패했을 경우 최대 3번까지 90초 간격으로 재시도
        if not temp or not temp[appid]["success"]:  # {"1891701":{"success":false}}
            for i in range(3):
                time.sleep(90)
                url = (
                    f"http://store.steampowered.com/api/appdetails?appids={appid}&cc=kr"
                )
                response = requests.get(url, params=params)
                temp = response.json()

                if temp and temp[appid]["success"]:
                    break
                print(f"The response of {appid, name} failed! {i + 1} times")
            else:
                print(f"Failed to get price of {appid, name}!")

        if not temp or not temp[appid]["success"]:
            temp = {
                "success": False,
                "data": {
                    "steam_appid": int(appid),
                    "price_overview": {
                        "final": 999999999,
                        "initial": 999999999,
                        "discount_percent": 0,
                        "final_formatted": "₩ 999,999,999",
                    },
                },
            }
        else:
            temp = temp[appid]

        print(appid, name)
        # print(temp)
        data.append(temp)

    return data


# 3. API 응답들이 담긴 리스트를 JSON으로 저장
@task
def save_to_json(data):
    data = {"raw_game_price": data}
    result = json.dumps(data, ensure_ascii=False)

    return result


with DAG(
    dag_id="game_price_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="0 0 * * *",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=15),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    data = get_price()
    data_json = save_to_json(data)

    bucket_name = "de-2-1-bucket"

    current_time = "{{ data_interval_end.in_timezone('Asia/Seoul') }}"
    year = "{{ data_interval_end.in_timezone('Asia/Seoul').year }}"
    month = "{{ data_interval_end.in_timezone('Asia/Seoul').month }}"
    day = "{{ data_interval_end.in_timezone('Asia/Seoul').day }}"
    hour = "{{ data_interval_end.in_timezone('Asia/Seoul').hour }}"
    table_name = "raw_game_price"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
