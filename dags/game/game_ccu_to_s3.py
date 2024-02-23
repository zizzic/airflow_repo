from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

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
def get_ccu():
    # 1. GCE의 RDS의 GAME_INFO 테이블에서 게임 리스트 가져오기
    cur = connect_to_mysql()
    sql = """
            SELECT game_id, game_nm
            FROM GAME_INFO
            WHERE
                CASE
                WHEN IS_TOP300 = "T" THEN TRUE
                WHEN IS_TOP300 = "F" AND DATE(REDO_TIME) >= CURDATE() THEN TRUE
                ELSE FALSE
                END;
            """
    cur.execute(sql)
    records = cur.fetchall()

    data = []  # 모든 API 응답을 저장할 리스트

    # 2. Steam API를 통해 게임별 현재 접속자 수 가져오기
    for appid, game in records:
        url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1?appid={appid}"
        response = requests.get(url)
        temp = response.json()["response"]

        if temp["result"] != 1:  # 실패했을 경우 최대 3번까지 3초 간격으로 재시도
            for _ in range(3):
                time.sleep(3)
                response = requests.get(url)
                temp = response.json()["response"]
                if temp["result"] == 1:
                    break
            else:
                print(f"Failed to get CCU of {game}!")
        else:
            temp["game_id"] = appid
            data.append(temp)  # API 응답을 리스트에 추가

        print("temp", temp)

    return data


# 3. API 응답들이 담긴 리스트를 JSON으로 저장
@task
def save_to_json(data):
    data = {"raw_game_ccu": data}
    result = json.dumps(data, ensure_ascii=False)

    return result


with DAG(
    dag_id="game_ccu_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="0,30 * * * *",  # 매 시간 0분과 30분에 시작
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=15),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    data = get_ccu()
    data_json = save_to_json(data)

    bucket_name = "de-2-1-bucket"

    current_time = "{{ data_interval_end.in_timezone('Asia/Seoul') }}"
    year = "{{ data_interval_end.in_timezone('Asia/Seoul').year }}"
    month = "{{ data_interval_end.in_timezone('Asia/Seoul').month }}"
    day = "{{ data_interval_end.in_timezone('Asia/Seoul').day }}"
    hour = "{{ data_interval_end.in_timezone('Asia/Seoul').hour }}"
    table_name = "raw_game_ccu"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/hour={hour}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
