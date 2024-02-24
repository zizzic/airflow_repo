from datetime import datetime, timedelta

import requests

import slack
import time

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from requests.exceptions import RequestException


def get_game_info_from_api(appid, params):
    session = requests.Session()  # 세션을 사용하여 연결 재사용
    url = f"http://store.steampowered.com/api/appdetails?appids={appid}&cc=kr"

    for _ in range(3):  # 최대 3번 재시도
        try:
            response = session.get(url, params=params)
            response.raise_for_status()  # 상태 코드가 200이 아니면 예외 발생
            data = response.json()
            if data[appid]["success"]:
                return data[appid]["data"]["header_image"]
        except (RequestException, KeyError):
            time.sleep(90)  # 재시도 전 90초 대기
    return None  # 3번 시도 후 성공하지 못하면 None 반환


def update_banner_url_in_db(appid, banner_url):
    mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    update_stmt = """
        UPDATE GAME_INFO
        SET GAME_BANNER_URL = %s
        WHERE GAME_ID = %s
    """
    mysql_hook.run(update_stmt, parameters=(banner_url, appid))


def get_rds_game_info():
    mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    query = """
        SELECT game_id, game_nm
        FROM GAME_INFO
        WHERE IS_TOP300 IN ('T', 'F');
    """
    records = mysql_hook.get_records(query) or []

    api_key = Variable.get("steam_api_key")
    params = {"key": api_key, "json": "1"}

    for appid, name in records:
        banner_url = get_game_info_from_api(str(appid), params)
        if banner_url:
            update_banner_url_in_db(str(appid), banner_url)
        else:
            print(f"Failed to get banner for {name} (AppID: {appid})")


# dag codes
with DAG(
    "steam_game_banner_raw",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 17),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@once",
    tags=["game", "banner"],
    catchup=False,
) as dag:

    get_steam_game_banner = PythonOperator(
        task_id="get_steam_game_banner_task",
        python_callable=get_rds_game_info,
        on_failure_callback=slack.on_failure_callback,
    )


get_steam_game_banner
