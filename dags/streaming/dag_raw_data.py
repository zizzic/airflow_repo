from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException


import json
import os
import requests
from requests.exceptions import RequestException
import logging
from datetime import datetime, timedelta
import slack


# get streamer_list in rds
def get_s_list(**kwargs):
    # RDS 연결 설정
    try:
        mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
        result = mysql_hook.get_records(
            "SELECT STREAMER_ID, CHZ_ID, AFC_ID FROM STREAMER_INFO;"
        )
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        logging.error((error_msg))

        raise AirflowException(error_msg)

    chzzk, afc = [], []
    for row in result:
        if row[1]:
            chzzk.append((row[0], row[1]))
        if row[2]:
            afc.append((row[0], row[2]))

    kwargs["ti"].xcom_push(key="chzzk", value=chzzk)
    kwargs["ti"].xcom_push(key="afc", value=afc)


def chzzk_raw(current_time, **kwargs):
    chzzk_ids = kwargs["ti"].xcom_pull(key="chzzk", task_ids="get_s_list_task")
    live_stream_data = []

    for s_id, id in chzzk_ids:
        res = requests.get(
            f"https://api.chzzk.naver.com/polling/v2/channels/{id}/live-status"
        )

        if res.status_code == 200:
            live_data = res.json()
            try:
                live = live_data["content"]["status"]
                if live == "OPEN":
                    stream_data = {
                        "streamer_id": s_id,
                        "chzzk_s_id": id,
                        "current_time": current_time,
                        **live_data,
                    }
                    live_stream_data.append(stream_data)
            except KeyError as e:
                error_msg = f"Error occurred: {str(e)}"
                logging.error((error_msg))

                raise AirflowException(error_msg)

    with open(f"./chzzk_{current_time}.json", "w") as f:
        json.dump(live_stream_data, f, indent=4)


def afreeca_raw(current_time, **kwargs):
    afreeca_ids = kwargs["ti"].xcom_pull(key="afc", task_ids="get_s_list_task")

    live_stream_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }

    def get_live_status(bjid, headers):
        # BJ의 생방송 상태와 game category, broadcast title 등의 정보를 조회하는 함수
        try:
            live_status_url = (
                f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bjid}"
            )
            live_res = requests.post(
                live_status_url, headers=headers, data={"bid": bjid, "type": "live"}
            ).json()["CHANNEL"]
        except RequestException:
            live_res = {}
        return live_res

    def get_broad_info(bjid, headers):
        # BJ의 생방송 상태, 실시간 시청자 수, timestamp를 조회하는 함수
        broad_url = f"https://bjapi.afreecatv.com/api/{bjid}/station"
        try:
            broad_res = requests.get(broad_url, headers=headers).json()
        except RequestException:
            broad_res = {}

        return broad_res

    if afreeca_ids != []:
        for s_id, bjid in afreeca_ids:
            live_res = get_live_status(bjid, headers)
            broad_res = get_broad_info(bjid, headers)

            if broad_res != {}:
                broad_info = broad_res.get("broad")
            if live_res != {}:
                live_stat = live_res.get("RESULT")

            try:
                if live_stat and broad_info:
                    combined_res = {"live_status": live_res, "broad_info": broad_res}

                    stream_data = {
                        "streamer_id": s_id,
                        "afc_s_id": bjid,
                        "current_time": current_time,
                        **combined_res,
                    }
                    live_stream_data.append(stream_data)
            except (AttributeError, TypeError) as e:
                error_msg = f"Error occurred: {str(e)}"
                logging.error((error_msg))
                raise AirflowException(error_msg)

    with open(f"./afc_{current_time}.json", "w") as f:
        json.dump(live_stream_data, f, indent=4)


def merge_json(local_path, **kwargs):
    # 파일 읽고 기존 데이터 로드
    try:
        with open(f"./chzzk_{current_time}.json", "r") as f:
            chzzk_data = json.load(f)
    except FileNotFoundError:
        chzzk_data = []

    try:
        with open(f"./afc_{current_time}.json", "r") as f:
            afreeca_data = json.load(f)
    except FileNotFoundError:
        afreeca_data = []

    # 'stream_data'라는 최상위 키로 전체 데이터를 감싸는 새로운 딕셔너리 생성
    stream_data = {
        "stream_data": {
            "chzzk": chzzk_data,
            "afreeca": afreeca_data,
        }
    }

    with open(f"{local_path}", "w") as f:
        json.dump(stream_data, f, indent=4)


def upload_file_to_s3_without_reading(
    local_file_path, bucket_name, s3_key, aws_conn_id
):
    # S3Hook을 사용하여 파일을 S3에 업로드
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_file(
            filename=local_file_path, bucket_name=bucket_name, replace=True, key=s3_key
        )
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        logging.error((error_msg))

        raise AirflowException(error_msg)


def delete_file(file_path):
    """주어진 경로의 파일을 삭제하는 함수"""
    if os.path.exists(file_path):
        os.remove(file_path)
        # print(f"Deleted {file_path}")


def delete_files(**kwargs):
    # 플랫폼별로 파일 삭제
    platforms = ["afc", "chzzk"]
    for platform in platforms:
        file_path = f"./{platform}_{current_time}.json"
        delete_file(file_path)

    # local_path를 사용하여 파일 삭제
    file_path = f"{local_path}"
    delete_file(file_path)


bucket_name = "de-2-1-bucket"

# dag codes
with DAG(
    "get_raw_data",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 17),
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    # load
    current_time = "{{ data_interval_end}}"
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = "raw_live_viewer"
    local_name = "local_raw_live"
    local_path = f"./{local_name}_{current_time}.json"

    task_get_s_list = PythonOperator(
        task_id="get_s_list_task",
        python_callable=get_s_list,
        on_failure_callback=slack.on_failure_callback,
    )
    task_raw_chzzk = PythonOperator(
        task_id="chzzk_raw_task",
        python_callable=chzzk_raw,
        op_kwargs={"current_time": current_time},
        on_failure_callback=slack.on_failure_callback,
    )
    task_raw_afreeca = PythonOperator(
        task_id="afreeca_raw_task",
        python_callable=afreeca_raw,
        op_kwargs={"current_time": current_time},
        on_failure_callback=slack.on_failure_callback,
    )
    task_merge_json = PythonOperator(
        task_id="merge_json_task",
        python_callable=merge_json,
        op_kwargs={"local_path": local_path},
        on_failure_callback=slack.on_failure_callback,
    )
    task_load_raw_data = PythonOperator(
        task_id="upload_file_directly_to_s3",
        python_callable=upload_file_to_s3_without_reading,
        op_kwargs={
            "local_file_path": local_path,
            "bucket_name": bucket_name,
            "s3_key": f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
            "aws_conn_id": "aws_conn_id",
        },
        on_failure_callback=slack.on_failure_callback,
    )

    delete_local_files = PythonOperator(
        task_id="delete_local_files_task",
        python_callable=delete_files,
    )

    # 파일이 존재하는지 먼저 확인
    if not os.path.exists(local_path):
        # 파일이 존재하지 않으면 빈 JSON 객체로 새 파일 생성
        with open(local_path, "w") as f:
            json.dump({}, f)

    # 파일을 읽고 쓰기 모드로 열기
    with open(local_path, "r+") as f:
        # 파일 내용 읽기
        try:
            data_json = json.load(f)
        except json.JSONDecodeError:
            # 파일이 비어있거나 JSON 형식이 아닐 경우 빈 객체 사용
            data_json = {}

    data_json = json.dumps(data_json)

task_get_s_list >> [task_raw_chzzk, task_raw_afreeca] >> task_merge_json
task_merge_json >> task_load_raw_data >> delete_local_files
