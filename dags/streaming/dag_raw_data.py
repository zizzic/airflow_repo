from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

import json
import os
import requests
from datetime import datetime,timedelta

# get streamer_list in rds
def get_s_list(**kwargs):
    # RDS 연결 설정
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="get_data_using_query")

    return data

def process_data(**kwargs):
    ti = kwargs['ti']
    # XCom에서 SQL 쿼리 결과 가져오기
    sql_data = ti.xcom_pull(task_ids='get_s_list_task')
    chzzk = []
    afc = []
    for row in sql_data:
        if row[1] != '':
            chzzk.append(row[1])
        if row[2] != '':
            afc.append(row[2])
    # 가공된 데이터 반환
    return [chzzk, afc]


def chzzk_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='processing_task')
    print(lists)
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

    
    with open('./live_stream_data_chzzk.json', 'w') as f:
        json.dump(live_stream_data, f, indent=4)


def afreeca_raw(**kwargs):
    ti = kwargs['ti']
    lists = ti.xcom_pull(task_ids='processing_task')
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

    with open('./live_stream_data_afreeca.json', 'w') as f:
        json.dump(live_stream_data, f, indent=4)
    

def merge_json():
    # 파일 읽고 기존 데이터 로드
    try:
        with open('./live_stream_data_chzzk.json', 'r') as f:
            chzzk_data = json.load(f)  
    except FileNotFoundError:
        chzzk_data = {}

    try:
        with open('./live_stream_data_afreeca.json', 'r') as f:
            afreeca_data = json.load(f) 
    except FileNotFoundError:
        afreeca_data = {}

    # 'stream_data'라는 최상위 키로 전체 데이터를 감싸는 새로운 딕셔너리 생성
    stream_data = {'stream_data': {
        'chzzk':chzzk_data,
        'afreeca':afreeca_data,
    }}

    with open('./live_stream_data.json', 'w') as f:
        json.dump(stream_data, f, indent=4)

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
    
    task_get_s_list = MySqlOperator(
        task_id="get_s_list_task",
        sql='select STREAMER_ID,CHZ_ID,AFC_ID from STREAMER_INFO;',
        mysql_conn_id="aws_rds_conn_id"
    )
    task_processing_list = PythonOperator(
        task_id='processing_task',
        python_callable= process_data
    )
    task_raw_chzzk = PythonOperator(
        task_id='chzzk_raw_task',
        python_callable=chzzk_raw
    )
    task_raw_afreeca = PythonOperator(
        task_id='afreeca_raw_task',
        python_callable=afreeca_raw
    )

    task_merge_json = PythonOperator(
        task_id='merge_json_task',
        python_callable=merge_json
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
    data_json = json.dumps(data_json)
    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
task_get_s_list >> task_processing_list >> [task_raw_chzzk, task_raw_afreeca] >> task_merge_json >> task_load_raw_data

