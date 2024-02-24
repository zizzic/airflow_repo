from datetime import datetime, timedelta

import logging
import requests

import slack

from requests.exceptions import RequestException

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException


# get streamer_list in rds
def get_s_list(**kwargs):
    # RDS 연결 설정
    mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    query = "SELECT STREAMER_ID, CHZ_ID, AFC_ID FROM STREAMER_INFO;"
    try:
        result = mysql_hook.get_records(query) or []
        chzzk = [(row[0], row[1]) for row in result if row[1]]
        afc = [(row[0], row[2]) for row in result if row[2]]
    except Exception as e:
        logging.error(f"Error occurred while fetching streamer list: {e}")
        raise AirflowException("Failed to fetch streamer list from RDS.")

    kwargs["ti"].xcom_push(key="chzzk", value=chzzk)
    kwargs["ti"].xcom_push(key="afc", value=afc)


def chzzk_banner(**kwargs):
    chzzk_ids = kwargs["ti"].xcom_pull(key="chzzk", task_ids="get_s_list_task")
    mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")

    for s_id, chzzk_id in chzzk_ids:
        try:
            res = requests.get(
                f"https://api.chzzk.naver.com/service/v1/channels/{chzzk_id}"
            )
            res.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code.
            banner_url = res.json()["content"]["channelImageUrl"]

            update_stmt = """
                UPDATE STREAMER_INFO
                SET CHZ_BANNER_URL = %s
                WHERE CHZ_ID = %s
            """
            mysql_hook.run(update_stmt, parameters=(banner_url, chzzk_id))
        except RequestException as e:
            logging.error(f"Request failed: {e}")
        except KeyError as e:
            logging.error(f"Key error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise AirflowException(
                f"Failed to update CHZ_BANNER_URL for CHZ_ID {chzzk_id}"
            )


def afreeca_banner(**kwargs):
    afreeca_ids = kwargs["ti"].xcom_pull(key="afc", task_ids="get_s_list_task")
    mysql_hook = MySqlHook(mysql_conn_id="aws_rds_conn_id")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }

    for s_id, bjid in afreeca_ids:
        try:
            res = requests.get(
                f"https://bjapi.afreecatv.com/api/{bjid}/station", headers=headers
            )
            res.raise_for_status()
            banner_url = f'https:{res.json().get("profile_image")}'

            update_stmt = """
                UPDATE STREAMER_INFO
                SET AFC_BANNER_URL = %s
                WHERE AFC_ID = %s
            """
            mysql_hook.run(update_stmt, parameters=(banner_url, bjid))
        except RequestException as e:
            logging.error(f"Request failed: {e}")
        except KeyError as e:
            logging.error(f"Key error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise AirflowException(f"Failed to update AFC_BANNER_URL for AFC_ID {bjid}")


# dag codes
with DAG(
    "streamer_banner_raw",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 17),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@once",
    tags=["Streamer", "banner"],
    catchup=False,
) as dag:

    # load

    task_get_s_list = PythonOperator(
        task_id="get_s_list_task",
        python_callable=get_s_list,
        on_failure_callback=slack.on_failure_callback,
    )
    task_raw_chzzk = PythonOperator(
        task_id="chzzk_raw_task",
        python_callable=chzzk_banner,
        on_failure_callback=slack.on_failure_callback,
    )
    task_raw_afreeca = PythonOperator(
        task_id="afreeca_raw_task",
        python_callable=afreeca_banner,
        on_failure_callback=slack.on_failure_callback,
    )

task_get_s_list >> [task_raw_chzzk, task_raw_afreeca]
