from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import slack

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack.on_failure_callback,
}

def connect_to_redshift():
    db_hook = PostgresHook(postgres_conn_id="aws_redshift_conn_id")
    conn = db_hook.get_conn()
    conn.autocommit = True
    return conn.cursor()

def add_ccu(**kwargs):
    cur = connect_to_redshift()
    print("Successfully connected to Redshift")
    conn = cur.connection

    try:
        conn.autocommit = False

        sql = """
            CREATE TEMP TABLE newCCU AS
                SELECT *
                FROM dev.external_raw_data.table_name_raw_game_ccu
                WHERE collect_time > (SELECT MAX(collect_time) FROM dev.raw_data.raw_game_ccu);
        """
        cur.execute(sql)
        print("Make temp table newCCU")

        sql_insert = """
            INSERT INTO raw_data.raw_game_ccu(
                game_id,
                collect_time,
                game_ccu,
                year,
                month,
                day,
                hour
            )
            SELECT
                game_id,
                collect_time,
                game_ccu,
                year,
                month,
                day,
                hour
            FROM
                newCCU;
        """
        cur.execute(sql_insert)
        print("Successfully inserted new data into raw_data.raw_game_ccu")
        conn.commit()
        print("Successfully committed the transaction")

    except Exception as e:
        print("Error occurred. Start to rollback", e)
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()
        print("Connection to Redshift is closed")

with DAG(
    dag_id="raw_data_game_CCU_I",
    start_date=datetime(2024, 3, 7),
    catchup=False,
    tags=["ETL", "raw_data", "GAME", "incre"],
    schedule_interval=None,  # 이 DAG는 명시적으로 트리거될 때만 실행되도록 설정됩니다.
    default_args=default_args,
) as dag:

    add_ccu_task = PythonOperator(
        task_id='add_ccu',
        python_callable=add_ccu,
        provide_context=True,
    )
