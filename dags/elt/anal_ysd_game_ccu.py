from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import slack


def connect_to_redshift():
    db_hook = PostgresHook(postgres_conn_id="aws_redshift_conn_id")
    conn = db_hook.get_conn()
    conn.autocommit = True

    return conn.cursor()


@task
def elt():
    # 1. reshift external_raw_data 스키마
    cur = connect_to_redshift()
    print("Successfully connected to Redshift")
    conn = cur.connection

    try:
        # Start a new transaction
        conn.autocommit = False

        # analytics.ANAL_YSD_GAME_CCU 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.ANAL_YSD_GAME_CCU;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_YSD_GAME_CCU")

        # SELECT 쿼리의 결과를 analytics.ANAL_YSD_GAME_CCU 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_YSD_GAME_CCU(GAME_NM, CCU_AVG, created_date)
            SELECT
                b.game_nm AS GAME_NM,
                AVG(a.game_ccu) AS CCU_AVG,
                current_date AS CREATED_DATE
            FROM (SELECT GAME_ID, GAME_CCU
                FROM external_raw_data.table_name_raw_game_ccu
                WHERE CAST(collect_time AS timestamp) 
                    BETWEEN GETDATE() - INTERVAL '1 day' + INTERVAL '6 hours'
                AND GETDATE() + INTERVAL '6 hours') a
            JOIN external_raw_data.game_info b
            ON a.GAME_ID = b.GAME_ID
            GROUP BY b.GAME_NM;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_YSD_GAME_CCU")

        # 트랜잭션 commit
        conn.commit()
        print("Successfully committed the transaction")

    except Exception as e:
        # Rollback
        print("Error occurred. Start to rollback", e)
        conn.rollback()
        raise

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Connection to Redshift is closed")


with DAG(
    dag_id="ELT_Anal_YSD_GAME_CCU",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "game_ccu"],
    schedule_interval="0 22 * * *",  # 매일 7시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
