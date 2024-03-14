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
                DELETE FROM analytics.ANAL_TDY_GAME_RATING;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_TDY_GAME_RATING")

        # SELECT 쿼리의 결과를 analytics.ANAL_YSD_GAME_CCU 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_TDY_GAME_RATING(GAME_NM, POSITIVE_PERCENT, CREATED_DATE)
            SELECT
                b.game_nm AS GAME_NM,
                (a.all_positive_percent)*0.05 AS POSITIVE_PERCENT,
                current_date AS CREATED_DATE
            FROM (SELECT game_id, all_positive_percent
                FROM external_raw_data.table_name_raw_game_rating
                WHERE collect_date = current_date) a
            JOIN external_raw_data.game_info b
            ON a.game_id = b.game_id ;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_TDY_GAME_RATING")

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
    dag_id="ELT_ANAL_TDY_GAME_RATING",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "game_rating"],
    schedule_interval="0 22 * * *",  # 매일 7시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
