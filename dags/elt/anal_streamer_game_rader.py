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

        # analytics.ANAL_STREAMER_GAME_RADER 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.ANAL_STREAMER_GAME_RADER;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_STREAMER_GAME_RADER")

        # SELECT 쿼리의 결과를 analytics.ANAL_STREAMER_GAME_RADER 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_STREAMER_GAME_RADER(STREAMER_NM, GAME_NM, AVG_VIEWER_NUM, MAX_VIEWER_NUM, TOTAL_BROADCAST_COUNT, AVG_BROADCAST_DURATION, MAX_BROADCAST_DURATION, MIN_BROADCAST_DURATION, CREATED_DATE)
            SELECT 
                streamer_nm, 
                game_nm, 
                AVG(avg_viewer_num) AS AVG_VIEWER_NUM,
                MAX(max_viewer_num) AS MAX_VIEWER_NUM,
                COUNT(broadcast_id) AS TOTAL_BROADCAST_COUNT,
                AVG(game_duration) AS AVG_BROADCAST_DURATION,
                MAX(game_duration) AS MAX_BROADCAST_DURATION,
                MIN(game_duration) AS MIN_BROADCAST_DURATION,
                current_date AS CREATED_TIME
            FROM analytics.anal_broadcast
                WHERE game_nm NOT IN ('talk', '-', 'Virtual', '종합 게임')
            GROUP BY 1, 2;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_STREAMER_GAME_RADER")

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
    dag_id="ELT_ANAL_STREAMER_GAME_RADER",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "rader_chart"],
    schedule_interval="0 23 * * *",  # 매일 8시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
