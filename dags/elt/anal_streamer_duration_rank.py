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
                DELETE FROM analytics.ANAL_STREAMER_DURATION_RANK;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_STREAMER_DURATION_RANK")

        # SELECT 쿼리의 결과를 analytics.ANAL_YSD_GAME_CCU 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_STREAMER_DURATION_RANK(STEAMER_NM, GAME_NM, PLATFORM, DURATION_RANK, TOTAL_BROADCAST_DURATION, GAME_BANNER_URL, CREATED_DATE)
            WITH DURATION_RANK_TABLE AS (
            SELECT 
                streamer_nm, 
                game_nm, 
                platform,
                ROW_NUMBER() OVER (PARTITION BY streamer_nm, platform ORDER BY SUM(game_duration) DESC) as DURATION_RANK,
                SUM(game_duration) AS TOTAL_BROADCAST_DURATION
            FROM analytics.anal_broadcast
                WHERE game_nm NOT IN ('talk', '-', 'Virtual', '종합 게임')
            GROUP BY 1, 2, 3
            )
            SELECT 
                rg.*, 
                gi.game_banner_url,
                current_date AS CREATED_TIME
            FROM DURATION_RANK_TABLE rg
                INNER JOIN external_raw_data.game_info gi 
                ON rg.game_nm=gi.game_nm
            WHERE rg.DURATION_RANK < 4;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_STREAMER_DURATION_RANK")

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
    dag_id="ELT_ANAL_STREAMER_DURATION_RANK",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "duration_rank"],
    schedule_interval="0 23 * * *",  # 매일 8시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
