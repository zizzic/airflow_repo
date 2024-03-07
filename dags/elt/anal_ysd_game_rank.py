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
    # # Redshift에 연결
    cur = connect_to_redshift()
    print("Successfully connected to Redshift")
    conn = cur.connection

    try:
        # Start a new transaction
        conn.autocommit = False

        # analytics.ANAL_YSD_GAME_RANK 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.ANAL_YSD_GAME_RANK;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_YSD_GAME_RANK")

        # SELECT 쿼리의 결과를 analytics.ANAL_YSD_GAME_RANK 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_YSD_GAME_RANK(GAME_NM, GAME_BANNER_URL, TOTAL_VIEWER_AVG, CREATED_DATE)
            SELECT
                sq.game_nm AS GAME_NM,
                gi.game_banner_url AS GAME_BANNER_URL, 
                sq.total_avg_viewer AS TOTAL_VIEWER_AVG,
                current_date AS CREATED_TIME
            FROM (
            SELECT
                game_nm,
                SUM(avg_viewer_num) AS total_avg_viewer
            FROM
                analytics.anal_broadcast 
            WHERE 
                game_nm NOT IN ('talk', '종합 게임')
                AND broadcast_start_time >= CONVERT_TIMEZONE('Asia/Tokyo','UTC', (CURRENT_DATE - INTERVAL '18 hours'))
                AND broadcast_start_time < CONVERT_TIMEZONE('Asia/Tokyo','UTC',(CURRENT_DATE + INTERVAL '6 hours'))
            GROUP BY game_nm
            ORDER BY total_avg_viewer DESC
            ) AS sq
            INNER JOIN external_raw_data.game_info gi ON sq.game_nm = gi.game_nm
            ORDER BY sq.total_avg_viewer DESC
            limit 5;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_YSD_GAME_RANK")

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
    dag_id="ELT_ANAL_YSD_GAME_RANK",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "game_rank"],
    schedule_interval="0 23 * * *",  # 매일 08시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
