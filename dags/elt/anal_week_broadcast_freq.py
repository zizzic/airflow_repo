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
    # 1. GCE의 RDS의 GAME_INFO 테이블에서 게임 리스트 가져오기
    cur = connect_to_redshift()
    print("Successfully connected to Redshift")
    conn = cur.connection

    try:
        # Start a new transaction
        conn.autocommit = False

        # analytics.anal_week_game_viewer 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.anal_week_broadcast_freq;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.anal_week_broadcast_freq")

        # SELECT 쿼리의 결과를 analytics.anal_week_game_viewer 테이블에 삽입
        sql = """
            INSERT INTO analytics.anal_week_broadcast_freq(GAME_NM, STREAMER_NM, BROADCAST_FREQ, CREATED_DATE)
            select
                game_nm AS GAME_NM,
                streamer_nm AS STREAMER_NM,
                COUNT(distinct broadcast_id) AS BROADCAST_FREQ,
                current_date as created_date
            from dev.analytics.anal_broadcast
            group by 1,2
            order by 4 DESC;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.anal_week_broadcast_freq")

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
    dag_id="ELT_Anal_Week_Broadcast_Freq",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "broadcast"],
    schedule_interval="0 23 * * *",  # 08:00(KST)
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
