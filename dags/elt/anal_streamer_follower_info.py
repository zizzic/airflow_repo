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

        # analytics.ANAL_STREAMER_FOLLOWER_INFO 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.ANAL_STREAMER_FOLLOWER_INFO;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_STREAMER_FOLLOWER_INFO")

        # SELECT 쿼리의 결과를 analytics.ANAL_STREAMER_FOLLOWER_INFO 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_STREAMER_FOLLOWER_INFO (STREAMER_NM, CHZ_BANNER_URL, AFC_BANNER_URL, CHZ_FOLLOWER_NUM, AFC_FOLLOWER_NUM, CREATED_DATE)
            WITH RankedRecords AS(
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY streamer_id ORDER BY collect_time DESC) AS rn
            from external_raw_data.table_name_followers
            )
            SELECT 
                si.streamer_nm, 
                si.chz_banner_url, 
                si.afc_banner_url, 
            round(1.0*rr.chz_follower_num/10000,2) as chz_follower_num, 
            round(1.0*rr.afc_follower_num/10000,2) as afc_follower_num,
            current_date AS CREATED_TIME
            FROM RankedRecords rr
                JOIN external_raw_data.streamer_info si 
                    ON si.streamer_id = rr.streamer_id
                where rr.rn=1;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_STREAMER_FOLLOWER_INFO")

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
    dag_id="ELT_ANAL_STREAMER_FOLLOWER_INFO",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "follower_info"],
    schedule_interval="0 23 * * *",  # 매일 08시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
