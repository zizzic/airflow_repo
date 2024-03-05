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
                DELETE FROM analytics.anal_week_game_viewer;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.anal_week_game_viewer")

        # SELECT 쿼리의 결과를 analytics.anal_week_game_viewer 테이블에 삽입
        sql = """
            INSERT INTO analytics.anal_week_game_viewer
            SELECT GAME_NM, CAST(HOUR AS INTEGER), si.streamer_nm as STREAMER_NM, AVG(viewer_num) AS VIEWER_AVG, TO_DATE(TO_CHAR(live_collect_time::DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS CREATED_DATE
            FROM (
                SELECT gi.game_nm ,rlv.game_code, rlv.live_collect_time, rlv.viewer_num, rlv.year, rlv.month, rlv.day, CAST(rlv.hour AS INTEGER), rlv.streamer_id
                FROM external_raw_data.table_name_raw_live_viewer AS rlv
                INNER JOIN external_raw_data.game_info AS gi
                ON rlv.game_code = gi.chz_game_code

                UNION

                SELECT gi.game_nm, rlv.game_code, rlv.live_collect_time, rlv.viewer_num, rlv.year, rlv.month, rlv.day, CAST(rlv.hour AS INTEGER), rlv.streamer_id
                FROM external_raw_data.table_name_raw_live_viewer AS rlv
                INNER JOIN external_raw_data.game_info AS gi
                ON rlv.game_code = LPAD(gi.afc_game_code, LENGTH(gi.afc_game_code) + 3, '0')
            ) AS subquery
            INNER JOIN external_raw_data.streamer_info AS si
            ON subquery.streamer_id = si.streamer_id
            GROUP BY GAME_NM, CREATED_DATE, HOUR, si.streamer_nm
            ORDER BY GAME_NM, CREATED_DATE, HOUR, si.streamer_nm;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.anal_week_game_viewer")

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
    dag_id="ELT_Anal_Week_Game_Viewer",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "game_viewer"],
    schedule_interval="0 23 * * *",  # 매일 자정
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
