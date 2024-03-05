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
                DELETE FROM analytics.ANAL_BROADCAST;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.ANAL_BROADCAST")

        # SELECT 쿼리의 결과를 analytics.ANAL_YSD_GAME_CCU 테이블에 삽입
        sql = """
            INSERT INTO analytics.ANAL_BROADCAST(STREAMER_NM, BROADCAST_ID, GAME_NM, PLATFORM, AVG_VIEWER_NUM, BROADCAST_START_TIME, BROADCAST_END_TIME, GAME_DURATION, CREATED_DATE)
            WITH ParsedData AS (
                SELECT *, live_collect_time::TIMESTAMPTZ AS parsed_time
                FROM external_raw_data.table_name_raw_live_viewer
            ),
            RankedData AS(
                SELECT 
                    *,
                    LAG(parsed_time, 1) OVER (
                        PARTITION BY streamer_id, broadcast_id, game_code
                        ORDER BY parsed_time
                    ) AS prev_timestamp
                FROM ParsedData
            ),
            GroupedData AS(
                SELECT *,
                    CASE
                        WHEN parsed_time - INTERVAL '5' MINUTE > prev_timestamp THEN 1
                        ELSE 0
                    END AS is_new_group,
                    COALESCE(NULLIF(game_code, ''), 'talk') AS normalized_game_code
                FROM RankedData
            ),
            GroupIDs AS (
                SELECT *,
                    SUM(is_new_group) OVER (
                        PARTITION BY streamer_id, broadcast_id, normalized_game_code
                        ORDER BY parsed_time
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS group_id,
                    normalized_game_code AS n_game_code
                FROM GroupedData
            )
            SELECT
                s_info.streamer_nm AS STREAMER_NM,
                g_ids.broadcast_id AS BROADCAST_ID,
                COALESCE(g_info.game_nm,g_ids.n_game_code) AS GAME_NM,
                g_ids.platform AS PLATFORM,
                AVG(g_ids.viewer_num)::integer AS AVG_VIEWER_NUM,
                MIN(g_ids.parsed_time) AS start_time,
                MAX(g_ids.parsed_time) AS end_time,
                (EXTRACT(EPOCH FROM MAX(g_ids.parsed_time)) - EXTRACT(EPOCH FROM MIN(g_ids.parsed_time))) / 60 AS GAME_DURATION,
                CURRENT_DATE AS created_date
            FROM GroupIDs AS g_ids
            LEFT JOIN external_raw_data.streamer_info AS s_info ON s_info.streamer_id = g_ids.streamer_id
            LEFT JOIN external_raw_data.game_info AS g_info
            ON g_ids.n_game_code = g_info.chz_game_code OR LTRIM(g_ids.n_game_code, '0') = g_info.afc_game_code
            WHERE g_ids.broadcast_id IS NOT NULL
                AND g_ids.parsed_time >= (CURRENT_DATE - INTERVAL '7 days' + INTERVAL '6 hours')
                AND g_ids.parsed_time < (CURRENT_DATE + INTERVAL '6 hours')    
            GROUP BY STREAMER_NM, BROADCAST_ID, GAME_NM, PLATFORM, g_ids.group_id, g_ids.n_game_code
            ORDER BY STREAMER_NM, BROADCAST_ID, start_time; 
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.ANAL_BROADCAST")

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
    dag_id="ELT_ANAL_BROADCAST",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "broadcast"],
    schedule_interval="0 22 * * *",  # 매일 07시
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
