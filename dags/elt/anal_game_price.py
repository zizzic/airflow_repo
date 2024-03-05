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
    # Redshift에 연결
    cur = connect_to_redshift()
    print("Successfully connected to Redshift")
    conn = cur.connection

    try:
        # Start a new transaction
        conn.autocommit = False

        # analytics.anal_game_price 테이블의 모든 데이터 삭제
        sql = """
                DELETE FROM analytics.anal_game_price;
                """
        cur.execute(sql)
        print("Successfully deleted all data from analytics.anal_game_price")

        # SELECT 쿼리의 결과를 analytics.anal_game_price 테이블에 삽입
        sql = """
            INSERT INTO analytics.anal_game_price
            WITH price_changes AS (
                SELECT 
                    game_nm, 
                    price, 
                    collect_date, 
                    current_price,
                    LAG(current_price) OVER (PARTITION BY game_nm ORDER BY collect_date) AS prev_price
                FROM external_raw_data.table_name_raw_game_price tnrgp
                JOIN external_raw_data.game_info gi 
                ON tnrgp.game_id = gi.game_id 
                GROUP BY collect_date, game_nm, price, current_price
            )
            SELECT 
                game_nm, 
                collect_date as modified_date, 
                current_price
            FROM price_changes
            WHERE (current_price < price AND (prev_price = price OR prev_price IS NULL)) 
            OR (current_price = price AND prev_price < price)
            ORDER BY game_nm, collect_date;
            """
        cur.execute(sql)
        print("Successfully inserted data into analytics.anal_game_price")

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
    dag_id="ELT_Anal_Game_Price",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ELT", "analytics", "game_price"],
    schedule_interval="0 22 * * *",  # KST 07:00
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    elt()
