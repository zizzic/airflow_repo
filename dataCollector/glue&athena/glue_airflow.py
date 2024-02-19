from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.python import PythonOperator


# 전날 날짜를 계산하는 함수
def set_glue_crawler_params():
    yesterday = datetime.now() - timedelta(1)
    date_str = yesterday.strftime("%Y-%m-%d")
    # 여기서 필요한 파라미터를 설정할 수 있습니다.
    # 예: S3 경로 내 날짜 기반 폴더
    return {"date": date_str}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "glue_crawler_dag",
    default_args=default_args,
    description="DAG for running AWS Glue Crawler",
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    # 파라미터 설정 Task
    set_params = PythonOperator(
        task_id="set_glue_crawler_params",
        python_callable=set_glue_crawler_params,
    )

    # Glue Crawler 실행 Task
    run_glue_crawler = AwsGlueJobOperator(
        task_id="run_glue_crawler",
        job_name="your_glue_crawler_name",  # AWS Glue에서 설정한 Crawler 이름
        script_location="s3://your-bucket/path/to/glue/script",  # Glue 스크립트 위치
        region_name="your-aws-region",
        # 필요한 경우 아래에 추가 파라미터를 전달할 수 있습니다.
        # 예: '--additional-python-modules', 'pyarrow==2, pandas'
    )

    set_params >> run_glue_crawler
