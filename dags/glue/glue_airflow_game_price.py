from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from jinja2 import Template


def upload_rendered_script_to_s3(
    bucket_name, template_s3_key, rendered_s3_key, aws_conn_id, **kwargs
):
    # S3Hook 인스턴스 생성
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # S3에서 Jinja 템플릿 파일 읽기
    template_str = s3_hook.read_key(template_s3_key, bucket_name)

    # Jinja 템플릿 렌더링
    template = Template(template_str)
    rendered_script = template.render(**kwargs)

    # 렌더링된 스크립트를 S3에 업로드
    s3_hook.load_string(
        string_data=rendered_script,
        bucket_name=bucket_name,
        key=rendered_s3_key,
        replace=True,
    )


with DAG(
    "glue_airflow_game_price",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 2, 22),
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 1 * * *",
    tags=["Steam_Glue", "Game_Price"],
    catchup=False,
) as dag:

    bucket_name = "de-2-1-bucket"
    current_time = "{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S+00:00') }}"
    year = "{{ data_interval_end.year.in_timezone('Asia/Seoul') }}"
    month = "{{ data_interval_end.month.in_timezone('Asia/Seoul') }}"
    day = "{{ data_interval_end.day.in_timezone('Asia/Seoul') }}"

    upload_script = PythonOperator(
        task_id="upload_script_to_s3",
        python_callable=upload_rendered_script_to_s3,
        op_kwargs={
            "bucket_name": bucket_name,
            "aws_conn_id": "aws_conn_id",
            "template_s3_key": "source/script/glue_game_price_template.py",
            "rendered_s3_key": "source/script/glue_game_price_script.py",
            # into template
            "input_path": f"s3://de-2-1-bucket/source/json/table_name=raw_game_price/year={year}/month={month}/day={day}/",
            "output_path": f"s3://de-2-1-bucket/source/parquet/table_name=raw_game_price/year={year}/month={month}/day={day}/",
            "collect_date": f"{year}-{month}-{day}",
        },
    )

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="DE-2-1-glue_game_price_job",
        script_location="s3://de-2-1-bucket/source/script/glue_game_price_script.py",
        aws_conn_id="aws_conn_id",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-crawler",
        dag=dag,
    )

upload_script >> run_glue_job
