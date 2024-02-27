from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
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
    "glue_game_rating",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 2, 22),
        "retries": 3,
        "retry_delay": timedelta(seconds=15),
    },
    max_active_runs=1,
    schedule_interval="0 1 * * *",
    tags=["glue", "Game_Rating"],
    catchup=True,
) as dag:

    bucket_name = "de-2-1-bucket"
    current_time = "{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S+00:00') }}"
    year = "{{ data_interval_end.in_timezone('Asia/Seoul').year }}"
    month = "{{ data_interval_end.in_timezone('Asia/Seoul').month }}"
    day = "{{ data_interval_end.in_timezone('Asia/Seoul').day }}"
    hour = "{{ (data_interval_end - macros.timedelta(hours=1)).in_timezone('Asia/Seoul') }}"  # before 1 hour

    upload_script = PythonOperator(
        task_id="upload_script_to_s3",
        python_callable=upload_rendered_script_to_s3,
        op_kwargs={
            "bucket_name": bucket_name,
            "aws_conn_id": "aws_conn_id",
            "template_s3_key": "source/script/glue_game_rating_template.py",
            "rendered_s3_key": "source/script/glue_game_rating_script.py",
            # into template
            "input_path": f"s3://de-2-1-bucket/source/json/table_name=raw_game_rating/year={year}/month={month}/day={day}/",
            "output_path": f"s3://de-2-1-bucket/source/parquet/table_name=raw_game_rating/year={year}/month={month}/day={day}/",
            "collect_date": f"{year}-{month}-{day}",
        },
    )

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="de-2-1_game_rating",
        script_location="s3://de-2-1-bucket/source/script/glue_game_rating_script.py",
        aws_conn_id="aws_conn_id",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-crawler",
        dag=dag,
    )

    wait_for_job = GlueJobSensor(  # trigger
        task_id="wait_for_job_game_rating_glue_job",  # task_id 직관적으로 알 수 있도록 변경 권장
        job_name="de-2-1_game_rating",
        # Job ID extracted from previous Glue Job Operator task
        run_id=run_glue_job.output,
        aws_conn_id="aws_conn_id",
    )

upload_script >> run_glue_job >> wait_for_job