from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from jinja2 import Template


def upload_script_to_s3(bucket_name, s3_key, template_path, aws_conn_id, **kwargs):
    # S3Hook 인스턴스 생성
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # Jinja 템플릿 파일 읽기
    with open(template_path, 'r') as file:
        template = Template(file.read())

    # 템플릿 렌더링
    rendered_script = template.render(**kwargs)

    # 렌더링된 스크립트를 S3에 업로드
    s3_hook.load_string(
        string_data=rendered_script,
        bucket_name=bucket_name,
        key=s3_key,
        replace=True
    )

with DAG(
    "get_raw_data",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 17),
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:

    bucket_name = "de-2-1-bucket"
    local_path = f'./glue_script.py'
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"

    upload_script = PythonOperator(
            task_id='upload_script_to_s3',
            python_callable=upload_script_to_s3,
            op_kwargs={
                "bucket_name": bucket_name,
                "aws_conn_id": "aws_conn_id",
                "s3_key": f"source/script/glue_script.py",
                'template_path': './glue_template.py',
                # into template
                'input_path': f's3://de-2-1-bucket/source/json/table_name=raw_live_viewer/year={year}/month={month}/day={day}/',
                'output_path': 's3://de-2-1-bucket/source/parquet/', # do change_path
            }
        )

    run_glue_job = GlueJobOperator(
        task_id='run_glue_job',
        job_name='DE-2-1-testjob',
        script_location='s3://your_bucket_name/path/to/your_script.py',
        aws_conn_id='aws_conn_id',
        region_name='ap-northeast-2',
        dag=dag,
    )

upload_script >> run_glue_job
