"""Example Airflow DAG for testing task dependencies using EmptyOperator."""

import airflow.utils.dates
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# DAG 정의
dag = DAG(
    dag_id="dag_cycle_test",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
    description="A simple test DAG using EmptyOperator",  # DAG 설명 추가
)

# 태스크 정의
t1 = EmptyOperator(task_id="t1", dag=dag)
t2 = EmptyOperator(task_id="t2", dag=dag)
t3 = EmptyOperator(task_id="t3", dag=dag)

# 태스크 종속성 설정
t1 >> t2 >> t3
