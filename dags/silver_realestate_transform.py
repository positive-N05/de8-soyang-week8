from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="silver_realestate_transform",
    # 과제 01 DAG 와 같은 스케줄·기간으로 맞춰야 같은 달의 실행끼리 연결된다
    start_date=datetime(2024, 11, 1),
    end_date=datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,  # Spark 잡이 한 번에 하나씩만 돌도록 (Spark UI 4040 포트도 하나)
    tags=["realestate", "silver"],
) as dag:
    # 과제 01 DAG(bronze_realestate_collect)의 같은 달 실행이 끝날 때까지 대기
    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,  # DAG 전체가 성공할 때까지
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    # spark-submit 으로 Silver 정제 잡 실행 (인자: 처리할 달 yyyymm)
    silver_spark = BashOperator(
        task_id="silver_spark_submit",
        bash_command=(
            "spark-submit --master 'local[2]' "
            "/opt/airflow/scripts/q2/silver_spark.py {{ data_interval_start.strftime('%Y%m') }}"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    wait_bronze >> silver_spark
