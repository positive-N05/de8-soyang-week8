from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

# PostgreSQL 에 적재할 Gold 테이블 5개
GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def check_row_counts():
    # 적재 후 검증: 5개 테이블 모두 row count > 0 이어야 성공
    hook = PostgresHook(postgres_conn_id="gold_postgres")
    for table in GOLD_TABLES:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table}: {count} rows")
        if count == 0:
            raise ValueError(f"{table} 테이블이 비어 있습니다")


with DAG(
    dag_id="gold_realestate_aggregate",
    # 과제 01·02 DAG 와 같은 스케줄·기간
    start_date=datetime(2024, 11, 1),
    end_date=datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["realestate", "gold"],
) as dag:
    # 과제 02 DAG(silver_realestate_transform)의 같은 달 실행이 끝날 때까지 대기
    wait_silver = ExternalTaskSensor(
        task_id="wait_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    # spark-submit 으로 Gold 집계 + PostgreSQL 적재 (인자: 이번 달 yyyymm)
    gold_spark = BashOperator(
        task_id="gold_spark_submit",
        bash_command=(
            "spark-submit --master 'local[2]' "
            "/opt/airflow/scripts/q3/gold_spark_sql.py {{ data_interval_start.strftime('%Y%m') }}"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    check_tables = PythonOperator(task_id="check_row_counts", python_callable=check_row_counts)

    wait_silver >> gold_spark >> check_tables
