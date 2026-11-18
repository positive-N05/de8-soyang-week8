from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# git-sync 로 레포의 DAG 가 Airflow 에 들어왔는지 확인하는 테스트 DAG (BashOperator 만 사용)
with DAG(
    dag_id="hello_gitsync",
    description="git-sync 로 배포됐는지 확인하는 테스트 DAG",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 직접 트리거할 때만 실행
    catchup=False,
    tags=["gitsync", "week8"],
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync - $(date)"',
    )
