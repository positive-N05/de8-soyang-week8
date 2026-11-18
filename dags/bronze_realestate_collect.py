import os
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import unquote

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

# 국토교통부 아파트 매매 실거래가 API
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

# 수집할 시군구 코드 6개 (강남구, 서초구, 송파구, 마포구, 용산구, 성동구)
LAWD_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]

# 저장할 S3 버킷 (docker-compose.yml 의 REALESTATE_BUCKET)
BUCKET = os.environ.get("REALESTATE_BUCKET", "realestate-soyang")


def collect(lawd_cd, data_interval_start):
    print(f'collector=soyang, time={datetime.now()}, lawd={lawd_cd}')

    # 이번 실행이 맡은 달 (예: 202411)
    deal_ymd = data_interval_start.strftime("%Y%m")
    # 인증키는 인코딩된 형태(%2B 등)로 넣어도 되도록 원래 문자로 되돌린 뒤 사용 (requests 가 다시 인코딩함)
    service_key = unquote(os.environ.get("DATA_GO_KR_API_KEY", ""))
    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": 1,
        "numOfRows": 1000,
    }
    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()
    xml_text = response.text

    # 응답 XML 을 파싱해서 결과 코드와 건수 확인
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"XML 파싱 실패: {e}")
        return {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": "parse_error", "count": 0}

    result_code = root.findtext(".//resultCode")
    total_count = int(root.findtext(".//totalCount") or 0)

    # 한 번에 다 못 받았으면 전체 건수만큼 다시 요청 (원본 XML 1개로 저장하기 위해)
    if total_count > params["numOfRows"]:
        params["numOfRows"] = total_count
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        xml_text = response.text
        root = ET.fromstring(response.content)

    items = root.findall(".//item")
    print(f"resultCode={result_code}, totalCount={total_count}, items={len(items)}")

    if result_code not in ("00", "000"):
        status = "api_error"
    elif len(items) == 0:
        status = "no_data"
    else:
        status = "ok"

    # 원본 XML 은 다음 task(upload_to_s3)가 그대로 S3 에 올림
    return {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": status, "count": len(items), "xml": xml_text}


def choose_branch(ti):
    results = ti.xcom_pull(task_ids=[f"collect_group.collect_{code}" for code in LAWD_CODES])
    ok = [r for r in results if r and r["status"] == "ok"]
    bad = [r for r in results if not r or r["status"] != "ok"]
    print(f"정상 {len(ok)}개, 0건/파싱 실패 {len(bad)}개")

    # 정상 응답이 있으면 업로드, 0건/파싱 실패가 있으면 건너뛰기 경로 (둘 다 있으면 둘 다)
    next_tasks = []
    if ok:
        next_tasks.append("upload_to_s3")
    if bad:
        next_tasks.append("skip_upload")
    return next_tasks


def upload_to_s3(ti):
    results = ti.xcom_pull(task_ids=[f"collect_group.collect_{code}" for code in LAWD_CODES])
    hook = S3Hook(aws_conn_id="aws_default")

    # 버킷이 없으면 서울 리전에 만든다
    if not hook.check_for_bucket(BUCKET):
        hook.create_bucket(bucket_name=BUCKET, region_name="ap-northeast-2")

    for r in results:
        if r and r["status"] == "ok":
            key = f"bronze/{r['deal_ymd']}/{r['lawd_cd']}.xml"
            hook.load_string(r["xml"], key=key, bucket_name=BUCKET, replace=True)
            print(f"uploaded s3://{BUCKET}/{key} ({r['count']}건)")


def skip_upload(ti):
    results = ti.xcom_pull(task_ids=[f"collect_group.collect_{code}" for code in LAWD_CODES])
    for r in results:
        if r and r["status"] != "ok":
            print(f"skip lawd={r['lawd_cd']} ym={r['deal_ymd']} status={r['status']}")


with DAG(
    dag_id="bronze_realestate_collect",
    start_date=datetime(2024, 11, 1),
    end_date=datetime(2024, 12, 1),  # 2024년 11월, 12월 두 달 수집
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["realestate", "bronze"],
) as dag:
    # 시군구 6개를 TaskGroup 안에서 병렬로 수집
    with TaskGroup(group_id="collect_group") as collect_group:
        for code in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{code}",
                python_callable=collect,
                op_kwargs={"lawd_cd": code},
                retries=2,
            )

    # 응답 0건/XML 파싱 실패 여부에 따라 분기
    branch = BranchPythonOperator(task_id="branch_after_collect", python_callable=choose_branch)

    upload = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)
    skip = PythonOperator(task_id="skip_upload", python_callable=skip_upload)
    done = EmptyOperator(task_id="summary_done", trigger_rule="none_failed_min_one_success")

    collect_group >> branch >> [upload, skip] >> done
