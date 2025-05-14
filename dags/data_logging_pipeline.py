from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import logging
import random
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task()
def realistic_data_processing():
    logger.info("[Step 1] 데이터 소스 연결 중...")
    time.sleep(0.5)
    logger.info("데이터 소스 연결 완료")

    logger.info("[Step 2] 데이터 수집 시작")
    records = [random.random() for _ in range(100)]
    logger.info(f"총 {len(records)}건 데이터 수집 완료")

    logger.info("[Step 3] 데이터 전처리 시작")
    cleaned_records = [r for r in records if 0 <= r <= 1]
    logger.info(f"정상 데이터 {len(cleaned_records)}건 전처리 완료")

    logger.info("[Step 4] 데이터 변환 및 저장")
    processed_data = [r * 100 for r in cleaned_records]
    time.sleep(0.5)
    logger.info(f"데이터 변환 및 저장 완료 (총 {len(processed_data)}건)")

    logger.info("파이프라인 전체 완료")


@task()
def dummy_log_generator():
    """추가 로그 생성 (테스트용)"""
    logger.info("서브 시스템 상태 체크 시작")
    for _ in range(3):
        level = random.choice(['INFO', 'WARNING'])
        msg = f"서브 시스템 체크 결과: {random.choice(['정상', '주의 필요'])}"
        if level == 'INFO':
            logger.info(msg)
        else:
            logger.warning(msg)
        time.sleep(0.2)


# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'realistic_data_pipeline',
    default_args=default_args,
    description='데이터 처리 파이프라인',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['logging', 'data_pipeline'],
) as dag:

    generate_dummy_logs = dummy_log_generator()
    process_realistic_data = realistic_data_processing()

    generate_dummy_logs >> process_realistic_data
