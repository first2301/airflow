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
    time.sleep(1)
    if random.random() < 0.05:
        logger.error("데이터 소스 연결 실패: 타임아웃 발생")
        raise Exception("데이터 소스 연결 실패")
    logger.info("데이터 소스 연결 완료")

    logger.info("[Step 2] 데이터 수집 시작")
    records = []
    for i in range(5):
        batch_size = random.randint(950, 1050)
        records.extend([random.random() for _ in range(batch_size)])
        logger.info(f"Batch {i+1} 수집 완료 ({batch_size}건)")
        time.sleep(random.uniform(0.5, 1.0))
    logger.info(f"총 {len(records)}건 데이터 수집 완료")

    logger.info("[Step 3] 데이터 전처리 시작")
    cleaned_records = []
    for idx, rec in enumerate(records):
        if rec is None:
            logger.warning(f"{idx}번 데이터: 결측치 발견")
        elif rec < 0 or rec > 1:
            logger.warning(f"{idx}번 데이터: 범위 초과값 발견 ({rec})")
        else:
            cleaned_records.append(rec)
        if idx % 1000 == 0 and idx > 0:
            logger.info(f"전처리 진행률: {idx/len(records)*100:.2f}%")
        time.sleep(0.001)
    logger.info("데이터 전처리 완료")

    logger.info("[Step 4] 데이터 변환 및 저장")
    processed_data = [r * 100 for r in cleaned_records]
    time.sleep(2)
    if random.random() < 0.02:
        logger.error("데이터 저장 중 오류 발생: 디스크 공간 부족")
        raise Exception("디스크 공간 부족")
    logger.info(f"데이터 변환 및 저장 완료 (총 {len(processed_data)}건)")

    logger.info("파이프라인 전체 완료")


@task()
def dummy_log_generator():
    """추가 로그 생성 (테스트용)"""
    logger.info("서브 시스템 상태 체크 시작")
    time.sleep(1)
    for _ in range(10):
        level = random.choices(['INFO', 'WARNING', 'ERROR'], weights=[0.7, 0.2, 0.1])[0]
        msg = f"시스템 체크 결과: {random.choice(['정상', '지연 발생', '에러 발생'])}"
        if level == 'INFO':
            logger.info(msg)
        elif level == 'WARNING':
            logger.warning(msg)
        else:
            logger.error(msg)
        time.sleep(random.uniform(0.2, 0.5))


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
    description='현실감 있는 데이터 처리 파이프라인',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['logging', 'data_pipeline'],
) as dag:

    generate_dummy_logs = dummy_log_generator()
    process_realistic_data = realistic_data_processing()

    generate_dummy_logs >> process_realistic_data