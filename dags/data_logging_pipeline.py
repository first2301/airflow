from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import random
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_logs():
    for i in range(1000):
        log_level = random.choice(['INFO', 'WARNING', 'ERROR'])
        message = f"로그 메시지 #{i}: {random.random()}"
        
        if log_level == 'INFO':
            logger.info(message)
        elif log_level == 'WARNING':
            logger.warning(message)
        else:
            logger.error(message)
        
        # 로그 생성 간격을 랜덤하게 설정
        time.sleep(random.uniform(0.1, 0.5))

def process_data():
    """데이터 처리 및 로깅 함수"""
    logger.info("데이터 처리 시작")
    
    # 데이터 처리 시뮬레이션
    for i in range(100):
        logger.info(f"데이터 청크 {i} 처리 중...")
        time.sleep(0.2)
        
        if random.random() < 0.1:  # 10% 확률로 경고 로그 생성
            logger.warning(f"데이터 청크 {i}에서 이상치 발견")
    
    logger.info("데이터 처리 완료")

with DAG(
    'data_logging_pipeline',
    default_args=default_args,
    description='대량의 로그를 생성하는 데이터 파이프라인',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['logging', 'data_pipeline'],
) as dag:

    generate_logs_task = PythonOperator(
        task_id='generate_logs',
        python_callable=generate_logs,
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    generate_logs_task >> process_data_task 