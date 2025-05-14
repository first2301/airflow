from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import os

# 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 데이터 생성 함수
def create_sample_data():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        data = {
            'date': pd.date_range(start='2024-01-01', periods=100),
            'value': np.random.randint(1, 100, size=100),
            'category': np.random.choice(['A', 'B', 'C'], size=100)
        }
        df = pd.DataFrame(data)
        df.to_csv(os.path.join(DATA_DIR, 'raw_data.csv'), index=False)
        print('샘플 데이터 생성 완료')
    except Exception as e:
        print(f"데이터 생성 실패: {e}")
        raise

# 데이터 처리 함수
def process_data():
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, 'raw_data.csv'))
        df['processed_value'] = df['value'] * 2
        df['processed_date'] = pd.to_datetime(df['date'])
        result = df.groupby('category').agg({
            'processed_value': ['mean', 'sum', 'count']
        }).reset_index()
        result.columns = ['_'.join(col).strip('_') for col in result.columns.values]  # 컬럼 정리
        result.to_csv(os.path.join(DATA_DIR, 'processed_data.csv'), index=False)
        print('데이터 처리 완료')
    except Exception as e:
        print(f"데이터 처리 실패: {e}")
        raise

# DAG 정의
with DAG(
    dag_id='sample_data_pipeline',
    default_args=default_args,
    description='간단한 데이터 파이프라인 예제',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'etl']
) as dag:

    create_data_task = PythonOperator(
        task_id='create_sample_data',
        python_callable=create_sample_data
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    create_data_task >> process_data_task
