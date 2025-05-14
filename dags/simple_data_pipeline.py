from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task  # 최신 스타일
import pandas as pd
import os

# DAG 기본 인자 설정
default_args: dict[str, Any] = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='simple_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    @task
    def collect_data() -> str:
        """데이터 수집"""
        data: dict[str, Any] = {
            'date': pd.date_range(start='2024-01-01', periods=5),
            'value': [1, 2, 3, 4, 5],
        }
        df: pd.DataFrame = pd.DataFrame(data)
        
        os.makedirs('data/raw', exist_ok=True)
        df.to_csv('data/raw/input_data.csv', index=False)
        return 'Data collected successfully'

    @task
    def transform_data() -> str:
        """데이터 변환"""
        df: pd.DataFrame = pd.read_csv('data/raw/input_data.csv')
        df['value'] = df['value'] * 10
        
        os.makedirs('data/processed', exist_ok=True)
        df.to_csv('data/processed/transformed_data.csv', index=False)
        return 'Data transformed successfully'

    @task
    def load_data() -> str:
        """데이터 저장"""
        df: pd.DataFrame = pd.read_csv('data/processed/transformed_data.csv')
        
        print("Processed data:")
        print(df)
        return 'Data loaded successfully'

    # 태스크 흐름 설정
    collect_data() >> transform_data() >> load_data()
