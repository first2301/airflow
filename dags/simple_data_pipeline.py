from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import os
import csv

from airflow import DAG
from airflow.decorators import task  # 최신 스타일

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
    description='A simple data pipeline DAG without pandas',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    @task
    def collect_data() -> str:
        """데이터 수집"""
        start_date = datetime(2024, 1, 1)
        dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(5)]
        values = [1, 2, 3, 4, 5]

        os.makedirs('data/raw', exist_ok=True)
        with open('data/raw/input_data.csv', mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['date', 'value'])
            for date, value in zip(dates, values):
                writer.writerow([date, value])

        return 'Data collected successfully'

    @task
    def transform_data() -> str:
        """데이터 변환"""
        os.makedirs('data/processed', exist_ok=True)

        with open('data/raw/input_data.csv', mode='r') as f_in, \
             open('data/processed/transformed_data.csv', mode='w', newline='') as f_out:
            
            reader = csv.DictReader(f_in)
            fieldnames = ['date', 'value']
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                transformed_row = {
                    'date': row['date'],
                    'value': str(int(row['value']) * 10)
                }
                writer.writerow(transformed_row)

        return 'Data transformed successfully'

    @task
    def load_data() -> str:
        """데이터 저장"""
        with open('data/processed/transformed_data.csv', mode='r') as f:
            reader = csv.DictReader(f)
            print("Processed data:")
            for row in reader:
                print(row)

        return 'Data loaded successfully'

    # 태스크 흐름 설정
    collect_data() >> transform_data() >> load_data()
