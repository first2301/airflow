from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# DAG 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'simple_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# 데이터 수집 함수
def collect_data():
    # 예시 데이터 생성
    data = {
        'date': pd.date_range(start='2024-01-01', periods=5),
        'value': [1, 2, 3, 4, 5]
    }
    df = pd.DataFrame(data)
    
    # 데이터 저장
    os.makedirs('data/raw', exist_ok=True)
    df.to_csv('data/raw/input_data.csv', index=False)
    return 'Data collected successfully'

# 데이터 변환 함수
def transform_data():
    # 데이터 읽기
    df = pd.read_csv('data/raw/input_data.csv')
    
    # 간단한 변환: 값에 10을 곱하기
    df['value'] = df['value'] * 10
    
    # 변환된 데이터 저장
    os.makedirs('data/processed', exist_ok=True)
    df.to_csv('data/processed/transformed_data.csv', index=False)
    return 'Data transformed successfully'

# 데이터 저장 함수
def load_data():
    # 변환된 데이터 읽기
    df = pd.read_csv('data/processed/transformed_data.csv')
    
    # 여기서는 간단히 출력만 하도록 설정
    print("Processed data:")
    print(df)
    return 'Data loaded successfully'

# 태스크 정의
collect_task = PythonOperator(
    task_id='collect_data',
    python_callable=collect_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# 태스크 의존성 설정
collect_task >> transform_task >> load_task 