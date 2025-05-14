
# How to install airflow
- WSL2 환경
- python3 -m venv {가상환경 이름}
- pip install "apache-airflow==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"

export AIRFLOW_HOME=/mnt/f/final_rnd/airflow


# Airflow 데이터 파이프라인 프로젝트

## 설치 방법
1. WSL2 환경 설정
2. Python 가상환경 생성: `python3 -m venv {가상환경 이름}`
3. 가상환경 활성화: `source {가상환경 이름}/bin/activate`
4. 의존성 설치: `pip install -r requirements.txt`



## 프로젝트 구조
```
.
├── dags/                    # Airflow DAG 파일들이 위치하는 디렉토리
│   └── sample_pipeline.py   # 샘플 데이터 파이프라인 DAG
├── data/                    # 데이터 파일들이 저장되는 디렉토리
├── requirements.txt         # 프로젝트 의존성 파일
└── README.md               # 프로젝트 설명서
```

## 파이프라인 설명
이 샘플 파이프라인은 기본적인 ETL(추출, 변환, 적재) 프로세스를 보여줍니다:

1. 데이터 생성 단계
   - 날짜, 값, 카테고리 정보를 포함한 샘플 데이터 생성
   - 생성된 데이터는 `data/raw_data.csv`에 저장

2. 데이터 처리 단계
   - 값들을 2배로 증가
   - 날짜 형식 변환
   - 카테고리별 통계 계산 (평균, 합계, 개수)
   - 처리된 데이터는 `data/processed_data.csv`에 저장

## 파이프라인 실행 방법
1. Airflow 웹서버 실행: `airflow webserver -p 8080`
2. Airflow 스케줄러 실행: `airflow scheduler`
3. 웹 브라우저에서 Airflow UI 접속: http://localhost:8080
4. 로그인 정보:
   - 사용자명: admin
   - 비밀번호: WK7vA7GcQhNV9aNT
5. UI에서 'sample_data_pipeline' DAG 활성화

## 데이터 파일 설명
- 원본 데이터: `data/raw_data.csv`
  - 날짜, 값, 카테고리 정보 포함
  - 100개의 샘플 데이터 생성
- 처리된 데이터: `data/processed_data.csv`
  - 카테고리별 통계 정보 포함
  - 평균, 합계, 데이터 개수 계산

## 주의사항
- 파이프라인은 매일 자동으로 실행됩니다
- 데이터는 `data` 디렉토리에 자동으로 생성됩니다
- 실행 결과는 Airflow UI에서 모니터링할 수 있습니다
- Processed data: `data/processed_data.csv`