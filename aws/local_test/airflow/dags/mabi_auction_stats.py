from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

# 로깅 설정
logger = logging.getLogger(__name__)

# 통계 계산 및 DB 저장 함수
def calculate_item_stats():
    # 1. Postgres 연결 (Airflow UI에서 등록한 postgres_default 사용)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 2. SQL 쿼리: 1시간 동안의 로그를 집계
    # 주의: mabi_auction.auction_logs 스키마/테이블이 실제 존재해야 합니다.
    query = """
        SELECT 
            item_name,
            ROUND(AVG(CASE WHEN event_type = 'REGISTER' THEN price END), 0) as avg_registration_price,
            ROUND(AVG(CASE WHEN event_type = 'SOLD' THEN price END), 0) as avg_sale_price,
            COUNT(*) as total_logs
        FROM auction_logs  -- 'mabi_auction.' 제거 완료
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        GROUP BY item_name;
    """
    
    # 3. 데이터 가져오기 (Pandas DataFrame)
    try:
        df = pg_hook.get_pandas_df(query)
    except Exception as e:
        logger.error(f"데이터 조회 중 오류 발생: {e}")
        raise

    # 4. 결과 처리 및 저장
    if df.empty:
        logger.info("최근 1시간 내에 분석할 경매장 로그 데이터가 없습니다.")
    else:
        # 분석 시간 컬럼 추가 (PK 또는 시계열 분석용)
        df['analyzed_at'] = datetime.now()
        
        logger.info(f"=== {df['analyzed_at'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} 기준 시세 분석 결과 ===")
        logger.info("\n" + df.to_string(index=False))
        
        # 5. DB 저장 로직 (SQLAlchemy Engine 활용)
        try:
            engine = pg_hook.get_sqlalchemy_engine()
            df.to_sql(
                name='mabi_item_price_averages', 
                con=engine, 
                if_exists='append', 
                index=False
            )
            logger.info("성공적으로 mabi_item_price_averages 테이블에 저장되었습니다.")
        except Exception as e:
            logger.error(f"DB 저장 중 오류 발생: {e}")
            raise

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='mabi_item_price_analysis_hourly',
    default_args=default_args,
    description='매시간 아이템별 평균 등록 및 판매 시세 계산 및 저장',
    schedule_interval='0 * * * *', # 매시 정각 실행
    start_date=datetime(2025, 1, 1), # 시작 날짜
    catchup=False,
    tags=['mabi', 'analysis', 'postgres']
) as dag:

    calculate_task = PythonOperator(
        task_id='calculate_averages',
        python_callable=calculate_item_stats,
    )