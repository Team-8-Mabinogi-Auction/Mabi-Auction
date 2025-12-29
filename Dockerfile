# 파이썬 실행 환경 설정
FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. requirements.txt 복사 및 설치
COPY kafka_auction/requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# 2. [중요] kafka_auction 폴더 전체를 통째로 복사
# 이렇게 해야 /app/kafka_auction/collector... 구조가 유지됩니다.
COPY kafka_auction/ ./kafka_auction/

# 3. 파이썬이 /app을 루트로 인식하게 설정
ENV PYTHONPATH=/app