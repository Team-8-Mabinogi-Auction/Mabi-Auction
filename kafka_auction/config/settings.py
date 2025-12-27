import os
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경 변수 로드 (있으면)

# ─────────────────────────────
# Nexon / API 설정
# ─────────────────────────────
NEXON_API_KEY = os.getenv("NEXON_API_KEY", "")

AUCTION_LISTINGS_URL = os.getenv(
    "AUCTION_LISTINGS_URL",
    "https://open.api.nexon.com/mabinogi/v1/auction/list"
)
AUCTION_HISTORY_URL = os.getenv(
    "AUCTION_HISTORY_URL",
    "https://open.api.nexon.com/mabinogi/v1/auction/history"
)


# ─────────────────────────────
# Kafka 설정
# ─────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BROKER",

# AWS EC2(도커 내부망) 환경에서는 기본 9092 포트를 사용합니다.
# 로컬 맥북에서 도커 외부 Producer로 테스트할 경우 'localhost:9094'로 수정하여 사용하세요.
    "localhost:9092" 
)

TOPIC_LISTINGS_EQUIPMENTS = os.getenv("KAFKA_TOPIC_LISTINGS_EQUIPMENTS", "auction.raw.listings.equipments")
TOPIC_LISTINGS_CONSUMABLE = os.getenv("KAFKA_TOPIC_LISTINGS_CONSUMABLE", "auction.raw.listings.consumables")
TOPIC_HISTORY = os.getenv("KAFKA_TOPIC_RAW_HISTORY", "auction.raw.history")

# ─────────────────────────────
# 로깅
# ─────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
