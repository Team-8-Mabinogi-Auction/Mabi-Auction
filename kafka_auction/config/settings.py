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
    "localhost:9092" 
)

TOPIC_LISTINGS_EQUIPMENTS = os.getenv("KAFKA_TOPIC_LISTINGS_EQUIPMENTS", "auction.raw.listings.equipments")
TOPIC_LISTINGS_CONSUMABLE = os.getenv("KAFKA_TOPIC_LISTINGS_CONSUMABLE", "auction.raw.listings.consumables")
TOPIC_HISTORY = os.getenv("KAFKA_TOPIC_RAW_HISTORY", "auction.raw.history")

# ─────────────────────────────
# 로깅
# ─────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
