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

# ─────────────────────────────
# Kafka 설정
# ─────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC_LISTINGS = os.getenv("TOPIC_LISTINGS", "auction.raw.listings")

# ─────────────────────────────
# 로깅
# ─────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
