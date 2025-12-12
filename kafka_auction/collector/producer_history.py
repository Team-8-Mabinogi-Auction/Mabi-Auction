import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests

from kafka_auction.config.settings import (
    NEXON_API_KEY,
    AUCTION_HISTORY_URL,
    TOPIC_HISTORY,
    LOG_LEVEL,
)
from kafka_auction.collector.kafka_producer import send_message, producer
from kafka_auction.processor.utils import generate_match_signature, generate_full_signature

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def fetch_history_page(
    cursor: str,
    auction_item_category: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    커서 기반으로 경매장 리스트를 1페이지(최대 500개) 조회.
    반환: (items, next_cursor)
    """
    headers = {"x-nxopen-api-key": NEXON_API_KEY}
    params = {
        "cursor": cursor,
        "auction_item_category": auction_item_category,
    }

    resp = requests.get(
        AUCTION_HISTORY_URL,
        params=params,
        headers=headers,
    )
    # resp.raise_for_status()
    data = resp.json()

    items = data.get("auction_history", [])
    next_cursor = data.get("next_cursor", "")

    return items, next_cursor


def run_history_cycle() -> None:
    """
    '한 번의 호출 주기'에 해당하는 전체 수집 사이클.

    - cursor = "" 부터 시작해서 next_cursor가 없어질 때까지 반복
    - 각 페이지(최대 500개)를 받아서 바로 Kafka로 전송
    - 이 사이클 전체를 식별하는 cycle_id(타임스탬프 문자열)를 부여 
    """
    # 주기 시작 시각 (사이클 ID 및 Kafka timestamp 기준) 
    cycle_started_at_dt = datetime.now(timezone.utc)
    cycle_started_at = cycle_started_at_dt.isoformat()

    auction_item_category = "모자/가발"

    logger.info(
        "Starting history cycle for category='%s', cycle_id=%s",
        auction_item_category,
        cycle_started_at,
    )

    cursor = ""

    while True:
        logger.info("Fetching history:  cursor='%s'", cursor)

        try:
            items, next_cursor = fetch_history_page(cursor, auction_item_category)
        except Exception:
            logger.exception("Error while fetching listings (cursor=%s)", cursor)
            break

        
        logger.info(
            "Received %d items on page_index=%d (cursor='%s')",
            len(items),
            cursor,
        )

        for item in items:
            # 매칭용 해시
            match_signature = generate_match_signature(item)
            item["match_signature"] = match_signature


            event = {
                "type": "HISTORY",
                "cycle_id": cycle_started_at,   # 이 호출 주기 식별자 (나중에 S3에서 묶어보기 좋음)
                "item_id" : match_signature,
                "raw": item,
            }

            
            send_message(
                topic=TOPIC_HISTORY,
                key=match_signature,
                value=event,
            )


        if not next_cursor or not items:
            logger.info("No next_cursor or no items. Ending cycle.")
            break

        cursor = next_cursor

    # 사이클 종료 컨트롤 메시지
    end_event = {
        "type": "END_OF_CYCLE",
    }

    send_message(
        topic=TOPIC_HISTORY,
        #key=,
        value=end_event,
        flush=True,  # 마지막에는 flush
    )

    logger.info(
        "HIstory cycle finished. cycle_id=%s, total_items=%d",
        cycle_started_at,
    )


if __name__ == "__main__":
    try:
        run_history_cycle()
    finally:
        producer.flush()
        producer.close()
