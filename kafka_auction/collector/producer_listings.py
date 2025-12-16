import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import time

import requests

from kafka_auction.config.settings import (
    NEXON_API_KEY,
    AUCTION_LISTINGS_URL,
    TOPIC_LISTINGS_EQUIPMENTS,
    LOG_LEVEL,
)
from kafka_auction.collector.kafka_producer import send_message, producer
from kafka_auction.processor.utils import generate_match_signature, generate_full_signature

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def fetch_listings_page(
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
        AUCTION_LISTINGS_URL,
        params=params,
        headers=headers,
    )
    # resp.raise_for_status()
    data = resp.json()

    items = data.get("auction_item", [])
    next_cursor = data.get("next_cursor", "")

    return items, next_cursor


def run_listings_cycle() -> None:
    """
    '한 번의 호출 주기'에 해당하는 전체 수집 사이클.

    - cursor = "" 부터 시작해서 next_cursor가 없어질 때까지 반복
    - 각 페이지(최대 500개)를 받아서 바로 Kafka로 전송
    - 이 사이클 전체를 식별하는 cycle_id(타임스탬프 문자열)를 부여 -> 추후 dag 호출 시간으로 변경
    """
    # 주기 시작 시각 (사이클 ID 및 Kafka timestamp 기준) -> 추후  dag 호출 시간으로 변경
    cycle_started_at_dt = datetime.now(timezone.utc)
    cycle_started_at = cycle_started_at_dt.isoformat()

    auction_item_category = "모자/가발"

    logger.info(
        "Starting listings cycle for category='%s', cycle_id=%s",
        auction_item_category,
        cycle_started_at,
    )

    cursor = ""
    # 로깅용 변수 추가
    page_index = 0      
    total_items = 0

    while True:
        logger.info("Fetching listings:  cursor='%s'", cursor)

        try:
            items, next_cursor = fetch_listings_page(cursor, auction_item_category)
        except Exception:
            logger.exception("Error while fetching listings (cursor=%s)", cursor)
            break

        
        # 집계 정보 업데이트 및 로깅 인자 수정
        current_count = len(items)
        total_items += current_count
        page_index += 1
        
        logger.info(
            "Received %d items on page_index=%d (cursor='%s')",
            current_count,
            page_index,
            cursor,
        )

        for item in items:
            # 매칭용 해시
            full_signature = generate_full_signature(item)
            item["full_signature"] = full_signature


            event = {
                "type": "LISTING",
                "cycle_id": cycle_started_at,   # 이 호출 주기 식별자 (나중에 S3에서 묶어보기 좋음)
                "item_id" : full_signature,
                "raw": item,
            }

            
            send_message(
                topic=TOPIC_LISTINGS_EQUIPMENTS,
                key=full_signature,
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
        topic=TOPIC_LISTINGS_EQUIPMENTS,
        #key=,
        value=end_event,
        flush=True,  # 마지막에는 flush
    )

    # 수정 - 로깅 인자 개수 맞춤
    logger.info(
        "Listings cycle finished. cycle_id=%s, total_items=%d",
        cycle_started_at,
        total_items,
    )


if __name__ == "__main__":
    try:
        cycle_count = 0
        while True:
            cycle_count += 1
            if cycle_count > 1:
                # 2회차부터는 3분 대기
                logger.info(">>> Waiting for 3 minutes (180 seconds) before next cycle...")
                time.sleep(180)
            
            logger.info(f">>> Execution Start (Cycle {cycle_count})")
            run_listings_cycle()
    except KeyboardInterrupt:
        logger.info("Manual interruption detected. Stopping loop.")

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer resources closed. Program terminated.")
