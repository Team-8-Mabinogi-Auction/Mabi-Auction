import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import time

import requests

from kafka_auction.config.settings import (
    NEXON_API_KEY,
    AUCTION_LISTINGS_URL,
    TOPIC_LISTINGS_CONSUMABLE,
    LOG_LEVEL,
)
from kafka_auction.collector.kafka_producer import send_message, producer
from kafka_auction.processor.utils import generate_match_signature, generate_full_signature

# 소비 카테고리 목록
CONSUMABLE_CATEGORIES = [
    "개조석", "기타", "기타 소모품", "기타 스크롤", "기타 재료", "낭만농장/달빛섬", "던전 통행증", "도면",
    "마법가루", "마비노벨", "마족 스크롤", "말풍선 스티커", "매직 크래프트", "변신 메달", "보석", "분양 메달",
    "불타래", "뷰티 쿠폰", "생활 도구", "스케치", "알반 훈련석", "염색 앰플", "원거리 소모품", "음식",
    "의자/사물", "인챈트 스크롤", "제련/블랙스미스", "제스처", "주머니", "책", "허브", "힐웬 공학",
    "퍼퓸", "페이지", "포션", "피니 펫", "핀즈비즈",
]

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


def run_listings_cycle(category: str, cycle_id: str) -> None:
    """
    '한 번의 호출 주기'에 해당하는 전체 수집 사이클.

    - cursor = "" 부터 시작해서 next_cursor가 없어질 때까지 반복
    - 각 페이지(최대 500개)를 받아서 바로 Kafka로 전송
    - 이 사이클 전체를 식별하는 cycle_id(타임스탬프 문자열)를 부여

    Args:
        category: 처리할 소비 카테고리
        cycle_id: 전체 카테고리 공통 사이클 식별자(END_OF_CYCLE과 동일한 값)
    """
    category_started_at_dt = datetime.now(timezone.utc)

    auction_item_category = category

    logger.info(
        "Starting consumables cycle for category='%s', cycle_id=%s",
        auction_item_category,
        cycle_id,
    )

    cursor = ""
    cursor_cnt = 0
    items_cnt = 0

    while True:
        logger.info(
            "Fetching consumables: page_number='%d' (cursor='%s')",
            cursor_cnt,
            cursor,
        )

        try:
            items, next_cursor = fetch_listings_page(cursor, auction_item_category)
        except Exception:
            logger.exception(
                "Error while fetching consumables: page_number='%d' (cursor='%s')",
                cursor_cnt,
                cursor,
            )
            break

        logger.info(
            "Received %d items: page_number='%d' (cursor='%s')",
            len(items),
            cursor_cnt,
            cursor,
        )

        items_cnt += len(items)
        cursor_cnt += 1

        for item in items:
            full_signature = generate_full_signature(item)
            match_signature = generate_match_signature(item)
            item["full_signature"] = full_signature
            item["match_signature"] = match_signature

            event = {
                "type": "LISTING",
                "cycle_id": cycle_id,
                "item_id": full_signature,
                "match_id": match_signature,
                "raw": item,
            }

            send_message(
                topic=TOPIC_LISTINGS_CONSUMABLE,
                key=full_signature,
                value=event,
            )

        if not next_cursor or not items:
            logger.info("No next_cursor or no items. Ending cycle.")
            break

        cursor = next_cursor

    category_ended_at_dt = datetime.now(timezone.utc)
    elapsed_seconds = (category_ended_at_dt - category_started_at_dt).total_seconds()

    logger.info(
        "Consumables cycle finished. cycle_id=%s, total_items_cnt=%d, page_cnt=%d, elapsed_time=%.2f seconds (%.2f minutes)",
        cycle_id,
        items_cnt,
        cursor_cnt,
        elapsed_seconds,
        elapsed_seconds / 60.0,
    )


if __name__ == "__main__":
    try:
        cycle_count = 0
        while True:
            cycle_count += 1
            if cycle_count > 1:
                logger.info(">>> Waiting for 3 minutes (180 seconds) before next cycle...")
                time.sleep(210)

            cycle_id = datetime.now(timezone.utc).isoformat()
            logger.info(f">>> Execution Start (Cycle {cycle_count}), cycle_id={cycle_id}")

            for category in CONSUMABLE_CATEGORIES:
                logger.info(f">>> Processing category: '{category}'")
                run_listings_cycle(category=category, cycle_id=cycle_id)
                logger.info(f">>> Category '{category}' completed")

            # 모든 카테고리 완료 후 END_OF_CYCLE 1회 발행
            end_event = {
                "type": "END_OF_CYCLE",
                "cycle_id": cycle_id,
            }
            send_message(
                topic=TOPIC_LISTINGS_CONSUMABLE,
                value=end_event,
                flush=True,  # 마지막에는 flush
            )

            logger.info(f">>> All consumable categories completed for Cycle {cycle_count}")

    except KeyboardInterrupt:
        logger.info("Manual interruption detected. Stopping loop.")

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer resources closed. Program terminated.")