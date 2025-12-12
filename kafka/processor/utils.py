import hashlib
import json
from typing import Any, Dict

def generate_match_signature(item: Dict[str, Any]) -> str:
    """
    동일 조건의 매물들이 동일한 해시를 갖도록 만드는 '매칭용' 해시.
    매물 비교를 위한 해시
    "이름 + 가격 + 옵션" -> 해시값
    """

    normalized = {
        "item_name": item.get("item_name"),
        "auction_price_per_unit": item.get("auction_price_per_unit"),
        "item_option": item.get("item_option"),
    }
    raw = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def generate_full_signature(item: Dict[str, Any]) -> str:
    """
    매물 구분을 위한 해시
    "이름 + 가격 + 만료시각 + 옵션" -> 해시값
    """
    normalized = {
        "item_name": item.get("item_name"),
        "auction_price_per_unit": item.get("auction_price_per_unit"),
        "date_auction_expire": item.get("date_auction_expire"),
        "item_option": item.get("item_option"),
    }
    raw = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()