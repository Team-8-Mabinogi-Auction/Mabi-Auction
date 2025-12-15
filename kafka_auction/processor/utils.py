import hashlib
import json
from typing import Any, Dict

def normalize_option_list(option_list):
    """
    item_option: List[Dict] 구조를 정렬 가능하고
    안정적인 형태로 정규화하는 함수.
    """
    normalized = []

    for opt in option_list:

        # 각 key-value를 "key=value" 문자열로 정규화
        # None은 빈 문자열로 변환
        parts = []
        for k, v in sorted(opt.items()):
            v_str = "" if v is None else str(v)
            parts.append(f"{k}={v_str}")

        # 각 옵션 dict → 하나의 문자열로 변환
        normalized.append("|".join(parts))

    # 옵션 전체 리스트도 정렬 (순서 무관하도록)
    normalized.sort()

    # 최종 결과는 tuple로 리턴 (불변이므로 해시 안정성↑)
    return tuple(normalized)


def generate_match_signature(item: Dict[str, Any]) -> str:
    """
    동일 조건의 매물들이 동일한 해시를 갖도록 만드는 '매칭용' 해시.
    매물 비교를 위한 해시
    "이름 + 가격 + 옵션" -> 해시값
    - 옵션 구조 정렬 및 정규화
    - sha256 64자리 → 32자리 축소
    - f-string 기반 lightweight raw string 생성
    """

    name = item.get("item_display_name")
    price = item.get("auction_price_per_unit")
    option_list = item.get("item_option") or []

    # 옵션 정규화
    normalized_option = normalize_option_list(option_list)

    # 해싱 대상 문자열 f-string
    raw = f"{name}|{price}|{normalized_option}"

    # sha256 → 앞 32글자(16 bytes)만 사용
    hash_short = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    return hash_short

def generate_full_signature(item: Dict[str, Any]) -> str:
    """
    매물 구분을 위한 해시
    "이름 + 가격 + 만료시각 + 옵션" -> 해시값
    - 옵션 구조 정렬 및 정규화
    - sha256 64자리 → 32자리 축소
    - f-string 기반 lightweight raw string 생성
    """

    name = item.get("item_display_name")
    price = item.get("auction_price_per_unit")
    date_auction_expire = item.get("date_auction_expire")
    option_list = item.get("item_option") or []

    # 옵션 정규화
    normalized_option = normalize_option_list(option_list)

    # 해싱 대상 문자열 f-string
    raw = f"{name}|{price}|{date_auction_expire}|{normalized_option}"

    # sha256 → 앞 32글자(16 bytes)만 사용
    hash_short = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    return hash_short
