import hashlib
import json
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta, timezone

# KST 타임존 설정
KST = timezone(timedelta(hours=9))

def normalize_option_list(option_list: List[Dict]) -> Tuple[str]:
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
    [Match ID]
    동일 조건의 매물들이 동일한 해시를 갖도록 만드는 '매칭용' 해시.
    구성: "이름 + 가격 + 옵션"
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
    [Item ID / Full Signature]
    개별 매물 구분을 위한 고유 해시 (DB Primary Key)
    구성: "이름 + 가격 + 만료시각 + 옵션"
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


def convert_utc_to_kst(utc_time_str: str) -> str:
    """UTC 문자열을 KST ISO 포맷으로 변환"""
    if not utc_time_str: return None
    try:
        # Python 3.7+ fromisoformat은 'Z'를 바로 처리 못하는 경우가 있어 replace 처리
        dt_utc = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
        dt_kst = dt_utc.astimezone(KST)
        return dt_kst.isoformat()
    except Exception as e:
        # 로그는 호출부에서 처리하거나 필요 시 추가
        return None


def calculate_start_time(expire_time_str: str):
    """
    만료 시간을 기반으로 등록 시간과 지속 시간을 역산하는 함수
    (최초 등록 시점 등을 추정하기 위함)
    """
    now_kst = datetime.now(KST)
    DEFAULT_DURATION = 36 
    
    if not expire_time_str:
        return now_kst.isoformat(), now_kst.isoformat(), DEFAULT_DURATION
    
    try:
        expire_time_kst_str = convert_utc_to_kst(expire_time_str)
        if not expire_time_kst_str:
             return now_kst.isoformat(), now_kst.isoformat(), DEFAULT_DURATION

        expire_dt_kst = datetime.fromisoformat(expire_time_kst_str)
        
        # 남은 시간 계산
        diff_seconds = expire_dt_kst.timestamp() - now_kst.timestamp()
        hours_left = diff_seconds / 3600
        
        # 이미 만료된 경우 처리
        if hours_left <= 0:
            start_dt = expire_dt_kst - timedelta(hours=DEFAULT_DURATION)
            return start_dt.isoformat(), expire_time_kst_str, DEFAULT_DURATION
            
        # 등록 기간 후보군 (마비노기 경매장 시스템 기준)
        candidates = [6, 12, 24, 36, 48, 72]
        duration = candidates[-1] # 기본값: 최대 시간
        
        for cand in candidates:
            if cand >= hours_left:
                duration = cand
                break
                
        start_dt = expire_dt_kst - timedelta(hours=duration)
        
        # 리턴: (추정 등록시간, KST 만료시간, 추정 지속시간)
        return start_dt.isoformat(), expire_dt_kst.isoformat(), duration
        
    except Exception:
        return now_kst.isoformat(), now_kst.isoformat(), DEFAULT_DURATION