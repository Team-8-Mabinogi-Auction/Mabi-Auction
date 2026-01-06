import json
import time
import redis
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone, timedelta 
import os
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict, deque

from kafka_auction.processor.utils import (
    calculate_start_time, 
    generate_match_signature,
    is_first_batch_ambiguous,
    convert_utc_to_kst,
    require_env
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [STACKABLE] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StackableProcessor")

# 환경변수 로드
current_file = Path(__file__).resolve()
env_path = current_file.parent.parent / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# --- 설정 ---
KAFKA_BOOTSTRAP_SERVERS = [require_env("KAFKA_BROKER")]
TOPIC_LISTINGS_CONSUMABLE = require_env("KAFKA_TOPIC_LISTINGS_CONSUMABLE")
TOPIC_RAW_HISTORY = require_env("KAFKA_TOPIC_RAW_HISTORY")
TOPIC_EVENTS = require_env("KAFKA_TOPIC_EVENTS")

REDIS_HOST = require_env("REDIS_HOST")
REDIS_PORT = int(require_env("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Time Constants
WARMUP_DURATION = int(os.getenv("WARMUP_DURATION", 300))
GHOST_TTL_SECONDS = int(os.getenv("GHOST_TTL_SECONDS", 600)) 
KST = timezone(timedelta(hours=9))

# 소비 아이템 카테고리 (필터링용)
CONSUMABLE_CATEGORIES = {
    "개조석", "기타", "기타 소모품", "기타 스크롤", "기타 재료", "낭만농장/달빛섬", "던전 통행증", "도면",
    "마법가루", "마비노벨", "마족 스크롤", "말풍선 스티커", "매직 크래프트", "변신 메달", "보석", "분양 메달",
    "불타래", "뷰티 쿠폰", "생활 도구", "스케치", "알반 훈련석", "염색 앰플", "원거리 소모품", "음식",
    "의자/사물", "인챈트 스크롤", "제련/블랙스미스", "제스처", "주머니", "책", "허브", "힐웬 공학",
    "퍼퓸", "페이지", "포션", "피니 펫", "핀즈비즈"
}

# Redis Keys
KEY_SNAPSHOT_CURRENT = 'stackable:snapshot:current'
KEY_SNAPSHOT_PREV = 'stackable:snapshot:prev'
KEY_META_INFO = 'stackable:meta'
KEY_PREFIX_HISTORY_LIST = 'stackable:hist_list:' 
KEY_DIRTY_SIGS = 'stackable:dirty_sigs'
KEY_RECENT_ITEMS = 'recent:items'          

# 초기화
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT, 
    db=REDIS_DB, 
    decode_responses=True,
    ssl=True,
    ssl_cert_reqs=None,
    socket_timeout=5,     # 연결 실패 시 5초 후 에러 발생 (무한 대기 방지)
    socket_connect_timeout=5

)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id=require_env('KAFKA_PROCESSOR_STACKABLE_GROUP_ID'), 
    auto_offset_reset='latest'
)
consumer.subscribe([TOPIC_LISTINGS_CONSUMABLE, TOPIC_RAW_HISTORY])

BOOT_TIMESTAMP = time.time()
logger.info("Processor Started")

def check_redis_status(redis_client, key):
    logger.info(f"--- Redis 상태 진단 시작 (Key: {key}) ---")
    
    try:
        # 1. 연결 테스트 (Ping)
        start_ping = time.perf_counter()
        logger.info("1단계: Redis Ping 테스트 중...")
        redis_client.ping()
        logger.info(f"-> Ping 성공 (소요시간: {time.perf_counter() - start_ping:.4f}s)")

        # 2. Key 존재 여부 확인 (문제가 의심되는 구간)
        logger.info("2단계: Key 존재 여부 확인 중 (exists)...")
        start_exists = time.perf_counter()
        
        # exists() 호출
        exists_result = redis_client.exists(key)
        
        duration = time.perf_counter() - start_exists
        is_first_cycle = not bool(exists_result)
        
        logger.info(f"-> 조회 완료 (소요시간: {duration:.4f}s)")
        logger.info(f"-> 결과: exists={exists_result}, is_first_cycle={is_first_cycle}")
        
        return is_first_cycle

    except Exception as e:
        logger.error(f"!!! 에러 발생: {type(e).__name__} - {str(e)}")
        # 연결 문제일 경우 상세 정보 출력
        if "Timeout" in str(e):
            logger.error("팁: 네트워크 타임아웃이 발생했습니다. Redis 서버 주소와 포트를 다시 확인하세요.")
        return None

is_first_cycle = check_redis_status(redis_client, KEY_SNAPSHOT_PREV)

# 상태 플래그
# is_first_cycle = not bool(redis_client.exists(KEY_SNAPSHOT_PREV))

if is_first_cycle:
    logger.info("Initial Run: Will sync current listings (skip_anomaly=True, is_ghost=False).")

def emit_event(event_type, full_sig, match_sig, count, meta_data, history_record=None, 
               skip_anomaly=False, is_ghost=False):
    if count <= 0: return
    
    item_count = count
    price = meta_data.get('price') or 0
    options = []
    
    buy_id = None
    date_buy = None
    
    if history_record:
        buy_id = history_record.get('auction_buy_id')
        raw_date = history_record.get('date_auction_buy')
        date_buy = convert_utc_to_kst(raw_date) if raw_date else None

    normalized_payload = {
        'item_name': meta_data.get('item_name'),
        'item_display_name': meta_data.get('item_name'),
        'item_count': item_count,
        'auction_price_per_unit': price,
        'auction_item_category': meta_data.get('auction_item_category'), # 메타데이터에서 카테고리 추출
        'item_option': options,
        'start_time': meta_data.get('start_time'),
        'date_auction_expire': meta_data.get('expire_time'),
        'auction_duration': meta_data.get('auction_duration'),
        'auction_buy_id': buy_id,
        'date_auction_buy': date_buy
    }

    event = {
        'event_type': event_type,
        'item_type': 'CONSUMABLE',
        'item_id': full_sig,
        'match_id': match_sig,
        'processed_at': datetime.now(KST).isoformat(),
        'skip_anomaly_detection': skip_anomaly,
        'is_ghost': is_ghost,
        'payload': normalized_payload
    }
    producer.send(TOPIC_EVENTS, event)

def process_listing(data):
    global is_first_cycle

    if data.get('type') == 'END_OF_CYCLE':
        cycle_id = data.get('cycle_id')
        calculate_and_distribute(cycle_id)
        return

    raw_item = data.get('raw')
    if not raw_item: return

    full_sig = data.get('item_id')   
    match_sig = data.get('match_id') 
    count = raw_item.get('item_count', 0)

    raw_expire = raw_item.get('date_auction_expire')
    expire_dt = datetime.fromisoformat(convert_utc_to_kst(raw_expire))
    now_dt = datetime.now(KST)
    
    calc_start_str, expire_kst, duration = calculate_start_time(raw_expire)
    final_start_time = calc_start_str

    if is_first_cycle:
        final_start_time = None
    elif (time.time() - BOOT_TIMESTAMP) < WARMUP_DURATION:
        diff_seconds = (expire_dt - now_dt).total_seconds()
        hours_left = diff_seconds / 3600
        if is_first_batch_ambiguous(hours_left):
            final_start_time = None

    pipe = redis_client.pipeline()
    pipe.hincrby(KEY_SNAPSHOT_CURRENT, full_sig, count)
    
    meta = {
        'match_sig': match_sig,
        'item_name': raw_item.get('item_display_name'),
        'price': raw_item.get('auction_price_per_unit'),
        'auction_item_category': raw_item.get('auction_item_category'), 
        'start_time': final_start_time,
        'expire_time': expire_kst,
        'auction_duration': duration
    }
    pipe.hset(KEY_META_INFO, full_sig, json.dumps(meta))

    if final_start_time: 
        recent_info = {
            'item_id': full_sig,
            'item_name': raw_item.get('item_display_name'),
            'item_category': raw_item.get('auction_item_category'),
            'price_per_unit': raw_item.get('auction_price_per_unit'),
            'item_count': count,
            'total_price': (raw_item.get('auction_price_per_unit') or 0) * count,
            'registered_at': final_start_time, 
            'expire_time': expire_kst,
            'duration': duration,
            'item_option': []
        }
        try:
            dt_obj = datetime.fromisoformat(final_start_time.replace('Z', '+00:00'))
            score = dt_obj.timestamp()
            
            pipe.zadd(KEY_RECENT_ITEMS, {json.dumps(recent_info, ensure_ascii=False): score})
            
            pipe.zremrangebyrank(KEY_RECENT_ITEMS, 0, -101)
        except Exception as e:
            logger.warning(f"Failed to add to recent:items: {e}")

    pipe.execute()

def process_history(data):
    raw_history = data.get('raw')
    if not raw_history: return

    category = raw_history.get('auction_item_category')
    if category not in CONSUMABLE_CATEGORIES:
        return

    try:
        match_sig = generate_match_signature(raw_history)
    except Exception as e:
        logger.error(f"Signature generation failed: {e}")
        return

    if 'received_at' not in raw_history:
        raw_history['received_at'] = time.time()

    key = f"{KEY_PREFIX_HISTORY_LIST}{match_sig}"
    
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps(raw_history))
    pipe.sadd(KEY_DIRTY_SIGS, match_sig)
    pipe.execute()

def calculate_and_distribute(cycle_id):
    global is_first_cycle
    logger.info(f"Starting Calculation Cycle: {cycle_id}")

    curr_snapshot = redis_client.hgetall(KEY_SNAPSHOT_CURRENT)
    prev_snapshot = redis_client.hgetall(KEY_SNAPSHOT_PREV)
    
    dirty_sigs = redis_client.smembers(KEY_DIRTY_SIGS)
    all_full_sigs = set(curr_snapshot.keys()) | set(prev_snapshot.keys())
    
    pipe = redis_client.pipeline()
    full_sig_list = list(all_full_sigs)
    for sig in full_sig_list:
        pipe.hget(KEY_META_INFO, sig)
    meta_results = pipe.execute()

    groups = defaultdict(list)
    meta_map = {}
    
    for i, full_sig in enumerate(full_sig_list):
        meta_json = meta_results[i]
        if meta_json:
            meta = json.loads(meta_json)
            match_sig = meta.get('match_sig')
            if match_sig:
                groups[match_sig].append(full_sig)
                meta_map[full_sig] = meta

    processed_cnt = 0
    # 첫 사이클이면 재고는 반영하되 탐지는 스킵
    base_skip_flag = is_first_cycle 

    batch_registers = [] 
    batch_sales = []     

    active_match_sigs = set(groups.keys()) | dirty_sigs
    leftover_histories_map = defaultdict(list)
    current_time = time.time()

    for match_sig in active_match_sigs:
        full_sigs = groups.get(match_sig, [])
        
        hist_key = f"{KEY_PREFIX_HISTORY_LIST}{match_sig}"
        raw_hist_list = redis_client.lrange(hist_key, 0, -1)
        
        history_queue = deque()
        for h_str in raw_hist_list:
            history_queue.append(json.loads(h_str))

        changes = []
        for full_sig in full_sigs:
            qty_curr = int(curr_snapshot.get(full_sig, 0))
            qty_prev = int(prev_snapshot.get(full_sig, 0))
            delta = qty_curr - qty_prev
            
            changes.append({
                'full_sig': full_sig,
                'delta': delta,
                'meta': meta_map.get(full_sig)
            })

        # 1. 증가분 (REGISTER)
        increased_items = [c for c in changes if c['delta'] > 0]
        for item in increased_items:
            batch_registers.append((
                'REGISTER', item['full_sig'], match_sig, item['delta'], item['meta'], None, 
                base_skip_flag, False 
            ))

        # 2. 감소분 (SOLD / CANCEL)
        decreased_items = [c for c in changes if c['delta'] < 0]
        for item in decreased_items:
            decreased_amount = abs(item['delta'])
            current_meta = item['meta']
            
            while decreased_amount > 0 and history_queue:
                hist_rec = history_queue[0]
                hist_count = hist_rec.get('item_count', 0)
                consume = min(decreased_amount, hist_count)
                
                batch_sales.append((
                    'SOLD', item['full_sig'], match_sig, consume, current_meta, hist_rec, 
                    base_skip_flag, False 
                ))
                
                decreased_amount -= consume
                hist_rec['item_count'] -= consume
                if hist_rec['item_count'] <= 0: history_queue.popleft() 

            if decreased_amount > 0:
                batch_sales.append((
                    'CANCEL', item['full_sig'], match_sig, decreased_amount, current_meta, None, 
                    base_skip_flag, False 
                ))

        # 3. 매칭되지 않고 남은 History 처리 (Defer vs Ghost Sold)
        while history_queue:
            hist_rec = history_queue.popleft()
            leftover_count = hist_rec.get('item_count', 0)
            if leftover_count <= 0: continue

            received_at = hist_rec.get('received_at', 0)
            
            if current_time - received_at < GHOST_TTL_SECONDS:
                leftover_histories_map[match_sig].append(hist_rec)
            else:
                # [최종 수정] Ghost Sold 메타데이터 생성 시 History 정보 사용
                # 매칭된 Listing이 없으므로(full_sigs empty), History record에서 정보 복원
                
                if full_sigs:
                    fallback_meta = meta_map.get(full_sigs[0])
                else:
                    # History 데이터에서 메타정보 복원 (카테고리 포함!)
                    fallback_meta = {
                        'item_name': hist_rec.get('item_display_name', 'Unknown'),
                        'price': hist_rec.get('auction_price_per_unit', 0),
                        'auction_item_category': hist_rec.get('auction_item_category'), # [중요] 카테고리 필드 추가
                        'start_time': None,
                        'expire_time': None,
                        'auction_duration': None
                    }
                
                fallback_id = full_sigs[0] if full_sigs else f"ghost:{match_sig}"
                
                batch_sales.append((
                    'SOLD', fallback_id, match_sig, leftover_count, fallback_meta, hist_rec, 
                    True, True # skip=True, is_ghost=True
                ))
                logger.warning(f"Ghost Sold: MatchID {match_sig}, Count {leftover_count}")

        processed_cnt += 1

    # 전송
    for evt in batch_registers:
        emit_event(*evt) # unpack
        
    for evt in batch_sales:
        emit_event(*evt) # unpack

    # 정리
    redis_client.delete(KEY_SNAPSHOT_PREV)
    if curr_snapshot:
        redis_client.hset(KEY_SNAPSHOT_PREV, mapping=curr_snapshot)
    
    redis_client.delete(KEY_SNAPSHOT_CURRENT)
    
    if dirty_sigs:
        keys_to_del = [f"{KEY_PREFIX_HISTORY_LIST}{sig}" for sig in dirty_sigs]
        redis_client.delete(*keys_to_del)
        redis_client.delete(KEY_DIRTY_SIGS)

    if leftover_histories_map:
        pipe = redis_client.pipeline()
        for match_sig, hist_list in leftover_histories_map.items():
            key = f"{KEY_PREFIX_HISTORY_LIST}{match_sig}"
            for h in hist_list:
                pipe.rpush(key, json.dumps(h))
            pipe.sadd(KEY_DIRTY_SIGS, match_sig)
        pipe.execute()
        logger.info(f"Deferred {len(leftover_histories_map)} history groups to next cycle.")

    if is_first_cycle:
        is_first_cycle = False
        logger.info("Snapshot initialized.")
    else:
        logger.info(f"Cycle {cycle_id} Processed. {processed_cnt} groups checked.")

try:
    logger.info("Processor Started")
    while True:
        msg_pack = consumer.poll(timeout_ms=100)
        
        for tp, messages in msg_pack.items():
            for message in messages:
                if tp.topic == TOPIC_LISTINGS_CONSUMABLE:
                    process_listing(message.value)
                elif tp.topic == TOPIC_RAW_HISTORY:
                    process_history(message.value)
                    
except Exception as e:
    logger.error(f"Critical Error: {e}", exc_info=True)
finally:
    consumer.close()
    producer.close()
    redis_client.close()