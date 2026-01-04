import json
import time
import redis
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta, timezone
import os
from kafka_auction.processor.utils import convert_utc_to_kst, calculate_start_time, is_first_batch_ambiguous, require_env
from pathlib import Path
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [EQUIPMENT] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AuctionProcessor")

# 환경변수 설정
current_file = Path(__file__).resolve() 
env_path = current_file.parent.parent / 'config' / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loaded environment variables from {env_path}")
else:
    logger.error(f"Cannot find .env file at {env_path}")

# 설정
KAFKA_BOOTSTRAP_SERVERS = [require_env("KAFKA_BROKER")]
TOPIC_RAW_LISTINGS_EQUIPMENTS = require_env("KAFKA_TOPIC_LISTINGS_EQUIPMENTS")
TOPIC_RAW_HISTORY = require_env("KAFKA_TOPIC_RAW_HISTORY")
TOPIC_EVENTS = require_env("KAFKA_TOPIC_EVENTS")

REDIS_HOST = require_env("REDIS_HOST")
REDIS_PORT = int(require_env("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Redis Keys
KEY_LISTINGS_ACTIVE = 'listings:active'          
KEY_MAP_ITEM_TO_MATCH = 'listings:map:item_to_match'
KEY_SNAPSHOT_MASTER = 'listings:snapshot:master' 
KEY_SNAPSHOT_WORKING = 'listings:snapshot:working'
KEY_LISTINGS_PENDING = 'listings:pending'        
KEY_HISTORY_CANDIDATES = 'history:candidates'    
KEY_PROCESSED_IDS = 'history:processed_ids'      
KEY_PREFIX_HISTORY_SEEN = 'history:seen:'
KEY_HISTORY_GHOST_BUFFER = 'history:ghost_buffer'
KEY_RECENT_ITEMS = 'recent:items'
KEY_STATUS_LAST_UPDATE = 'status:last_cycle_time'

# Time Constants
PROCESSED_TTL = int(os.getenv("PROCESSED_TTL", 600))
PENDING_GRACE_PERIOD = int(os.getenv("PENDING_GRACE_PERIOD", 600))
GHOST_TTL_SECONDS = int(os.getenv("GHOST_TTL_SECONDS", 400))
HISTORY_DEDUP_TTL = int(os.getenv("HISTORY_DEDUP_TTL", 3660))
WARMUP_DURATION = int(os.getenv("WARMUP_DURATION", 300))
KST = timezone(timedelta(hours=9))

# 소비 아이템 카테고리 (필터링용)
CONSUMABLE_CATEGORIES = {
    "개조석", "기타", "기타 소모품", "기타 스크롤", "기타 재료", "낭만농장/달빛섬", "던전 통행증", "도면",
    "마법가루", "마비노벨", "마족 스크롤", "말풍선 스티커", "매직 크래프트", "변신 메달", "보석", "분양 메달",
    "불타래", "뷰티 쿠폰", "생활 도구", "스케치", "알반 훈련석", "염색 앰플", "원거리 소모품", "음식",
    "의자/사물", "인챈트 스크롤", "제련/블랙스미스", "제스처", "주머니", "책", "허브", "힐웬 공학",
    "퍼퓸", "페이지", "포션", "피니 펫", "핀즈비즈"
}

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
    group_id=require_env('KAFKA_PROCESSOR_EVENTS_GROUP_ID'), 
    auto_offset_reset='latest'
)
consumer.subscribe([TOPIC_RAW_LISTINGS_EQUIPMENTS, TOPIC_RAW_HISTORY])

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

is_first_cycle = check_redis_status(redis_client, KEY_SNAPSHOT_MASTER)
# is_first_cycle = not bool(redis_client.exists(KEY_SNAPSHOT_MASTER))

if is_first_cycle:
    logger.info("Initial Run: Syncing listings to Detector (skip_anomaly=True, is_ghost=False).")
else:
    logger.info("Recovery Run: Snapshot found. Normal operation.")


# [수정] is_ghost 추가
def emit_event(event_type, item_id, match_id=None, listing_data=None, history_data=None, 
               skip_anomaly=False, is_ghost=False):
    # 1. 데이터 소스 결정
    raw_data = listing_data if listing_data else history_data
    if not raw_data: return

    # 2. 시간 정보 추출
    start_time = raw_data.get('start_time')
    expire_time = raw_data.get('date_auction_expire')
    
    event_time = None
    if listing_data:
        event_time = start_time
    elif history_data:
        event_time = history_data.get('date_auction_buy')

    if event_time is None:
        skip_anomaly = True

    # 3. Payload 표준화 & Null Safety
    normalized_payload = {
        'item_name': raw_data.get('item_name'),
        'item_display_name': raw_data.get('item_display_name'),
        'item_count': raw_data.get('item_count') or 1,        
        'auction_price_per_unit': raw_data.get('auction_price_per_unit') or 0,
        'auction_item_category': raw_data.get('auction_item_category'),
        'item_option': raw_data.get('item_option') or [],     
        'start_time': start_time,
        'date_auction_expire': expire_time,
        'auction_duration': raw_data.get('auction_duration'),
        'auction_buy_id': raw_data.get('auction_buy_id'),
        'date_auction_buy': raw_data.get('date_auction_buy'),
    }

    event = {
        'event_type': event_type,
        'item_type': 'EQUIPMENT',   
        'item_id': item_id,
        'match_id': match_id,
        'processed_at': datetime.now(KST).isoformat(),
        'event_time': event_time,
        'skip_anomaly_detection': skip_anomaly,
        'is_ghost': is_ghost, # [추가]
        'payload': normalized_payload
    }
    
    producer.send(TOPIC_EVENTS, event)


def validate_time(redis_meta, raw_history):
    start_time_str = redis_meta.get('start_time')
    sold_time_kst_str = raw_history.get('date_auction_buy')
    
    if start_time_str and sold_time_kst_str:
        dt_start = datetime.fromisoformat(start_time_str)
        dt_sold = datetime.fromisoformat(sold_time_kst_str)
        tolerance = timedelta(minutes=2)
        
        if dt_sold < (dt_start - tolerance):
            return False
            
    return True

def process_hybrid_history(data):
    target_match_id = data.get('match_id')
    raw_history = data.get('raw')
    raw_history['date_auction_buy'] = convert_utc_to_kst(raw_history.get('date_auction_buy'))
    
    if not target_match_id or not raw_history: return
    auction_buy_id = raw_history.get('auction_buy_id')

    active_val = redis_client.hget(KEY_LISTINGS_ACTIVE, target_match_id)
    
    if active_val:
        redis_meta = json.loads(active_val)
        item_id = redis_meta.get('item_id') 
        
        if not validate_time(redis_meta, raw_history): return

        candidate_data = {
            'buy_id': auction_buy_id,
            'history_data': raw_history,
            'match_id': target_match_id
        }
        redis_client.hset(KEY_HISTORY_CANDIDATES, item_id, json.dumps(candidate_data))
        return

    if redis_client.get(f"{KEY_PROCESSED_IDS}:{target_match_id}"): return

    raw_history['received_at'] = time.time()
    history_str = json.dumps(raw_history)
    
    redis_client.hset(KEY_HISTORY_GHOST_BUFFER, target_match_id, history_str)
    # logger.info(f"Ghost Candidate {target_match_id} buffered.")


def process_listing_message(data):
    global is_first_cycle

    if data.get('type') == 'END_OF_CYCLE':
        logger.info("Cycle End Processing...")

        cycle_time = data.get('cycle_id')
        if cycle_time:
            redis_client.set(KEY_STATUS_LAST_UPDATE, cycle_time)
            logger.info(f"Updated Web Status Time: {cycle_time}")
        else:
            now_kst = datetime.now(KST).isoformat()
            redis_client.set(KEY_STATUS_LAST_UPDATE, now_kst)
            logger.warning("No timestamp in END_OF_CYCLE. Used current time.")
        
        active_item_ids = set(redis_client.hkeys(KEY_MAP_ITEM_TO_MATCH))
        working_item_ids = redis_client.smembers(KEY_SNAPSHOT_WORKING)
        
        missing_item_ids = active_item_ids - working_item_ids
        
        pipeline = redis_client.pipeline()
        
        for missing_id in missing_item_ids:
            match_id = redis_client.hget(KEY_MAP_ITEM_TO_MATCH, missing_id)
            
            if redis_client.exists(f"{KEY_PROCESSED_IDS}:{missing_id}"):
                if match_id: pipeline.hdel(KEY_LISTINGS_ACTIVE, match_id)
                pipeline.hdel(KEY_MAP_ITEM_TO_MATCH, missing_id)
                pipeline.hdel(KEY_HISTORY_CANDIDATES, missing_id)
                continue
            
            active_val = None
            if match_id:
                active_val = redis_client.hget(KEY_LISTINGS_ACTIVE, match_id)
            
            if not active_val: 
                pipeline.hdel(KEY_MAP_ITEM_TO_MATCH, missing_id)
                continue

            candidate_str = redis_client.hget(KEY_HISTORY_CANDIDATES, missing_id)
            
            if candidate_str:
                cand = json.loads(candidate_str)
                # 정상 매칭: skip=False, ghost=False
                emit_event('SOLD', item_id=missing_id, match_id=match_id, history_data=cand['history_data'])
                
                pipeline.setex(f"{KEY_PROCESSED_IDS}:{missing_id}", PROCESSED_TTL, cand['buy_id'])
                if match_id: pipeline.hdel(KEY_LISTINGS_ACTIVE, match_id)
                pipeline.hdel(KEY_MAP_ITEM_TO_MATCH, missing_id)
                pipeline.hdel(KEY_HISTORY_CANDIDATES, missing_id)
            else:
                meta = json.loads(active_val)
                meta['deleted_at'] = time.time()
                
                pipeline.hset(KEY_LISTINGS_PENDING, missing_id, json.dumps(meta))
                if match_id: pipeline.hdel(KEY_LISTINGS_ACTIVE, match_id)
                pipeline.hdel(KEY_MAP_ITEM_TO_MATCH, missing_id)
                
        pipeline.execute()
        
        redis_client.delete(KEY_SNAPSHOT_MASTER)
        redis_client.rename(KEY_SNAPSHOT_WORKING, KEY_SNAPSHOT_MASTER)
        if is_first_cycle: is_first_cycle = False
        return

    item_id = data.get('item_id')
    match_id = data.get('match_id')
    raw_item = data.get('raw')

    if not item_id or not raw_item: return
    
    pipe = redis_client.pipeline()

    if not redis_client.hexists(KEY_LISTINGS_ACTIVE, match_id):
        
        raw_expire = raw_item.get('date_auction_expire')
        expire_dt = datetime.fromisoformat(convert_utc_to_kst(raw_expire))
        now_dt = datetime.now(KST)
        
        skip_anomaly = False
        calc_start_str, _, duration = calculate_start_time(raw_expire)

        if is_first_cycle:
             # 첫 사이클: 재고 동기화 (ghost=False)
             skip_anomaly = True 
             raw_item['start_time'] = None 

        elif (time.time() - BOOT_TIMESTAMP) < WARMUP_DURATION:
            skip_anomaly = True
            diff_seconds = (expire_dt - now_dt).total_seconds()
            hours_left = diff_seconds / 3600
            
            if is_first_batch_ambiguous(hours_left):
                raw_item['start_time'] = None
            else:
                raw_item['start_time'] = calc_start_str

        else:
            skip_anomaly = False 
            raw_item['start_time'] = calc_start_str
        
        _, expire_kst, duration = calculate_start_time(raw_expire)
        
        raw_item['auction_duration'] = duration
        raw_item['date_auction_expire'] = expire_kst
        
        emit_event('REGISTER', item_id=item_id, match_id=match_id, listing_data=raw_item, 
                   skip_anomaly=skip_anomaly, is_ghost=False)

        meta = {
            'item_id': item_id, 
            'start_time': raw_item.get('start_time'),
            'expire_time': expire_kst,
            'auction_duration': duration,
            'item_name': raw_item.get('item_name'),
            'item_display_name': raw_item.get('item_display_name'),
            'auction_price_per_unit': raw_item.get('auction_price_per_unit'),
            'auction_item_category': raw_item.get('auction_item_category'),
            'item_option': raw_item.get('item_option')
        }
        pipe.hset(KEY_LISTINGS_ACTIVE, match_id, json.dumps(meta))
        pipe.hset(KEY_MAP_ITEM_TO_MATCH, item_id, match_id)

        start_time_str = raw_item.get('start_time')
        
        if start_time_str:
            recent_info = {
                'item_id': item_id,
                'item_name': raw_item.get('item_name'),
                'item_category': raw_item.get('auction_item_category'),
                'price_per_unit': raw_item.get('auction_price_per_unit'),
                'item_count': raw_item.get('item_count'),
                'total_price': raw_item.get('auction_price_per_unit') * raw_item.get('item_count'),
                'registered_at': start_time_str, 
                'expire_time': raw_item.get('date_auction_expire'),
                'duration': raw_item.get('auction_duration'),
                'item_option': raw_item.get('item_option')
            }
            try:
                score = datetime.fromisoformat(start_time_str.replace('Z', '+00:00')).timestamp()
                pipe.zadd(KEY_RECENT_ITEMS, {json.dumps(recent_info, ensure_ascii=False): score})
                pipe.zremrangebyrank(KEY_RECENT_ITEMS, 0, -101)
            except Exception as e:
                logger.warning(f"Failed to add to recent:items: {e}")
    
    pipe.sadd(KEY_SNAPSHOT_WORKING, item_id)
    pipe.execute()

    ghost_history_str = redis_client.hget(KEY_HISTORY_GHOST_BUFFER, match_id)
    
    if ghost_history_str:
        history_data = json.loads(ghost_history_str)
        
        meta_for_check = {
            'start_time': raw_item.get('start_time'), 
            'expire_time': raw_item.get('date_auction_expire')
        }
        
        if validate_time(meta_for_check, history_data):
            buy_id = history_data.get('auction_buy_id')
            
            # Late Matching (정상): skip=False, ghost=False
            emit_event('SOLD', item_id=item_id, match_id=match_id, history_data=history_data)
            logger.info(f"Late Matching Success! {item_id}")

            p2 = redis_client.pipeline()
            p2.setex(f"{KEY_PROCESSED_IDS}:{match_id}", PROCESSED_TTL, buy_id)
            p2.hdel(KEY_LISTINGS_ACTIVE, match_id)
            p2.hdel(KEY_HISTORY_GHOST_BUFFER, match_id)
            p2.hdel(KEY_MAP_ITEM_TO_MATCH, item_id)
            p2.execute()

def check_timeouts():
    now = time.time()
    pipe = redis_client.pipeline()
    
    # [Pending -> Cancel]
    all_pending = redis_client.hgetall(KEY_LISTINGS_PENDING)
    for item_id, val_str in all_pending.items():
        try:
            meta = json.loads(val_str)
            if now - meta.get('deleted_at', 0) > PENDING_GRACE_PERIOD:
                pipe.hdel(KEY_LISTINGS_PENDING, item_id)
                match_id = meta.get('match_id')
                
                listing_data = {
                    'item_name': meta.get('item_name'),
                    'item_display_name': meta.get('item_display_name'),
                    'auction_price_per_unit': meta.get('auction_price_per_unit'),
                    'auction_item_category': meta.get('auction_item_category'),
                    'item_option': meta.get('item_option'),
                    'start_time': meta.get('start_time'),
                    'date_auction_expire': meta.get('expire_time'),
                    'auction_duration': meta.get('auction_duration'),
                    'item_count': 1
                }
                emit_event('CANCEL', item_id=item_id, match_id=match_id, listing_data=listing_data)
        except: continue
        
    # [Ghost Buffer -> Timeout]
    ghost_buffer = redis_client.hgetall(KEY_HISTORY_GHOST_BUFFER)
    ghost_count = 0
    
    for match_id, val_str in ghost_buffer.items():
        try:
            history_data = json.loads(val_str)
            received_at = history_data.get('received_at', 0)
            
            if now - received_at > GHOST_TTL_SECONDS:
                # [장비 Ghost 발생] is_ghost=True, skip_anomaly=True
                emit_event('SOLD', item_id=None, match_id=match_id, history_data=history_data,
                           skip_anomaly=True, is_ghost=True)
                
                pipe.setex(f"{KEY_PROCESSED_IDS}:{match_id}", PROCESSED_TTL, history_data.get('auction_buy_id'))
                pipe.hdel(KEY_HISTORY_GHOST_BUFFER, match_id)
                ghost_count += 1
        except: continue

    pipe.execute()
    if ghost_count > 0:
        logger.info(f"Timeout: {ghost_count} items confirmed as GHOST-SOLD.")

last_timeout_check = time.time()

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=100)
        
        if msg_pack:
            for tp, messages in msg_pack.items():
                for message in messages:
                    
                    if tp.topic == TOPIC_RAW_LISTINGS_EQUIPMENTS:
                        process_listing_message(message.value)
                        
                    elif tp.topic == TOPIC_RAW_HISTORY:
                        raw_history = message.value.get('raw')
                        if raw_history:
                            # 소비 아이템 필터링
                            category = raw_history.get('auction_item_category')
                            if category in CONSUMABLE_CATEGORIES:
                                continue

                            buy_id = raw_history.get('auction_buy_id')
                            dedup_key = f"{KEY_PREFIX_HISTORY_SEEN}{buy_id}"
                            
                            if redis_client.set(dedup_key, "1", ex=HISTORY_DEDUP_TTL, nx=True):
                                process_hybrid_history(message.value)

        if time.time() - last_timeout_check > 10:
            check_timeouts()
            last_timeout_check = time.time()

except Exception as e:
    logger.error(f"Unexpected Error: {e}", exc_info=True)
finally:
    consumer.close()
    producer.close()
    redis_client.close()
    logger.info("Program Terminated")