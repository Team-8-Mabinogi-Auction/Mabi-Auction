import json
import time
import redis
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta, timezone
from utils import convert_utc_to_kst, calculate_start_time, is_first_batch_ambiguous

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AuctionProcessor")

# 설정
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
REDIS_HOST = 'localhost'

TOPIC_RAW_LISTINGS = 'auction.raw.listings.equipments' 
TOPIC_RAW_HISTORY = 'auction.raw.history'   
TOPIC_EVENTS = 'auction.events'   

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

# Time Constants
PROCESSED_TTL = 600       
PENDING_GRACE_PERIOD = 600 
GHOST_TTL_SECONDS = 400  
HISTORY_DEDUP_TTL = 3660 
WARMUP_DURATION = 300    # 서버 가동 후 첫 데이터 처리 기간 (5분)
KST = timezone(timedelta(hours=9))

# 초기화
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='processor-group-final-ghost-buffer-v1', 
    auto_offset_reset='latest'
)
consumer.subscribe([TOPIC_RAW_LISTINGS, TOPIC_RAW_HISTORY])

# 프로세서 시작 시간 기록
BOOT_TIMESTAMP = time.time()
logger.info("Processor Started")

# 최초 실행 확인
is_first_cycle = not bool(redis_client.exists(KEY_SNAPSHOT_MASTER))

if is_first_cycle:
    logger.info("Initial Run : 'start_time' will be None. Time validation skipped.")
else:
    logger.info("Recovery Run : Snapshot found. Time validation active.")


def emit_event(event_type, item_id, match_id=None, listing_data=None, history_data=None, skip_anomaly=False):
    if listing_data :
        event_time = listing_data.get('start_time')
    
    elif history_data:
        event_time = history_data.get('date_auction_buy')

    if event_time is None:
        skip_anomaly = True

    event = {
        'event_type': event_type,
        'item_id': item_id,       # Unique Signature
        'match_id': match_id,     # Match Signature
        'processed_at': datetime.now().isoformat(),
        'event_time': event_time,
        'payload': {
            'skip_anomaly_detection': skip_anomaly # 이상탐지 건너뛰기 여부
        }
    }

    if event_type == 'REGISTER' and listing_data:
        event['payload']['listing_data'] = listing_data
    elif event_type == 'SOLD' and history_data:
        event['payload']['history_data'] = history_data
    elif event_type == 'CANCEL':
        pass 
    
    producer.send(TOPIC_EVENTS, event)


def validate_time(redis_meta, raw_history):
    start_time_str = redis_meta.get('start_time')
    sold_time_kst_str = raw_history.get('date_auction_buy')
    
    # start_time_str이 존재하는 경우에만 검증
    if start_time_str and sold_time_kst_str:
        dt_start = datetime.fromisoformat(start_time_str)
        dt_sold = datetime.fromisoformat(sold_time_kst_str)
        tolerance = timedelta(minutes=2)
        
        # 판매 시간이 등록 시간보다 과도하게 이르면 무시
        if dt_sold < (dt_start - tolerance):
            return False
            
    return True

# 핵심 로직

def process_hybrid_history(data):
    # History match_id -> Active 조회
    target_match_id = data.get('match_id')
    raw_history = data.get('raw')
    raw_history['date_auction_buy'] = convert_utc_to_kst(raw_history.get('date_auction_buy'))
    
    if not target_match_id or not raw_history: return
    auction_buy_id = raw_history.get('auction_buy_id')

    # 1. Active 확인
    active_val = redis_client.hget(KEY_LISTINGS_ACTIVE, target_match_id)
    
    if active_val:
        # 리스트에 있음 -> Candidate 등록
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

    # 2. Active에 없음 -> 이미 처리됐는지 확인
    if redis_client.get(f"{KEY_PROCESSED_IDS}:{target_match_id}"): return

    # 3. Active X, Pending X -> Listing이 늦게 올 수 있으므로 일단 버퍼에 보관
    raw_history['received_at'] = time.time()
    history_str = json.dumps(raw_history)
    
    redis_client.hset(KEY_HISTORY_GHOST_BUFFER, target_match_id, history_str)
    logger.info(f"Ghost Candidate {target_match_id} buffered.")


def process_listing_message(data):
    global is_first_cycle

    # [Cycle End] 처리 로직
    if data.get('type') == 'END_OF_CYCLE':
        logger.info("Cycle End Processing...")
        
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

    # 개별 매물 등록 처리
    item_id = data.get('item_id')
    match_id = data.get('match_id')
    raw_item = data.get('raw')

    if not item_id or not raw_item: return
    
    pipe = redis_client.pipeline()

    # Active에 없는 신규 매물인 경우
    if not redis_client.hexists(KEY_LISTINGS_ACTIVE, match_id):
        
        raw_expire = raw_item.get('date_auction_expire')
        expire_dt = datetime.fromisoformat(convert_utc_to_kst(raw_expire))
        now_dt = datetime.now()
        
        # 셧다운 예외처리 및 등록 시간 계산 로직
        
        skip_anomaly = False
        now_dt = datetime.now(KST)

        # 1. 완전 첫 가동
        if is_first_cycle:
             skip_anomaly = True 
             raw_item['start_time'] = None 

        # 2. 재가동 + Warm-up 기간
        elif (time.time() - BOOT_TIMESTAMP) < WARMUP_DURATION:
            
            skip_anomaly = True
            
            # 남은 시간(hour) 계산
            diff_seconds = (expire_dt - now_dt).total_seconds()
            hours_left = diff_seconds / 3600
            
            if is_first_batch_ambiguous(hours_left):
                raw_item['start_time'] = None
            else:
                calc_start_str, _, duration = calculate_start_time(raw_expire)
                raw_item['start_time'] = calc_start_str

        # 3. 평상시
        else:
            skip_anomaly = False 
            calc_start_str, _, duration = calculate_start_time(raw_expire)
            raw_item['start_time'] = calc_start_str
        
        _, expire_kst, duration = calculate_start_time(raw_expire)
        
        raw_item['auction_duration'] = duration
        raw_item['date_auction_expire'] = expire_kst
        
        # Kafka 등록 이벤트 발행
        emit_event('REGISTER', item_id=item_id, match_id=match_id, listing_data=raw_item, 
                   skip_anomaly=skip_anomaly)

        # Redis Active 저장
        meta = {
            'item_id': item_id, 
            'start_time': raw_item['start_time'],
            'expire_time': expire_kst,
            'auction_duration': duration
        }
        pipe.hset(KEY_LISTINGS_ACTIVE, match_id, json.dumps(meta))

        # Redis Map 저장
        pipe.hset(KEY_MAP_ITEM_TO_MATCH, item_id, match_id)

        # 최신 매물 100개 유지 (start_time 있는 경우만)
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
            }
            
            try:
                # ISO 시간 문자열을 숫자로 변환
                score = datetime.fromisoformat(start_time_str.replace('Z', '+00:00')).timestamp()
                pipe.zadd(KEY_RECENT_ITEMS, {json.dumps(recent_info, ensure_ascii=False): score})
                pipe.zremrangebyrank(KEY_RECENT_ITEMS, 0, -101)
            except Exception as e:
                logger.warning(f"Failed to add to recent:items: {e}")
    
    # Snapshot 업데이트
    pipe.sadd(KEY_SNAPSHOT_WORKING, item_id)
    pipe.execute()

    # 판매 데이터보다 늦게 전송된 Listing 처리
    ghost_history_str = redis_client.hget(KEY_HISTORY_GHOST_BUFFER, match_id)
    
    if ghost_history_str:
        history_data = json.loads(ghost_history_str)
        
        meta_for_check = {
            'start_time': raw_item.get('start_time'), 
            'expire_time': raw_item.get('date_auction_expire')
        }
        
        if validate_time(meta_for_check, history_data):
            buy_id = history_data.get('auction_buy_id')
            
            emit_event('SOLD', item_id=item_id, match_id=match_id, history_data=history_data)
            logger.info(f"Late Matching Success! {item_id}")

            p2 = redis_client.pipeline()
            p2.setex(f"{KEY_PROCESSED_IDS}:{match_id}", PROCESSED_TTL, buy_id)
            p2.hdel(KEY_LISTINGS_ACTIVE, match_id)
            p2.hdel(KEY_HISTORY_GHOST_BUFFER, match_id)
            p2.hdel(KEY_MAP_ITEM_TO_MATCH, item_id)
            p2.execute()

# 타임 아웃 처리 (취소 매물 체크)
def check_timeouts():
    now = time.time()
    pipe = redis_client.pipeline()
    
    # 1. Pending Timeout 처리
    all_pending = redis_client.hgetall(KEY_LISTINGS_PENDING)
    for item_id, val_str in all_pending.items():
        try:
            meta = json.loads(val_str)
            if now - meta.get('deleted_at', 0) > PENDING_GRACE_PERIOD:
                pipe.hdel(KEY_LISTINGS_PENDING, item_id)
                match_id = meta.get('match_id')
                emit_event('CANCEL', item_id=item_id, match_id=match_id)
        except: continue
        
    # 2. 버퍼에 너무 오래 머무른 History는 진짜 유령매물로 처리
    ghost_buffer = redis_client.hgetall(KEY_HISTORY_GHOST_BUFFER)
    ghost_count = 0
    
    for match_id, val_str in ghost_buffer.items():
        try:
            history_data = json.loads(val_str)
            received_at = history_data.get('received_at', 0)
            
            if now - received_at > GHOST_TTL_SECONDS:
                # Ghost SOLD 이벤트 발행
                emit_event('SOLD', item_id=None, match_id=match_id, history_data=history_data)
                
                # 처리 기록 및 버퍼 삭제
                pipe.setex(f"{KEY_PROCESSED_IDS}:{match_id}", PROCESSED_TTL, history_data.get('auction_buy_id'))
                pipe.hdel(KEY_HISTORY_GHOST_BUFFER, match_id)
                ghost_count += 1
        except: continue

    pipe.execute()
    if ghost_count > 0:
        logger.info(f"Timeout: {ghost_count} items confirmed as GHOST-SOLD.")

# 실행 루프

last_timeout_check = time.time()

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=100)
        
        if msg_pack:
            for tp, messages in msg_pack.items():
                for message in messages:
                    
                    if tp.topic == TOPIC_RAW_LISTINGS:
                        process_listing_message(message.value)
                        
                    elif tp.topic == TOPIC_RAW_HISTORY:
                        # History 데이터 처리
                        raw_history = message.value.get('raw')
                        if raw_history:
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