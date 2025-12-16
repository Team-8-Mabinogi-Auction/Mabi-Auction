import json
import time
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer, KafkaProducer

from utils import generate_match_signature

# 설정] ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

TOPIC_RAW_LISTINGS = 'auction.raw.listings.equipments' 
TOPIC_RAW_HISTORY = 'auction.raw.history'   
TOPIC_EVENTS = 'auction.events'   

KEY_SNAPSHOT_MASTER = 'snapshot:master'
KEY_SNAPSHOT_WORKING = 'snapshot:working'
KEY_VERIFY_PENDING = 'verify:pending'

KEY_PREFIX_HISTORY_SEEN = 'history:seen:'

HISTORY_BUFFER_TTL = 120
TIMEOUT_SECONDS = 200 # 넉넉하게 잡음
KST = timezone(timedelta(hours=9))

# DB 접속 정보
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword',
    'port': 5432
}

# 연결
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='processor-group-combined-test2', 
    auto_offset_reset='latest'
)
consumer.subscribe([TOPIC_RAW_LISTINGS, TOPIC_RAW_HISTORY])

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"데이터 수신 대기중... (Buffer TTL: {HISTORY_BUFFER_TTL}s)")


# 유틸리티 함수

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def convert_utc_to_kst(utc_time_str):
    if not utc_time_str: return None
    try:
        dt_utc = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
        dt_kst = dt_utc.astimezone(KST)
        return dt_kst.isoformat()
    except Exception as e:
        print(f"시간 변환 오류 ({utc_time_str}): {e}")
        return None

def calculate_start_time(expire_time_str):
    now_kst = datetime.now(KST)
    DEFAULT_DURATION = 36 
    if not expire_time_str:
        return now_kst.isoformat(), now_kst.isoformat(), DEFAULT_DURATION
    try:
        expire_time_kst_str = convert_utc_to_kst(expire_time_str)
        expire_dt_kst = datetime.fromisoformat(expire_time_kst_str)
        diff_seconds = expire_dt_kst.timestamp() - now_kst.timestamp()
        hours_left = diff_seconds / 3600
        if hours_left <= 0:
            start_dt = expire_dt_kst - timedelta(hours=DEFAULT_DURATION)
            return start_dt.isoformat(), DEFAULT_DURATION
        candidates = [6, 12, 24, 36, 48, 72]
        duration = candidates[-1]
        for cand in candidates:
            if cand >= hours_left:
                duration = cand
                break
        start_dt = expire_dt_kst - timedelta(hours=duration)
        return start_dt.isoformat(), expire_dt_kst.isoformat(), duration
    except Exception:
        return now_kst.isoformat(), now_kst.isoformat(), DEFAULT_DURATION

def emit_event(event_type, item_data, final_price=None, sold_time=None):
    if isinstance(item_data, str):
        item_data = json.loads(item_data)
    event = {
        'event_type': event_type,
        'processed_at': datetime.now().isoformat(),
        'item_id': item_data.get('item_id'),
        'item_display_name': item_data.get('item_display_name') or item_data.get('item_name'),
        'raw_data': item_data
    }
    if final_price: event['sold_price'] = final_price
    if sold_time: event['sold_time'] = sold_time
    producer.send(TOPIC_EVENTS, event)

def get_db_item_signature(db_data):
    # 1. 이름 매핑
    name = db_data.get('item_display_name')
    price = db_data.get('price')
    options = db_data.get('item_option') or []

    adapter = {
        "item_display_name": name,
        "auction_price_per_unit": price,
        "item_option": options
    }

    return generate_match_signature(adapter)

def fetch_items_from_db(item_ids):
    if not item_ids: return []
    conn = get_db_connection()
    results = []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            batch_size = 1000
            total_ids = list(item_ids)
            for i in range(0, len(total_ids), batch_size):
                batch_list = total_ids[i : i + batch_size]
                batch_tuple = tuple(batch_list)
                
                sql = """
                    SELECT 
                        l.*,
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'option_type', o.option_type,
                                    'option_sub_type', o.option_sub_type,
                                    'option_value', o.option_value,
                                    'option_value2', o.option_value2,
                                    'option_desc', o.option_desc
                                )
                            ) FILTER (WHERE o.item_id IS NOT NULL), 
                            '[]'
                        ) as item_option
                    FROM mabi_auction.current_listings l
                    LEFT JOIN mabi_auction.item_options o ON l.item_id = o.item_id
                    WHERE l.item_id IN %s
                    GROUP BY l.item_id
                """
                
                cur.execute(sql, (batch_tuple,))
                rows = cur.fetchall()
                for row in rows:
                    for k, v in row.items():
                        if isinstance(v, datetime): row[k] = v.isoformat()
                    results.append(row)
    except Exception as e:
        print(f"DB 조회 오류: {e}")
    finally:
        conn.close()
    return results


# 매물(Listings) 처리

def process_listing_message(data):
    if data.get('type') == 'END_OF_CYCLE':
        cycle_id = data.get('cycle_id')
        print(f"\n 사이클 종료({cycle_id}). 삭제 감지 시작...")
        
        deleted_uuids = redis_client.sdiff(KEY_SNAPSHOT_MASTER, KEY_SNAPSHOT_WORKING)
        
        if deleted_uuids:
            print(f"{len(deleted_uuids)}개의 사라진 ID 발견. DB 조회 시작")
            deleted_items = fetch_items_from_db(deleted_uuids)
            
            pipe = redis_client.pipeline()
            for item_data in deleted_items:
                uuid = item_data['item_id']
                print(f"[DB-DEL] {item_data.get('item_display_name')} ({item_data.get('price'):,}G) | ID:{uuid}")
                pending_val = {
                    'data': json.dumps(item_data, default=str), 
                    'deleted_at': time.time()
                }
                pipe.hset(KEY_VERIFY_PENDING, uuid, json.dumps(pending_val))
            
            pipe.execute()
            print(f"   -> {len(deleted_items)}개 매물 Pending 이동 완료.")
        else:
            print("   -> 사라진 매물이 없습니다.")
        
        redis_client.delete(KEY_SNAPSHOT_MASTER)
        redis_client.rename(KEY_SNAPSHOT_WORKING, KEY_SNAPSHOT_MASTER)
        final_listing_count = redis_client.scard(KEY_SNAPSHOT_MASTER)
        print(f"Listings Cycle Complete. 현재 총 매물: {final_listing_count}개\n")
        return

    process_listing(data)

def process_listing(data):
    item_id = data.get('item_id')
    raw_item = data.get('raw')
    if not raw_item: return
    
    raw_item['item_id'] = item_id
    pipe = redis_client.pipeline()

    if not redis_client.sismember(KEY_SNAPSHOT_MASTER, item_id):
        start_kst, expire_kst, duration = calculate_start_time(raw_item.get('date_auction_expire'))
        raw_item['start_time'] = start_kst
        raw_item['auction_duration'] = duration
        raw_item['date_auction_expire'] = expire_kst
        emit_event('REGISTER', raw_item)

    pipe.sadd(KEY_SNAPSHOT_WORKING, item_id)    
    pipe.execute()


# 판매(History) 처리

def process_history_batch(history_items):
    """
    history_items: [{'raw': item, 'ts': timestamp, 'sig': signature}, ...]
    """
    if not history_items:
        return set()

    all_pending = redis_client.hgetall(KEY_VERIFY_PENDING)
    if not all_pending:
        return set()

    # Lookup Table 생성
    pending_map = {}
    for uuid, val_str in all_pending.items():
        try:
            db_data = json.loads(json.loads(val_str)['data'])
            sig = get_db_item_signature(db_data)
            pending_map[sig] = (uuid, db_data)
        except:
            continue

    pipeline = redis_client.pipeline()
    matched_uuids = set()
    matched_signatures = set()
    sold_count = 0

    # 매칭 시작
    for history_item_wrapper in history_items:
        history_raw = history_item_wrapper['raw'] 
        h_sig = history_item_wrapper['sig']

        # 표시용 정보
        h_name = history_raw.get('item_display_name')
        h_price = history_raw.get('auction_price_per_unit')    
        
        if h_sig in pending_map:
            uuid, db_data = pending_map[h_sig]
            
            if uuid not in matched_uuids:
                matched_uuids.add(uuid)
                matched_signatures.add(h_sig)
                
                pipeline.hdel(KEY_VERIFY_PENDING, uuid)

                raw_sold_time = history_raw.get('date_auction_buy')
                sold_time_kst = convert_utc_to_kst(raw_sold_time)
                
                emit_event(
                    'SOLD', 
                    db_data,
                    final_price=history_raw.get('auction_price_per_unit'),
                    sold_time=sold_time_kst
                )
                sold_count += 1
                
                del pending_map[h_sig]
                
                print(f"판매 확인: {db_data.get('item_display_name')} - {history_raw.get('auction_price_per_unit')} Gold")
        else:
            print(f"[MISS..] {h_name} ({h_price:,}G) | Sig:{h_sig}")

    if sold_count > 0:
        pipeline.execute()
        print(f"총 {sold_count}건 판매 확인 완료")

    return matched_signatures


# 타임아웃 관리

def check_timeouts():
    all_pending = redis_client.hgetall(KEY_VERIFY_PENDING)
    now = time.time()
    count = 0
    pipeline = redis_client.pipeline()
    
    for uuid, val_str in all_pending.items():
        try:
            pending_obj = json.loads(val_str)
            deleted_at = pending_obj.get('deleted_at', 0)
            
            if now - deleted_at > TIMEOUT_SECONDS:
                pipeline.hdel(KEY_VERIFY_PENDING, uuid)
                db_data = json.loads(pending_obj['data'])
                emit_event('CANCEL', db_data)
                count += 1
        except:
            continue
            
    if count > 0:
        pipeline.execute()
        print(f"[타임아웃] {count}개 아이템 CANCEL 처리 완료")


# 실행 루프

last_timeout_check = time.time()
last_history_process = time.time()
history_buffer = []

HISTORY_PROCESS_INTERVAL = 1.0

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=100)
        
        # [수정] NoneType Error 방어 코드
        if msg_pack is None:
            continue

        for tp, messages in msg_pack.items():
            # [수정] NoneType Error 방어 코드
            if messages is None:
                continue

            topic_name = tp.topic
            
            for message in messages:
                data = message.value
                
                if topic_name == TOPIC_RAW_LISTINGS:
                    process_listing_message(data)

                elif topic_name == TOPIC_RAW_HISTORY:

                    raw_item = data.get('raw')
                    
                    if raw_item and data.get('type') != 'END_OF_CYCLE':
                        sig = data.get('item_id') or generate_match_signature(raw_item)
                        
                        sold_time = raw_item.get('date_auction_buy')
                        dedup_key = f"{KEY_PREFIX_HISTORY_SEEN}{sig}:{sold_time}"
                        
                        if redis_client.set(dedup_key, "1", ex=3600, nx=True):
                            history_buffer.append({
                                'raw': raw_item,
                                'ts': time.time(), 
                                'sig': sig 
                            })

        # History 배치 처리 ---
        current_time = time.time()
        if current_time - last_history_process >= HISTORY_PROCESS_INTERVAL:
            if history_buffer:
                matched_sigs = process_history_batch(history_buffer)
                
                new_buffer = []
                expire_count = 0
                
                for wrapper in history_buffer:
                    if wrapper['sig'] in matched_sigs:
                        continue
                        
                    if current_time - wrapper['ts'] > HISTORY_BUFFER_TTL:
                        expire_count += 1
                        continue
                        
                    new_buffer.append(wrapper)
                
                if expire_count > 0:
                    print(f"{expire_count}개 History 아이템 만료되어 버퍼 제거")
                    
                history_buffer = new_buffer
            
            last_history_process = current_time

        if current_time - last_timeout_check > 10:
            check_timeouts()
            last_timeout_check = current_time

except KeyboardInterrupt:
    print("프로세서 종료 요청됨")

except Exception as e:
    print(f"치명적인 오류 발생: {e}")
    import traceback
    traceback.print_exc() # 에러 위치 상세 확인
    
finally:
    consumer.close()
    producer.close()
    redis_client.close()
    print("프로그램 종료")