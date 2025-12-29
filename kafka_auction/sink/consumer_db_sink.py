import json
import time
import psycopg2
import logging
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch, RealDictCursor, Json # [수정] Json 추가
from datetime import datetime, timezone, timedelta
import os
from kafka_auction.processor.utils import require_env
from pathlib import Path
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AuctionDbSink")

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
TOPIC_EVENTS = require_env("KAFKA_TOPIC_EVENTS")
GROUP_ID = require_env("KAFKA_DB_SINK_GROUP_ID")

DB_CONFIG = {
    'host': require_env("DB_HOST"),
    'dbname': require_env("DB_NAME"),
    'user': require_env("DB_USER"),
    'password': require_env("DB_PASSWORD"),
    'port': int(require_env("DB_PORT")),
}

BATCH_SIZE = int(os.getenv("DB_SINK_BATCH_SIZE", 500))
FLUSH_INTERVAL = float(os.getenv("DB_SINK_FLUSH_INTERVAL", 1.0))
KST = timezone(timedelta(hours=9))

class AuctionDbSink:
    def __init__(self):
        self.conn = None
        self.connect_db()
        self.event_buffer = [] 
        self.last_flush_time = time.time()

    def connect_db(self):
        try:
            if self.conn: self.conn.close()
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False
            logger.info("DB 연결 성공")
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}", exc_info=True) 
            time.sleep(5)
            self.connect_db()

    def parse_time(self, time_str):
        if not time_str: return None
        try:
            dt = datetime.fromisoformat(str(time_str))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt
        except:
            return None

    def add_event(self, event):
        self.event_buffer.append(event)

    def _fetch_existing_items(self, item_ids):
        """삭제/판매 처리를 위해 기존 매물 정보를 DB에서 일괄 조회"""
        if not item_ids: return {}
        
        items_map = {}
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                sql = """
                    SELECT * FROM mabi_raw.current_listings 
                    WHERE item_id = ANY(%s)
                """
                cur.execute(sql, (list(item_ids),))
                rows = cur.fetchall()
                for row in rows:
                    items_map[row['item_id']] = row
        except Exception as e:
            logger.error(f"[Fetch Error] {e}", exc_info=True)
            self.conn.rollback()
        return items_map

    def flush(self):
        if not self.event_buffer: return

        # 1. DB 조회 대상 식별 (SOLD, CANCEL)
        lookup_ids = set()
        for evt in self.event_buffer:
            if evt['event_type'] in ('SOLD', 'CANCEL'):
                target_id = evt.get('item_id') or evt.get('match_id')
                if target_id:
                    lookup_ids.add(target_id)
        
        # 2. DB 일괄 조회
        db_items_map = self._fetch_existing_items(lookup_ids)

        # 3. 배치 데이터 준비
        batch_logs = []       
        batch_ins_list = []   
        # batch_ins_opt = [] # [수정] 옵션 테이블 사용 안 함
        batch_del_ids = []    

        for evt in self.event_buffer:
            e_type = evt.get('event_type')
            item_id = evt.get('item_id') or evt.get('match_id')
            payload = evt.get('payload', {})
            processed_at = self.parse_time(evt.get('processed_at'))

            # REGISTER (등록)
            if e_type == 'REGISTER':
                raw = payload.get('listing_data', {})
                if not raw: continue

                start_time = self.parse_time(raw.get('start_time'))
                expire_time = self.parse_time(raw.get('date_auction_expire'))
                duration = raw.get('auction_duration')
                price = raw.get('auction_price_per_unit', 0)
                
                # [수정] 옵션 데이터를 JSONB로 저장
                options_json = raw.get('item_option', [])

                # Current Listings Insert (options 컬럼 추가)
                batch_ins_list.append((
                    item_id,
                    raw.get('item_name'),
                    raw.get('item_display_name'),
                    raw.get('item_count', 1),
                    raw.get('auction_item_category'),
                    price,
                    start_time,
                    expire_time,
                    duration,
                    processed_at,
                    Json(options_json) # [추가] JSONB
                ))

                # Auction Log Insert (options 컬럼 추가)
                batch_logs.append((
                    'REGISTER',
                    item_id,
                    None, # auction_buy_id
                    raw.get('item_name'),
                    raw.get('item_display_name'),
                    raw.get('item_count', 1),
                    raw.get('auction_item_category'),
                    price,
                    start_time,  # registered_at
                    duration,
                    expire_time, # ended_at (만료예정시각)
                    processed_at,
                    Json(options_json) # [추가] JSONB
                ))

            # SOLD (판매)
            elif e_type == 'SOLD':
                history = payload.get('history_data', {})
                db_item = db_items_map.get(item_id)
                
                # DB 데이터가 있으면 우선 사용
                target_data = db_item if db_item else history
                
                name = target_data.get('item_name')
                disp_name = target_data.get('item_display_name')
                count = target_data.get('item_count', 1)
                category = target_data.get('item_category') if db_item else history.get('auction_item_category')
                
                # DB에서 등록 정보(start_time, duration) 가져오기
                # Ghost 매물일 경우(db_item 없음) None
                registered_at = db_item.get('start_time') if db_item else None
                duration = db_item.get('auction_duration') if db_item else None
                
                buy_id = history.get('auction_buy_id')
                sold_price = history.get('auction_price_per_unit')
                sold_time = self.parse_time(history.get('date_auction_buy'))
                
                # 판매 시점의 옵션 정보 저장
                current_opts = target_data.get('options') if db_item else target_data.get('item_option', [])
                
                # Log Insert
                batch_logs.append((
                    'SOLD',
                    item_id,
                    buy_id,
                    name,
                    disp_name,
                    count,
                    category,
                    sold_price,
                    registered_at,
                    duration,
                    sold_time,      # ended_at (판매된 시간)
                    processed_at,
                    Json(current_opts) # [추가] JSONB
                ))

                if db_item:
                    batch_del_ids.append((item_id,))

            # CANCEL (취소)
            elif e_type == 'CANCEL':
                db_item = db_items_map.get(item_id)
                
                if db_item:
                    batch_logs.append((
                        'CANCEL',
                        item_id,
                        None,
                        db_item.get('item_name'),
                        db_item.get('item_display_name'),
                        db_item.get('item_count'),
                        db_item.get('item_category'),
                        db_item.get('price'),
                        db_item.get('start_time'),        # registered_at
                        db_item.get('auction_duration'),  # duration
                        db_item.get('expire_time'),  # ended_at (만료 시각)
                        processed_at,
                        Json(db_item.get('options', [])) # [추가] JSONB
                    ))
                    batch_del_ids.append((item_id,))

        # 4. DB 반영
        try:
            with self.conn.cursor() as cur:
                # (1) Log Insert - [수정] options 컬럼 추가
                if batch_logs:
                    sql_log = """
                        INSERT INTO mabi_raw.auction_logs
                        (event_type, item_id, auction_buy_id, item_name, item_display_name, item_count, item_category, price, registered_at, auction_duration, ended_at, created_at, options)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """
                    execute_batch(cur, sql_log, batch_logs)

                # (2) Delete Listings
                if batch_del_ids:
                    sql_del = "DELETE FROM mabi_raw.current_listings WHERE item_id = %s"
                    execute_batch(cur, sql_del, batch_del_ids)

                # (3) Insert Listings - [수정] options 컬럼 추가
                if batch_ins_list:
                    sql_ins = """
                        INSERT INTO mabi_raw.current_listings
                        (item_id, item_name, item_display_name, item_count, item_category, price, start_time, expire_time, auction_duration, created_at, options)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (item_id) DO NOTHING
                    """
                    execute_batch(cur, sql_ins, batch_ins_list)

            self.conn.commit()
            logger.info(f"Batch Processed: Log({len(batch_logs)}), Ins({len(batch_ins_list)}), Del({len(batch_del_ids)})")

        except Exception as e:
            logger.error(f"[Transaction Error] {e}", exc_info=True)
            self.conn.rollback()
        finally:
            self.event_buffer.clear()
            self.last_flush_time = time.time()

# 실행
consumer = KafkaConsumer(
    TOPIC_EVENTS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest' 
)

sink = AuctionDbSink()
logger.info(f"DB Sink Started for {TOPIC_EVENTS}")

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=500)
        
        for tp, messages in msg_pack.items():
            for message in messages:
                sink.add_event(message.value)
        
        curr_time = time.time()
        is_full = (len(sink.event_buffer) >= BATCH_SIZE)
        is_timeout = (curr_time - sink.last_flush_time >= FLUSH_INTERVAL)

        if sink.event_buffer and (is_full or is_timeout):
            sink.flush()

except KeyboardInterrupt:
    logger.info("Stopping...")
    sink.flush()
    consumer.close()
    if sink.conn: sink.conn.close()