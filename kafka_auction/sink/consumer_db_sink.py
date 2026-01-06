import json
import time
import psycopg2
import logging
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch, RealDictCursor, Json
from datetime import datetime, timezone, timedelta
import os
from kafka_auction.processor.utils import require_env
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AuctionDbSink")

current_file = Path(__file__).resolve() 
env_path = current_file.parent.parent / 'config' / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    logger.error(f"Cannot find .env file at {env_path}")

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
        self.cleanup_expired_items()
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

    def cleanup_expired_items(self):
        try:
            logger.info("Checking for expired listings...")
            now = datetime.now(KST)
            
            with self.conn.cursor() as cur:
                # expire_time이 현재 시간보다 작은(과거인) 행 삭제
                sql = "DELETE FROM mabi_raw.current_listings WHERE expire_time < %s"
                cur.execute(sql, (now,))
                deleted_count = cur.rowcount
            
            self.conn.commit()
            if deleted_count > 0:
                logger.info(f"Startup Cleanup: Deleted {deleted_count} expired items from DB.")
            else:
                logger.info("Startup Cleanup: No expired items found.")
                
        except Exception as e:
            logger.error(f"Failed to cleanup expired items: {e}", exc_info=True)
            self.conn.rollback()

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

        lookup_ids = set()
        
        # [1] KeyError 방지를 위해 .get() 사용
        for evt in self.event_buffer:
            e_type = evt.get('event_type')
            if e_type in ('SOLD', 'CANCEL'):
                target_id = evt.get('item_id') or evt.get('match_id')
                if target_id:
                    lookup_ids.add(target_id)
        
        db_items_map = self._fetch_existing_items(lookup_ids)

        batch_logs = []       
        batch_ins_list = []   
        batch_del_ids = []    

        for evt in self.event_buffer:
            # [2] event_type 없는 불량 메시지 스킵
            e_type = evt.get('event_type')
            if not e_type: 
                continue 

            item_id = evt.get('item_id') or evt.get('match_id')
            processed_at = self.parse_time(evt.get('processed_at'))
            
            # Payload 파싱
            raw = evt.get('payload', {})
            
            # [Null Safety] 
            item_name = raw.get('item_name') or 'Unknown'
            disp_name = raw.get('item_display_name') or 'Unknown'
            category = raw.get('auction_item_category') or 'Unknown' 
            
            item_count = raw.get('item_count') or 1
            price = raw.get('auction_price_per_unit') or 0
            
            start_time = self.parse_time(raw.get('start_time'))
            expire_time = self.parse_time(raw.get('date_auction_expire'))
            duration = raw.get('auction_duration')
            
            if not duration and start_time and expire_time:
                duration = int((expire_time - start_time).total_seconds() / 3600)

            options_json = raw.get('item_option') or []
            
            buy_id = raw.get('auction_buy_id')
            sold_time = self.parse_time(raw.get('date_auction_buy')) or processed_at

            if e_type == 'REGISTER':
                batch_ins_list.append((
                    item_id, item_name, disp_name, item_count, category,
                    price, start_time, expire_time, duration, processed_at,
                    Json(options_json)
                ))
                
                batch_logs.append((
                    'REGISTER', item_id, None, item_name, disp_name, item_count, category,
                    price, start_time, duration, expire_time, processed_at,
                    Json(options_json)
                ))

            elif e_type == 'SOLD':
                db_item = db_items_map.get(item_id)
                
                if db_item:
                    # DB 정보 우선 (Ghost SOLD 대비)
                    item_name = db_item.get('item_name')
                    disp_name = db_item.get('item_display_name')
                    category = db_item.get('item_category')
                    start_time = db_item.get('start_time')
                    duration = db_item.get('auction_duration')
                    options_json = db_item.get('options', [])
                    batch_del_ids.append((item_id,))
                
                batch_logs.append((
                    'SOLD', item_id, buy_id, item_name, disp_name, item_count, category,
                    price, start_time, duration, sold_time, processed_at,
                    Json(options_json)
                ))

            elif e_type == 'CANCEL':
                db_item = db_items_map.get(item_id)
                if db_item:
                    # [3] item_category 필드 포함 (총 13개 확인)
                    batch_logs.append((
                        'CANCEL',                           # 1
                        item_id,                            # 2
                        None,                               # 3
                        db_item.get('item_name'),           # 4
                        db_item.get('item_display_name'),   # 5
                        item_count,                         # 6
                        db_item.get('item_category'),       # 7
                        db_item.get('price'),               # 8
                        db_item.get('start_time'),          # 9
                        db_item.get('auction_duration'),    # 10
                        db_item.get('expire_time'),         # 11
                        processed_at,                       # 12
                        Json(db_item.get('options', []))    # 13
                    ))
                    batch_del_ids.append((item_id,))

        try:
            with self.conn.cursor() as cur:
                if batch_logs:
                    # SQL Placeholder 13개
                    sql_log = """
                        INSERT INTO mabi_raw.auction_logs
                        (event_type, item_id, auction_buy_id, item_name, item_display_name, item_count, item_category, price, registered_at, auction_duration, ended_at, created_at, options)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """
                    execute_batch(cur, sql_log, batch_logs)

                if batch_del_ids:
                    sql_del = "DELETE FROM mabi_raw.current_listings WHERE item_id = %s"
                    execute_batch(cur, sql_del, batch_del_ids)

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