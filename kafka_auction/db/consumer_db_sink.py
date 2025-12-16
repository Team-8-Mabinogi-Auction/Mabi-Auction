import json
import time
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch
from datetime import datetime

# 설정
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_EVENTS = 'auction.events'
GROUP_ID = 'db-sink-mabi-auction-test2'

# DB 접속 정보
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword',
    'port': 5432
}

# 배치 설정
BATCH_SIZE = 500
FLUSH_INTERVAL = 1.0

class AuctionDbSink:
    def __init__(self):
        self.conn = None
        self.connect_db()
        
        # 버퍼 초기화
        self.buf_listings_ins = []  # current_listings INSERT
        self.buf_options_ins = []   # item_options INSERT
        self.buf_logs_ins = []      # auction_logs INSERT
        self.buf_listings_del = []  # current_listings & options DELETE
        
        self.last_flush_time = time.time()

    def connect_db(self):
        """DB 재접속 로직"""
        try:
            if self.conn:
                self.conn.close()
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False
            print("DB 연결 성공")
        except Exception as e:
            print(f"DB 연결 실패: {e}")
            time.sleep(5)
            self.connect_db()

    def parse_time(self, time_str):
        """ISO 포맷 문자열을 datetime 객체로 변환 (DB 적재용)"""
        if not time_str:
            return None
        try:
            return datetime.fromisoformat(str(time_str))
        except:
            return datetime.now()

    def add_event(self, event):
        event_type = event.get('event_type')
        item_id = event.get('item_id')
        raw = event.get('raw_data', {})
        
        # 공통 데이터 추출
        processed_at = self.parse_time(event.get('processed_at'))
        
        # Processor에서 계산되어 넘어온 값들
        registered_at = self.parse_time(raw.get('start_time'))
        duration = raw.get('auction_duration', 36) 
        plan_expire_time = self.parse_time(raw.get('date_auction_expire'))

        # 가격 결정 (판매시엔 판매가, 그외엔 등록가)
        price = event.get('sold_price') if event_type == 'SOLD' else raw.get('auction_price_per_unit', 0)

        ended_at = None
        
        if event_type in ('REGISTER', 'CANCEL'):
            ended_at = plan_expire_time
            
        elif event_type == 'SOLD':
            ended_at = self.parse_time(event.get('sold_time'))
            
        log_row = (
            event_type,
            item_id,
            raw.get('item_name'),
            event.get('item_display_name'),
            raw.get('item_count', 1),
            raw.get('item_category'),
            price,
            registered_at,
            duration,
            ended_at,
            processed_at 
        )
        self.buf_logs_ins.append(log_row)

        # 상태별 로직
        if event_type == 'REGISTER':
            # Current Listings 추가
            listing_row = (
                item_id,
                raw.get('item_name'),
                event.get('item_display_name'),
                raw.get('item_count', 1),
                raw.get('item_category'),
                price,
                registered_at, 
                plan_expire_time,
                duration,
                processed_at
            )
            self.buf_listings_ins.append(listing_row)

            # Item Options 추가
            options = raw.get('options', [])
            if isinstance(options, list):
                for opt in options:
                    opt_row = (
                        item_id,
                        opt.get('option_type'),
                        opt.get('option_sub_type'),
                        opt.get('option_value'),
                        opt.get('option_value2'),
                        opt.get('option_desc')
                    )
                    self.buf_options_ins.append(opt_row)

        elif event_type in ['SOLD', 'CANCEL']:
            self.buf_listings_del.append((item_id,))

    def flush(self):
        """버퍼의 내용을 DB에 한 번에 반영"""
        total_ops = len(self.buf_listings_ins) + len(self.buf_listings_del) + len(self.buf_logs_ins)
        if total_ops == 0:
            return

        try:
            with self.conn.cursor() as cur:
                # auction_logs INSERT
                if self.buf_logs_ins:
                    sql_log = """
                        INSERT INTO mabi_auction.auction_logs
                        (event_type, item_id, item_name, item_display_name, item_count, item_category, price, registered_at, auction_duration, ended_at, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (item_id, event_type) DO NOTHING;
                    """
                    execute_batch(cur, sql_log, self.buf_logs_ins)

                # current_listings & item_options DELETE
                if self.buf_listings_del:
                    # item_options DELETE
                    sql_del_opt = "DELETE FROM mabi_auction.item_options WHERE item_id = %s"
                    execute_batch(cur, sql_del_opt, self.buf_listings_del)

                    # current_listings DELETE
                    sql_del_list = "DELETE FROM mabi_auction.current_listings WHERE item_id = %s"
                    execute_batch(cur, sql_del_list, self.buf_listings_del)

                # current_listings INSERT
                if self.buf_listings_ins:
                    sql_list = """
                        INSERT INTO mabi_auction.current_listings
                        (item_id, item_name, item_display_name, item_count, item_category, price, start_time, expire_time, auction_duration, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (item_id) DO NOTHING
                    """
                    execute_batch(cur, sql_list, self.buf_listings_ins)

                # item_options INSERT
                if self.buf_options_ins:
                    sql_opt = """
                        INSERT INTO mabi_auction.item_options
                        (item_id, option_type, option_sub_type, option_value, option_value2, option_desc)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """
                    execute_batch(cur, sql_opt, self.buf_options_ins)

            self.conn.commit()
            print(f"DB 저장: Log({len(self.buf_logs_ins)}), Add({len(self.buf_listings_ins)}), Del({len(self.buf_listings_del)})")

        except Exception as e:
            print(f"DB 트랜잭션 실패: {e}")
            self.conn.rollback()
        finally:
            # 버퍼 비우기
            self.buf_listings_ins.clear()
            self.buf_options_ins.clear()
            self.buf_logs_ins.clear()
            self.buf_listings_del.clear()
            self.last_flush_time = time.time()

# 실행 로직

consumer = KafkaConsumer(
    TOPIC_EVENTS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest' 
)

sink = AuctionDbSink()

print(f"Mabi Auction DB Sink 시작 (Topic: {TOPIC_EVENTS})")

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=500)
        
        for tp, messages in msg_pack.items():
            for message in messages:
                sink.add_event(message.value)
        
        curr_time = time.time()
        is_full = (len(sink.buf_logs_ins) >= BATCH_SIZE)
        is_timeout = (curr_time - sink.last_flush_time >= FLUSH_INTERVAL)

        if (is_full or is_timeout):
            sink.flush()

except KeyboardInterrupt:
    print("\n종료 요청. 남은 데이터 처리 중...")
    sink.flush()
    consumer.close()
    if sink.conn:
        sink.conn.close()
    print("종료 완료.")