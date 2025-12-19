import json
import time
from datetime import datetime, timezone, timedelta
import psycopg2 
from kafka import KafkaConsumer, KafkaProducer
from window_buffer import WindowBuffer
import logging
import sys

# 설정
KAFKA_BROKER = 'localhost:9092'
TOPIC_EVENTS = 'auction.events'
TOPIC_ANOMALY = 'auction.anomaly'

# DB 접속 정보
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword',
    'port': 5432
}

DETECTOR_CONFIG = {
    'SWEEPING_RATIO': 0.0001,      # 매점 판단 비율
    'MANIPULATION_RATIO': 0.01,    # 매석 판단 비율
    'HIGH_PRICE_MULTIPLIER': 1.2,  # 고가 판단 기준
    'ALERT_COOLDOWN_SEC': 10       # 알림 재발송 쿨타임
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AnomalyDetector")

# KST 타임존
KST = timezone(timedelta(hours=9))


class DBHandler:
    """
    DB 연결을 유지하고 관리하는 핸들러 클래스
    """
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
            
        try:
            self.conn = psycopg2.connect(**self.config)
            self.conn.autocommit = False 
            logger.info("[DB] Connected successfully.")
        except Exception as e:
            logger.error(f"[DB] Connection failed: {e}")
            self.conn = None

    def check_connection(self):
        """연결 상태 확인 및 재연결"""
        if self.conn is None:
            self.connect()
            return
            
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("[DB] Connection lost. Reconnecting...")
            self.connect()

    def fetch_market_count(self, item_name):
        """초기 매물 수량 조회"""
        self.check_connection()
        if not self.conn: return 0

        count = 0
        try:
            with self.conn.cursor() as cur:
                sql = """
                    SELECT COALESCE(SUM(item_count), 0) 
                    FROM mabi_auction.current_listings 
                    WHERE item_name = %s OR item_display_name = %s
                """
                cur.execute(sql, (item_name, item_name))
                result = cur.fetchone()
                if result:
                    count = int(result[0])
            self.conn.commit()
        except Exception as e:
            logger.error(f"[DB Error] Fetch count failed: {e}", exc_info=True)
            self.conn.rollback()
        
        return count

    def save_log(self, log_data):
        """이상 탐지 로그 저장"""
        self.check_connection()
        if not self.conn: return

        try:
            with self.conn.cursor() as cur:
                sql = """
                    INSERT INTO mabi_auction.anomaly_logs 
                    (detection_time, item_name, anomaly_type, description, id)
                    VALUES (%s, %s, %s, %s, %s)
                """
                description_json = json.dumps(log_data.get('details', {}), ensure_ascii=False)
                
                cur.execute(sql, (
                    log_data['timestamp'],
                    log_data['item_name'],
                    log_data['type'],
                    description_json,
                    log_data['unique_id']
                ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"[DB Error] Save log failed: {e}", exc_info=True)
            self.conn.rollback()
    
    def close(self):
        if self.conn:
            self.conn.close()


# 초기화
wb = WindowBuffer()
db_handler = DBHandler(DB_CONFIG) # DB 핸들러 생성
initialized_items = set()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    TOPIC_EVENTS,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='anomaly-detection-group'
)

logger.info(f"Anomaly Detector Started... Listening on {TOPIC_EVENTS}")

try:
    # 실행 루프
    for message in consumer:
        event = message.value
        event_type = event.get('event_type')

        event_time_str = event.get("event_time")
        if not event_time_str:
            continue

        event_ts = datetime.fromisoformat(event_time_str).timestamp()
        now_ts = time.time()

        payload = event.get('payload', {})
        
        # 셧다운 복구 기간 매물은 탐지 제외
        if payload.get('skip_anomaly_detection') is True:
            continue
        
        # 1. 아이템 이름 추출
        item_data = payload.get('listing_data') or payload.get('history_data') or {}
        item_name = item_data.get('item_name') or item_data.get('item_display_name')

        if not item_name:
            continue

        item_price = float(item_data.get('auction_price_per_unit', 0))

        # 2. 고유 ID 추출
        unique_id = None
        if event_type == 'REGISTER':
            unique_id = event.get('item_id')
        elif event_type == 'SOLD':
            unique_id = payload.get('auction_buy_id')
            if not unique_id:
                unique_id = event.get('match_id')

        # 3. 현재 매물 수 동기화 (최초 1회 DB 조회)
        if item_name not in initialized_items:
            current_wb_count = wb.get_current_total_count(item_name)
            
            if current_wb_count == 0:
                # 핸들러를 통해 DB 조회
                db_count = db_handler.fetch_market_count(item_name)
                if db_count > 0:
                    logger.info(f"[Init] '{item_name}' initialized from DB: {db_count} items")
                    wb.update_market_count(item_name, db_count)
            
            initialized_items.add(item_name)

        # 4. 실시간 매물 수 업데이트
        if event_type == 'REGISTER':
            wb.update_market_count(item_name, 1)
        elif event_type in ['SOLD', 'EXPIRE', 'CANCEL']:
            wb.update_market_count(item_name, -1)

        if not unique_id:
            continue

        # 탐지 로직

        # [매점 감지] Trigger: SOLD
        if event_type == 'SOLD':
            sold_velocity, avg_sold_price = wb.add_sold_event_atomic(item_name, unique_id, item_price, event_ts)
            total_market_count = wb.get_current_total_count(item_name)
            
            if total_market_count > 0:
                sweeping_ratio = sold_velocity / total_market_count
                
                logger.debug(f"CHECK: {item_name} | Sold: {sold_velocity} | Market: {total_market_count} -> Ratio: {sweeping_ratio:.2%}")

                if sweeping_ratio >= DETECTOR_CONFIG['SWEEPING_RATIO']:
                    if wb.check_and_set_cooldown(item_name, "CAUTION", DETECTOR_CONFIG['ALERT_COOLDOWN_SEC']):
                        
                        logger.warning(f"[CAUTION] {item_name}: Sweeping detected! (Ratio: {sweeping_ratio:.2%})")
                        wb.add_alert_caution(item_name)

                        timestamp_now = datetime.now(KST).isoformat()

                        alert_payload = {
                            "type": "CAUTION",
                            "reason": "SWEEPING",
                            "item_name": item_name,
                            "ratio": round(sweeping_ratio, 4),
                            "velocity_10min": sold_velocity,
                            "current_avg_price": avg_sold_price,
                            "total_market": total_market_count,
                            "timestamp": timestamp_now,
                            "event_time": event_time_str
                        }
                        producer.send(TOPIC_ANOMALY, alert_payload)

                        description_msg = f"[매점감지] 판매율:{sweeping_ratio:.1%} | 10분판매:{sold_velocity}개 | 재고:{total_market_count}개 | 평균가:{avg_sold_price:,.0f}"

                        # 핸들러를 통해 로그 저장
                        db_handler.save_log({
                            "timestamp": timestamp_now,
                            "event_time": event_time_str,
                            "item_name": item_name,
                            "type": "CAUTION",
                            "unique_id": unique_id,
                            "details": description_msg
                        })

        # [매석 감지] Trigger: REGISTER
        elif event_type == 'REGISTER':
            wb.buffer_register(item_name, unique_id, event_ts)
        
        ready_regs = wb.pop_ready_registers(item_name, now_ts)

        for reg_id in ready_regs:
            if wb.is_in_caution(item_name):
                current_ref_price = wb.get_current_avg_price(item_name, event_ts)
                
                if current_ref_price > 0:
                    threshold = current_ref_price * DETECTOR_CONFIG['HIGH_PRICE_MULTIPLIER']
                    status = "HIGH" if item_price >= threshold else "NORM"
                    
                    reg_total, reg_high = wb.add_register_event_atomic(item_name, unique_id, status, event_ts)
                    
                    if reg_total > 0:
                        high_ratio = reg_high / reg_total
                        
                        if high_ratio >= DETECTOR_CONFIG['MANIPULATION_RATIO']:
                            if wb.check_and_set_cooldown(item_name, "WARNING", DETECTOR_CONFIG['ALERT_COOLDOWN_SEC']):
                                
                                logger.warning(f"[WARNING] {item_name}: Manipulation detected! (High Ratio: {high_ratio:.2%})")
                                wb.add_alert_warning(item_name)
                                
                                timestamp_now = datetime.now(KST).isoformat()
                                
                                alert_payload = {
                                    "type": "WARNING",
                                    "reason": "MANIPULATION",
                                    "item_name": item_name,
                                    "ratio": round(high_ratio, 4),
                                    "ref_price_avg": current_ref_price,
                                    "reg_price": item_price,
                                    "timestamp": timestamp_now,
                                    "event_time": event_time_str
                                }
                                producer.send(TOPIC_ANOMALY, alert_payload)

                                description_msg = f"[시세조작] 고가비율:{high_ratio:.1%} | 등록가:{item_price:,.0f} | 평균가:{current_ref_price:,.0f} | 기준가:{threshold:,.0f}"

                                # 핸들러를 통해 로그 저장
                                db_handler.save_log({
                                    "timestamp": timestamp_now,
                                    "event_time": event_time_str,
                                    "item_name": item_name,
                                    "type": "WARNING",
                                    "unique_id": unique_id,
                                    "details": description_msg
                                })

except Exception as e:
    logger.critical(f"[Detector Error] {e}", exc_info=True)
finally:
    db_handler.close()
    consumer.close()
    producer.close()
    logger.info("Anomaly Detector Stopped.")