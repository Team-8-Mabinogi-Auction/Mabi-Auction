import json
import time
from datetime import datetime, timezone, timedelta
import psycopg2 
from kafka import KafkaConsumer, KafkaProducer
from kafka_auction.processor.window_buffer import WindowBuffer
import logging
import sys
import os
from kafka_auction.processor.utils import require_env
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AnomalyDetector")

current_file = Path(__file__).resolve() 
env_path = current_file.parent.parent / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# --- 설정 ---
KAFKA_BROKER = require_env("KAFKA_BROKER")
TOPIC_EVENTS = require_env("KAFKA_TOPIC_EVENTS")
TOPIC_ANOMALY = require_env("KAFKA_TOPIC_ANOMALY")
GROUP_ID = 'anomaly-detection-group'

KAFKA_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER,
    'group_id': GROUP_ID,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': 'latest',
    'max_poll_records': 500,        # 배치 처리를 위해 넉넉히 설정
    'max_poll_interval_ms': 600000, 
    'session_timeout_ms': 45000,   
    'heartbeat_interval_ms': 15000, 
}

DB_CONFIG = {
    "host": require_env("DB_HOST"),
    "dbname": require_env("DB_NAME"),
    "user": require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
    "port": int(require_env("DB_PORT")),
}

DETECTOR_CONFIG = {
    "SWEEPING_RATIO": float(os.getenv("SWEEPING_RATIO", 0.1)),
    "MANIPULATION_RATIO": float(os.getenv("MANIPULATION_RATIO", 0.1)),
    "HIGH_PRICE_MULTIPLIER": float(os.getenv("HIGH_PRICE_MULTIPLIER", 1.2)),
    "ALERT_COOLDOWN_SEC": int(os.getenv("ALERT_COOLDOWN_SEC", 60)),
    # MIN_MARKET_SIZE 제거됨 (사용자 요청)
}

KST = timezone(timedelta(hours=9))
WARMUP_DURATION = int(os.getenv("WARMUP_DURATION", 600))

# 이벤트 처리 우선순위 (낮을수록 먼저 실행)
# REGISTER가 가장 먼저 실행되어야 재고 부족으로 인한 오탐지를 막을 수 있음
EVENT_PRIORITY = {
    'REGISTER': 0,
    'SOLD': 1,
    'CANCEL': 2,
    'EXPIRE': 2
}

class DBHandler:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn: self.conn.close()
        except: pass
        try:
            self.conn = psycopg2.connect(**self.config)
            self.conn.autocommit = False 
            logger.info("[DB] Connected successfully.")
        except Exception as e:
            logger.error(f"[DB] Connection failed: {e}")
            self.conn = None

    def check_connection(self):
        if self.conn is None:
            self.connect()
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("[DB] Connection lost. Reconnecting...")
            self.connect()

    def save_log(self, log_data):
        self.check_connection()
        if not self.conn: return
        try:
            with self.conn.cursor() as cur:
                sql = """
                    INSERT INTO mabi_raw.anomaly_logs 
                    (detection_time, event_time, item_name, anomaly_type, description, id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                description_json = json.dumps(log_data.get('details', {}), ensure_ascii=False)
                cur.execute(sql, (
                    log_data['timestamp'],
                    log_data.get('event_time'),
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
        if self.conn: self.conn.close()


# --- Main Logic ---

wb = WindowBuffer()
db_handler = DBHandler(DB_CONFIG)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(TOPIC_EVENTS, **KAFKA_CONFIG)

logger.info(f"Anomaly Detector Started... Listening on {TOPIC_EVENTS}")
logger.info(f"WARM-UP Mode Active: {WARMUP_DURATION} seconds.")

server_start_ts = time.time()

try:
    while True:
        # [핵심 1] 배치 단위로 데이터 가져오기 (순서 재정렬을 위함)
        msg_pack = consumer.poll(timeout_ms=1000, max_records=500)
        
        if not msg_pack:
            continue

        # 모든 파티션의 메시지를 하나의 리스트로 통합
        batch_events = []
        for topic_partition, messages in msg_pack.items():
            for msg in messages:
                batch_events.append(msg.value)

        # [핵심 2] 이벤트 재정렬 (Event Reordering)
        # REGISTER(0) -> SOLD(1) -> CANCEL/EXPIRE(2) 순서로 강제 정렬
        batch_events.sort(key=lambda x: EVENT_PRIORITY.get(x.get('event_type'), 99))

        for event in batch_events:
            now_ts = time.time()
            is_warmup = (now_ts - server_start_ts) < WARMUP_DURATION

            event_type = event.get('event_type')
            item_type = event.get('item_type', 'EQUIPMENT')

            # 플래그 추출
            should_skip = event.get('skip_anomaly_detection') is True
            is_ghost = event.get('is_ghost') is True

            event_time_str = event.get("event_time") or event.get("processed_at")
            if not event_time_str: continue
            
            try:
                dt = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
                event_ts = dt.timestamp()
            except:
                event_ts = now_ts

            payload = event.get('payload', {})
            item_name = payload.get('item_name')
            item_price = float(payload.get('auction_price_per_unit') or 0)
            item_count = int(payload.get('item_count') or 1)

            unique_id = None
            if item_type == 'CONSUMABLE':
                unique_id = event.get('item_id')
            else:
                if event_type == 'REGISTER':
                    unique_id = event.get('item_id')
                else:
                    unique_id = payload.get('auction_buy_id') or event.get('match_id')

            if not item_name or not unique_id:
                continue

            # -----------------------------------------------------------
            # [재고 수량 업데이트] 
            # REGISTER가 먼저 처리되므로 재고 부족 현상 완화
            # -----------------------------------------------------------
            if event_type == 'REGISTER':
                wb.update_market_count(item_name, item_count) 
            elif event_type in ['EXPIRE', 'CANCEL']: 
                wb.update_market_count(item_name, -item_count)
            elif event_type == 'SOLD':
                if not is_ghost:
                    # Ghost가 아니면 정상적으로 차감 (과거 데이터 제외)
                    if event_ts >= server_start_ts:
                        wb.update_market_count(item_name, -item_count)

            # -----------------------------------------------------------
            # [이상 탐지 게이트]
            # -----------------------------------------------------------
            if is_ghost or should_skip or is_warmup:
                continue

            # --- 탐지 로직 (SOLD - 매점 감지) ---
            if event_type == 'SOLD':
                sold_velocity, avg_sold_price = wb.add_sold_event_atomic(
                    item_name, unique_id, item_price, event_ts, count=item_count
                )
                
                # 남은 재고 조회
                current_remaining_stock = wb.get_current_total_count(item_name)
                
                # [분모 보정] 원래 총량 = 남은 재고 + 방금 팔린 양
                # 이렇게 하면 비율이 100%를 넘지 않음 (판매율 100% 초과 해결)
                estimated_total_market = current_remaining_stock + sold_velocity
                if estimated_total_market <= 0: estimated_total_market = 1
                
                # 최소 모수 체크 로직 삭제됨 (사용자 요청)

                sweeping_ratio = sold_velocity / estimated_total_market
                
                if sweeping_ratio >= DETECTOR_CONFIG['SWEEPING_RATIO']:
                    if wb.check_and_set_cooldown(item_name, "CAUTION", DETECTOR_CONFIG['ALERT_COOLDOWN_SEC']):
                        
                        logger.warning(f"[CAUTION] {item_name}: Sweeping detected! (Ratio: {sweeping_ratio:.2%})")
                        wb.add_alert_caution(item_name)

                        timestamp_now = datetime.now(KST).isoformat()
                        description_msg = f"[매점감지] 판매율:{sweeping_ratio:.1%} | 10분판매:{sold_velocity} | 재고:{current_remaining_stock} | 평균가:{avg_sold_price:,.0f}"

                        alert_payload = {
                            "type": "CAUTION",
                            "reason": "SWEEPING",
                            "item_name": item_name,
                            "ratio": round(sweeping_ratio, 4),
                            "velocity_10min": sold_velocity,
                            "current_avg_price": avg_sold_price,
                            "total_market": current_remaining_stock,
                            "timestamp": timestamp_now,
                            "event_time": event_time_str
                        }
                        producer.send(TOPIC_ANOMALY, alert_payload)
                        wb.save_alert_to_redis(alert_payload)
                        db_handler.save_log({
                            "timestamp": timestamp_now,
                            "event_time": event_time_str,
                            "item_name": item_name,
                            "type": "CAUTION",
                            "unique_id": unique_id,
                            "details": description_msg
                        })

            # --- 탐지 로직 (REGISTER - 시세 조작 감지) ---
            elif event_type == 'REGISTER':
                wb.buffer_register(item_name, unique_id, event_ts, count=item_count)
            
            ready_members = wb.pop_ready_registers(item_name, time.time())
            
            for member_str in ready_members:
                try:
                    reg_id, count_str = member_str.rsplit(':', 1)
                    reg_count = int(count_str)
                except ValueError:
                    reg_id = member_str
                    reg_count = 1

                # CAUTION 상태일 때만 WARNING 체크 (매점 후 고가 등록 패턴)
                if wb.is_in_caution(item_name):
                    current_ref_price = wb.get_current_avg_price(item_name, event_ts)
                    
                    if current_ref_price > 0:
                        threshold = current_ref_price * DETECTOR_CONFIG['HIGH_PRICE_MULTIPLIER']
                        status = "HIGH" if item_price >= threshold else "NORM"
                        
                        reg_total, reg_high = wb.add_register_event_atomic(
                            item_name, reg_id, status, event_ts, count=reg_count
                        )
                        
                        if reg_total > 0:
                            high_ratio = reg_high / reg_total
                            if high_ratio >= DETECTOR_CONFIG['MANIPULATION_RATIO']:
                                if wb.check_and_set_cooldown(item_name, "WARNING", DETECTOR_CONFIG['ALERT_COOLDOWN_SEC']):
                                    
                                    logger.warning(f"[WARNING] {item_name}: Manipulation detected! (High Ratio: {high_ratio:.2%})")
                                    
                                    # [우선순위 처리] WARNING 발생 시, 하위 단계인 CAUTION 상태 제거
                                    # 이렇게 하면 웹에서는 WARNING 상태만 남게 되어 우선순위가 보장됨
                                    wb.add_alert_warning(item_name)
                                    wb.remove_alert_caution(item_name)
                                    
                                    timestamp_now = datetime.now(KST).isoformat()
                                    description_msg = f"[시세조작] 비율:{high_ratio:.1%} | 가격:{item_price:,.0f} | 평균:{current_ref_price:,.0f}"

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
                                    wb.save_alert_to_redis(alert_payload)

                                    db_handler.save_log({
                                        "timestamp": timestamp_now,
                                        "event_time": event_time_str,
                                        "item_name": item_name,
                                        "type": "WARNING",
                                        "unique_id": unique_id,
                                        "details": description_msg
                                    })
                                    
                                    logger.info(f"[Detector] {item_name}: CAUTION cleared -> WARNING set.")

except Exception as e:
    logger.critical(f"[Detector Error] {e}", exc_info=True)
finally:
    db_handler.close()
    consumer.close()
    producer.close()
    logger.info("Anomaly Detector Stopped.")