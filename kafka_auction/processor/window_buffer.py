import redis
import json
import os
from kafka_auction.processor.utils import require_env
import logging
from pathlib import Path
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("WindowBuffer")

# 환경변수 설정
current_file = Path(__file__).resolve() 
env_path = current_file.parent.parent / 'config' / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    logger.error(f"Cannot find .env file at {env_path}")


# Redis 접속 정보 및 윈도우 설정
REDIS_CONFIG = {
    'host': require_env('REDIS_HOST'),
    'port': int(require_env('REDIS_PORT')),
    'db': int(require_env('REDIS_DB')),
    'decode_responses': True,
    'ssl': True,
    'ssl_cert_reqs': 'none',
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
    'retry_on_timeout': True
}

WINDOW_SETTINGS = {
    'SOLD_SEC': int(os.getenv('SOLD_WINDOW_SEC', 600)),
    'REG_SEC': int(os.getenv('REG_WINDOW_SEC', 300)),
    'REGISTER_GRACE_SEC': int(os.getenv('REGISTER_GRACE_SEC', 30)),
    'CAUTION_TTL_SEC': int(os.getenv('CAUTION_TTL_SEC', 3600))
}

class WindowBuffer:
    """
    Redis Wrapper Class (Stackable Support Added)
    """

    def __init__(self):
        self.r = redis.StrictRedis(**REDIS_CONFIG)
        self.SOLD_WINDOW_SEC = WINDOW_SETTINGS['SOLD_SEC']
        self.REG_WINDOW_SEC = WINDOW_SETTINGS['REG_SEC']
        self.REGISTER_GRACE_SEC = WINDOW_SETTINGS['REGISTER_GRACE_SEC']
        self.CAUTION_TTL_SEC = WINDOW_SETTINGS['CAUTION_TTL_SEC']

        # [수정] 판매 스크립트: Price:Count:ID 파싱 및 가중 평균 계산
        self.script_add_sold_with_avg = self.r.register_script("""
            local key = KEYS[1]
            local score = ARGV[1]   -- Event Timestamp
            local member = ARGV[2]  -- "Price:Count:EventID"
            local min_score = ARGV[3]
            
            -- 1. 데이터 추가 및 청소
            redis.call('ZADD', key, score, member)
            redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)
            
            -- 2. 집계 (가중 평균)
            local all_members = redis.call('ZRANGE', key, 0, -1)
            
            local total_qty = 0
            local total_volume_price = 0
            
            for i, m in ipairs(all_members) do
                -- Lua 패턴 매칭으로 Price와 Count 추출
                -- 형식: Price:Count:ID
                local p_str, c_str = string.match(m, "^([%d%.]+):(%d+):")
                
                if p_str and c_str then
                    local p = tonumber(p_str)
                    local c = tonumber(c_str)
                    
                    total_qty = total_qty + c
                    total_volume_price = total_volume_price + (p * c)
                end
            end
            
            if total_qty > 0 then
                return {total_qty, total_volume_price / total_qty} -- [총판매량, 가중평균가]
            else
                return {0, 0}
            end
        """)

        # [수정] 등록 스크립트: ID:Status:Count 파싱 및 수량 기준 비율 계산
        self.script_add_reg = self.r.register_script("""
            local key = KEYS[1]
            local score = ARGV[1]
            local member = ARGV[2] -- "UUID:Status:Count"
            local min_score = ARGV[3]

            redis.call('ZADD', key, score, member)
            redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)
            
            local all_members = redis.call('ZRANGE', key, 0, -1)
            
            local total_qty = 0
            local high_qty = 0
            
            for i, m in ipairs(all_members) do
                -- 형식: UUID:Status:Count (Status는 문자열)
                -- UUID에 콜론이 없다고 가정하거나, 뒤에서부터 파싱해야 안전함
                -- 여기서는 단순화를 위해 :로 split 유사하게 처리
                
                local last_colon = string.find(m, ":", -string.len(m)) -- 뒤에서 검색 필요하나 Lua 5.1 한계
                -- 안전한 패턴 매칭 사용: (아무문자):(Status):(Count)
                -- Status는 NORM 또는 HIGH
                
                local status, c_str = string.match(m, ":(%w+):(%d+)$")
                
                if status and c_str then
                    local c = tonumber(c_str)
                    total_qty = total_qty + c
                    
                    if status == "HIGH" then
                        high_qty = high_qty + c
                    end
                end
            end
            
            return {total_qty, high_qty} -- [전체 등록 수량, 고가 등록 수량]
        """)

        # [수정] 평균가 조회 스크립트 (판매 스크립트와 동일 로직)
        self.script_get_avg_price = self.r.register_script("""
            local key = KEYS[1]
            local min_score = ARGV[1]
            
            redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)
            
            local all_members = redis.call('ZRANGE', key, 0, -1)
            
            local total_qty = 0
            local total_volume_price = 0
            
            for i, m in ipairs(all_members) do
                local p_str, c_str = string.match(m, "^([%d%.]+):(%d+):")
                if p_str and c_str then
                    local p = tonumber(p_str)
                    local c = tonumber(c_str)
                    total_qty = total_qty + c
                    total_volume_price = total_volume_price + (p * c)
                end
            end
            
            if total_qty > 0 then
                return total_volume_price / total_qty
            else
                return 0
            end
        """)
    
    # [수정] count 인자 추가 및 Member 포맷 변경
    def add_sold_event_atomic(self, item_name, event_id, price, event_ts, count=1):
        """판매 이벤트 추가 (Stackable 지원)"""
        key = f"stats:window:{item_name}"
        min_score = event_ts - self.SOLD_WINDOW_SEC
        
        # Format: Price:Count:EventID
        member = f"{price}:{count}:{event_id}"

        result = self.script_add_sold_with_avg(
            keys=[key],
            args=[event_ts, member, min_score]
        )

        avg_price = float(result[1]) if result[1] else 0.0
        return int(result[0]), avg_price

    # [수정] count 인자 추가 및 Member 포맷 변경
    def add_register_event_atomic(self, item_name, uuid, status, event_ts, count=1):
        """등록 이벤트 추가 (Stackable 지원)"""
        key = f"stats:reg_window:{item_name}"
        min_score = event_ts - self.REG_WINDOW_SEC
        
        # Format: UUID:Status:Count
        member = f"{uuid}:{status}:{count}"
        
        result = self.script_add_reg(
            keys=[key],
            args=[event_ts, member, min_score]
        )
        return result[0], result[1]

    def get_current_avg_price(self, item_name, ref_ts):
        """최근 10분 판매 윈도우의 평균가 조회"""
        key = f"stats:window:{item_name}"
        min_score = ref_ts - self.SOLD_WINDOW_SEC
        avg = self.script_get_avg_price(keys=[key], args=[min_score])
        return float(avg) if avg else 0.0
    
    def buffer_register(self, item_name, event_id, event_ts, count=1):
        """등록 매물 buffer에 추가 (수량 정보 포함)"""
        key = f"buffer:register:{item_name}"
        member = f"{event_id}:{count}"  # ID와 수량을 묶어서 저장
        self.r.zadd(key, {member: event_ts})

    def pop_ready_registers(self, item_name, now_ts):
        """buffer에서 대기 시간 지난 매물들 pop"""
        key = f"buffer:register:{item_name}"
        ready_ts = now_ts - self.REGISTER_GRACE_SEC
        
        # 점수가 ready_ts 이하인 멤버들 조회
        members = self.r.zrangebyscore(key, '-inf', ready_ts)
        
        if members:
            self.r.zrem(key, *members)
            
        return members # ["ID:Count", "ID:Count", ...]

    def update_market_count(self, item_name, amount):
        """현재 시장 총 매물 수 동기화 (단순 수량 증감이라 수정 불필요)"""
        key = f"stats:counts:{item_name}"
        val = self.r.incrby(key, amount)
        if val < 0:
            self.r.set(key, 0)
            return 0
        return val

    def get_current_total_count(self, item_name):
        c = self.r.get(f"stats:counts:{item_name}")
        return int(c) if c else 0

    def check_and_set_cooldown(self, item_name, alert_type, duration_sec):
        key = f"alert:cooldown:{item_name}:{alert_type}"
        return self.r.set(key, "1", ex=duration_sec, nx=True)
    
    def save_alert_to_redis(self, payload):
        alert_key = "view:recent_alerts" 
        ttl_seconds = 21600 # 6시간
        
        try:
            payload_json = json.dumps(payload, ensure_ascii=False)
            pipe = self.r.pipeline()
            pipe.lpush(alert_key, payload_json)
            pipe.ltrim(alert_key, 0, 9)
            pipe.expire(alert_key, ttl_seconds)
            pipe.execute()
        except Exception as e:
            print(f"Redis Cache Error: {e}")

    def add_alert_caution(self, item_name):
        key = f"state:caution:{item_name}"
        self.r.set(key, "1", ex=self.CAUTION_TTL_SEC)

    def is_in_caution(self, item_name):
        key = f"state:caution:{item_name}"
        return self.r.exists(key) > 0
    
    def add_alert_warning(self, item_name):
        self.r.sadd("alert:warning", item_name)
    
    def remove_alert_caution(self, item_name):
        """주의 상태(감시 목록)에서 제거"""
        key = f"state:caution:{item_name}"
        self.r.delete(key)