import redis
import json
import os
from kafka_auction.processor.utils import require_env

# Redis 접속 정보 및 윈도우 설정
REDIS_CONFIG = {
    'host': require_env('REDIS_HOST'),
    'port': int(require_env('REDIS_PORT')),
    'db': int(require_env('REDIS_DB')),
    'decode_responses': True
}

WINDOW_SETTINGS = {
    'SOLD_SEC': int(os.getenv('SOLD_WINDOW_SEC', 600)),
    'REG_SEC': int(os.getenv('REG_WINDOW_SEC', 300)),
    'REGISTER_GRACE_SEC': int(os.getenv('REGISTER_GRACE_SEC', 30)),
    'CAUTION_TTL_SEC': int(os.getenv('CAUTION_TTL_SEC', 3600))
}

class WindowBuffer:
    """
    Redis Wrapper Class
    - Lua Script를 이용한 Atomic 연산 및 Sliding Window 구현
    - 실시간 평균가 계산 및 재고 관리 기능 포함
    """

    def __init__(self):
        self.r = redis.StrictRedis(**REDIS_CONFIG)
        self.SOLD_WINDOW_SEC = WINDOW_SETTINGS['SOLD_SEC']
        self.REG_WINDOW_SEC = WINDOW_SETTINGS['REG_SEC']
        self.REGISTER_GRACE_SEC = WINDOW_SETTINGS['REGISTER_GRACE_SEC']
        self.CAUTION_TTL_SEC = WINDOW_SETTINGS['CAUTION_TTL_SEC']

        # 판매 이벤트 추가 + 실시간 평균가 계산
        # 데이터 추가 -> 만료 삭제 -> 현재 윈도우 내 평균가 및 판매량 리턴
        self.script_add_sold_with_avg = self.r.register_script("""
            local key = KEYS[1]
            local score = ARGV[1]  -- 현재 시간
            local member = ARGV[2] -- "Price:EventID"
            local min_score = ARGV[3] -- 삭제 기준 시간
            
            -- 1. 데이터 추가 및 윈도우 청소
            redis.call('ZADD', key, score, member)
            redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)
            
            -- 2. 집계 (개수 및 가격 합계 계산)
            local all_members = redis.call('ZRANGE', key, 0, -1)
            local count = #all_members
            local sum_price = 0
            
            if count > 0 then
                for i, m in ipairs(all_members) do
                    local sep_index = string.find(m, ":")
                    if sep_index then
                        local price_str = string.sub(m, 1, sep_index - 1)
                        sum_price = sum_price + tonumber(price_str)
                    end
                end
                return {count, sum_price / count} -- [판매량, 평균가] 반환
            else
                return {0, 0}
            end
        """)

        # 등록 이벤트 추가 + 고가 매물 비율 계산
        self.script_add_reg = self.r.register_script("""
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
            
            local all_members = redis.call('ZRANGE', KEYS[1], 0, -1)
            local total = #all_members
            local high_count = 0
            
            for i, member in ipairs(all_members) do
                if string.find(member, ":HIGH") then
                    high_count = high_count + 1
                end
            end
            
            return {total, high_count} -- [전체 등록 수, 고가 등록 수]
        """)

        # 현재 윈도우 평균가 조회
        self.script_get_avg_price = self.r.register_script("""
            local key = KEYS[1]
            local min_score = ARGV[1]
            
            -- 정확한 계산을 위해 청소 먼저 수행
            redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)
            
            local all_members = redis.call('ZRANGE', key, 0, -1)
            local count = #all_members
            local sum_price = 0
            
            if count > 0 then
                for i, m in ipairs(all_members) do
                    local sep_index = string.find(m, ":")
                    if sep_index then
                        local price_str = string.sub(m, 1, sep_index - 1)
                        sum_price = sum_price + tonumber(price_str)
                    end
                end
                return sum_price / count
            else
                return 0
            end
        """)
    
    def add_sold_event_atomic(self, item_name, event_id, price, event_ts):
        """판매 이벤트 추가 후 (판매량, 평균판매가) 반환"""
        key = f"stats:window:{item_name}"
        min_score = event_ts - self.SOLD_WINDOW_SEC
        member = f"{price}:{event_id}"

        result = self.script_add_sold_with_avg(
            keys=[key],
            args=[event_ts, member, min_score]
        )

        avg_price = float(result[1]) if result[1] else 0.0

        return int(result[0]), avg_price

    def add_register_event_atomic(self, item_name, uuid, status, event_ts):
        """등록 이벤트 추가 후 (전체등록수, 고가등록수) 반환"""
        key = f"stats:reg_window:{item_name}"
        min_score = event_ts - self.REG_WINDOW_SEC
        member = f"{uuid}:{status}"
        
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
    
    def buffer_register(self, item_name, event_id, event_ts):
        """등록 매물 buffer에 추가"""
        key = f"buffer:register:{item_name}"
        self.r.zadd(key, {event_id: event_ts})

    def pop_ready_registers(self, item_name, now_ts):
        """buffer에서 대기 시간 지난 매물들 pop"""
        key = f"buffer:register:{item_name}"
        ready_ts = now_ts - self.REGISTER_GRACE_SEC
        ids = self.r.zrangebyscore(key, '-inf', ready_ts)
        if ids:
            self.r.zrem(key, *ids)
        return ids

    def update_market_count(self, item_name, amount):
        """현재 시장 총 매물 수 동기화"""
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
        """
        중복 알림 방지 (알람 쿨타임 적용)
        """
        key = f"alert:cooldown:{item_name}:{alert_type}"
        return self.r.set(key, "1", ex=duration_sec, nx=True)
    
    def save_alert_to_redis(self, payload):
        """
        메인 페이지용 캐시 저장 (views.py가 읽어갈 데이터)
        """
        alert_key = "view:recent_alerts" 
        ttl_seconds = 6 * 60 * 60  # 6시간 = 21600초
        
        try:
            # 1. JSON 변환
            payload_json = json.dumps(payload, ensure_ascii=False)
            
            pipe = self.r.pipeline()
            
            pipe.lpush(alert_key, payload_json)         # 데이터 넣기
            pipe.ltrim(alert_key, 0, 9)                 # 10개만 남기고 자르기
            pipe.expire(alert_key, ttl_seconds)         # [추가됨] 수명을 6시간으로 초기화
            
            pipe.execute() # 실행
            
        except Exception as e:
            print(f"Redis Cache Error: {e}")

    def add_alert_caution(self, item_name):
        """주의 상태 설정 (TTL 적용으로 자동 해제)"""
        key = f"state:caution:{item_name}"
        # 값이 무엇인지는 중요하지 않음, 키의 존재 여부가 중요
        self.r.set(key, "1", ex=self.CAUTION_TTL_SEC)

    # [수정] 주의 상태 확인: sismember -> exists
    def is_in_caution(self, item_name):
        """주의 상태 확인"""
        key = f"state:caution:{item_name}"
        return self.r.exists(key) > 0
    
    def add_alert_warning(self, item_name):
        self.r.sadd("alert:warning", item_name)