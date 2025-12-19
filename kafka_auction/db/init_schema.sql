CREATE SCHEMA IF NOT EXISTS mabi_auction;

SET search_path TO mabi_auction;

-- [데이터 초기화 및 기존 테이블 삭제]
DROP TABLE IF EXISTS mabi_auction.current_listings CASCADE;
DROP TABLE IF EXISTS mabi_auction.auction_logs CASCADE;
DROP TABLE IF EXISTS mabi_auction.anomaly_logs CASCADE;

-- 1. 현재 게시 중인 매물 (Current Listings)
CREATE TABLE current_listings (
    item_id             VARCHAR(64) PRIMARY KEY, -- 아이템 고유 해시 ID
    item_name           VARCHAR(100) NULL,       -- 아이템 이름
    item_display_name   VARCHAR(100) NOT NULL,   -- 아이템 전체 이름 (접두/접미 포함)
    item_count          INT NOT NULL,            -- 수량
    item_category       VARCHAR(50),             -- 분류
    price               BIGINT NOT NULL,         -- 가격
    start_time          TIMESTAMPTZ,             -- 등록 시간 (Fresh Start시 NULL 가능)
    expire_time         TIMESTAMPTZ NOT NULL,    -- 만료 예정 시간
    auction_duration    INT,                     -- 등록 기간 (6, 12, 24, 36, 48, 72)
    options             JSONB,                   -- 아이템 상세 옵션 (JSONB)
    created_at          TIMESTAMPTZ DEFAULT NOW() -- 데이터 호출 시각
);

CREATE INDEX idx_curr_name ON current_listings(item_display_name);
CREATE INDEX idx_curr_category ON current_listings(item_category);

CREATE INDEX idx_curr_options ON current_listings USING GIN (options);


-- 2. 전체 거래 및 변경 로그 (Auction Logs)
CREATE TABLE auction_logs (
    id                  BIGSERIAL PRIMARY KEY,   -- 로그 고유 ID (순서 보장용)
    event_type          VARCHAR(20) NOT NULL,    -- 'REGISTER', 'SOLD', 'CANCEL'
    item_id             VARCHAR(64) NOT NULL,    -- 아이템 ID
    auction_buy_id      VARCHAR(64),             -- 판매 시 구매 ID
    item_name           VARCHAR(100),
    item_display_name   VARCHAR(100) NOT NULL,
    item_count          INT NOT NULL,
    item_category       VARCHAR(50) NOT NULL,
    price               BIGINT NOT NULL,         -- 등록가 or 판매가
    registered_at       TIMESTAMPTZ,             -- 등록 시간
    auction_duration    INT,                     -- 등록 기간
    ended_at            TIMESTAMPTZ NOT NULL,    -- 만료/판매 시각
    options             JSONB,                   -- 당시 아이템 옵션 스냅샷 (JSONB)
    created_at          TIMESTAMPTZ DEFAULT NOW(), -- 데이터 호출 시각
    
    -- 동일 아이템의 동일 이벤트 중복 적재 방지
    CONSTRAINT uq_log_event UNIQUE (item_id, event_type)
);

CREATE INDEX idx_logs_registered ON auction_logs(registered_at); -- 등록 시간대별 분석
CREATE INDEX idx_logs_ended ON auction_logs(ended_at);           -- 판매/종료 시간대별 분석
CREATE INDEX idx_logs_id ON auction_logs(item_id);               -- 특정 아이템 이력 추적
CREATE INDEX idx_logs_name_event ON auction_logs(item_name, event_type);

CREATE INDEX idx_logs_options ON auction_logs USING GIN (options);


-- 3. 이상 탐지 로그 (Anomaly Logs)
CREATE TABLE anomaly_logs (
    anomaly_id      BIGSERIAL PRIMARY KEY,
    detection_time  TIMESTAMPTZ DEFAULT NOW(), -- 탐지 시간
    event_time      TIMESTAMPTZ NOT NULL,      -- 등록 or 판매 시간
    item_name       VARCHAR(100) NOT NULL,
    anomaly_type    VARCHAR(20) NOT NULL,      -- 'CAUTION', 'WARNING'
    description     TEXT,                      -- 상세 내용
    id              VARCHAR(64)                -- item_id 또는 auction_buy_id
);