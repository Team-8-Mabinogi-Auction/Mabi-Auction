CREATE SCHEMA IF NOT EXISTS mabi_auction;

SET search_path TO mabi_auction;

-- 기존 테이블이 있다면 삭제
DROP TABLE IF EXISTS item_options CASCADE;
DROP TABLE IF EXISTS current_listings CASCADE;
DROP TABLE IF EXISTS auction_logs CASCADE;
DROP TABLE IF EXISTS anomaly_logs CASCADE;

-- 1. 현재 게시 중인 매물
CREATE TABLE current_listings (
    item_id       VARCHAR(64) PRIMARY KEY, -- 아이템 고유 해시 ID
    item_name       VARCHAR(100) NULL,   -- 아이템 이름 (예: 켈틱 로열 나이트 소드)
    item_display_name   VARCHAR(100) NOT NULL,  -- 아이템 전체 이름
    item_count      INT NOT NULL,   -- 아이템 수량
    item_category   VARCHAR(50),             -- 분류
    price           BIGINT NOT NULL,         -- 등록 가격
    start_time      TIMESTAMPTZ NOT NULL,    -- 등록 시간
    expire_time     TIMESTAMPTZ NOT NULL,    -- 만료 예정 시간
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스: 이름으로 검색, 카테고리별 조회 속도 향상
CREATE INDEX idx_curr_name ON current_listings(item_display_name);
CREATE INDEX idx_curr_category ON current_listings(item_category);


-- 2. 전체 거래 및 변경 로그 (이력)
-- 용도: 시세 통계 산출, 머신러닝 학습 데이터, 매점(싹쓸이) 감지
CREATE TABLE auction_logs (
    event_type      VARCHAR(20) NOT NULL,    -- 'REGISTER', 'SOLD', 'CANCEL'
    item_id       VARCHAR(64) NOT NULL,    -- current_listings와 연결되는 ID
    item_name       VARCHAR(100) NULL,   -- 아이템 이름 (예: 켈틱 로열 나이트 소드)
    item_display_name   VARCHAR(100) NOT NULL,  -- 아이템 전체 이름
    item_count      INT NOT NULL,   -- 아이템 수량
    item_category   VARCHAR(50),             -- 분류
    price           BIGINT NOT NULL,         -- 등록 가격
    event_time      TIMESTAMPTZ NOT NULL,    -- 이벤트 발생 시간
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스: 시계열 조회, 특정 아이템의 시세 변동 조회용
CREATE INDEX idx_logs_time ON auction_logs(event_time);
CREATE INDEX idx_logs_uuid ON auction_logs(item_id);
CREATE INDEX idx_logs_name_event ON auction_logs(item_name, event_type);


-- 3. 아이템 상세 옵션 (1:N 관계)
-- 용도: 장비의 세공, 인챈트, 개조 정보 저장
CREATE TABLE item_options (
    item_id       VARCHAR(64) NOT NULL,    -- 어떤 아이템의 옵션인지 식별
    option_type     VARCHAR(50) NOT NULL,    -- 옵션 유형
    option_sub_type     VARCHAR(50) NOT NULL,    -- 옵션 하위 유형
    option_value   VARCHAR(100),            -- 옵션 값
    option_value2   VARCHAR(100),            -- 옵션 값
    option_desc    NUMERIC,                 -- 옵션 부가 정보
    
    -- 외래키 제약조건을 걸지 않음 (Logs에는 있는데 Current에는 없을 수 있으므로 논리적 연결만 수행)
    CONSTRAINT uq_item_option UNIQUE (item_id, option_type, option_sub_type) 
);

-- 인덱스: 특정 옵션(예: 세공 1랭크)을 가진 아이템 검색용
CREATE INDEX idx_options_uuid ON item_options(item_uuid);
CREATE INDEX idx_options_search ON item_options(option_type, option_value);


-- 4. 이상 탐지 로그 (Anomaly Detection)
-- 용도: 매점/매석 발생 시 기록
CREATE TABLE anomaly_logs (
    anomaly_id      BIGSERIAL PRIMARY KEY,
    detection_time  TIMESTAMPTZ DEFAULT NOW(),
    item_name       VARCHAR(100) NOT NULL,
    anomaly_type    VARCHAR(20) NOT NULL,    -- 'CAUTION'(매점), 'WARNING'(매석)
    description     TEXT,                    -- 상세 내용 (예: "평균가 대비 150% 급등, 재고 15% 이하")
    item_id    VARCHAR(64)              -- 관련된 매물 ID (있을 경우)
);