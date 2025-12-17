	CREATE SCHEMA IF NOT EXISTS mabi_auction;
	
	SET search_path TO mabi_auction;
	
	-- [데이터 초기화 및 기존 테이블 삭제]
	DROP TABLE IF EXISTS mabi_auction.item_options CASCADE;
	DROP TABLE IF EXISTS mabi_auction.current_listings CASCADE;
	DROP TABLE IF EXISTS mabi_auction.auction_logs CASCADE;
	
	-- 1. 현재 게시 중인 매물 (Current Listings)
	CREATE TABLE current_listings (
	    item_id             VARCHAR(64) PRIMARY KEY, -- 아이템 고유 해시 ID
	    item_name           VARCHAR(100) NULL,       -- 아이템 이름
	    item_display_name   VARCHAR(100) NOT NULL,   -- 아이템 전체 이름 (접두/접미 포함)
	    item_count          INT NOT NULL,            -- 수량
	    item_category       VARCHAR(50),             -- 분류
	    price               BIGINT NOT NULL,         -- 가격
	    start_time          TIMESTAMPTZ,    -- 등록 시간
	    expire_time         TIMESTAMPTZ NOT NULL,   -- 만료 예정 시간
	    auction_duration    INT,            -- 등록 기간 (6, 12, 24, 36, 48, 72)
	    created_at          TIMESTAMPTZ DEFAULT NOW() -- 로그 적재 시각
	);
	
	CREATE INDEX idx_curr_name ON current_listings(item_display_name);
	CREATE INDEX idx_curr_category ON current_listings(item_category);
	
	
	-- 2. 전체 거래 및 변경 로그 (Auction Logs)
	CREATE TABLE auction_logs (
	    id                  BIGSERIAL PRIMARY KEY,   -- 로그 고유 ID (순서 보장용)
	    event_type          VARCHAR(20) NOT NULL,    -- 'REGISTER', 'SOLD', 'CANCEL'
	    item_id             VARCHAR(64) NOT NULL,    -- 아이템 ID
	    auction_buy_id      VARCHAR(64),
	    item_name           VARCHAR(100),
	    item_display_name   VARCHAR(100) NOT NULL,
	    item_count          INT NOT NULL,
	    item_category       VARCHAR(50) NOT NULL,
	    price               BIGINT NOT NULL,         -- 등록가 or 판매가
	    registered_at       TIMESTAMPTZ,             -- 등록 시간
	    auction_duration    INT,                     -- 등록 기간
	    ended_at            TIMESTAMPTZ NOT NULL,             -- 만료/판매 시각 (등록, 취소의 경우 만료시각 제공)  
	    created_at          TIMESTAMPTZ DEFAULT NOW(), -- 로그 적재 시각
	    
	    CONSTRAINT uq_log_event UNIQUE (item_id, event_type)
	);
	
	CREATE INDEX idx_logs_registered ON auction_logs(registered_at); -- 등록 시간대별 분석
	CREATE INDEX idx_logs_ended ON auction_logs(ended_at);           -- 판매/종료 시간대별 분석
	CREATE INDEX idx_logs_id ON auction_logs(item_id);               -- 특정 아이템 이력 추적
	CREATE INDEX idx_logs_name_event ON auction_logs(item_name, event_type);
	
	
	-- 3. 아이템 상세 옵션 (Item Options)
	CREATE TABLE item_options (
	    item_id             VARCHAR(64) NOT NULL,
	    option_type         VARCHAR(50) NOT NULL,
	    option_sub_type     VARCHAR(50),
	    option_value        VARCHAR(100),
	    option_value2       VARCHAR(100),
	    option_desc         VARCHAR(255),
	    
	    CONSTRAINT uq_item_option UNIQUE (item_id, option_type, option_sub_type) 
	);
	
	CREATE INDEX idx_options_id ON item_options(item_id);
	CREATE INDEX idx_options_search ON item_options(option_type, option_value);