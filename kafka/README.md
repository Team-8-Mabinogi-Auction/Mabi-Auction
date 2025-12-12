```
mabi-auction/
├── config/
│   ├── init.py
│   └── settings.py                # KAFKA, API KEY, Redis/DB 접속 정보
|
├── collector/                     # 데이터 수집 (Producers)
│   ├── init.py
│   ├── kafka_producer.py          # Kafka Producer Wrapper
│   ├── producer_listings.py
│   └── producer_history.py
|
├── processor/                     # 스트림 처리 및 이벤트 판별 (Consumers)
│   ├── init.py
│   ├── utils.py                   # generate_item_signature 등 유틸 함수
│   ├── processor_events.py        # 핵심 이벤트 판별 로직 (REG/SOLD/CANCEL)
│   ├── window_buffer.py           # 이상 탐지 윈도우 유지
│   └── anomaly_detector.py        # 이상 탐지 로직
|
├── sink/                          # 최종 데이터 저장 (DB, S3)
│   ├── init.py
│   ├── consumer_db_sink.py        # events 토픽 소비 및 배치 처리
│   ├── postgres_client.py         # DB 연결 및 트랜잭션/Bulk Insert 관리
│   └── s3_connector_config/
│       └── s3_sink_listings.json  # S3 Sink Connector 설정 파일
|
├── utils/
│   ├── init.py
│   └── logger.py
|
└── db/
		└── init_schema.sql            # DB 테이블 정의 스크립트
```
