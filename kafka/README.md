```
mabi-auction/
│
├── README.md
├── .gitignore
├── docker-compose.yml             # 로컬 개발용 (Kafka/Redis/Postgres 정도만)
│
│
├── AWS/
│   ├── env/                       # AWS 환경 설정               
│   └── redis/                     # Redis key 정의 및 관리 도구
│        ├── redis_schema.md
│        └── cli_helpers.py        # key dump, cleanup 등 util
│
├── kafka/                       
│   ├── config/                    # KAFKA, API KEY, Redis/DB 접속 정보
│   ├── collector/                 # 데이터 수집 (Producers)
│   ├── processor/                 # 스트림 처리 및 이벤트 판별 (Consumers)
│   ├── sink/                      # 최종 데이터 저장 (DB, S3)
│   ├── utils/                     # logger.py
│   └── db/                        # DB 테이블 정의 스크립트  
│
├── airflow/                  # Airflow 전체 업로드 X — 우리가 수정하는 파일만
│   ├── dags/
│   ├── requirements.txt         # Airflow worker에 넣을 패키지만
│   └── Dockerfile               # 우리가 수정한 Dockerfile만
│
├── django/                   # 웹 대시보드
│   ├── manage.py
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── detector_api/         # REST API (등록/판매/시세/탐지결과)
│   ├── dashboard/            # Web UI
│   ├── templates/
│   ├── static/
│   └── settings/
│
│   # docs 파일은 readme 제작 후 필요 없으면 삭제  
├── docs/                      # 팀 문서
│   ├── architecture.md        # 전체 AWS 아키텍처 다이어그램
│   ├── dataflow.md            # end-to-end 데이터 흐름 설명
│   ├── detector_rules.md      # 이상탐지 룰 정의서
│   ├── erd.md                 # DB ERD (PostgreSQL 스키마)
│   └── redis_memory_calc.xlsx # Redis 메모리 계산 시트
│ 
└── scripts/
    ├── local_run_collector.sh
    ├── local_run_streaming.sh
    ├── deploy.sh
    └── benchmark_memory.py
```