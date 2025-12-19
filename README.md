🎮 Mabinogi Auction System (Django)
이 프로젝트는 마비노기 경매장 데이터를 기반으로 한 데이터 시각화 및 분석 웹 서비스입니다. 현재는 대량의 CSV 데이터를 효율적으로 처리하는 로직을 구현하였으며, 추후 데이터베이스(DB) 시스템으로의 확장을 염두에 두고 설계되었습니다.

🚀 주요 기능 (Core Features)
1. 데이터 처리 및 확장성 (Data Handling & Scalability)
현재 아키텍처: ex_data.csv와 total_details.csv를 더미 데이터로 활용하여 실시간 매물을 구현했습니다.

DB 전환 로드맵: 현재의 CSV 파싱 로직은 추후 SQLite/PostgreSQL 등 관계형 데이터베이스(RDBMS)로 즉시 전환할 수 있도록 모듈화되어 있습니다.

지능형 로딩: 서버 부하 방지를 위해 초기 로딩 시 100개, 추가 요청 시 300개씩 데이터를 비동기(AJAX)로 호출합니다.

2. 고도화된 인터랙션 (Interactive UI)
동적 테이블 정렬 (Sorting):

금액(숫자), 아이템 이름(문자), 남은 시간별 맞춤 정렬 기능을 제공합니다.

정렬 시 제목 옆 ▲/▼ 화살표 표시 및 해당 컬럼 강조색을 통해 상태를 직관적으로 표현합니다.

하이브리드 페이징: 서버 데이터를 클라이언트 단에서 10개 단위로 쪼개어 가독성을 높였습니다.

인터랙티브 호버: 마우스 커서가 위치한 행(Row)을 강조하여 대량 데이터 탐색을 돕습니다.

3. 지능형 분석 및 시각화 (Analysis)
시세 변동 차트: Chart.js를 사용하여 특정 아이템의 시간별 가격 추이를 꺾은선 그래프로 제공합니다.

사재기 탐지 시스템: warning_items.json 기반으로 매물 급감 등 이상 징후가 포착된 아이템을 사이드바에 경고/의심 등급으로 실시간 노출합니다.

🛠 기술 스택 (Tech Stack)
Backend: Python, Django

Frontend: JavaScript (ES6+), HTML5, CSS3

Visualization: Chart.js

Data Source: CSV, JSON (Current) → DB Migration Planned

📂 프로젝트 구조 (Project Structure)
Plaintext

├── Mabi_Auction_app/      # 핵심 로직 (Views, URLs)
├── static/                # 정적 파일 관리
│   ├── test_data/         # CSV/JSON 데이터 (DB 전환 대상)
│   ├── images/            # 아이템 썸네일 및 로고
│   └── js/                # 정렬 및 페이징 로직
├── templates/             # index, detail 화면 구성
└── manage.py              # 실행 스크립트
⚙️ 실행 방법 (Setup)
1. 레포지토리 클론

Bash

git clone https://github.com/your-username/mabi-auction-system.git

2. Django 서버 실행

Bash

python manage.py runserver

3. 데이터 확인 static/test_data/ 내의 CSV 파일을 수정하여 더미 데이터를 테스트할 수 있습니다.


💡 구현 상세: 데이터 정렬 로직
시간 데이터: "H_M" 형태를 정수(분)로 환산하여 정확한 오름차순/내림차순 정렬을 구현했습니다.

이미지 폴백: 사진이 없는 아이템은 onerror 이벤트를 통해 즉시 기본 로고(mabi_logo.jpg)로 교체하여 UI 깨짐을 방지합니다.

Sorting Logic: 표 상단의 헤더 클릭 시 data-dir 속성을 통해 오름차순/내림차순을 토글하며, 정렬이 완료된 컬럼은 sorted-col 클래스를 부여하여 시각적으로 강조합니다.

Hover Logic: 사용자가 탐색 중인 행을 명확히 인지하도록 tr:hover 시 배경색 변화 효과를 적용했습니다.