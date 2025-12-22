import csv
import os
import json
import math
import urllib.parse
from datetime import datetime, timedelta, timezone
from django.shortcuts import render
from django_redis import get_redis_connection
from django.conf import settings

CDN_BASE_URL = os.getenv('CDN_BASE_URL', '')

# ---------------------------------------------------------
# 1. 아이템 상세 정보 및 썸네일 맵 로드
# ---------------------------------------------------------
def get_item_details_map():
    details_csv = os.path.join(settings.BASE_DIR, 'static', 'images', 'mabinogi_details_chunks', 'total_details_fixed.csv')
    item_details_map = {}
    if os.path.exists(details_csv):
        try:
            with open(details_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name_key = row.get('아이템명', '').strip()
                    if name_key:
                        raw_path = row.get('아이템이미지_썸네일', '')
                        item_details_map[name_key] = {
                            'thumb': raw_path.replace('\\', '/').replace('static/', '').strip(),
                            'desc': row.get('아이템설명', '상세 설명이 없습니다.')
                        }
        except: pass
    return item_details_map

# ---------------------------------------------------------
# 2. 실시간 사재기 알림 로드 (Redis)
# ---------------------------------------------------------
def get_recent_alerts(limit=5):
    alerts = []
    con = get_redis_connection("default")
    raw_list = con.lrange("view:recent_alerts", 0, limit - 1)
    
    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)

    for item in raw_list:
        try:
            data = json.loads(item.decode('utf-8'))
            
            # [1] 시간 파싱
            ts_str = data.get('timestamp', '')
            if ts_str:
                dt_obj = datetime.fromisoformat(ts_str)
                diff = now - dt_obj
                total_seconds = int(diff.total_seconds())
                if total_seconds < 60: data['display_time'] = "방금 전"
                elif total_seconds < 3600: data['display_time'] = f"{total_seconds // 60}분 전"
                else: data['display_time'] = f"{total_seconds // 3600}시간 전"

            # [2] 페이로드 타입별 수치 기반 문구 생성
            ratio = data.get('ratio', 0)
            alert_type = data.get('type') # WARNING or CAUTION
            reason_code = data.get('reason') # MANIPULATION or SWEEPING

            if alert_type == "WARNING":
                # 경고 (시세조작 의심)
                reg_price = data.get('reg_price', 0)
                data['level_text'] = "경고"
                data['level_class'] = "badge-danger"
                # 2행: 상세 수치 포함 (평균가 대비 등록가)
                data['reason_text'] = f"평균 대비 {ratio:.0f}배 높은 가격에 등록되었습니다."
                # 3행: 뱃지 태그
                data['badge_tag'] = "시세조작"

            elif alert_type == "CAUTION":
                # 의심 (휩쓸기/대량구매)
                velocity = data.get('velocity_10min', 0)
                data['level_text'] = "의심"
                data['level_class'] = "badge-warn"
                # 2행: 상세 수치 포함 (10분당 판매 속도)
                data['reason_text'] = f"{velocity}개의 매물이 소진되었습니다. ({ratio*100:.0f}% 소진)"
                # 3행: 뱃지 태그
                data['badge_tag'] = "대량구매"

            alerts.append(data)
        except Exception as e:
            print(f"Parsing Error: {e}")
            continue
            
    return alerts

# ---------------------------------------------------------
# 3. 메인 페이지 뷰 (전체 정렬 및 페이지네이션)
# ---------------------------------------------------------
def index(request):
    # 파라미터 수집
    sort_by = request.GET.get('sort', '')  # 기본값: 남은시간순
    direction = request.GET.get('dir', 'asc')      # 기본값: 오름차순
    try:
        page = int(request.GET.get('page', 1))
    except:
        page = 1
    page_size = 20

    item_details_map = get_item_details_map()
    alerts = get_recent_alerts(limit=5)
    all_items = []

    try:
        con = get_redis_connection("default")
        # Redis에서 모든 아이템 키 가져오기
        redis_data = con.zrevrange("recent:items", 0, -1)
        current_time = datetime.now().timestamp()

        for raw_bytes in redis_data:
            try:
                row = json.loads(raw_bytes.decode('utf-8'))
                name = row.get('item_name')
                if not name: continue

                # 카테고리 매핑 (item_category 키 우선 사용)
                category = row.get('item_category') or row.get('category') or '기타'
                
                # 정렬을 위해 숫자형으로 변환
                unit_price = int(row.get('price_per_unit') or 0)
                count = int(row.get('item_count') or 0)
                
                # 남은 시간 계산 및 정렬 값(sort_val) 생성
                expire_str = row.get('expire_time')
                display_time = "정보없음"
                sort_val = 9999999999 # 시간 정보 없을 때 뒤로 밀기

                if expire_str:
                    try:
                        expire_dt = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
                        diff = expire_dt.timestamp() - current_time
                        if diff > 0:
                            h, m = int(diff // 3600), int((diff % 3600) // 60)
                            display_time = f"{h}시간 {m}분"
                            sort_val = diff
                        else:
                            display_time = "종료됨"
                            sort_val = -1
                    except: pass

                # 이미지 경로 처리
                detail = item_details_map.get(name, {})
                filename = f"{name}.png"
                encoded_filename = urllib.parse.quote(filename)
                
                all_items.append({
                    'name': name,
                    'category': category,
                    'count': count,
                    'unit_price': unit_price,
                    'total_price': unit_price * count,
                    'display_time': display_time,
                    'sort_val': sort_val,
                    'display_img': f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}",
                    'desc': detail.get('desc', '')
                })
            except: continue

        # ---------------------------------------------------------
        # [정렬 로직] 전체 리스트를 먼저 정렬
        # ---------------------------------------------------------
        is_reverse = (direction == 'desc')
        
        if sort_by:
            is_reverse = (direction == 'desc')
            if sort_by in ['unit_price', 'total_price', 'count', 'sort_val']:
                all_items.sort(key=lambda x: x.get(sort_by, 0), reverse=is_reverse)
            else:
                all_items.sort(key=lambda x: str(x.get(sort_by, '')), reverse=is_reverse)

    except Exception as e:
        print(f"🚨 Redis Error: {e}")

    # [페이지네이션] 정렬이 완료된 리스트를 자름
    total_count = len(all_items)
    total_pages = math.ceil(total_count / page_size)
    if page < 1: page = 1
    if page > total_pages and total_pages > 0: page = total_pages

    start_idx = (page - 1) * page_size
    paginated_items = all_items[start_idx : start_idx + page_size]
    
    # 출력용 콤마 포맷팅
    for item in paginated_items:
        # 1. 개당 가격 콤마
        u_price = item.get('unit_price', 0)
        item['unit_price_display'] = format(int(u_price), ',')
        
        # 2. 총 금액 계산 및 콤마
        count = item.get('count', 0)
        item['total_price_display'] = format(int(u_price * count), ',')

    return render(request, 'index.html', {
        'items': paginated_items, 
        'alerts': alerts,
        'current_page': page,
        'total_pages': total_pages,
        'page_range': range(1, total_pages + 1),
        'sort_by': sort_by or '',
        'direction': direction,
        'total_count': total_count
    })

# ---------------------------------------------------------
# 4. 아이템 상세 페이지
# ---------------------------------------------------------
def item_detail(request, item_name):
    auction_csv = os.path.join(settings.BASE_DIR, 'static', 'test_data', 'ex_data.csv')
    item_details_map = get_item_details_map()
    
    detail = item_details_map.get(item_name, {})
    item_info = {
        "name": item_name,
        "desc": detail.get('desc', '설명 정보가 없습니다.'),
        "thumb": detail.get('thumb') if detail.get('thumb') and detail.get('thumb') != "NULL" else 'images/mabi_logo.jpg'
    }

    current_listings = []
    if os.path.exists(auction_csv):
        try:
            with open(auction_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('아이템 이름', '') == item_name:
                        time_raw = row.get('남은시간', '0_0')
                        try:
                            h, m = map(int, time_raw.split('_'))
                            sort_val = h * 60 + m
                            display_time = f"{h}시간 {m}분"
                        except:
                            sort_val = 0
                            display_time = "정보없음"

                        current_listings.append({
                            'count': row.get('수량'),
                            'unit_price': format(int(row.get('개당금액') or 0), ','),
                            'total_price': format(int(row.get('총 금액') or 0), ','),
                            'display_time': display_time,
                            'sort_val': sort_val
                        })
            current_listings.sort(key=lambda x: x['sort_val'])
        except: pass

    return render(request, 'item_detail.html', {
        'item': item_info,
        'listings': current_listings
    })