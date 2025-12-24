import csv
import os
import json
import math
import urllib.parse
from datetime import datetime, timedelta, timezone

from django.shortcuts import render
from django.core.paginator import Paginator # 페이지네이션 클래스 사용
from django_redis import get_redis_connection
from django.conf import settings
from django.db.models import Q
from .models import *

CDN_BASE_URL = os.getenv('CDN_BASE_URL', '')

# ---------------------------------------------------------
# 1. 아이템 상세 정보 및 썸네일 맵 로드 (그대로 유지)
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
# 2. 실시간 사재기 알림 로드 (그대로 유지)
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
            
            ts_str = data.get('timestamp', '')
            if ts_str:
                dt_obj = datetime.fromisoformat(ts_str)
                diff = now - dt_obj
                total_seconds = int(diff.total_seconds())
                if total_seconds < 60: data['display_time'] = "방금 전"
                elif total_seconds < 3600: data['display_time'] = f"{total_seconds // 60}분 전"
                else: data['display_time'] = f"{total_seconds // 3600}시간 전"

            ratio = data.get('ratio', 0)
            reason_code = data.get('reason')

            if reason_code == "MANIPULATION":
                data['level_text'] = "경고"
                data['level_class'] = "badge-danger"
                data['reason_text'] = f"평균 대비 {ratio:.0f}배 높은 가격에 등록되었습니다."
                data['badge_tag'] = "시세조작"

            elif reason_code == "SWEEPING":
                velocity = data.get('velocity_10min', 0)
                data['level_text'] = "의심"
                data['level_class'] = "badge-warn"
                data['reason_text'] = f"{velocity}개의 매물이 소진되었습니다. ({ratio*100:.0f}% 소진)"
                data['badge_tag'] = "대량구매"

            alerts.append(data)
        except Exception as e:
            print(f"Parsing Error: {e}")
            continue
            
    return alerts

# ---------------------------------------------------------
# 3. 메인 페이지 뷰 (로직 수정됨)
# ---------------------------------------------------------
def index(request):
    # 파라미터 수집
    search_query = request.GET.get('search', '').strip()
    category_query = request.GET.get('category', '').strip()
    sort_by = request.GET.get('sort', '')
    direction = request.GET.get('dir', 'asc')
    
    # 페이지 번호 안전하게 가져오기
    page_number = request.GET.get('page', 1)

    item_details_map = get_item_details_map()
    alerts = get_recent_alerts(limit=5)

    # 마지막 업데이트 시간
    con = get_redis_connection("default")
    last_update_raw = con.get("status:last_cycle_time") 
    
    # 기본값 설정
    last_update_text = "-"
    
    if last_update_raw:
        try:
            # 1. Redis에서 문자열 읽기 (ISO 포맷)
            ts_str = last_update_raw.decode('utf-8')
            
            # 2. datetime 객체로 변환
            last_dt = datetime.fromisoformat(ts_str)
            
            # 3. 원하는 포맷으로 문자열 변환 (년-월-일 시:분:초)
            # 예: "마지막 업데이트 : 2025-12-22 14:30:00"
            formatted_time = last_dt.strftime('%Y-%m-%d %H:%M:%S')
            last_update_text = f"마지막 업데이트 : {formatted_time}"
                
        except Exception as e:
            print(f"Date Parse Error: {e}")
            pass
    
    # 통합될 아이템 리스트
    all_items = []

    # ---------------------------------------------------------
    # [A] 검색어 OR 카테고리 존재 -> Postgres 조회
    # ---------------------------------------------------------
    if search_query or category_query:
        try:
            qs = CurrentListings.objects.all()

            if search_query:
                qs = qs.filter(item_display_name__icontains=search_query)
            if category_query:
                qs = qs.filter(item_category=category_query)

            db_items = qs.order_by('price') # 가격 싼 순서로 기본 정렬 추천

            # 현재 시간 (Timestamp로 통일하여 계산)
            current_ts = datetime.now().timestamp()

            for row in db_items:
                name_key = row.item_name if row.item_name else row.item_display_name
                detail = item_details_map.get(name_key, {})
                filename = f"{name_key}.png"
                encoded_filename = urllib.parse.quote(filename)
                
                # [남은 시간 계산 로직] Redis와 동일하게 맞춤
                display_time = "정보없음"
                sort_val = 9999999999 # 시간 정보 없으면 맨 뒤로

                # 모델에 expire_time 필드가 있다고 가정
                # 만약 필드명이 date_auction_expire라면 row.date_auction_expire로 변경 필요
                if hasattr(row, 'expire_time') and row.expire_time:
                    try:
                        # DB의 datetime 객체를 timestamp로 변환
                        expire_ts = row.expire_time.timestamp()
                        diff = expire_ts - current_ts
                        
                        if diff > 0:
                            h = int(diff // 3600)
                            m = int((diff % 3600) // 60)
                            display_time = f"{h}시간 {m}분"
                            sort_val = diff
                        else:
                            display_time = "종료됨"
                            sort_val = -1
                    except: pass
                
                # 만약 expire_time은 없는데 start_time만 있다면 (예비책)
                elif row.start_time:
                     display_time = row.start_time.strftime('%Y-%m-%d %H:%M')

                all_items.append({
                    'name': row.item_display_name,
                    'category': row.item_category or '기타',
                    'count': row.item_count,
                    'unit_price': row.price,
                    'total_price': row.price * row.item_count,
                    'options': row.options,
                    'display_time': display_time, # 계산된 "N시간 N분"
                    'sort_val': sort_val,         # 정렬용 남은 초(seconds)
                    'display_img': f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}",
                    'desc': detail.get('desc', '')
                })

        except Exception as e:
            print(f"🚨 Postgres Filter Error: {e}")

    # ---------------------------------------------------------
    # [B] 조건 없음 -> Redis (전체 실시간) 조회
    # ---------------------------------------------------------
    else:
        try:
            con = get_redis_connection("default")
            redis_data = con.zrevrange("recent:items", 0, -1)
            current_time = datetime.now().timestamp()

            for raw_bytes in redis_data:
                try:
                    row = json.loads(raw_bytes.decode('utf-8'))
                    name = row.get('item_name')
                    if not name: continue

                    category = row.get('item_category') or row.get('category') or '기타'
                    unit_price = int(row.get('price_per_unit') or 0)
                    count = int(row.get('item_count') or 0)
                    options = row.get('options') # Redis에 옵션 정보가 있다면 가져옴

                    expire_str = row.get('expire_time')
                    display_time = "정보없음"
                    sort_val = 9999999999

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

                    detail = item_details_map.get(name, {})
                    filename = f"{name}.png"
                    encoded_filename = urllib.parse.quote(filename)
                    
                    all_items.append({
                        'name': name,
                        'category': category,
                        'count': count,
                        'unit_price': unit_price,
                        'total_price': unit_price * count,
                        'options': options,
                        'display_time': display_time,
                        'sort_val': sort_val,
                        'display_img': f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}",
                        'desc': detail.get('desc', '')
                    })
                except: continue
        except Exception as e:
            print(f"🚨 Redis Error: {e}")

    # ---------------------------------------------------------
    # [공통] 정렬 로직
    # ---------------------------------------------------------
    is_reverse = (direction == 'desc')
    if sort_by:
        is_reverse = (direction == 'desc')
        # 숫자형 필드와 문자형 필드 구분 정렬
        if sort_by in ['unit_price', 'total_price', 'count', 'sort_val']:
            all_items.sort(key=lambda x: x.get(sort_by, 0), reverse=is_reverse)
        else:
            all_items.sort(key=lambda x: str(x.get(sort_by, '')), reverse=is_reverse)

    # ---------------------------------------------------------
    # [공통] 페이지네이션 (Paginator 클래스 사용)
    # ---------------------------------------------------------
    paginator = Paginator(all_items, 20) # 20개씩
    page_obj = paginator.get_page(page_number)

    # 현재 페이지의 아이템들에만 콤마 포맷팅 적용 (UI 표시용)
    # page_obj는 순회 가능하므로 바로 가공 가능하지만, 딕셔너리이므로 원본 리스트가 수정됨
    for item in page_obj:
        u_price = item.get('unit_price', 0)
        item['unit_price_display'] = format(int(u_price), ',')
        
        t_price = item.get('total_price', 0)
        item['total_price_display'] = format(int(t_price), ',')

    return render(request, 'index.html', {
        'page_obj': page_obj,      # 템플릿에서 이걸 사용해 루프를 돕니다 ({% for item in page_obj %})
        'alerts': alerts,
        'sort_by': sort_by or '',
        'direction': direction,
        'total_count': len(all_items),
        'last_update_text': last_update_text,
        'search_query': search_query,
        'current_category': category_query, 
    })

# ---------------------------------------------------------
# 4. 아이템 상세 페이지 (DB 연동 + 페이지네이션 적용)
# ---------------------------------------------------------
def item_detail(request, item_name):
    # [1] 정적 정보 로드 (설명, 이미지)
    item_details_map = get_item_details_map()
    detail = item_details_map.get(item_name, {})
    
    item_info = {
        "name": item_name,
        "desc": detail.get('desc', '설명 정보가 없습니다.'),
        "thumb": detail.get('thumb') if detail.get('thumb') and detail.get('thumb') != "NULL" else 'images/mabi_logo.jpg'
    }

    # [2] DB에서 해당 아이템의 매물 조회 (CSV 제거)
    # 아이템 이름으로 필터링, 가격 낮은 순 정렬
    listings_qs = CurrentListings.objects.filter(item_display_name=item_name).order_by('price')
    
    # [3] 페이지네이션 설정 (20개씩)
    paginator = Paginator(listings_qs, 20)
    page_number = request.GET.get('page')
    
    # get_page는 잘못된 페이지 번호가 와도 알아서 처리해줌
    page_obj = paginator.get_page(page_number)

    # [4] 템플릿 렌더링
    return render(request, 'item_detail.html', {
        'item': item_info,
        'page_obj': page_obj,  # listings 대신 page_obj 전달
    })