import csv
import os
import json
import math
import urllib.parse
from datetime import datetime, timedelta

from django.shortcuts import render
from django.core.paginator import Paginator
from django_redis import get_redis_connection
from django.conf import settings
from django.db.models import Q

# models.py에서 정의한 모든 모델 import
from .models import *

# CDN 도메인 주소 로드
CDN_BASE_URL = os.getenv('CDN_BASE_URL', '')

# ---------------------------------------------------------
# 1. 헬퍼 함수들 (데이터 로드, 파싱)
# ---------------------------------------------------------

def get_item_details_map():
    """
    [Index 페이지용] CSV에서 아이템 상세 설명 및 썸네일 경로 로드
    (상세 페이지에서는 이제 사용하지 않지만, 메인 페이지를 위해 남겨둠)
    """
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

def get_recent_alerts(limit=5):
    """Redis에서 실시간 사재기 알림 로드"""
    alerts = []
    con = get_redis_connection("default")
    raw_list = con.lrange("view:recent_alerts", 0, limit - 1)
    
    # 시스템 시간 사용 (Naive)
    now = datetime.now()

    for item in raw_list:
        try:
            data = json.loads(item.decode('utf-8'))
            
            ts_str = data.get('timestamp', '')
            if ts_str:
                dt_obj = datetime.fromisoformat(ts_str)
                # Redis 데이터가 Timezone 정보를 가지고 있다면 제거
                if dt_obj.tzinfo is not None:
                    dt_obj = dt_obj.replace(tzinfo=None)

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

def format_mabi_options(options_data):
    """
    JSON 리스트 형태의 옵션을 템플릿용 객체 리스트로 변환
    """
    if not options_data or not isinstance(options_data, list):
        return []

    formatted_list = []
    for opt in options_data:
        if not isinstance(opt, dict):
            continue

        o_type = opt.get('option_type', '')
        o_sub = opt.get('option_sub_type', '')
        val1 = opt.get('option_value', '')
        val2 = opt.get('option_value2', '')
        
        color_css = None

        # 색상 옵션 전처리
        if '색' in str(o_type) or '색' in str(o_sub):
            if isinstance(val1, str) and ',' in val1:
                try:
                    rgb_parts = [x.strip() for x in val1.split(',')]
                    val1 = ", ".join(rgb_parts)
                    if len(rgb_parts) >= 3:
                        color_css = f"rgb({rgb_parts[0]}, {rgb_parts[1]}, {rgb_parts[2]})"
                except:
                    pass

        text_parts = []
        if o_type:
            text_parts.append(f"[{o_type}]")
        if o_sub:
            text_parts.append(o_sub)
        
        value_part = str(val1)
        if val2:
            value_part += f" ({val2})"
        
        if value_part:
            text_parts.append(f": {value_part}")
        
        full_text = " ".join(text_parts)

        formatted_list.append({
            'text': full_text,
            'color_css': color_css
        })
    
    return formatted_list


# ---------------------------------------------------------
# 2. 메인 페이지 뷰 (index)
# ---------------------------------------------------------
def index(request):
    search_query = request.GET.get('search', '').strip()
    category_query = request.GET.get('category', '').strip()
    sort_by = request.GET.get('sort', '')
    direction = request.GET.get('dir', 'asc')
    page_number = request.GET.get('page', 1)

    item_details_map = get_item_details_map()
    alerts = get_recent_alerts(limit=5)

    # Redis: 마지막 업데이트 시간
    con = get_redis_connection("default")
    last_update_raw = con.get("status:last_cycle_time") 
    last_update_text = "-"
    
    if last_update_raw:
        try:
            ts_str = last_update_raw.decode('utf-8')
            last_dt = datetime.fromisoformat(ts_str)
            last_update_text = f"마지막 업데이트 : {last_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        except: pass
    
    all_items = []

    # [A] DB 조회 (검색/카테고리)
    if search_query or category_query:
        try:
            qs = CurrentListings.objects.all()
            if search_query:
                qs = qs.filter(item_display_name__icontains=search_query)
            if category_query:
                qs = qs.filter(item_category=category_query)
            
            db_items = qs.order_by('price')
            current_ts = datetime.now().timestamp()

            for row in db_items:
                name_key = row.item_name if row.item_name else row.item_display_name
                detail = item_details_map.get(name_key, {})
                filename = f"{name_key}.png"
                encoded_filename = urllib.parse.quote(filename)
                
                # 남은 시간 계산
                display_time = "정보없음"
                sort_val = 9999999999
                
                if hasattr(row, 'expire_time') and row.expire_time:
                    try:
                        expire_ts = row.expire_time.timestamp()
                        diff = expire_ts - current_ts
                        if diff > 0:
                            h, m = int(diff // 3600), int((diff % 3600) // 60)
                            display_time = f"{h}시간 {m}분"
                            sort_val = diff
                        else:
                            display_time = "종료됨"
                            sort_val = -1
                    except: pass
                elif row.start_time:
                     display_time = row.start_time.strftime('%Y-%m-%d %H:%M')

                all_items.append({
                    'name': row.item_display_name,
                    'category': row.item_category or '기타',
                    'count': row.item_count,
                    'unit_price': row.price,
                    'total_price': row.price * row.item_count,
                    'options': row.options,
                    'display_time': display_time,
                    'sort_val': sort_val,
                    'display_img': f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}",
                    'desc': detail.get('desc', '')
                })
        except Exception as e:
            print(f"🚨 Postgres Filter Error: {e}")

    # [B] Redis 조회 (전체 실시간)
    else:
        try:
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
                    options = row.get('item_option')
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

    # [정렬 및 페이징]
    is_reverse = (direction == 'desc')
    if sort_by:
        is_reverse = (direction == 'desc')
        if sort_by in ['unit_price', 'total_price', 'count', 'sort_val']:
            all_items.sort(key=lambda x: x.get(sort_by, 0), reverse=is_reverse)
        else:
            all_items.sort(key=lambda x: str(x.get(sort_by, '')), reverse=is_reverse)

    paginator = Paginator(all_items, 20)
    page_obj = paginator.get_page(page_number)

    # 콤마 포맷팅
    for item in page_obj:
        u_price = item.get('unit_price', 0)
        item['unit_price_display'] = format(int(u_price), ',')
        t_price = item.get('total_price', 0)
        item['total_price_display'] = format(int(t_price), ',')

    return render(request, 'index.html', {
        'page_obj': page_obj,
        'alerts': alerts,
        'sort_by': sort_by or '',
        'direction': direction,
        'total_count': len(all_items),
        'last_update_text': last_update_text,
        'search_query': search_query,
        'current_category': category_query, 
    })


# ---------------------------------------------------------
# 3. 아이템 상세 페이지 (item_detail) - CSV 제거 / DB 기반 로직 적용
# ---------------------------------------------------------
def item_detail(request, item_name):
    """
    item_name: URL 파라미터로 넘어온 '풀네임' (예: "고대의 마력의 롱소드")
    """

    # ---------------------------------------------------------
    # 1. 베이스 이름(순수 아이템명) 추출 - 오직 DB만 사용
    # ---------------------------------------------------------
    base_name = item_name  # 기본값: 넘어온 이름 그대로 (매칭 실패 시 대비)
    
    # [1단계] 현재 매물 테이블에서 순수 이름(item_name 컬럼) 찾기
    listing_obj = CurrentListings.objects.filter(item_display_name=item_name).first()
    if listing_obj and listing_obj.item_name:
        base_name = listing_obj.item_name

    # [2단계] 없다면 과거 판매 내역에서 순수 이름 찾기
    # 1단계에서 못 찾았거나, 원래 이름과 같은 경우(접두어가 없을 수도 있음)
    if base_name == item_name: 
        history_obj = AuctionLog.objects.filter(item_display_name=item_name).first()
        if history_obj and history_obj.item_name:
            base_name = history_obj.item_name
            
    # 이제 base_name은 접두/접미어가 제거된 "롱소드" 형태가 됩니다.

    # ---------------------------------------------------------
    # 2. 정적 정보(이미지, 설명) 조회 - ItemStaticInfo DB 사용
    # ---------------------------------------------------------
    item_desc = "아이템 상세 설명이 없습니다."
    external_link = ""
    ingame_img_url = None
    thumbnail_url = ""

    try:
        # 추출한 base_name으로 정적 정보 조회
        static_info = ItemStaticInfo.objects.get(item_name=base_name)
        
        # 썸네일 이미지
        if static_info.img_thumbnail:
            # DB에 저장된 경로가 있다면 사용
            # (보통 DB에 경로가 없으면 이름으로 생성하는 규칙을 따름)
            filename = f"{base_name}.png"
            encoded_filename = urllib.parse.quote(filename)
            thumbnail_url = f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}"
        
        # 아이템 설명
        if static_info.item_desc:
            item_desc = static_info.item_desc.strip()
            
        # 외부 링크
        external_link = static_info.external_link

        # 인게임 이미지
        if static_info.img_ingame:
            raw_filename = os.path.basename(str(static_info.img_ingame))
            encoded_filename = urllib.parse.quote(raw_filename)
            ingame_img_url = f"{CDN_BASE_URL}item_images/{encoded_filename}"

    except ItemStaticInfo.DoesNotExist:
        # DB에 정적 정보가 없는 경우 (신규 아이템 등)
        pass

    # [안전장치] 여전히 썸네일 URL이 없다면, base_name 기준으로 URL 강제 생성
    # (Cloudflare R2 등에 파일은 있는데 DB에 메타데이터만 없는 경우 대비)
    if not thumbnail_url:
        filename = f"{base_name}.png"
        encoded_filename = urllib.parse.quote(filename)
        thumbnail_url = f"{CDN_BASE_URL}thumbnail_images/{encoded_filename}"


    # ---------------------------------------------------------
    # 3. 매물 및 차트 데이터 조회 (기존 로직 유지)
    # ---------------------------------------------------------
    
    # [주의] 매물 필터링은 'item_name'(풀네임)을 사용해야 정확한 인챈트 매물이 나옴
    current_listings = CurrentListings.objects.filter(item_display_name=item_name).order_by('price')
    sales_history = AuctionLog.objects.filter(item_display_name=item_name, event_type='SOLD').order_by('-ended_at')

    # 시간 설정 (USE_TZ=False 가정, Naive Time)
    now = datetime.now()

    # 카테고리 정보 확보
    category = "기타"
    if current_listings.exists():
        category = current_listings[0].item_category
    elif sales_history.exists():
        category = sales_history[0].item_category

    # 옵션 유무 판단
    listings_has_opt = current_listings.exclude(options__isnull=True).exclude(options__exact='').exists()
    history_has_opt = sales_history.exclude(options__isnull=True).exclude(options__exact='').exists()
    has_options = listings_has_opt or history_has_opt

    # 객체에 파싱된 옵션 및 시간 정보 붙이기
    for item in current_listings:
        # 옵션 파싱 (JSON 문자열 -> List)
        raw_options = item.options
        if isinstance(raw_options, str):
            try:
                raw_options = json.loads(raw_options)
            except ValueError:
                raw_options = []
        
        if raw_options:
            item.parsed_options = format_mabi_options(raw_options)
        else:
            item.parsed_options = []
        
        # 남은 시간 계산
        if item.expire_time:
            diff = item.expire_time - now
            total_seconds = int(diff.total_seconds())

            if total_seconds > 0:
                days = total_seconds // 86400
                hours = (total_seconds % 86400) // 3600
                minutes = (total_seconds % 3600) // 60
                
                if days > 0:
                    item.display_time = f"{days}일 {hours}시간"
                elif hours > 0:
                    item.display_time = f"{hours}시간 {minutes}분"
                else:
                    item.display_time = f"{minutes}분 전 (마감임박)"
                    item.is_urgent = True 
            else:
                item.display_time = "만료됨"
        else:
            item.display_time = "-"

    for item in sales_history:
        # 판매 이력 옵션 파싱
        raw_options = item.options
        if isinstance(raw_options, str):
            try:
                raw_options = json.loads(raw_options)
            except ValueError:
                raw_options = []
        
        if raw_options:
            item.parsed_options = format_mabi_options(raw_options)
        else:
            item.parsed_options = []

    # 4. 차트 데이터 생성 (Naive Time 사용)
    seven_days_ago = now - timedelta(days=7)
    
    stats_qs = ItemHourlyStats.objects.filter(
        item_display_name=item_name, 
        bucket_hour__gte=seven_days_ago
    ).order_by('bucket_hour')
    
    chart_data = {
        'dates': [], 'timestamps': [],
        'reg_avg': [], 'reg_min': [], 'reg_max': [], 'reg_count': [],
        'sold_avg': [], 'sold_min': [], 'sold_max': [], 'sold_count': []
    }
    
    for s in stats_qs:
        chart_data['dates'].append(s.bucket_hour.strftime('%m-%d %H:%M'))
        chart_data['timestamps'].append(s.bucket_hour.timestamp()) 
        
        # 등록 데이터
        chart_data['reg_avg'].append(int(s.reg_price_avg) if s.reg_price_avg is not None else None)
        chart_data['reg_min'].append(int(s.reg_price_min) if s.reg_price_min is not None else None)
        chart_data['reg_max'].append(int(s.reg_price_max) if s.reg_price_max is not None else None)
        chart_data['reg_count'].append(int(s.reg_count) if s.reg_count is not None else 0)
        
        # 판매 데이터
        chart_data['sold_avg'].append(int(s.sold_price_avg) if s.sold_price_avg is not None else None)
        chart_data['sold_min'].append(int(s.sold_price_min) if s.sold_price_min is not None else None)
        chart_data['sold_max'].append(int(s.sold_price_max) if s.sold_price_max is not None else None)
        chart_data['sold_count'].append(int(s.sold_count) if s.sold_count is not None else 0)

    # JSON 변환
    serialized_chart_data = {
        'dates': json.dumps(chart_data['dates']),
        'timestamps': json.dumps(chart_data['timestamps']), 
        'reg_avg': json.dumps(chart_data['reg_avg']),
        'reg_min': json.dumps(chart_data['reg_min']),
        'reg_max': json.dumps(chart_data['reg_max']),
        'reg_count': json.dumps(chart_data['reg_count']),
        'sold_avg': json.dumps(chart_data['sold_avg']),
        'sold_min': json.dumps(chart_data['sold_min']),
        'sold_max': json.dumps(chart_data['sold_max']),
        'sold_count': json.dumps(chart_data['sold_count']),
    }

    # 5. Context 전달
    context = {
        'item_name': item_name,        # 화면 표시용 (풀네임)
        'base_item_name': base_name,   # (디버깅용) 순수 이름
        'item_category': category,
        'item_img': thumbnail_url,
        'ingame_img_url': ingame_img_url,
        'item_desc': item_desc,
        'external_link': external_link,
        
        'current_listings': current_listings,
        'sales_history': sales_history,
        
        'chart_data': serialized_chart_data,
        'has_options': has_options,
        
        # 템플릿 호환성을 위한 static_info 객체
        'static_info': {
             'img_thumbnail': thumbnail_url,
        }
    }

    return render(request, 'item_detail.html', context)