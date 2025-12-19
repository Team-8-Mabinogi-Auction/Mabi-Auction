import csv
import os
import json # JSON 처리를 위해 추가
from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings
from .models import MabiPriceStats

# 공통 함수: 상세 설명 및 썸네일 맵 로드 (기존 유지)
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

# [신규 추가] 사재기 경고 아이템 로드 함수
def get_warning_items():
    json_path = os.path.join(settings.BASE_DIR, 'static', 'test_data', 'warning_items.json')
    warning_items = []
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                warning_items = json.load(f)
        except: pass
    return warning_items

# [1] 메인 페이지 뷰
def index(request):
    search_query = request.GET.get('search', '').strip()
    offset = int(request.GET.get('offset', 0))
    
    # --- [수정] 첫 로딩(offset 0) 시 100개, 이후 300개 ---
    limit = 100 if offset == 0 else 300
    
    item_details_map = get_item_details_map()
    warning_items = get_warning_items()

    all_items = []
    
    # --- [데이터 연결 수정: CSV 대신 DB 조회] ---
    if search_query:
        db_stats = MabiPriceStats.objects.filter(item_name__contains=search_query).order_by('-analyzed_at')
    else:
        db_stats = MabiPriceStats.objects.all().order_by('-analyzed_at')

    for stat in db_stats:
        name = stat.item_name.strip()
        detail = item_details_map.get(name, {})
        processed_item = {
            'name': name,
            'count': stat.total_logs,
            'unit_price': format(stat.avg_sale_price, ','),
            'total_price': format(stat.avg_registration_price, ','),
            'time_raw': stat.analyzed_at.strftime('%H_%M'),
            'display_img': detail.get('thumb') if detail.get('thumb') and detail.get('thumb') != "NULL" else 'images/mabi_logo.jpg',
            'desc': detail.get('desc', ''),
            'sort_val': 0,
            'display_time': stat.analyzed_at.strftime('%m-%d %H:%M')
        }
        all_items.append(processed_item)
    # ------------------------------------------

    paginated_items = all_items[offset : offset + limit]
    has_more = len(all_items) > (offset + limit)

    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        return JsonResponse({'items': paginated_items, 'has_more': has_more})

    return render(request, 'index.html', {
        'items': paginated_items, 
        'warning_items': warning_items,
        'search_query': search_query,
        'has_more': has_more
    })

# [2] 상세 페이지 뷰 (기존 유지)
def item_detail(request, item_name):
    item_details_map = get_item_details_map()
    
    detail = item_details_map.get(item_name, {})
    item_info = {
        "name": item_name,
        "desc": detail.get('desc', '설명 정보가 없습니다.'),
        "thumb": detail.get('thumb') if detail.get('thumb') and detail.get('thumb') != "NULL" else 'images/mabi_logo.jpg'
    }

    current_listings = []
    
    # --- [데이터 연결 수정: CSV 대신 DB 조회] ---
    price_stats = MabiPriceStats.objects.filter(item_name__contains=item_name.strip()).order_by('-analyzed_at').first()
    
    if price_stats:
        current_listings.append({
            'count': price_stats.total_logs,
            'unit_price': format(price_stats.avg_sale_price, ','),
            'total_price': format(price_stats.avg_registration_price, ','),
            'display_time': price_stats.analyzed_at.strftime('%m-%d %H:%M'),
            'sort_val': 0,
            'raw_price': price_stats.avg_sale_price
        })
    # ------------------------------------------

    return render(request, 'item_detail.html', {
        'item': item_info,
        'listings': current_listings,
        'search_query': '',
        'price_stats': price_stats
    })