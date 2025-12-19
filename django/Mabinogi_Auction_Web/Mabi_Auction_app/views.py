import csv
import os
import json # JSON 처리를 위해 추가
from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings

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
    
    auction_csv = os.path.join(settings.BASE_DIR, 'static', 'test_data', 'ex_data.csv')
    item_details_map = get_item_details_map()
    warning_items = get_warning_items()

    all_items = []
    if os.path.exists(auction_csv):
        with open(auction_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('아이템 이름', '')
                if search_query and search_query not in name:
                    continue

                detail = item_details_map.get(name, {})
                processed_item = {
                    'name': name,
                    'count': row.get('수량'),
                    'unit_price': format(int(row.get('개당금액')), ','),
                    'total_price': format(int(row.get('총 금액')), ','),
                    'time_raw': row.get('남은시간'),
                    'display_img': detail.get('thumb') if detail.get('thumb') and detail.get('thumb') != "NULL" else 'images/mabi_logo.jpg',
                    'desc': detail.get('desc', '')
                }
                try:
                    h, m = map(int, processed_item['time_raw'].split('_'))
                    processed_item['sort_val'] = h * 60 + m
                    processed_item['display_time'] = f"{h}시간 {m}분"
                except:
                    processed_item['sort_val'] = 0
                    processed_item['display_time'] = "정보없음"
                all_items.append(processed_item)

            all_items.sort(key=lambda x: x.get('sort_val', 0), reverse=True)

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
                        'unit_price': format(int(row.get('개당금액')), ','),
                        'total_price': format(int(row.get('총 금액')), ','),
                        'display_time': display_time,
                        'sort_val': sort_val,
                        'raw_price': int(row.get('개당금액'))
                    })
    
    current_listings.sort(key=lambda x: x['sort_val'])

    return render(request, 'item_detail.html', {
        'item': item_info,
        'listings': current_listings,
        'search_query': ''
    })