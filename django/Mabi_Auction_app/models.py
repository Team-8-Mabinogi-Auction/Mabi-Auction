from django.db import models


class CurrentListings(models.Model):
    item_id = models.CharField(max_length=64, primary_key=True) # 아이템 고유 해시 ID
    item_name = models.CharField(max_length=100, null=True)     # 아이템 이름
    item_display_name = models.CharField(max_length=100)        # 아이템 전체 이름 (검색 대상)
    item_count = models.IntegerField()                          # 수량
    item_category = models.CharField(max_length=50, null=True)  # 분류
    price = models.BigIntegerField()                            # 가격
    start_time = models.DateTimeField(null=True)                # 등록 시간
    expire_time = models.DateTimeField()                        # 만료 예정 시간
    auction_duration = models.IntegerField(null=True)           # 등록 기간
    options = models.JSONField(null=True)                       # 옵션 (JSONB -> JSONField)
    created_at = models.DateTimeField()                         # 데이터 호출 시각

    class Meta:
        db_table = '"mabi_raw"."current_listings"'
        managed = False


class MabiPriceStats(models.Model):
    item_name = models.CharField(max_length=255, primary_key=True)
    avg_registration_price = models.BigIntegerField()
    avg_sale_price = models.BigIntegerField()
    total_logs = models.IntegerField()
    analyzed_at = models.DateTimeField()

    class Meta:
        managed = False  # Django가 테이블을 새로 만들지 않도록 설정
        db_table = '"mabi_analytics"."mabi_item_price_averages:'  # 실제 DB 테이블 이름

# 1. 아이템 정적 정보 (이미지, 설명, 링크 등)
class ItemStaticInfo(models.Model):
    item_name = models.CharField(primary_key=True, max_length=200)
    item_desc = models.TextField(null=True)
    img_thumbnail = models.CharField(max_length=500, null=True)
    img_ingame = models.CharField(max_length=500, null=True)
    external_link = models.CharField(max_length=500, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False  # Django가 테이블을 생성/수정하지 않음
        db_table = '"mabi_raw"."item_info"' # 스키마.테이블명


# 2. 아이템 시간별 시세 통계 (차트용)
class ItemHourlyStats(models.Model):
    # 복합키지만 조회용으로 bucket_hour를 PK로 설정 (실제 쓰기 작업은 안 하므로 무관)
    bucket_hour = models.DateTimeField(primary_key=True)
    item_display_name = models.CharField(max_length=100)
    item_category = models.CharField(max_length=50)

    # 등록 가격 (Reg)
    reg_price_max = models.BigIntegerField(null=True)
    reg_price_min = models.BigIntegerField(null=True)
    reg_price_avg = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    reg_count = models.BigIntegerField(null=True)

    # 판매 가격 (Sold)
    sold_price_max = models.BigIntegerField(null=True)
    sold_price_min = models.BigIntegerField(null=True)
    sold_price_avg = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    sold_count = models.BigIntegerField(null=True)

    class Meta:
        managed = False
        db_table = '"mabi_analytics"."item_display_hourly_stats"'

class AuctionLog(models.Model):
    id = models.BigAutoField(primary_key=True)
    event_type = models.CharField(max_length=20)
    item_id = models.CharField(max_length=64)
    auction_buy_id = models.CharField(max_length=64, null=True)
    item_name = models.CharField(max_length=100, null=True)
    item_display_name = models.CharField(max_length=100)
    item_count = models.IntegerField()
    item_category = models.CharField(max_length=50)
    price = models.BigIntegerField()     # 판매가
    registered_at = models.DateTimeField(null=True)
    auction_duration = models.IntegerField(null=True)
    ended_at = models.DateTimeField()    # 판매 시각
    options = models.JSONField(null=True) # JSONB 필드
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = '"mabi_raw"."auction_logs"'