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
        db_table = '"mabi_auction"."current_listings"'
        managed = False


class MabiPriceStats(models.Model):
    item_name = models.CharField(max_length=255, primary_key=True)
    avg_registration_price = models.BigIntegerField()
    avg_sale_price = models.BigIntegerField()
    total_logs = models.IntegerField()
    analyzed_at = models.DateTimeField()

    class Meta:
        managed = False  # Django가 테이블을 새로 만들지 않도록 설정
        db_table = 'mabi_item_price_averages'  # 실제 DB 테이블 이름