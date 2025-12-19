from django.db import models

class MabiPriceStats(models.Model):
    # Airflow가 Postgres에 만든 테이블 컬럼과 똑같이 맞춥니다.
    item_name = models.CharField(max_length=255, primary_key=True)
    avg_registration_price = models.BigIntegerField()
    avg_sale_price = models.BigIntegerField()
    total_logs = models.IntegerField()
    analyzed_at = models.DateTimeField()

    class Meta:
        managed = False  # Django가 테이블을 새로 만들지 않도록 설정
        db_table = 'mabi_item_price_averages'  # 실제 DB 테이블 이름