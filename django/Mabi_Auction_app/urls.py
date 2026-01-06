from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),  # 메인 리스트
    path('item/<str:item_name>/', views.item_detail, name='item_detail'),  # 상세 페이지
]