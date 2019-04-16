# maps.urls
from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views

urlpatterns = [
    path('', views.maps_home, name="maps_home"),
    path('<str:mid>/view/', views.map_view, name="map_view"),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)


    # path('<int:product_id>/', views.detail, name='detail'),
    # path('<int:product_id>/upvote', views.upvote, name='upvote'),
    # path('create', views.create, name='create'),
