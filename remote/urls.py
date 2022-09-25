
from django.conf.urls import url, include
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

""" BEGIN new remote using ViewSets and drf router """
from rest_framework.routers import DefaultRouter
router = DefaultRouter()
router.register('ds', views.DatasetViewSet)
router.register('coll', views.CollectionViewSet)
router.register('pl', views.PlaceViewSet)

""" END new remote"""

app_name = 'remote'


urlpatterns = [
    path('', include(router.urls)),
]