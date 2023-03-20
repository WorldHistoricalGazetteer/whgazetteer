
from django.conf.urls import include
from django.urls import path
from . import views

""" BEGIN new remote using ViewSets and drf router """
from rest_framework.routers import DefaultRouter
router = DefaultRouter()
router.register('ds', views.DatasetViewSet)
router.register('coll', views.CollectionViewSet)
router.register('pl', views.PlaceViewSet)
router.register('types', views.TypeViewSet)

""" END new remote"""

app_name = 'remote'


urlpatterns = [
    path('', include(router.urls)),
]