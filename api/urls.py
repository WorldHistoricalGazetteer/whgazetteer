# api.urls

from django.conf.urls import url, include
from rest_framework import routers
from . import views

# TODO: too much of a black box
router = routers.DefaultRouter()

router.register(r'datasets', views.DatasetViewSet)
router.register(r'places', views.PlaceViewSet)
router.register(r'geoms', views.GeomViewSet)
router.register(r'areas', views.AreaViewSet)
router.register(r'users', views.UserViewSet)
router.register(r'groups', views.GroupViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    url(r'^union$', views.union, name="union_index")
]
