# api.urls

from django.conf.urls import url, include
from django.urls import path
from rest_framework import routers
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

# TODO: refactor as for datasets, users
# ne way or the other
#router = routers.DefaultRouter()

#router.register(r'places', views.PlaceViewSet)
#router.register(r'geoms', views.GeomViewSet)
#router.register(r'areas', views.AreaViewSet)
#router.register(r'datasets', views.DatasetViewSet)
#router.register(r'users', views.UserViewSet)
#router.register(r'groups', views.GroupViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    #url(r'^', include(router.urls)),
    #url('api-auth/', include('rest_framework.urls', namespace='rest_framework')),

    # single union record, ?idx=whg02&_id={whg_id}
    # TODO: build from place_id
    url('union/', views.indexAPIView.as_view(), name='union_api'),

    path('api-auth/', include('rest_framework.urls')),
    path('', views.api_root),

    path('datasets/', views.DatasetList.as_view(),name='dataset-list'),    
    path('datasets/<int:pk>/', views.DatasetDetail.as_view(),name='dataset-detail'),

    # drf table browsing dataset places
    path('placetable/', views.PlaceTableViewSet.as_view({'get':'list'}),name='place-table'), 
    # places in a dataset
    path('places/<int:pk>/', views.PlaceList.as_view(),name='place-list'), 
    # a place
    path('place/<int:pk>/', views.PlaceDetail.as_view(),name='place-detail'),    

    path('geoms/', views.GeomViewSet.as_view({'get':'list'}),name='geom-list'),    

    path('areas/<int:pk>/', views.AreaViewSet.as_view({'get': 'retrieve'}),name='area-detail'),    
    #path('areas/<int:pk>/', views.AreaViewSet.as_view(),name='area-detail'),    
    
    path('users/', views.UserList.as_view(),name='user-list'),
    path('users/<int:pk>/', views.UserDetail.as_view(),name='user-detail'),    
]

#urlpatterns = format_suffix_patterns(urlpatterns)