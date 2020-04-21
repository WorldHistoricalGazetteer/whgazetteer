# api.urls

from django.conf.urls import url, include
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views


urlpatterns = [
    path('api-auth/', include('rest_framework.urls')),
    path('', views.api_root),

    # 
    # *** DATASETS ***
    # 
    # public datasets
    path('datasets/', views.DatasetAPIView.as_view(), name='dataset-list'),
    # single dataset record
    path('dataset/<int:pk>/', views.DatasetDetailAPIView.as_view(),name='dataset-detail'),
    # geom
    path('dataset/<int:pk>/geom/', views.DownloadGeomViewSet.as_view({'get':'list'}),name='dataset-geom'),


    # 
    # *** PLACES ***
    # 
    # search all places
    #path('places/', views.PlaceAPIView.as_view(),name='place-list'), 
    
    # places in a dataset
    # TODO: repurpose for full download
    path('places/<str:dslabel>/', views.PlaceAPIView.as_view(),name='place-list'), 
    # LP format FeatureCollection
    path('features/<str:dslabel>/', views.FeatureAPIView.as_view(),name='feature-list'), 

    # a place
    path('place/<int:pk>/', views.PlaceDetailAPIView.as_view(),name='place-detail'),    

    # drf table in dataset.html#browse
    path('placetable/', views.PlaceTableViewSet.as_view({'get':'list'}),name='place-table'), 

    # geometry for map in dataset.html#browse
    path('geoms/', views.GeomViewSet.as_view({'get':'list'}),name='geom-list'),    

    # 
    # *** AREAS ***
    # 
    path('areas/<int:pk>/', views.AreaViewSet.as_view({'get': 'retrieve'}),name='area-detail'),    
    
    # 
    # *** USERS ***
    #   
    path('users/', views.UserList.as_view(),name='user-list'),
    path('users/<int:pk>/', views.UserDetail.as_view(),name='user-detail'),
    
    # 
    # *** INDEX ***
    # 
    # single union record, ?idx=whg02&_id={whg_id}
    # TODO: build from place_id
    url('union/', views.indexAPIView.as_view(), name='union_api')
    
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'tsv', 'geojson'])