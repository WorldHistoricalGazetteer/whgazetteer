# api.urls

from django.conf.urls import url, include
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views


urlpatterns = [
    path('api-auth/', include('rest_framework.urls')),
    #path('', views.api_root),

    # *** SEARCH ***

    path('',views.SearchAPIView.as_view(),name='api-search'),
    path('index/',views.IndexAPIView.as_view(),name='api-index-search'),
    
    # experiment
    path('filter/',views.FilteredSearchAPIView.as_view(),name='api-filtered-search'),

    # *** DATASETS ***

    # use > list public datasets in usingapi.html :: DatasetSerializer
    path('datasets/', views.DatasetAPIView.as_view(), name='dataset-list'),
    # use > single dataset record in usingapi.html :: DatasetSerializer
    path('dataset/<int:pk>/', views.DatasetDetailAPIView.as_view(),name='dataset-detail'),
    
    # FOR DOWNLOAD
    # db places in a dataset, lp format :: PlaceSerializer
    #path('dataset/<str:dslabel>/lpf/', views.DownloadDatasetAPIView.as_view(),name='dataset-lpf'), 
    
    # simple geojson :: FeatureSerializer
    #path('dataset/<int:ds>/geom/', views.DownloadGeomsAPIView.as_view(),name='dataset-geom'),


    # *** PLACES ***

    # all places in a dataset
    path('places/<str:dslabel>/', views.PlaceAPIView.as_view(),name='place-list'), 
        
    # use > single place for dataset.html#browse:: PlaceSerializer
    path('place/<int:pk>/', views.PlaceDetailAPIView.as_view(),name='place-detail'),    

    # use > drf table in dataset.html#browse  :: PlaceSerializer
    path('placetable/', views.PlaceTableViewSet.as_view({'get':'list'}),name='place-table'), 

    # use > map in dataset.html#browse :: PlaceGeomSerializer
    path('geoms/', views.GeomViewSet.as_view({'get':'list'}),name='geom-list'),    

    # 
    # *** AREAS ***
    # 
    # use > single area in dataset.html#addtask
    path('area/<int:pk>/', views.AreaViewSet.as_view({'get': 'retrieve'}),name='area-detail'),    
    path('areas/', views.AreasListView.as_view(),name='area-list'),    
    
    # 
    # *** USERS ***
    #   
    path('users/', views.UserList.as_view(),name='user-list'),
    path('users/<int:pk>/', views.UserDetail.as_view(),name='user-detail'),
    
    # 
    # *** INDEX ***
    # 
    # use > single union record in usingapi.html ?idx=whg&_id={whg_id}
    # TODO: build from place_id
    url('union/', views.indexAPIView.as_view(), name='union_api')
    
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'tsv', 'geojson'])