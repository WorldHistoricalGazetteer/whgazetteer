# api.urls

from django.conf.urls import url, include
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

# app_name = "api"

urlpatterns = [
    path('api-auth/', include('rest_framework.urls')),
    #path('', views.api_root),

    # *** SEARCH (external) ***

    # database places
    path('db/',views.SearchAPIView.as_view(),name='api-search'),
    # index docs
    path('index/',views.IndexAPIView.as_view(),name='api-index-search'),
    # spatial (nearby or bbox)
    path('spatial/', views.SpatialAPIView.as_view(), name='api-spatial'),
    # traces (deprecated)
    # path('traces/', views.TracesAPIView.as_view(), name='traces-search'),

    # *** DATASETS ***

    # use: filter public datasets by id, label, term
    path('datasets/', views.DatasetAPIView.as_view(), name='dataset-list'),
    

    # *** PLACES ***
        
    # use: single place for ds_browse:: PlaceSerializer
    # also search.html if search scope = 'db'
    path('place/<int:pk>/', views.PlaceDetailAPIView.as_view(), name='place-detail'),    

    # places in a dataset
    # use: drf table in ds_browse  :: PlaceSerializer
    path('placetable/', views.PlaceTableViewSet.as_view({'get':'list'}), name='place-table'),
    # places in a collection
    path('placetable_coll/', views.PlaceTableCollViewSet.as_view({'get':'list'}), name='place-table-coll'), 

    # TODO: place/<str:dslabel>/<str:src_id>
    path('place/<str:dslabel>/<str:src_id>/', views.PlaceDetailSourceAPIView.as_view(),name='place-detail-src'), 


    # 
    # *** GEOMETRY ***
    # 
    # use: map in ds_browse, ds_places, collection_places :: PlaceGeomSerializer
    path('geoms/', views.GeomViewSet.as_view({'get':'list'}), name='geom-list'),
    # use: heatmap sources for collection_places.html
    path('geojson/', views.GeoJSONAPIView.as_view(), name='geojson'),    

    
    # 
    # *** AREAS ***
    # 
    # use: single area in dataset.html#addtask
    path('area/<int:pk>/', views.AreaViewSet.as_view({'get': 'retrieve'}),name='area-detail'),
    # returns list of simple objects (id, title) for home>autocomplete
    path('area_list/', views.AreaListView.as_view(),name='area-list'),
    # geojson for api
    path('area_features/', views.AreaFeaturesView.as_view(),name='area-features'), 
    
    # only UN regions, for teaching
    path('regions/', views.RegionViewSet, name='regions'),

    # 
    # *** USERS ***
    #   
    path('users/', views.UserList.as_view(),name='user-list'),
    path('user/<int:pk>/', views.UserDetail.as_view(),name='user-detail'),
    
    # 
    # *** INDEX ***
    # 
    # use: single union record in usingapi.html ?idx=whg&_id={whg_id}
    # TODO: build from place_id
    #url('union/', views.indexAPIView.as_view(), name='union_api')
    
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'tsv', 'geojson'])
