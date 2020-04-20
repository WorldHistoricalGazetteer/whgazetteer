# search/urls.py
from django.urls import path, include
from django.conf.urls import url

from search.views import (
    SearchView, FeatureContextView, TraceGeomView, LookupView, UpdateCountsView #advanced, 
)

urlpatterns = [
    #url(r'^tracegeom?$', TraceGeomView.as_view(), name='trace_geom'),
    url(r'^updatecounts?$', UpdateCountsView.as_view(), name='update_counts'),
    path('search/', SearchView.as_view(), name='search'), # home page search
    path('context/', FeatureContextView.as_view(), name='feature_context'), # place portal context
    path('tracegeom/', TraceGeomView.as_view(), name='trace_geom'), # trace features <- search & place portal
    #path('updatecounts/', UpdateCountsView.as_view(), name='update_counts') # 
]

#path('lookup/', LookupView.as_view(), name='lookup'), # gets _id for a place_id
#url(r'^advanced$', advanced, name="search_adv"),
#url(r'^analyzethis?$', DatasetFileUpdateView.as_view(), name="analyzethis"),
