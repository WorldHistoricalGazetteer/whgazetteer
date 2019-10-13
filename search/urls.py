# search/urls.py
from django.urls import path, include
from django.conf.urls import url

from search.views import (
    advanced, SearchView, FeatureContextView, TraceGeomView, UpdateCountsView, LookupView)

urlpatterns = [
    url(r'^search?$', SearchView.as_view(), name='search'),
    url(r'^lookup?$', LookupView.as_view(), name='lookup'), # gets _id for a place_id
    url(r'^features?$', FeatureContextView.as_view(), name='feature_context'),
    url(r'^tracegeom?$', TraceGeomView.as_view(), name='trace_geom'),
    url(r'^updatecounts?$', UpdateCountsView.as_view(), name='update_counts'),
    url(r'^advanced$', advanced, name="search_adv"),
]
