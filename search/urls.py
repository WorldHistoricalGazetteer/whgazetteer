# search/urls.py
from django.urls import path, include
from django.conf.urls import url

from search.views import (
    advanced, SuggestView,
    fetchArea, FeatureContextView, TraceGeomView)

urlpatterns = [
    url(r'^suggest?$', SuggestView.as_view(), name='suggest'),
    url(r'^features?$', FeatureContextView.as_view(), name='feature_context'),
    url(r'^tracegeom?$', TraceGeomView.as_view(), name='trace_geom'),
    url(r'^advanced$', advanced, name="search_adv"),
]
