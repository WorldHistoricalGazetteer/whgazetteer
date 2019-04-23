# search/urls.py
from django.urls import path, include
from django.conf.urls import url

from search.views import (
    advanced, SuggestView,
    fetchArea, FeatureContextView, TraceGeomView, TraceFullView )

urlpatterns = [
    url(r'^suggest?$', SuggestView.as_view(), name='suggest'),
    url(r'^features?$', FeatureContextView.as_view(), name='feature_context'),
    url(r'^tracegeom?$', TraceGeomView.as_view(), name='trace_geom'),
    url(r'^tracefull?$', TraceFullView.as_view(), name='trace_full'),
    url(r'^advanced$', advanced, name="search_adv"),
]
