# search/urls.py
from django.urls import path #, include
from django.conf.urls import url

from search.views import (
    SearchView, FeatureContextView, TraceGeomView 
)

urlpatterns = [
    path('index/', SearchView.as_view(), name='search'), # home page search
    path('context/', FeatureContextView.as_view(), name='feature_context'), # place portal context
    path('tracegeom/', TraceGeomView.as_view(), name='trace_geom'), # trace features <- search & place portal
]

