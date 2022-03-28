# search/urls.py
from django.urls import path #, include

from search.views import (
  SearchView, SearchPageView, FeatureContextView, TraceGeomView,
  SearchDatabaseView
)

#app_name = "search"

urlpatterns = [
  path('', SearchPageView.as_view(), name='search-page'), # former home page
  path('index/', SearchView.as_view(), name='search'), # executes index search
  path('db/', SearchDatabaseView.as_view(), name='search-db'), # executes database search
  path('context/', FeatureContextView.as_view(), name='feature_context'), # place portal context
  path('tracegeom/', TraceGeomView.as_view(), name='trace_geom'), # trace features <- search & place portal

  path('db/', SearchDatabaseView.as_view(), name='search-db'), # executes database search
]

