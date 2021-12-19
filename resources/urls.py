# resources.urls (for teaching)

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views

# area actions
app_name = 'resources'

urlpatterns = [

    path('create/', views.CollectionCreateView.as_view(), name='collection-create'),
    # create handles create and update
    path('<int:id>/update', views.CollectionUpdateView.as_view(),
         name='collection-update'),
    path('<int:id>/delete', views.CollectionDeleteView.as_view(),
         name='collection-delete'),

    path('list_ds/', views.ListDatasetView.as_view(), name='list-ds'),
    path('remove_ds/<int:coll_id>/<int:ds_id>',
         views.remove_dataset, name='remove-ds'),

    # detail is the public view
    path('<int:pk>/detail', views.CollectionDetailView.as_view(),
         name='collection-detail'),
    # adding browse tab
    path('<int:id>/places', views.CollectionPlacesView.as_view(),
         name='collection-places'),

    path('<int:id>/geojson/', views.fetch_geojson_coll, name="geojson-coll"),

] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
