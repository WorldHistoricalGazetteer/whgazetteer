# collection.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt

from . import views
from traces.views import get_form, annotate

# area actions
app_name='collection'

urlpatterns = [

    # DATASET collections (datasets only)
    path('create_ds/', views.DatasetCollectionCreateView.as_view(), name='ds-collection-create'),
    path('<int:id>/update_ds', views.DatasetCollectionUpdateView.as_view(), name='ds-collection-update'),
    path('<int:pk>/summary_ds', views.DatasetCollectionSummaryView.as_view(), name='ds-collection-summary'),
    path('<int:id>/browse_ds', views.DatasetCollectionBrowseView.as_view(), name='ds-collection-browse'),

    # PLACE collections (datasets, indiv places, annotations, 'authored')
    path('create_pl/', views.PlaceCollectionCreateView.as_view(), name='place-collection-create'),
    path('<int:id>/update_pl', views.PlaceCollectionUpdateView.as_view(), name='place-collection-update'),
    path('<int:id>/browse_pl', views.PlaceCollectionBrowseView.as_view(), name='place-collection-browse'),
    path('flash_create/', views.flash_collection_create, name="collection-create-flash"),

    path('<int:id>/delete', views.CollectionDeleteView.as_view(), name='collection-delete'),

    # COLLECTION GROUPS (for classes, workshops)
    # path('create_collection_group/', views.create_collection_group, name='create-collection-group'),
    path('create_collection_group/', views.CollectionGroupCreateView.as_view(), name='collection-group-create'),
    path('group/<int:id>/update', views.CollectionGroupUpdateView.as_view(), name='collection-group-update'),
    path('group/<int:id>/delete', views.CollectionGroupDeleteView.as_view(), name='collection-group-delete'),
    path('group/<int:id>/detail', views.CollectionGroupDetailView.as_view(), name='collection-group-detail'),
    path('group/<int:id>/gallery', views.CollectionGroupGalleryView.as_view(), name='collection-group-gallery'),

    # UTILITY
    path('list_ds/', views.ListDatasetView.as_view(), name='list-ds'),
    path('add_ds/<int:coll_id>/<int:ds_id>', views.add_dataset, name='add-ds'),
    path('remove_ds/<int:coll_id>/<int:ds_id>', views.remove_dataset, name='remove-ds'),

    path('add_places/', views.add_places, name="collection-add-places"),
    path('remove_places/', views.remove_places, name="collection-remove-places"),
    path('create_link/', views.create_link, name="collection-create-link"),
    path('remove_link/<int:id>/', views.remove_link, name="remove-link"),

    # function-based views to process a trace annotation
    path('<int:id>/annotate', csrf_exempt(annotate), name="collection-annotate"),
    path('annoform/', get_form, name="get_form"),

    # switch off active bit
    path('inactive/', views.inactive, name="collection-inactive"),

    path('<int:id>/geojson/', views.fetch_geojson_coll, name="geojson-coll"),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
