# collection.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views

# area actions
app_name='collection'

urlpatterns = [
    # create new empty on the fly, returns new id in json
    path('flash_create/', views.flash_collection_create, name="collection-create-flash"),
    path('add_places/', views.add_places, name="collection-add-places"),

    # create handles create and update
    path('create/', views.CollectionCreateView.as_view(), name='collection-create'),
    path('createbeta/', views.CollectionCreateBetaView.as_view(), name='collection-create-beta'),

    path('<int:id>/update', views.CollectionUpdateView.as_view(), name='collection-update'),
    path('<int:id>/updatebeta', views.CollectionUpdateBetaView.as_view(), name='collection-update-beta'),

    # function-based to process a trace annotation independent of
    path('<int:id>/annotate', views.annotate, name="collection-annotate"),

    path('<int:id>/delete', views.CollectionDeleteView.as_view(), name='collection-delete'),
    
    path('list_ds/', views.ListDatasetView.as_view(), name='list-ds'),
    path('remove_ds/<int:coll_id>/<int:ds_id>', views.remove_dataset, name='remove-ds'),
    
    # detail is the public summary view
    path('<int:pk>/detail', views.CollectionDetailView.as_view(), name='collection-detail'),
    path('<int:pk>/detailbeta', views.CollectionDetailBetaView.as_view(), name='collection-detail-beta'),

    # places is the browse tab
    path('<int:id>/places', views.CollectionPlacesView.as_view(), name='collection-places'),
    path('<int:id>/placesbeta', views.CollectionPlacesBetaView.as_view(), name='collection-places-beta'),

    path('<int:id>/geojson/', views.fetch_geojson_coll, name="geojson-coll"),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
