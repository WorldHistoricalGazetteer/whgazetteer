# resources.urls (for teaching)

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views

# area actions
app_name = 'resources'

urlpatterns = [

    path('create/', views.ResourceCreateView.as_view(), name='resource-create'),
    # create handles create and update
    path('<int:id>/update', views.ResourceUpdateView.as_view(),
         name='resource-update'),
    path('<int:id>/delete', views.ResourceDeleteView.as_view(),
         name='resource-delete'),
    # detail is the public view
    path('<int:pk>/detail', views.ResourceDetailView.as_view(),
         name='resource-detail'),

    # path('list_ds/', views.ListDatasetView.as_view(), name='list-ds'),
    # path('remove_ds/<int:coll_id>/<int:ds_id>',
    #      views.remove_dataset, name='remove-ds'),


] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
