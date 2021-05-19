# collection.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views

# area actions
app_name='collection'

urlpatterns = [

    path('create/', views.CollectionCreateView.as_view(), name='collection-create'),
    # create handles create and update
    path('<int:id>/update', views.CollectionUpdateView.as_view(), name='collection-update'),
    path('<int:pk>/detail', views.CollectionDetailView.as_view(), name='collection-detail'),
    path('<int:id>/delete', views.CollectionDeleteView.as_view(), name='collection-delete'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
