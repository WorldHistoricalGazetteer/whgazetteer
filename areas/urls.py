# areas.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views

# area actions
app_name='areas'

urlpatterns = [

    path('create/', views.AreaCreateView.as_view(), name='area-create'),
    #path('<int:id>/detail', views.AreaDetailView.as_view(), name='area-detail'),
    path('<int:id>/update', views.AreaUpdateView.as_view(), name='area-update'),
    path('<int:id>/delete', views.AreaDeleteView.as_view(), name='area-delete'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
