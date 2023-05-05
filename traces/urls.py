# traces.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views

# place actions
app_name='traces'
urlpatterns = [

    # will eventually take purl
    path('<int:id>/detail', views.TraceDetailView.as_view(), name='trace-detail'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
