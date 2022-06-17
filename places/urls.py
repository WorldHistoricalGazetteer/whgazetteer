# places.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

from . import views
from elastic.es_utils import fetch

# place actions
app_name='places'
urlpatterns = [

    # will eventually take purl
    path('<int:id>/portal', views.PlacePortalView.as_view(), name='place-portal'),
    
    # single db record
    path('<int:id>/detail', views.PlaceDetailView.as_view(), name='place-detail'),
    # single db record for modal
    path('<int:id>/modal', views.PlaceModalView.as_view(), name='place-modal'),
    
    path('defer/<int:pid>/<str:auth>/<str:last>', views.defer_review, name='defer-review'),
    
    # # page to manage indexed place relocation
    # path('relocate/', TemplateView.as_view(template_name='places/place_relocate.html'), name='place-relocate'),
    # # gets db and index records for pid
    # path('fetch/', fetch, name='place-fetch'),
    
    # ??
    path('<int:id>/full', views.PlaceFullView.as_view(), name='place-full'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
