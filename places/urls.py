# places.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views

# place actions
app_name='places'
urlpatterns = [

    # will eventually take purl
    path('<int:id>/portal', views.PlacePortalView.as_view(), name='place-portal'),
    #path('<int:id>/modal', views.PlaceModalView.as_view(), name='place-modal'),
    # view of single db record
    path('<int:id>/detail', views.PlaceDetailView.as_view(), name='place-modal'),
    
    
    #path('<int:pid>/<str:auth>/defer', views.defer_review, name='defer-review'),
    path('defer/<int:pid>/<str:auth>/<str:last>', views.defer_review, name='defer-review'),
    
    # ??
    path('<int:id>/full', views.PlaceFullView.as_view(), name='place-full'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
