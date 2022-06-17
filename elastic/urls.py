# elastic.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

# from . import views
from elastic.es_utils import fetch, alt_parents
#
app_name='elastic'
urlpatterns = [

    path('admin/', TemplateView.as_view(template_name='elastic/index_admin.html'), name='index-admin'),
    path('fetch/', fetch, name='place-fetch'),
    path('alt_parents/', alt_parents, name='place-alt-parents'),

]
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
