# elastic.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

# from . import views
from elastic.es_utils import fetch, alt_parents, removeDatasetFromIndex
#
app_name='elastic'
urlpatterns = [

    # page for performing some index grooming
    path('admin/', TemplateView.as_view(template_name='elastic/index_admin.html'), name='index-admin'),

    # given pid, gets db and index records
    path('fetch/', fetch, name='place-fetch'),

    path('alt_parents/', alt_parents, name='place-alt-parents'),

    # remove all traces of dataset from whg index
    path('remove_dataset/<int:dsid>', removeDatasetFromIndex, name='remove-dataset'),

]
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
