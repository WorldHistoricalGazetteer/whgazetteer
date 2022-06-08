# main.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

# from . import views

#
app_name='elastic'
urlpatterns = [

    path('admin/', TemplateView.as_view(template_name='elastic/index_admin.html'), name='index-admin'),

]
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
