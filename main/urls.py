# main.urls

from django.urls import path, include
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

from . import views

# actions
app_name='main'
urlpatterns = [

    path(r'', TemplateView.as_view(template_name="main/tutorials.html"), name="tutorials"),
    path('guide/', TemplateView.as_view(template_name="tutorials/guide.html"), name="tute-guide"),
    path('choosing/', TemplateView.as_view(template_name="tutorials/choosing.html"), name="tute-choosing"),
    path('collections/', TemplateView.as_view(template_name="tutorials/collections.html"), name="tute-collections"),
    path('contributing/', TemplateView.as_view(template_name="tutorials/contributing.html"), name="tute-contributing"),
    path('walkthrough/', TemplateView.as_view(template_name="tutorials/walkthrough.html"), name="tute-walkthrough"),
    path('placecollections/', TemplateView.as_view(template_name="tutorials/place_collections.html"), name="tute-place_collections"),
    path('create_lptsv/', TemplateView.as_view(template_name="tutorials/create_lptsv.html"), name="tute-lptsv"),

    path('modal/', TemplateView.as_view(template_name="main/modal.html"), name="dynamic-modal"),
]
#] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
