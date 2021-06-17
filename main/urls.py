# main.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView

from . import views

# area actions
app_name='main'
urlpatterns = [

    path(r'', TemplateView.as_view(template_name="main/tutorials.html"), name="tutorials"),
    path('guide/', TemplateView.as_view(template_name="tutorials/guide.html"), name="tute-guide"),
    path('choosing/', TemplateView.as_view(template_name="tutorials/choosing.html"), name="tute-choosing"),
    path('collections/', TemplateView.as_view(template_name="tutorials/collections.html"), name="tute-collections"),
    path('contributing/', TemplateView.as_view(template_name="tutorials/contributing.html"), name="tute-contributing"),
    path('walkthrough/', TemplateView.as_view(template_name="tutorials/walkthrough.html"), name="tute-walkthrough"),
    path('traces/', TemplateView.as_view(template_name="tutorials/traces.html"), name="tute-traces"),
    path('create_lptsv/', TemplateView.as_view(template_name="tutorials/create_lptsv.html"), name="tute-lptsv"),

    # curricula
    path('asian_history/', TemplateView.as_view(template_name="curricula/asian_history.html"), name="curric-asian"),
    
    
    path('modal/', TemplateView.as_view(template_name="main/modal.html"), name="dynamic-modal"),
]
#] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

