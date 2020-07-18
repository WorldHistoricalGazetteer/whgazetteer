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
    path('walkthrough/', TemplateView.as_view(template_name="tutorials/walkthrough.html"), name="tute-walkthrough"),
    path('template/', TemplateView.as_view(template_name="tutorials/tute-template.html"), name="tute-template"),
    
    

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
