from django.conf.urls.static import static
from django.conf import settings
from django.contrib import admin
from django.urls import path, re_path, include
from django.views.generic.base import TemplateView

from main import views
from datasets.views import PublicListsView, DataListsView
from resources.views import TeachingPortalView

app_name='main'
#handler404 = 'datasets.views.handler404',
handler500 = 'main.views.custom_error_view'

urlpatterns = [
    path('', views.Home30a.as_view(), name="home"),
    
    path('libre/', views.LibreView.as_view(), name='libre'),
    
    # apps
    path('search/', include('search.urls')),
    path('datasets/', include('datasets.urls')),
    path('areas/', include('areas.urls')),
    path('collections/', include('collection.urls')),
    path('places/', include('places.urls')),
    path('elastic/', include('elastic.urls')),
    path('tutorials/', include('main.urls')),
    path('resources/', include('resources.urls')),
    path('teaching/', TeachingPortalView.as_view(), name="teaching-page"),

    # DEPRECATED
    # path('dashboard/', DashboardView.as_view(), name='dashboard'),

    ## DATA "DASHBOARD" LIST VIEWS
    # reverse name is parameter to DataListsView()
    path('mydata/', DataListsView.as_view(), name='data-datasets'),
    path('mycollections/', DataListsView.as_view(), name='data-collections'),
    path('mystudyareas/', DataListsView.as_view(), name='data-areas'),
    path('resourcelist/', DataListsView.as_view(), name='data-resources'),

    path('public_data/', PublicListsView.as_view(), name='public-lists'),

    # static content
    path('about/', TemplateView.as_view(template_name="main/about.html"), name="about"),
    path('contributing/', TemplateView.as_view(template_name="main/contributing.html"), name="contributing"),
    path('credits/', TemplateView.as_view(template_name="main/credits.html"), name="credits"),
    path('licensing/', TemplateView.as_view(template_name="main/licensing.html"), name="licensing"),
    path('system/', TemplateView.as_view(template_name="main/system.html"), name="system"),
    path('usingapi/', TemplateView.as_view(template_name="main/usingapi.html"), name="usingapi"),
    path('tinymce/', include('tinymce.urls')),

    path('comment/<int:rec_id>', views.CommentCreateView.as_view(), name='comment-create'),
    path('contact/', views.contactView, name='contact'),
    path('success/', views.contactSuccessView, name='success'),
    path('status/', views.statusView, name='status'),
    

    # backend stuff
    path('api/', include('api.urls')),
    path('remote/', include('remote.urls')),
    # path('accounts/', include('allauth.urls')),
    path('accounts/', include('accounts.urls')),
    path('admin/', admin.site.urls),
    path('captcha/', include('captcha.urls')),

    re_path(r'^celery-progress/', include('celery_progress.urls')),  # the endpoint is configurable
    

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)


if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
