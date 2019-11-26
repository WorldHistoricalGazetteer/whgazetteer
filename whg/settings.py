"""
Generated by 'django-admin startproject' using Django 2.0.7.

updates: 2.2.4 (20190819); 2.1.7 (?); 2.1.2 (20181013)

"""

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'saiz(s6w1+okoz@3duv!%3bv=4cei8--f+5jb=a*_3&l0u!!wr'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['testserver','localhost']

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.gis',

    'bootstrap_modal_forms',
    #'django_celery_beat',
    'django_celery_results',
    'django_extensions',
    'djgeojson',
    'fontawesome',
    'leaflet',
    'mathfilters',
    'rest_framework',
    'rest_framework_datatables',

    'accounts.apps.AccountsConfig',
    'api.apps.ApiConfig',
    'areas.apps.AreasConfig',
    'datasets.apps.DatasetsConfig',
    'main.apps.MainConfig',
    'maps.apps.MapsConfig',
    'places.apps.PlacesConfig',
    'search.apps.SearchConfig',
    'traces.apps.TracesConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'whg.urls'

PUBLIC_GROUP_ID = 'review'

TIME_ZONE = 'America/New_York'

CELERY_BROKER_URL = 'redis://localhost:6379'
CELERY_RESULT_BACKEND = 'django-db'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE

#REST_FRAMEWORK = {
    #'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    #'PAGE_SIZE': 10,
#}

# replacement section from drf-datatables
# https://django-rest-framework-datatables.readthedocs.io/en/latest/
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
        'rest_framework_datatables.renderers.DatatablesRenderer',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'rest_framework_datatables.filters.DatatablesFilterBackend',
    ),
    'DEFAULT_PAGINATION_CLASS': 'rest_framework_datatables.pagination.DatatablesPageNumberPagination',
    'PAGE_SIZE': 15000,
}

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'main/templates'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'debug': True,
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.request',
            ],
            'builtins': [
                'whg.builtins',
            ]
        },
    },
]

WSGI_APPLICATION = 'whg.wsgi.application'

WHG_BASETILES = 'https://api.mapbox.com/styles/v1/kgeographer/cjstfpenh6o1e1fldz95w8m6p/tiles/{z}/{x}/{y}?access_token=pk.eyJ1Ijoia2dlb2dyYXBoZXIiLCJhIjoiUmVralBPcyJ9.mJegAI1R6KR21x_CVVTlqw'

AWMC_BASETILES = 'https://api.mapbox.com/v4/isawnyu.map-knmctlkh/{z}/{x}/{y}.png?access_token=pk.eyJ1Ijoia2dlb2dyYXBoZXIiLCJhIjoiY2prcmgwc2cwMjRuZzNsdGhzZmVuMDRqbCJ9.MeLsyeOqwhTRdvt_Hgo7kg'

LEAFLET_CONFIG = {
    'TILES':[],
    'DEFAULT_CENTER': (35.0, 13.0),
    'DEFAULT_ZOOM': 1,
    'MIN_ZOOM': 1,
    'MAX_ZOOM': 14,
    'RESET_VIEW': False,
    #'MAX_BOUNDS_VISCOSITY': 0,
    'ATTRIBUTION_PREFIX': 
    "Tiles &copy; <a href='http://mapbox.com/' target='_blank'>MapBox</a> | "+
    "<a href='http://creativecommons.org/licenses/by-nc/3.0/deed.en_US' target='_blank'> CC-BY-NC 3.0</a>"
}
#"Tiles and Data &copy; 2013 <a href='http://www.awmc.unc.edu' target='_blank'>AWMC</a> | " +
# mapbox://styles/kgeographer/cjstfpenh6o1e1fldz95w8m6p
# pk.eyJ1Ijoia2dlb2dyYXBoZXIiLCJhIjoiUmVralBPcyJ9.mJegAI1R6KR21x_CVVTlqw

#EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
#EMAIL_USE_TLS = True
#EMAIL_HOST = 'smtp.gmail.com'
#EMAIL_PORT = 587
#EMAIL_HOST_USER = ''
#EMAIL_HOST_PASSWORD = ''


# Database
# https://docs.djangoproject.com/en/2.0/ref/settings/#databases

DATABASES = {
    # authentication, etc.
    'default': {
        # 'ENGINE': 'django.db.backends.postgresql',
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': 'whg',
        'USER':'',
        'PASSWORD':'',
        'HOST':'localhost',
        'PORT':'5432',
    }
}

# not implemented
# DATABASE_ROUTERS = ('whg.dbrouters.MyDBRouter',)

# Password validation
# https://docs.djangoproject.com/en/2.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/2.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.0/howto/static-files/
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')
MEDIA_URL = '/media/'

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'whg/static/'),
    os.path.join(BASE_DIR, 'datasets/static/'),
]

try:
    from .local_settings import *
except ImportError:
    pass