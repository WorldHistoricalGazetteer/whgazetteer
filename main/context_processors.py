# main/context_processors.py
from django.conf import settings

def add_view_name(request):
    # view_name = request.resolver_match.func.__name__
    return {'view_name': str(request.resolver_match.func.__name__)}

def add_debug_flag(request):
    return {'DEBUG': settings.DEBUG}
