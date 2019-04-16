# whg.builtins
from django import template
# from django.utils.html import escape
# from django.utils.safestring import mark_safe
import json

register = template.Library()


@register.filter(name='get')
def get(d, k):
    jd = json.loads(d)
    return jd.get(k, None)
