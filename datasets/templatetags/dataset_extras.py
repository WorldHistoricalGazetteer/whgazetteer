from django import template
from django.template.defaultfilters import stringfilter
import json

register = template.Library()

@register.filter
@stringfilter
def trimbrackets(value):
    """trims [ and ] from string, returns integer"""
    return int(value[1:-1])

@register.filter
def parsejson(value,key):
    """returns value for given key"""
    obj = json.loads(value.replace("'",'"'))
    return obj[key]

@register.filter
def parsedict(value,key):
    """returns value for given key"""
    return value[key]

@register.filter
def parse(obj,key):
    if '/' in key:
        key=key.split('/')
        return obj[key[0]][key[1]]
    else:
        return obj[key]
    """returns value for given key or sub-key"""
    # obj = json.loads(value.replace("'",'"'))
    # return obj[key]
