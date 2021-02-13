from django import template
from django.template.defaultfilters import stringfilter
import json, math

register = template.Library()

@register.filter
def time_estimate(numrows):
    seconds = round(numrows/3)
    return 'about '+str(round(seconds/60))+' minute(s)' \
           if seconds >= 60 else 'under 1 minute'
@register.filter
def time_estimate_sparql(numrows):
    seconds = numrows
    return 'about '+str(round(seconds/60))+' minute(s)' \
           if seconds >= 60 else 'under 1 minute'

@register.simple_tag
def define(val=None):
    return val

@register.filter
def addstr(arg1, arg2):
    """concatenate arg1 & arg2"""
    return str(arg1) + str(arg2)

@register.filter
def haskey(objlist, arg):
    """True if any obj in objlist has key arg"""
    return any(arg in x for x in objlist)

@register.filter
#@register.filter('startswith')
def startswith(text, starts):
    if isinstance(text, str):
        return text.startswith(starts)
    return False

def cut(value, arg):
    """Removes all values of arg from the given string"""
    return value.replace(arg, '')

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
    #print('parsedict value, key',value,key)
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

@register.filter
def sortts(objlist):
    foo = sorted(objlist, key=lambda x: x['start']['in'])
    return foo
