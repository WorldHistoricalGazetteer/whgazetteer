from django import template
from django.contrib.auth.models import Group
from django.template.defaultfilters import stringfilter
import json, re, validators, textwrap

register = template.Library()

# test user in group
# @register.filter
# def has_group(user, group_name):
#     group = Group.objects.get(name=group_name)
#     return True if group in user.groups.all() else False

@register.filter
def remove(str, tozap):
    print('remove string', str, type(str))
    return str.replace(tozap, '')

# truncates at previous word break
@register.filter
def trunc_it(str, numchars):
    return textwrap.shorten(str, width=numchars, placeholder="...")

@register.filter
def url_it(val):
    r1 = '((http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?)'
    reg = re.search(r1, val)
    return val.replace(reg.group(1),'<a href="'+reg.group(1)+'" target="_blank">link <i class="fa fa-external-link"></i> </>') if reg else val

@register.filter
def readmore(txt, numchars):
    dots = '<span id="dots">...</span>'
    link = '<a href="#" class="a_more">more</a><span class="more hidden">'
    
    if len(txt) <= numchars:
        return txt
    else:
        return txt[:numchars]+dots+link+txt[numchars:]+' <a href="#" class="ml-2 a_less hidden">less</a></span>'

@register.filter
def is_url(val):
    return True if validators.url(val) else False
    
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
def join(value,delimit):
    print('join', value, delimit)
    """joins list/array items"""
    if type(value[0]) == int:
        value=map(str,value)
    return delimit.join(value)

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
