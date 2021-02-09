from django.contrib.auth.models import User, Group
from django.shortcuts import get_object_or_404
#from django.test import Client
#from django.test import TestCase, SimpleTestCase, TransactionTestCase
#from django.test.client import RequestFactory

import os, sys, re
import simplejson as json

from datasets.models import Dataset, Hit
from datasets.tasks import es_lookup_wdlocal
from datasets.utils import hully
#from datasets.views import ds_recon
from places.models import Place

someuser = get_object_or_404(User, pk=14)
whgadmin = get_object_or_404(User, pk=1)


  
def wdlocal(ds):
  dsids = [int(x) for x in str(ds).split(',')]
  datasets = Dataset.objects.filter(id__in=dsids).values_list('label')
  print('datasets', datasets)
  [nohits, some_hits, total_hits, count_nohits] = [[],0,0,0]
  hit_parade = {"summary": {}, "hits": []}

  qs = Place.objects.filter(dataset__in = datasets)
  bounds = {'type': ['userarea'], 'id': ['0']}
  #scope = 'all',
  #language = 'en'
  for place in qs:
    [variants,geoms,types,ccodes,parents,links]=[[],[],[],[],[],[]]
    qobj = {"place_id":place.id,
            "src_id":place.src_id,
            "title":place.title,
            "fclasses":place.fclasses or []}
    # ccodes
    for c in place.ccodes:
      ccodes.append(c.upper())
    qobj['countries'] = place.ccodes
    # types
    for t in place.types.all():
      if t.jsonb['identifier'].startswith('aat:'):
        types.append(int(t.jsonb['identifier'].replace('aat:','')) )
    qobj['placetypes'] = types
    # names
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = variants
    # parents
    if len(place.related.all()) > 0:
      for rel in place.related.all():
        if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
          parents.append(rel.jsonb['label'])
      qobj['parents'] = parents
    else:
      qobj['parents'] = []
    # geoms
    if len(place.geoms.all()) > 0:
      g_list =[g.jsonb for g in place.geoms.all()]
      qobj['geom'] = hully(g_list)  
    # links
    if len(place.links.all()) > 0:
      l_list = [l.jsonb['identifier'] for l in place.links.all()]
      qobj['authids'] = l_list
    else:
      qobj['authids'] = []
      
    #print('qobj', qobj)
    # run pass0-pass2 ES queries
    result_obj = es_lookup_wdlocal(qobj, bounds=bounds)      

    if result_obj['hit_count'] == 0:
      count_nohits +=1
      nohits.append(result_obj['missed'])
    else:
      some_hits +=1
      for hit in result_obj['hits']:
        total_hits += 1
        hit_parade["hits"].append(hit)
    
  hits = hit_parade['hits']
  print('rows w/hits:'+str(some_hits)+'; total_hits: '+str(total_hits)+'; no hits: '+str(nohits))
  print(
    'pass0:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass0']))+'; ',
    'pass1:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass1']))+'; ',
    'pass2:'+str(len([h['_id'] for h in hits if h['pass'] == 'pass2'])),
  )
  print('hits:',hit_parade['hits'])

ds_array = input('one or more ds ids, comma delimited:   ')
wdlocal(ds_array)

#done [807, 812, 925, 927, 897]

#delthese=[]
#for d in delthese:
  #ds=get_object_or_404(Dataset,pk=d)
  #ds.delete()