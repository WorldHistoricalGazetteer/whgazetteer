from django.contrib.auth import get_user_model
User = get_user_model()
from django.shortcuts import get_object_or_404
from django.test import Client
from django.test import TestCase, SimpleTestCase, TransactionTestCase
from django.test.client import RequestFactory

import os, sys, re
import simplejson as json

from datasets.models import Dataset, Hit
from datasets.tasks import align_wdlocal, es_lookup_wdlocal, normalize
from datasets.utils import hully
from datasets.views import ds_recon
from places.models import Place

someuser = get_object_or_404(User, pk=14)
whgadmin = get_object_or_404(User, pk=1)


class ReconWD(SimpleTestCase):
#class ReconWD(TransactionTestCase):
#class ReconWD(TestCase):
  #databases = ['default']
  
  def testReconWDlocal(self):
    [nohits, count_hits, count_nohits] = [[],0,0]
    hit_parade = {"summary": {}, "hits": []}

    qs = Place.objects.filter(dataset__in=['rtowns_lpf_lessgeo'])
    bounds = {'type': ['userarea'], 'id': ['0']}
    scope = 'all',
    language = 'en'
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
        
      print('qobj', qobj)
      # run pass0-pass2 ES queries
      result_obj = es_lookup_wdlocal(qobj, bounds=bounds)      

      if result_obj['hit_count'] == 0:
        count_nohits +=1
        nohits.append(result_obj['missed'])
      else:
        for hit in result_obj['hits']:
          count_hits += 1
          hit_parade["hits"].append(hit)
      
      print(hit_parade['hits'])
    #self.assertEqual(response.status_code, 200)
    #self.assertEqual(count_hits, 7)
    #self.assertEqual(count_nohits, 3)
