# celery reconciliation tasks [align_tgn(), align_idx()] and related functions

from __future__ import absolute_import, unicode_literals
from celery.decorators import task
from django.conf import settings
from django.shortcuts import get_object_or_404, render, redirect
from django.contrib.gis.geos import Polygon, Point, LineString
import logging
##
import sys, os, re, json, codecs, datetime, time, csv, random
from copy import deepcopy
from pprint import pprint
from areas.models import Area
from datasets.es_utils import makeDoc, esInit
from datasets.models import Dataset, Hit
from datasets.regions import regions as region_hash
from datasets.static.hashes.parents import ccodes
from datasets.utils import *
from places.models import Place
##
import shapely.geometry as sgeo
from geopy import distance
from elasticsearch import Elasticsearch
es = settings.ES_CONN
##

def maxID(es):
  q={"query": {"bool": {"must" : {"match_all" : {}} }},
       "sort": [{"whg_id": {"order": "desc"}}],
       "size": 1  
       }
  try:
    res = es.search(index='whg', body=q)
    #maxy = int(res['hits']['hits'][0]['_id'])
    maxy = int(res['hits']['hits'][0]['_source']['whg_id'])
  except:
      maxy = 12345677
  return maxy 


def add_black(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=1)
  # get last identifier (used for whg_id & _id)
  whg_id=maxID(es)

  #dummies for testing
  bounds = {'type': ['userarea'], 'id': ['0']}

  # TODO: system for region creation
  hit_parade = {"summary": {}, "hits": []}
  [nohits, errors] = [[],[]] # 
  [count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3, count_errors, count_seeds] = [0,0,0,0,0,0,0,0]

  start = datetime.datetime.now()
  skipped=[]
  #one-time filter for black atlas children
  black_dupes = [h.place_id for h in Hit.objects.distinct('place_id').filter(task_id='a102377e-4645-4c2d-a932-b530994da2ba')]
  qs = ds.places.all().filter(id__in=black_dupes)
  for place in qs:
    count+=1
    qs_links=place.links.filter(black_parent__isnull=False)
    if len(qs_links) == 0:
      count_nohit +=1
      # no hits, create parent record immediately
      # for now, only if 'black'
      # TODO
      whg_id+=1
      #place=get_object_or_404(Place,id=result_obj['place_id'])
      #print('new whg_id',whg_id)
      #parent_obj = makeDoc(place,'none')
      parent_obj = makeDoc(place)
      parent_obj['relation']={"name":"parent"}
      parent_obj['whg_id']=whg_id
      # add its own names to the suggest field
      for n in parent_obj['names']:
        parent_obj['suggest']['input'].append(n['toponym']) 
      #index it
      try:
        res = es.index(index='whg', id=str(whg_id), body=json.dumps(parent_obj))
        count_seeds +=1
      except:
        print('failed indexing '+str(place.id), parent_obj)
        print(sys.exc_info[0])
        #errors_black.write(str({"pid":place.id, "title":place.title})+'\n')
      print('created parent:',place.id,place.title)
      #nohits.append(result_obj['missed'])
    elif len(qs_links) > 0:
      # there's a whg: link, index this record as its child
      count_hit +=1
      [count_kids,count_errors] = [0,0]
      #print("hit['_source']: ",result_obj['hits'][0]['_source'])
      hit = qs_links[0]
      # get the whg: link if there
        # it's either closeMatch or exactMatch
      if 'Match' in hit.jsonb['type']:
        
        # get _id (whg_id) of parent
        parent_pid = hit.black_parent
        q_parent={"query": {"bool": {"must": [{"match":{"place_id": parent_pid}}]}}}
        res = es.search(index='whg', body=q_parent)
        parent_whgid = res['hits']['hits'][0]['_id']; print(parent_whgid)
        
        # gather names, make an index doc
        match_names = [p.toponym for p in place.names.all()]
        #child_obj = makeDoc(place,'none')
        child_obj = makeDoc(place)
        child_obj['relation']={"name":"child","parent":parent_whgid}
        
        # index it
        try:
          res = es.index(index='whg',id=place.id,
                         routing=1,body=json.dumps(child_obj))
          count_kids +=1                
          print('adding '+place.title+'('+str(place.id) + ') as child of '+ str(parent_whgid))
        except:
          print('failed indexing '+str(place.id), child_obj)
          sys.exit(sys.exc_info())
        q_update = { "script": {
            "source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id)",
            "lang": "painless",
            "params":{"names": match_names, "id": str(place.id)}
          },
          "query": {"match":{"_id": parent_whgid}}}
        try:
          es.update_by_query(index='whg', body=q_update)
        except:
          err='failed updating '+str(parent_whgid)+' from child '+str(place.id)+'; index: '+str(count-1)
          print(err)
          skipped.append(err)
          pass
          #print(count_kids-1)
          #sys.exit(sys.exc_info())

  print(skipped)
  #print('also: 92,114')
  end = datetime.datetime.now()
  # ds.status = 'recon_whg'
  hit_parade['summary'] = {
    'count':count,
    'got_hits':count_hit,
    'no_hits': {'count': count_nohit },
    'elapsed': elapsed(end-start)
    #'skipped': count_errors
  }
  #if ds.label == 'black': errors_black.close()
  print("hit_parade['summary']",hit_parade['summary'])
  #return hit_parade['summary']

