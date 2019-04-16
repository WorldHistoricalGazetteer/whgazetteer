# es.py; index named dataset from database, spine initially
# 7 Feb 2019; rev 11 Mar 2019; rev 24 Mar (incorporate black)

from __future__ import absolute_import, unicode_literals
import sys, os, re, json, codecs, datetime, time, csv, random
from geopy import distance
import shapely.geometry
from pprint import pprint

from celery.decorators import task
from django.conf import settings
from django.shortcuts import get_object_or_404, render, redirect

from areas.models import Area
from datasets.es_utils import * # 
from datasets.models import Dataset, Hit
from datasets.regions import regions as region_hash
from datasets.utils import roundy, fixName, classy, bestParent, elapsed, hully
from places.models import Place

# 
# TODO: handle multiple parents (dplace: 124883,124900,125065,125132; ne_rivers: )
# TODO: conflate with align_whg reconcile operation; 
def indexDataset():
  import codecs
  dataset = input('dataset: ')
  qs = Place.objects.all().filter(dataset_id=dataset)
  f_err_multi = codecs.open('../_notes/err_multiparent_'+dataset+'.txt', mode='w', encoding='utf8')
  f_err_geom = codecs.open('../_notes/err_geom_'+dataset+'.txt', mode='w', encoding='utf8')
  count = 0
  multiparents=[]
  errors=[]
  # get last whg_id
  whg_id = maxID(es); print('max whg_id:',whg_id)  

  [count_seeds,count_kids,i] = [0,0,0]
  for place in qs:
    i +=1
    # place = qs[13]
    # 85924/118445; 81224 / 118507; 85924 / 118445; 118432 = !Kung
    # place=get_object_or_404(Place,id=122473) # Calusa/119778 (dplace)

    # build query object
    qobj = queryObject(place)

    # match if shared link; 
    matches = findMatch(qobj,es) if 'links' in qobj.keys() else {"parents":[], "names":[]}
    if len(matches['parents']) == 0:
      # it's a parent (seed)
      whg_id +=1
      parent_obj = makeDoc(place,'none')
      parent_obj['relation']={"name":"parent"}
      parent_obj['whg_id'] = whg_id
      # add its own names to the suggest field
      for n in parent_obj['names']:
        parent_obj['suggest']['input'].append(n['toponym']) 
      # index it
      try:
        res = es.index(index=idx, doc_type='place', id=whg_id, body=json.dumps(parent_obj))
        count_seeds +=1
      except:
        #print('failed indexing '+str(place.id), parent_obj)
        f_err_geom.write(str({"pid":place.id, "title":place.title, "matches":matches})+'\n')
        #errors.append({"pid":str(place.id), "pobj":parent_obj})
        pass
        #sys.exit(sys.exc_info())                
    else:
      # 1 or more matches, it's a child
      # TODO: can't have 2 parents though!!!!
      if len(matches['parents'])>1: 
        f_err_multi.write(str({"pid":place.id, "title":place.title, "matches":matches})+'\n')
        # multiparents.append({"pid":place.id, "title":place.title, "matches":matches})
      for pid in matches['parents']:
        child_obj = makeDoc(place,pid)
        child_obj['relation']={"name":"child","parent":pid}
        # index it
        try:
          res = es.index(index=idx,doc_type='place',id=place.id,
                         routing=1,body=json.dumps(child_obj))
          count_kids +=1                
          print('added '+str(place.id) + ' as child of '+ place.title + ': '+str(pid))
        except:
          print('failed indexing '+str(place.id), child_obj)
          sys.exit(sys.exc_info())

        # add its id, names to parent's children, suggest
        # TODO: ?? add its geometries to parent for home page disambiguation?
        q_update = {
          "script": {
            "source": "ctx._source.suggest.input.addAll(params.names);ctx._source.children.add(params.id)",
            "lang": "painless",
            "params":{"names": matches['names'],"id": str(place.id)}
          },
          "query": {"match":{"_id": pid}}
        }
        try:
          es.update_by_query(index=idx, doc_type='place', body=q_update)
        except:
          print('failed updating '+place.title+'('+str(pid)+') from child '+str(place.id))
          print(count_kids-1)
          sys.exit(sys.exc_info())

  print(multiparents)                    
  print(errors)                    
  print(str(count_seeds)+' fresh records added, '+str(count_kids)+' child records added')
  f_err_geom.close()
  f_err_multi.close()


def init():
  global es, idx, rows
  dataset = input('dataset: ')
  idx = 'whg' 

  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

  # zap dataset from index
  q_del = {"query": {"match": {"dataset": dataset}}}
  try:
    res=es.delete_by_query(idx,q_del)
    print(str(res['deleted'])+' docs deleted')
  except Exception as ex:
    print(ex)

