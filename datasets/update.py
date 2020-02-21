# testing ES whg02 index updating

from django.shortcuts import render, get_object_or_404
import simplejson as json
import codecs, tempfile, os, re, sys 
import pandas as pd
from places.models import *
from datasets.models import Dataset, Hit, DatasetFile
from datasets.utils import validate_lpf, goodtable
from elasticsearch import Elasticsearch
from es.es_utils import makeDoc
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
idx='whg02'

dsid=583 # 'diamonds' current file: user_whgadmin/diamonds135_rev3_g6cvm1l.tsv
ds = get_object_or_404(Dataset,pk=dsid)
oldids = list(Place.objects.filter(dataset=ds.label).values_list('src_id',flat=True))
curfile = 'user_whgadmin/diamonds135_rev3_g6cvm1l.tsv'
newfile = '/var/folders/f4/x09rdl7n3lg7r7gwt1n3wjsr0000gn/T/tmplstl8mdd.tsv'
adf = pd.read_csv('media/'+curfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
bdf = pd.read_csv(newfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
ids_a = adf['id'].tolist()
ids_b = bdf['id'].tolist()      
# 
delete_srcids = [str(x) for x in (set(ids_a)-set(ids_b))]
replace_srcids = set.intersection(set(ids_b),set(ids_a))
places = Place.objects.filter(dataset=ds.label)
rows_delete = list(places.filter(src_id__in=delete_srcids).values_list('id',flat=True))
rows_replace = list(places.filter(src_id__in=replace_srcids).values_list('id',flat=True))

qdel = {}
qrepl = {}

# for pid in rows_delete:
pid = rows_delete[0]

# get db record
place = get_object_or_404(Place, pk=pid)

# make an ES doc for it
pdoc = makeDoc(place,'none')

# find it in the index, if exists
res = es.search(index=idx, body=qget)
qget = {"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
hits = res['hits']['hits']

if len(hits) > 0: # if indexed
  # if child get parent's _id else self._id
  if hits[0]['_source']['relation']['name'] == 'child':
    parent_whgid = res['hits']['hits'][0]['_source']['relation']['parent']
  else:
    parent_whgid = res['hits']['hits'][0]['_id'] #; print(parent_whgid)
else:
  pass


# 1) matches parent_whgid
# 2) adds str(place.id) to its children[]
# 3) adds match_names[] values to its suggest field
q_update = { "script": {
    "source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id)",
    "lang": "painless",
    "params":{"names": match_names, "id": str(place.id)}
  },
  "query": {"match":{"_id": parent_whgid}}}
es.update_by_query(index=idx, doc_type='place', body=q_update, conflicts='proceed')

# DELETE algorithm
# for r in rows_delete:
#  get doc from ES
#    if doc._source.relation.name = 'parent'
#      if len(_source.children) > 0
#         make first a parent and any siblings children
#         delete doc
#      else
#         delete doc
#    else if doc._source.relation.name = 'child'
#      get its doc._source.relation.parent
#      remove pid from its children[] list
#      delete doc

# REPLACE algorithm
# for r in rows_replace:
#   get doc from ES
#    if doc._source.relation.name = 'parent'
#      
#      
#      
#      
#      
#    else if doc._source.relation.name = 'child'
#      
#      
#      
#   


  ## FILES (new has been validated [0], current became [1])
  #cur_fileobj = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-upload_date')[1]
  #cur_filename = cur_fileobj.file.name
  #cur_uribase = cur_fileobj.uri_base
  #new_fileobj = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-upload_date')[0]
  #new_filename = new_fileobj.file.name
  #new_uribase = new_fileobj.uri_base

  