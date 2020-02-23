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

dsid=586 # 'diamonds' current file: user_whgadmin/diamonds135_rev3_g6cvm1l.tsv
# start: diamonds135_rev3; proposed update: diamonds135_rev21-125
ds = get_object_or_404(Dataset,pk=dsid)
[keepg,keepl]=[True,True]
oldids = list(Place.objects.filter(dataset=ds.label).values_list('src_id',flat=True))
curfile = 'user_whgadmin/diamonds135_hfl3svn.tsv'
# post tgn recon: 135 places, 135 names, 10 links, 25 geoms
newfile = 'user_whgadmin/diamonds135_rev2a-125.tsv'
tempfile = '/var/folders/f4/x09rdl7n3lg7r7gwt1n3wjsr0000gn/T/tmpwb8q_u5i.tsv'
#adf = pd.read_csv('media/'+curfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
#bdf = pd.read_csv(newfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})

adf = pd.read_csv('media/'+curfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
bdf = pd.read_csv(tempfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
bdf = bdf.astype({"ccodes": str})

ids_a = adf['id'].tolist(); print(len(ids_a)) #135
ids_b = bdf['id'].tolist(); print(len(ids_b)) #125
# 
delete_srcids = [str(x) for x in (set(ids_a)-set(ids_b))]
replace_srcids = set.intersection(set(ids_b),set(ids_a))
places = Place.objects.filter(dataset=ds.label)
rows_delete = list(places.filter(src_id__in=delete_srcids).values_list('id',flat=True)); 
rows_replace = list(places.filter(src_id__in=replace_srcids).values_list('id',flat=True));
rows_add=[str(x) for x in (set(ids_b)-set(ids_a))];
print('del',len(rows_delete),'replace',len(rows_replace),'add',len(rows_add) ) 

# DATABASE actions
# delete places with ids missing in new data (CASCADE includes links & geoms)
places.filter(id__in=rows_delete).delete()


ncount=PlaceName.objects.filter(place_id_id__in=places).count() # 134
gcount=PlaceGeom.objects.filter(place_id_id__in=places).count() # 17
lcount=PlaceLink.objects.filter(place_id_id__in=places).count() # 3
print(ncount,gcount,lcount) #123 23 9
# delete related instances for the rest (except links and geoms)
PlaceName.objects.filter(place_id_id__in=places).delete()
PlaceType.objects.filter(place_id_id__in=places).delete()
PlaceWhen.objects.filter(place_id_id__in=places).delete()
PlaceDescription.objects.filter(place_id_id__in=places).delete()
PlaceDepiction.objects.filter(place_id_id__in=places).delete()
#
ncount=PlaceName.objects.filter(place_id_id__in=places).count() # 134
gcount=PlaceGeom.objects.filter(place_id_id__in=places).count() # 17
lcount=PlaceLink.objects.filter(place_id_id__in=places).count() # 3
print(ncount,gcount,lcount) #0 23 9

# keep links and/or geoms is a form choice (keepg, keepl)
# rows created during reconciliation review have a task_id
if keepg == 'false':
  # keep none (they are being replaced in update)
  PlaceGeom.objects.filter(place_id_id__in=places).delete()
else:
  # keep augmentation rows; delete the rest
  PlaceGeom.objects.filter(place_id_id__in=places,task_id=None).delete()
if keepl == 'false':
  # keep none (they are being replaced in update)
  PlaceLink.objects.filter(place_id_id__in=places).delete()
else:
  PlaceLink.objects.filter(place_id_id__in=places,task_id=None).delete()

ncount=PlaceName.objects.filter(place_id_id__in=places).count() # 134
gcount=PlaceGeom.objects.filter(place_id_id__in=places).count() # 17
lcount=PlaceLink.objects.filter(place_id_id__in=places).count() # 3
print(ncount,gcount,lcount) #0 9 9 (augment geoms & links retained)

# Place instances to be kept remain, related are gone
ds.places.count() #123

# now update values in places; recreate place_xxxxx rows
# from ds_update line 676-714
count_updated, count_new = [0,0]
# update remaining place instances w/data from new file
# AND add new
from datasets.views import add_rels_tsv
place_fields = {'id', 'title', 'ccodes'}
for index, row in bdf.iterrows():
  rd = row.to_dict()
  rdp = {key:rd[key] for key in place_fields}
  # look for corresponding current place
  p = places.filter(src_id=rdp['id']).first()
  if p != None:
    # place exists, update it
    count_updated +=1
    p.title = rdp['title']
    p.ccodes = [] if str(rdp['ccodes']) == 'nan' else rdp['ccodes'].replace(' ','').split(';') 
    p.save()
    #print('updated '+str(p.id)+', add related from '+str(rdrels))
    pobj = p
  else:
    # if not, entirely new place
    count_new +=1
    newpl = Place.objects.create(
      src_id = rdp['id'],
      title = re.sub('\(.*?\)', '', rdp['title']),
      ccodes = rdp['ccodes'].replace(' ','').split(';'),
      dataset = ds
    )
    newpl.save()
    pobj = newpl
    #print('new place, related:', newpl, rdrels)
  
  # create related records (place_name, etc)
  # pobj is either a current (now updated) place or entirely new
  # rd is row dict
  add_rels_tsv(pobj, rd)

pcount=ds.places.count()
ncount=PlaceName.objects.filter(place_id_id__in=places).count() # 134
gcount=PlaceGeom.objects.filter(place_id_id__in=places).count() # 17
lcount=PlaceLink.objects.filter(place_id_id__in=places).count() # 3
print(pcount,ncount,gcount,lcount) # 125 125 24 9
# 2 new rows, all w/names, 15+9 geoms, 9 links CHECK

# END DATABASE actions

# ES STUFF
# fetch place_ids for all docs in a dataset
def fetch_pids(dslabel):
  pids=[]
  esq_ds = {"size":10000, "query":{"match":{"dataset": dslabel}}}
  res = es.search(index=idx, body=esq_ds)
  docs = res['hits']['hits']
  for d in docs:
    pids.append(d['_source']['place_id'])
  return pids

def esq_get(pid):
  q = {"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
  return q

pdels = fetch_pids('diamonds')
# strip indexed places by Place.id
def idxStripByID(pdels):
  if len(pdels) > 0:
    delthese=[]
    # child: 6293916; parent with children: 13549548; parent w/no children 6293837
    for p in pdels[:3]:
      res = es.search(index=idx, body=esq_get(p))    
      doc = res['hits']['hits'][0]
      src = doc['_source']
      role = src['relation']['name']; print(role)
      variants = list(set(src['suggest']['input'])) # distinct only
      # ugh; stripping mistakenly entered lists
      searchy = list(set([item for item in src['searchy'] if type(item) != list])) 
      if role == 'parent':
        if len(src['children'] == 0):
          # childless, add to array for deletion
          delthese.append(doc['_id'])
        else:
          # any in pdels?
      elif role == 'parent' and len(src['children'] > 0):
        # process children
        for c in src['children']:
          qget = {"query": {"bool": {"must": [{"match":{"place_id": c }}]}}}
          res = es.search(index=idx, body=qget)
          # get 1st sibling if any
          sib = res['hits']['hits'][0]['_source']['children'].remove(c)[0]
      elif role == 'child':
        # get its parent
        parent = src['relation']['parent']
        qget = {"query": {"bool": {"must": [{"match":{"_id": parent }}]}}}
        res = es.search(index=idx, body=qget)
        # parent _source, suggest, searchy
        psrc = res['hits']['hits'][0]['_source']
        psugs = list(set(psrc['suggest']['input']))
        psearchy = list(set([item for item in psrc['searchy'] if type(item) != list]))
        # is parent slated for deletion? (walking dead)
        zombie = psrc['place_id'] in pdels
        if not zombie: # leave zombies; they're picked up with if role == 'parent':
          # remove this id from children and remove its variants from suggest.input and searchy
          newsugs = list(set(psugs)-set(variants))
          newsearchy = list(set(psearchy)-set(searchy))
          qupdate = {"script":{},"query":{}}
          try:
            es.update_by_query(index=idx,body=q_update)
          except:
            print('aw shit',sys.exit(sys.exc_info()))

        # and siblings
        sibs = doc['_source']['children']
        
      parent = doc['_source']['relation']['parent'] if role == 'child' else None
      children = doc['_source']['children']; print(children)
      print(doc)

# ES ACTIONS (database now current)
# 1) delete records in rows_delete[]
# 2) update records in rows_replace[]
# defer indexing new records in rows_add until reconciled


if len(rows_delete) > 0:  
  for r in rows_delete:
    res = es.search(index=idx, body=esq_get(r))    
    doc = res['hits']['hits'][0]
    print(doc)
    
# delete from rows_delete
qdel = {}

# update from rows_replace
print(len(rows_replace),rows_replace)
qrepl = {}

# get db record
place = get_object_or_404(Place, pk=pid)

# make an ES doc for it
pdoc = makeDoc(place,'none')

# find it in the index, if exists
qget = {"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
res = es.search(index=idx, body=qget)
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

  