# es_utils.py 7 Feb 2019; rev 5 Mar 2019; 02 Oct 2019; 21 Feb 2020 (create whg_test)
# misc supporting eleasticsearch tasks (es.py)

# ***
# replace docs in index by place_id
# ***
def replaceInIndex(es,idx,pids):
  print('in replaceInIndex():', pids)

# ***
# delete docs given place_id array
# if parent, promotes a child if any
# if child, removes references to in parent (children[], suggest.input[])
# TODO: confirm suggest.input[] is not distinct
#  i.e. what if variant was also contributed by parent or another child?
#
#from elasticsearch import Elasticsearch      
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
def deleteFromIndex(es,idx,pids):
  delthese=[]
  # child: 6293798; parent w/no children 6293937; DANGER!!! parent with children: 13549548; 
  for pid in pids:
    # get its index document
    res = es.search(index=idx, body=esq_get(pid))
    hits=res['hits']['hits']
    if len(hits) > 0:
      doc = hits[0]
      src = doc['_source']
      role = src['relation']['name']; print(role)
      sugs = list(set(src['suggest']['input'])) # distinct only
      searchy = list(set([item for item in src['searchy'] if type(item) != list])) 
      # role-dependent action
      if role == 'parent':
        # convert to integers
        # kids = ['5991423', '85196', '83140', '82439']
        kids = [int(x) for x in src['children']]
        eligible = list(set(kids)-set(pids)) # not slated for deletion
        if len(eligible) == 0:
          # add to array for deletion
          print('childless parent '+str(pid)+' tagged for deletion')
          delthese.append(pid)
        else:
          # > 0 eligible children, first promote one to parent
          # TODO: can we make a logical choice?
          newparent = eligible[0]
          newkids = eligible.pop(0)
          # get its index record and update it:
          # make it a parent, give it a whg_id - _id, 
          # update its sugs and searchy
          # update its children with newkids
          qget = {"query": {"bool": {"must": [{"match":{"place_id": newparent }}]}}}
          res = es.search(index=idx, body=qget)
          hit = res['hits']['hits'][0]
          _id = hit['_id']
          # elevate to parent
          q_update = {"script":{
                      "source": "ctx._source.whg_id = params._id; \
              ctx._source.relation.name = 'parent'; \
              ctx._source.relation.remove('parent'); \
              ctx._source.children.addAll(params.newkids); \
              ctx._source.suggest.input.addAll(params.sugs); \
              ctx._source.searchy.addAll(params.sugs);",
              "lang": "painless",
            "params":{"_id": _id, "newkids": newkids, "sugs": sugs }
            },
                                "query": {"match":{"place_id": newparent }}
                      }
          try:
            es.update_by_query(index=idx,body=q_update)
          except:
            print('aw shit',sys.exit(sys.exc_info()))
          # parent status transfered to 'eligible' child, add to list
          print('parent w/kids '+hit['_source']['title'],pid+' transferred resp to: '+parent+'; tagged for deletion')
          delthese.append(pid)
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
        zombie = psrc['place_id'] in pids
        if not zombie: # skip zombies here; picked up above with if role == 'parent':
          # remove this id from children and remove its variants (sugs) from suggest.input and searchy
          newsugs = list(set(psugs)-set(sugs))
          newsearchy = list(set(psearchy)-set(searchy))
          q_update = {"script":{
            "source": "ctx._source.children.remove(ctx._source.children.indexOf(params.val)); \
              ctx._source.suggest.input = params.sugs; ctx._source.searchy = params.searchy;",
            "lang": "painless",
            "params":{"val": str(pid), "sugs": newsugs, "searchy": newsearchy }
            },
            "query": {"match":{"_id": parent }}
          }
          try:
            es.update_by_query(index=idx,body=q_update)
            print('child '+psrc['title'],str(pid)+' excised from parent: '+parent+'; tagged for deletion')
          except:
            print('aw shit',sys.exit(sys.exc_info()))
        # child's presence in parent removed, add to delthese[]
        delthese.append(pid)
    elif len(hits) == 0:
      print('not indexed, skipping...')
  es.delete_by_query(idx,body={"query": {"terms": {"place_id": delthese}}})
  print('deleted '+str(len(delthese))+': '+str(delthese))

# ***
# given ds label, return list of place_id 
# ***
def fetch_pids(dslabel):
  pids=[]
  esq_ds = {"size":10000, "query":{"match":{"dataset": dslabel}}}
  res = es.search(index=idx, body=esq_ds)
  docs = res['hits']['hits']
  for d in docs:
    pids.append(d['_source']['place_id'])
  return pids

# ***
# query to get a document by place_id
# ***
def esq_get(pid):
  q = {"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
  return q

# ***
# in index :: bool
def escount_ds(idx,label):
  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  q = {"query":{"match":{"dataset": label }}}
  res = es.search(index=idx,body=q)

  return res['hits']['total']

def confirm(prompt=None, resp=False):
  """prompts for yes or no response from the user. Returns True for yes and
  False for no.
  """
  if prompt is None:
    prompt = 'Confirm'

  if resp:
    prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
  else:
    prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

  while True:
    ans = input(prompt)
    if not ans:
      return resp
    if ans not in ['y', 'Y', 'n', 'N']:
      print('please enter y or n.')
      continue
    if ans == 'y' or ans == 'Y':
      return True
    if ans == 'n' or ans == 'N':
      return False

#idx='whg_test'
def esInit(idx):
  import os, codecs
  os.chdir('/Users/karlg/Documents/Repos/_whgazetteer')

  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  #mappings = codecs.open('es/mappings_whg02.json', 'r', 'utf8').read()
  # added searchy, place_id, whg_id; were being auto-added by indexing
  mappings = codecs.open('es/mappings_whg02-searchy.json', 'r', 'utf8').read()

  # zap existing if exists, re-create
  if confirm(prompt='Zap index '+idx+'?', resp=False):
    try:
      es.indices.delete(idx)
    except Exception as ex:
      print(ex)
    try:
      es.indices.create(index=idx, ignore=400, body=mappings)
      print ('index "'+idx+'" created')
    except Exception as ex:
      print(ex)
  else:
    print('oh, okay')

def uriMaker(place):
  from django.shortcuts import get_object_or_404
  from datasets.models import Dataset
  ds = get_object_or_404(Dataset,id=place.dataset.id)
  if 'whgazetteer' in ds.uri_base:
    return ds.uri_base + str(place.id)
  else:
    return ds.uri_base + str(place.src_id)

#def findMatch(qobj,es):
  #matches = {"parents":[], "names":[]}
  #q_links_f = {"query": { 
    #"bool": {
    #"must": [
      #{"terms": {"links.identifier": qobj['links'] }}
    #]
    #}
  #}}

  #if len(qobj['links']) > 0: # if links, terms query
    #res = es.search(index='whg', doc_type='place', body=q_links_f)
    #hits = res['hits']['hits']
    #if len(hits) > 0:
      #for h in hits:
        ##print(h['_source']['names'])
        #matches['parents'].append( h['_id'] )
        ##matches['parents'].append( h['_source']['place_id'] )
        #for n in h['_source']['names']:
          #matches['names'].append(n['toponym'])
    ## else: create seed (and/or parent+child)
  #return matches

def makeDoc(place,parentid):
  cc_obj = {
      "relation": {},
        "children": [],
        "suggest": {"input":[]},
        "minmax": [],
        "place_id": place.id,
        "dataset": place.dataset.label,
        "src_id": place.src_id,
        "title": place.title,
        "uri": uriMaker(place),
        "ccodes": place.ccodes,
        "names": parsePlace(place,'names'),
        "types": parsePlace(place,'types'),
        "geoms": parsePlace(place,'geoms'),
        "links": parsePlace(place,'links'),
        "timespans": [],
        "descriptions": parsePlace(place,'descriptions'),
        "depictions": [], 
        "relations": [],
        "searchy": []
    }
  return cc_obj

def parsePlace(place,attr):
  qs = eval('place.'+attr+'.all()')
  arr = []
  for obj in qs:
    if attr == 'geoms':
      g = obj.jsonb
      geom={"location":{"type":g['type'],"coordinates":g['coordinates']}}
      if 'citation' in g.keys(): geom["citation"] = g['citation']
      if 'geowkt' in g.keys(): geom["geowkt"] = g['geowkt']
      arr.append(geom)
    else:
      arr.append(obj.jsonb)
  return arr

def jsonDefault(value):
  import datetime
  if isinstance(value, datetime.date):
    return dict(year=value.year, month=value.month, day=value.day)
  else:
    return value.__dict__

#def deleteDocs(ids):
  #from elasticsearch import Elasticsearch
  #es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  #for i in ids:
    #try:
      #es.delete(index='whg', doc_type='place', id=i)
    #except:
      #print('failed delete for: ',id)
      #pass

#def deleteKids(ids):
  #from elasticsearch import Elasticsearch
  #{"nested": {
      #"path": "is_conflation_of",
      #"query": 
        #{"nested" : {
        #"path" :  "is_conflation_of.types",
        #"query" : {"terms": {"is_conflation_of.place_id": ids}}
        #}
        #}
      #}}    
  #q={"query": {"terms": { "":ds }}}
  #es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  #for i in ids:
    #try:
      #es.delete(index='whg', doc_type='place', id=i)
    #except:
      #print('failed delete for: ',id)
      #pass

def deleteDataset(ds):
  q={"query": {"match": { "seed_dataset":ds }}}
  try:
    es.delete(es_index='whg', doc_type='place', body=q)
  except:
    print('failed delete for: ',ds)
    pass




def queryObject(place):
  from datasets.utils import hully
  qobj = {"place_id":place.id,"src_id":place.src_id,"title":place.title}
  variants=[]; geoms=[]; types=[]; ccodes=[]; parents=[]; links=[]

  # ccodes (2-letter iso codes)
  for c in place.ccodes:
    ccodes.append(c)
  qobj['ccodes'] = place.ccodes

  # types (Getty AAT identifiers)
  for t in place.types.all():
    types.append(t.jsonb['identifier'])
  qobj['types'] = types

  # names
  for name in place.names.all():
    variants.append(name.toponym)
  qobj['variants'] = variants

  # parents
  for rel in place.related.all():
    if rel.json['relation_type'] == 'gvp:broaderPartitive':
      parents.append(rel.jsonb['label'])
  qobj['parents'] = parents

  # links
  if len(place.links.all()) > 0:
    for l in place.links.all():
      links.append(l.jsonb['identifier'])
    qobj['links'] = links

  # geoms
  if len(place.geoms.all()) > 0:
    geom = place.geoms.all()[0].jsonb
    if geom['type'] in ('Point','MultiPolygon'):
      qobj['geom'] = place.geoms.first().jsonb
    elif geom['type'] == 'MultiLineString':
      qobj['geom'] = hully(geom)

  return qobj

def makeSeed(place, dataset, whgid):
  # whgid, place_id, dataset, src_id, title
  sobj = SeedPlace(whgid, place.id, dataset, place.src_id, place.title )

  # pull from name.json
  for n in place.names.all():
    sobj.suggest['input'].append(n.json['toponym'])

  # no place_when data yet
  if len(place.whens.all()) > 0:
    sobj['minmax'] = []

  sobj.is_conflation_of.append(makeChildConflate(place))

  return sobj

# abandoned for makeDoc()
class SeedPlace(object):
  def __init__(self, whgid, place_id, dataset, src_id, title):
    self.whgid = whgid
    self.representative_title = title
    self.seed_dataset = dataset
    self.representative_point = []
    self.representative_shape = []
    self.suggest = {"input":[]}
    self.minmax = []
    self.is_conflation_of = []

  def __str__(self):
    import json
    #return str(self.__class__) + ": " + str(self.__dict__)    
    return json.dumps(self.__dict__)

  def toJSON(self):
    import json
    return json.dumps(self, default=jsonDefault, sort_keys=True, indent=2)            

class IndexedPlaceFlat(object):
  def __init__(self, whg_id, place_id, dataset, src_id, title, uri):
    self.relation = {"name":"parent"}
    self.children = []
    self.suggest = {"input":[]}
    self.representative_point = []
    self.minmax = []

    self.whg_id = whg_id
    self.place_id = place_id
    self.dataset = dataset
    self.src_id = src_id
    self.title = title
    self.uri = uri

    self.ccodes = []
    self.names = []
    self.types = []
    self.geoms = []
    self.links = []
    self.timespans = []
    self.descriptions = []
    self.depictions = []
    self.relations = []

  def __str__(self):
    import json
    #return str(self.__class__) + ": " + str(self.__dict__)    
    return json.dumps(self.__dict__)

  def toJSON(self):
    import json
    return json.dumps(self, default=lambda o: o.__dict__, 
                          sort_keys=True, indent=2)    

# to be used in subsequent adds to is_conflation_of[]
class MatchRecord(object):
  def __init__(self, dataset, id, title, uri, exact):
    self.id = id
    self.title = title
    self.uri = uri
    self.source_gazetteer = dataset
    self.exact_matches = exact
    self.names = [{"name":title,"language": ""}]
    self.temporal_bounds = ["", "", "", "", ""]

  def __str__(self):
    import json
    return json.dumps(self.__dict__)    
  
# allpids(125) [6294620, 6294619, 6294573, 6294525, 6294495, 6294617, 6294616, 6294615, 6294614, 6294613, 6294612, 6294611, 6294610, 6294609, 6294608, 6294607, 6294606, 6294605, 6294604, 6294603, 6294602, 6294601, 6294600, 6294599, 6294598, 6294597, 6294596, 6294595, 6294594, 6294593, 6294592, 6294591, 6294590, 6294589, 6294588, 6294587, 6294586, 6294585, 6294584, 6294583, 6294582, 6294581, 6294580, 6294579, 6294578, 6294577, 6294576, 6294575, 6294574, 6294572, 6294571, 6294570, 6294569, 6294568, 6294567, 6294566, 6294565, 6294564, 6294563, 6294562, 6294561, 6294560, 6294559, 6294558, 6294557, 6294556, 6294555, 6294554, 6294553, 6294552, 6294551, 6294550, 6294549, 6294548, 6294547, 6294546, 6294545, 6294544, 6294543, 6294542, 6294541, 6294540, 6294539, 6294538, 6294537, 6294536, 6294535, 6294534, 6294533, 6294532, 6294531, 6294530, 6294529, 6294528, 6294527, 6294526, 6294524, 6294523, 6294522, 6294521, 6294520, 6294519, 6294518, 6294512, 6294511, 6294510, 6294509, 6294508, 6294507, 6294502, 6294501, 6294500, 6294499, 6294498, 6294497, 6294496, 6294494, 6294493, 6294492, 6294491, 6294490, 6294487, 6294486, 6294485, 6294484]
