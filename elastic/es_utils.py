# es_utils.py; created 2019-02-07;
# misc elasticsearch supporting tasks
# revs: 2021-03; 2020-03; 2019-10-01; 2019-03-05;

from django.conf import settings
from django.http import JsonResponse
from places.models import Place
from datasets.static.hashes.parents import ccodes as cchash
from elasticsearch7 import Elasticsearch
es = Elasticsearch([{'host': 'localhost',
                     'port': 9200,
                     'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                     'timeout': 30,
                     'max_retries': 10,
                     'retry_on_timeout': True
                     }])
from copy import deepcopy
import sys
# given pid, gets db and index records
# called by: elastic/index_admin.html
#
def fetch(request):
  from places.models import Place
  from django.contrib.auth.models import User
  user = User.objects.get(pk=1)
  idx='whg'
  if request.method == 'POST':
    print('fetch() request.POST',request.POST)
    pid=request.POST['pid']
    user=request.user
    place=Place.objects.get(pk=pid)
    # pid = 81228 (parent), 81229 (child)

    # database record
    order_list = ['id','title','src_id','dataset_id','ccodes']
    dbplace = {k:v for (k,v) in place.__dict__.items() if k in order_list}
    dbplace['links'] = [l.jsonb['identifier'] for l in place.links.all()]
    dbplace['names'] = [n.toponym for n in place.names.all()]
    dbplace['timespans'] = place.timespans or None
    dbplace['geom count'] = place.geoms.count()
    result = {'dbplace':dbplace}

    # index record(s)
    doc = es.search(index=idx, body=esq_pid(pid))['hits']['hits'][0]
    src = doc['_source']
    is_parent = 'whg_id' in doc['_source'].keys()
    whgid = src['whg_id'] if is_parent else 'n/a'
    idxplace = {'pid':pid,
                'whgid':whgid,
                'title':src['title'],
                'role':src['relation']['name'],
                }
    if is_parent:
      # fetch and parse children
      res = es.search(index=idx, body=esq_children(whgid))
      idxplace['children'] = res['hits']['hits']
    else:
      idx_altparents = []
      # fetch and parse children of parent (siblings)
      res_parent = es.search(index=idx, body=esq_id(src['relation']['parent']))
      parent_pid = res_parent['hits']['hits'][0]['_source']['place_id']
      # print('res_parent', res_parent)
      res = es.search(index=idx, body=esq_children(src['relation']['parent']))
      hits = res['hits']['hits']
      siblings = [{'pid':h['_source']['place_id']} for h in hits]
      # idxplace['siblings'] = siblings
      idxplace['parent'] = {'whgid': src['relation']['parent'],
                            'pid': parent_pid,
                            'title':src['title'],
                            'children': siblings,
                            'ccodes':src['ccodes']}

      # get alternate parents, omitting parent_pid
      result['altparents'] = alt_parents(place, src['relation']['parent'])
    result['idxplace'] = idxplace
    return JsonResponse(result, safe=False)

# basic search for alternate parents
def alt_parents(place, parent_pid):
  # place=Place.objects.get(id=request.POST['pid'])
  qobj = build_qobj(place)
  variants = list(set(qobj["variants"]))
  links = list(set(qobj["links"]))
  linklist = deepcopy(links)
  has_fclasses = len(qobj["fclasses"]) > 0
  has_geom = "geom" in qobj.keys()

  # empty result object
  result_obj = {
    'place_id': qobj['place_id'],
    'title': qobj['title'],
    'hits':[], 'missed':-1, 'total_hits':0,
    'hit_count': 0
  }
  qbase = {"size": 100,"query": {
    "bool": {
      "must": [
        # must share a variant (strict match)
        {"terms": {"names.toponym": variants}},
        {"exists": {"field": "whg_id"}}
      ],
      "should": [
        # bool::should adds to score
        {"terms": {"links.identifier": links }}
        ,{"terms": {"types.identifier": qobj["placetypes"]}}
      ],
      # spatial filters added according to what"s available
      "filter": []
    }
  }}
  # augment base
  if has_geom:
    # qobj["geom"] is always a polygon hull
    shape_filter = { "geo_shape": {
      "geoms.location": {
        "shape": {
          "type": qobj["geom"]["type"],
          "coordinates" : qobj["geom"]["coordinates"]},
        "relation": "intersects" }
    }}
    qbase["query"]["bool"]["filter"].append(shape_filter)
  if has_fclasses:
    qbase["query"]["bool"]["must"].append(
    {"terms": {"fclasses": qobj["fclasses"]}})
  # grab a copy
  q1 = qbase

  try:
    result1 = es.search(index='whg', body=q1)
    hits1 = result1["hits"]["hits"]
  except:
    print("q1, ES error:", q1, sys.exc_info())

  if len(hits1) > 0:
    for h in hits1:
      relation = h["_source"]["relation"]
      h["pass"] = "pass1"
      hitobj = {
        "_id": h['_id'],
        "pid": h["_source"]['place_id'],
        "title": h["_source"]['title'],
        "dataset": h["_source"]['dataset'],
        "pass": "pass1",
        "links": [l["identifier"] \
                  for l in h["_source"]["links"]],
        "role": relation["name"],
        "children": h["_source"]["children"]
      }
      if "parent" in relation.keys():
        hitobj["parent"] = relation["parent"]
      # omit current parent
      if h['_id'] != parent_pid:
        result_obj["hits"].append(hitobj)
      result_obj['total_hits'] = len(result_obj["hits"])
  else:
    result_obj['total_hits'] = 0
  return result_obj

  #
# def esq_addchild(_id):
#   q = {"query":{"bool":{"should": [
#         {"parent_id": {"type": "child","id":_id}},
#         {"match":{"_id":_id}}
#       ]}}}
#   return q

# def addChild(place, parent_id):
#   childobj = makeDoc(place)
#   childobj['relation']['name'] = 'child'
#   childobj['relation']['parent'] = str(parent_id)
#
#   # modify parent:
#   parent = es.search(index='whg', body=esq_addchild(parent_id))['hits']['hits'][0]
#   # - add place.id to children;
#   # - add names.toponym to searchy if absent
#
#   print('adding place doc', childobj, 'as child of', parent_id)



"""
topParent(parents, form)
parents is set or list 

"""
def topParent(parents, form):
  #print('topParent():', parents)   
  if form == 'set':
    # if eq # of kids, use lowest _id
    parents.sort(key=lambda x:(-x[1], x[0]))
    top = parents[0][0]
  else:
    # a list of external parent _ids
    # get one with most children, or just the first?
    top = parents[0]
  #print('winner_id is', top)
  return top

def ccDecode(codes):
  countries=[]
  #print('codes in ccDecode',codes)
  for c in codes:
    countries.append(cchash[0][c]['gnlabel'])
  return countries

"""
build query object qobj
"""
def build_qobj(place):
  from datasets.utils import hully
  #place=get_object_or_404(Place, pk=pid)
  #print('building qobj for ' + str(place.id) + ': ' + place.title)

  qobj = {"place_id":place.id, 
            "src_id":place.src_id, 
            "title":place.title,
            "fclasses":place.fclasses or []}
  [links,ccodes,types,variants,parents,geoms]=[[],[],[],[],[],[]]

  # links
  for l in place.links.all():
    links.append(l.jsonb['identifier'])
  qobj['links'] = links

  # ccodes (2-letter iso codes)
  for c in place.ccodes:
    ccodes.append(c)
  qobj['countries'] = list(set(place.ccodes))

  # types (Getty AAT identifiers)
  # if no aat mappings (srcLabel only), make assumption
  for t in place.types.all():
    if t.jsonb['identifier'] not in ['', None]:
      types.append(t.jsonb['identifier'])
    else:
      # no type? use inhabited place, cultural group, site
      types.extend(['aat:300008347','aat:300387171','aat:300000809'])
      # add fclasses
      qobj['fclasses'] = ['P','S']
  qobj['placetypes'] = list(set(types))

  # variants
  for name in place.names.all():
    variants.append(name.toponym)
  qobj['variants'] = [v.lower() for v in variants]

  # parents
  for rel in place.related.all():
    if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
      parents.append(rel.jsonb['label'])
  qobj['parents'] = parents

  # geoms
  if len(place.geoms.all()) > 0:
    # any geoms at all...
    g_list =[g.jsonb for g in place.geoms.all()]
    # make everything a simple polygon hull for spatial filter purposes
    qobj['geom'] = hully(g_list)

  return qobj


"""
summarize a WHG hit for analysis
"""
def profileHit(hit):
  #print('_source keys',hit['_source'].keys())
  _id = hit['_id']
  src = hit['_source']
  pid = src['place_id']
  
  relation = src['relation']
  profile = {
    '_id':_id,'pid':pid,'title':src['title'],
    'pass': hit['pass'], 'role':relation['name'],
    'dataset':src['dataset'],
    'score':hit['_score']
  }
  profile['parent'] = relation['parent'] if \
    relation['name']=='child' else None
  profile['children'] = src['children'] if \
    relation['name']=='parent' else None
  profile['minmax'] = [src['minmax']['gte'],src['minmax']['lte']] if type(src['minmax']) == dict else None
  profile['links'] = [l['identifier'] for l in src['links']]\
    if len(src['links'])>0 else None
  profile['countries'] = ccDecode(src['ccodes'])
  profile['variants'] = [n['toponym'] for n in src['names']]
  profile['types'] = [t['sourceLabel'] for t in src['types']]
  profile['related'] = [r['label'] for r in src['relations']]
  if len(src['descriptions']) > 0:
    profile['descriptions'] = [d['value'] for d in src['descriptions']]
  geom_objlist = []
  for g in src['geoms']:
    if g not in geom_objlist:
      geom_objlist.append(
        {'id':pid, 
         'ds':src['dataset'], 
         'coordinates':g['location']['coordinates'], 
         'type':g['location']['type']} 
      )
  profile['geoms'] = geom_objlist
  return profile

# ***
# index docs given place_id list
# ***
# TODO:
def indexSomeParents(es, idx, pids):
  from datasets.tasks import maxID
  from django.shortcuts import get_object_or_404
  from places.models import Place
  import sys,json
  whg_id=maxID(es,idx)
  for pid in pids:
    place=get_object_or_404(Place,id=pid)
    whg_id = whg_id+1
    print('new whg_id',whg_id)
    #parent_obj = makeDoc(place,'none')
    parent_obj = makeDoc(place)
    parent_obj['relation']={"name":"parent"}
    # parents get an incremented _id & whg_id
    parent_obj['whg_id']=whg_id
    # add its own names to the suggest field
    for n in parent_obj['names']:
      parent_obj['suggest']['input'].append(n['toponym'])
    # add its title
    if place.title not in parent_obj['suggest']['input']:
      parent_obj['suggest']['input'].append(place.title)
    parent_obj['searchy'] = parent_obj['suggest']['input']
    print('parent_obj',parent_obj)
    #index it
    try:
      res = es.index(index=idx, id=str(whg_id), body=json.dumps(parent_obj))
    except:
      print('failed indexing (as parent)'+str(pid),sys.exc_info())
      pass
    print('created parent:',idx,pid,place.title)    

# ***

# replace docs in index given place_id list
# ***
def replaceInIndex(es,idx,pids):
  from django.shortcuts import get_object_or_404
  from places.models import Place
  import simplejson as json
  #from  . import makeDoc, esq_pid, esq_id, uriMaker
  print('in replaceInIndex():', pids)
  # set counter
  repl_count = 0
  for pid in pids:
    # pid=6294527 (child of 13504937); also 6294533
    # a parent: 6294563
    res = es.search(index=idx, body=esq_pid(pid))
    # make sure it's in the index; in test, might not be
    if len(res['hits']['hits']) > 0:
      hits = res['hits']['hits']
      # get its key info 
      # TODO: what if more than one?
      doc = hits[0]
      src = doc['_source']
      role = src['relation']['name'] #; print(role)

      # get the db instance
      place = get_object_or_404(Place, pk=pid)

      # index doc child or parent?
      if role == 'child':
        # get parent _id
        parentid = src['relation']['parent']
        # write a new doc from db place
        #newchild = makeDoc(place, 'none')
        newchild = makeDoc(place)
        newchild['relation']={"name":"child","parent":parentid}
        # get names from replacement db record
        newnames = [x.toponym for x in place.names.all()]
        # update parent sugs and searchy
        q_update = {"script":{
          "source": "ctx._source.suggest.input.addAll(params.sugs); \
                      ctx._source.searchy.addAll(params.sugs);",
                    "lang": "painless",
          "params":{"sugs": newnames }
          },
                    "query": {"match":{"place_id": parentid }}
          }
        try:
          es.update_by_query(index=idx,body=q_update)
        except:
          print('aw shit',sys.exit(sys.exc_info()))        

        # delete the old
        es.delete_by_query(idx,body={"query":{"match":{"_id":doc['_id']}}})
        # index the new
        es.index(index=idx,id=doc['_id'],
                 routing=1,body=json.dumps(newchild))
        repl_count +=1
      elif role == 'parent':
        # get id, children, sugs from existing index doc
        kids_e = src['children']
        sugs_e = list(set(src['suggest']['input'])) # distinct only

        # new doc from db place; fill from existing
        #newparent = makeDoc(place, None)
        newparent = makeDoc(place)
        newparent['children'] = kids_e

        # merge old & new names in new doc
        previous = set([q['toponym'] for q in newparent['names']])
        names_union = list(previous.union(set(sugs_e)))
        newparent['whg_id'] = doc['_id']
        newparent['suggest']['input'] = names_union
        newparent['searchy'] = names_union
        newparent['relation']={"name":"parent"}

        # out with the old
        es.delete_by_query(idx,body=esq_id(doc['_id']))
        # in with the new
        es.index(index=idx,id=doc['_id'],
                 routing=1,body=json.dumps(newparent))

        repl_count +=1
        print('replaced parent', doc['_id'])
    else:
      print(str(pid)+' not in index, misplaced in pids[]')
      pass
  print('replaceInIndex() count:',repl_count)


# wrapper for removePlacesFromIndex()
# delete all docs for dataset from the whg index,
# whether record is in database or not
def removeDatasetFromIndex(request, *args, **kwargs):
  print('removeDatasetFromIndex() hands pids to removePlacesFromIndex()')
  print('args, kwargs',args, kwargs)
  from datasets.models import Dataset
  from elasticsearch7 import Elasticsearch
  ds = Dataset.objects.get(id = kwargs['dsid'])
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       'timeout': 30,
                       'max_retries': 10,
                       'retry_on_timeout': True}])
  q_pids = {"match": {"dataset": 'sample7h'}}
  # q_pids = {"match": {"dataset": ds.label}}
  res = es.search(index='whg', query=q_pids, _source=["title", "place_id"])
  pids = [h['_source']['place_id'] for h in res['hits']['hits']]
  print('pids in remove...()', pids)
  removePlacesFromIndex(es, 'whg', pids)
  ds.ds_status = 'wd-complete'
  ds.save()
  # for browser console
  return JsonResponse({ 'msg':'pids passed to removePlacesFromIndex('+str(ds.id)+')',
                        'ids': pids})

#
# delete docs in given pid list
# if parent, promotes a child if any
# if child, removes references to it in parent (children[], suggest.input[])
# called from ds_update() and removeDatasetFromIndex() above
def removePlacesFromIndex(es, idx, pids):
  delthese=[]
  print('pids in removePlacesFromIndex()', pids)
  # pids = [6880677, 6880677, 6880679, 6880679, 6880680, 6880680, 6880681,
  #         6880681, 6880682, 6880682, 6880683, 6880683]
  for pid in pids:
    # get index document
    res = es.search(index=idx, query=esq_pid(pid))
    hits=res['hits']['hits']
    print('hits, place', hits)
    # confirm it's in the index
    if len(hits) > 0:
      doc = hits[0]
      src = doc['_source']
      role = src['relation']['name']; print('role:',role)
      sugs = list(set(src['suggest']['input'])) # distinct only
      searchy = list(set([item for item in src['searchy'] if type(item) != list])) 
      # role-dependent action
      print('role', role)
      if role == 'parent':
        # has children?
        kids = [int(x) for x in src['children']]
        # get new parent possibilities
        eligible = list(set(kids)-set(pids)) # not slated for deletion
        if len(eligible) == 0:
          # add to array for deletion
          print('childless parent '+str(pid)+' was tagged for deletion')
          delthese.append(pid)
        else:
          # > 0 eligible children -> pick winner from confirmed children
          qeligible = {"bool": {
            "must": [{"terms":{"place_id": kids }}],
            "should": {"exists": {"field": "links"}}
          }}
          # only kids confirmed to exist
          res = es.search(index=idx, query=qeligible)
          # of those with any links...
          linked = [h['_source'] for h in res['hits']['hits'] if 'links' in h['_source']]
          # which has most?
          linked_len = [{'pid': h['place_id'], 'len': len(h['links'])} for h in linked if 'links' in h]
          # coalesce to 1st eligible if no kid have links
          winner = max(linked_len, key=lambda x: x['len'])['pid'] if len(linked_len)>0 \
            else eligible[0]
          # TODO: can we make a logical choice?
          newparent = winner
          newkids = eligible.pop(eligible.index(winner))
          # update winner
          # make it a parent, give it a whg_id _id,
          # update its children with newkids
          # update its suggest[] and searchy[] arrays (probably redundant)
          q_update = {"script":{"source": "ctx._source.whg_id = params._id; \
              ctx._source.relation.name = 'parent'; \
              ctx._source.relation.remove('parent'); \
              ctx._source.children.addAll(params.newkids); \
              ctx._source.suggest.input.addAll(params.sugs); \
              ctx._source.searchy.addAll(params.sugs);",
                                "lang": "painless",
              "params":{"_id": newparent, "newkids": newkids, "sugs": sugs }
            },
              "query": {"match":{"place_id": newparent }}}
          try:
            es.update_by_query(index=idx, body=q_update)
          except:
            print('update of new parent failed',sys.exit(sys.exc_info()))
          # parent status transfered to 'eligible' child, add to list
          print('parent w/kids, '+pid +' transferred resp to: '+newparent+' & was tagged for deletion')
        delthese.append(pid)
      elif role == 'child':
        # get its parent
        parent = src['relation']['parent']
        qget = {"bool": {"must": [{"match":{"_id": parent }}]}}
        res = es.search(index=idx, query=qget)
        # parent _source, suggest, searchy
        psrc = res['hits']['hits'][0]['_source']
        print('a child; parent src', pid, psrc)
        print('pids at this pojnt:', pids)
        psugs = list(set(psrc['suggest']['input']))
        psearchy = list(set([item for item in psrc['searchy'] if type(item) != list]))
        # is parent slated for deletion? (walking dead)
        zombie = psrc['place_id'] in pids
        print('zombie?', zombie)
        if not zombie: # skip zombies; picked up above with if role == 'parent':
          # remove this id from children and remove its variants (sugs) from suggest.input and searchy
          newsugs = list(set(psugs)-set(sugs))
          newsearchy = list(set(psearchy)-set(searchy))
          print("newsugs",newsugs)
          print("newsearchy",newsearchy)
          q_update = {"script":{
            "lang": "painless",
            "source": """
              ctx._source.suggest.input = params.sugs; 
              ctx._source.searchy = params.searchy;""",
            "params":{"sugs": newsugs, "searchy": newsearchy }
            },
              "query": {"match":{"_id": parent }}
            }
          print('q_update::542', q_update)
          # sometimes docs named as parent don't have the id of the child
          # in that case, don't look for the child id, it'll break ES
          if len(psrc['children']) > 0:
            q_update['script']['source'] = """
              ctx._source.children.remove(ctx._source.children.indexOf(params.val));
              ctx._source.suggest.input = params.sugs; ctx._source.searchy = params.searchy;"""
            q_update['script']['params']['val'] = str(pid)
          print('q_update::549', q_update)
          try:
            es.update_by_query(index=idx, body=q_update)
            print('child '+psrc['title'],str(pid)+' excised from parent: '+parent+'; tagged for deletion')
          except:
            print('update of parent losing child failed', sys.exit(sys.exc_info()))
            pass
        # child's presence in parent removed, add to delthese[]
        delthese.append(pid)

      # DB ACTIONS
      try:
        # get database record if it wasn't just deleted
        place = Place.objects.get(id=pid)
        place.indexed = False
        # delete previous hits from whg task
        place.hit_set.filter(authority='whg').delete()
        # reset review_whg status to null
        place.review_whg = None
        place.save()
      except:
        pass
    else:
      print(str(pid) + ' not in index, passed')
      pass
  es.delete_by_query(idx,body={"query": {"terms": {"place_id": delthese}}})
  print('deleted '+str(len(delthese))+': '+str(delthese))
  msg = 'deleted '+str(len(delthese))+': '+str(delthese)
  return JsonResponse(msg, safe=False)

# ***
# given ds label, return list of place_id 
# ***
def fetch_pids(dslabel):
  pids=[]
  esq_ds = {"size":10000, "query":{"match":{"dataset": dslabel}}}
  res = es.search(index='whg', body=esq_ds)
  docs = res['hits']['hits']
  for d in docs:
    pids.append(d['_source']['place_id'])
  return pids

# ***
# query to get a document by place_id
# ***
def esq_pid(pid):
  q = {"bool": {"must": [{"match":{"place_id": pid }}]}}
  return q

# ***
# query to get a document by _id
# ***
def esq_id(_id):
  q = {"bool": {"must": [{"match":{"_id": _id }}]}}
  return q

# ***
# query to get children or siblings
# ***
def esq_children(_id):
  q = {"query":{"bool":{"should": [
        {"parent_id": {"type": "child","id":_id}},
        {"match":{"_id":_id}}
      ]}}}
  return q

# ***
# count of dataset docs in index
# ***
def escount_ds(idx,label):
  from elasticsearch7 import Elasticsearch
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       'timeout':30,
                       'max_retries':10,
                       'retry_on_timeout':True}])
  q = {"match":{"dataset": label }}
  # TODO: match new pattern query={} across platform
  res = es.search(index=idx, query=q)

  return res['hits']['total']['value']

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

# ***
# create an index
# ***
def esInit(idx):
  import os, codecs
  from django.conf import settings
  os.chdir('/Users/karlg/Documents/Repos/_whgazetteer/')

  from elasticsearch7 import Elasticsearch
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       # 'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       # alternate (settings.ES_APIKEY_REMOTE on server)
                       'api_key': ('dlhTNGM0TUIxLVF2NVJzbUxlNnk6RDBFbVlwSURSb0N6Tk1kTXctazZSdw=='),
                       'timeout': 30,
                       'max_retries': 10,
                       'retry_on_timeout': True}])
  mappings = codecs.open('elastic/mappings/es_mappings_whg_20220925.json', 'r', 'utf8').read()

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

# ***
# called from makeDoc()
# ***
def uriMaker(place):
  from django.shortcuts import get_object_or_404
  from datasets.models import Dataset
  ds = get_object_or_404(Dataset,id=place.dataset.id)
  if 'whgazetteer' in ds.uri_base:
    return ds.uri_base + str(place.id)
  else:
    return ds.uri_base + str(place.src_id)

# ***
# make an ES doc from a Place instance
# called from ALL indexing functions (initial and updates)
# ***
def makeDoc(place):
  es_doc = {
    "relation": {},
    "children": [],
    "suggest": {"input":[]},
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
    # new, for index whg03
    "fclasses": place.fclasses,
    "timespans": [{"gte":t[0],"lte":t[1]} for t in place.timespans] if place.timespans not in [None,[]] else [],
    "minmax": {"gte":place.minmax[0],"lte":place.minmax[1]} if place.minmax not in [None,[]] else [],
    "descriptions": parsePlace(place,'descriptions'),
    "depictions": parsePlace(place,'depictions'), 
    "relations": parsePlace(place,'related'),
    "searchy": []
  }
  return es_doc

# ***
# fill ES doc arrays from database jsonb objects
# ***
def parsePlace(place, attr):
  qs = eval('place.'+attr+'.all()')
  arr = []
  for obj in qs:
    if attr == 'geoms':
      g = obj.jsonb
      geom={"location":{"type":g['type'],"coordinates":g['coordinates']}}
      if 'citation' in g.keys(): geom["citation"] = g['citation']
      if 'geowkt' in g.keys(): geom["geowkt"] = g['geowkt']
      arr.append(geom)
    elif attr == 'whens':
      when_ts = obj.jsonb['timespans']
      # TODO: index wants numbers, spec says strings
      # expect strings, including operators
      for t in when_ts:
        x={"start": int(t['start'][list(t['start'])[0]]),
           "end": int(t['end'][list(t['end'])[0]]) }
        arr.append(x)
    else:
      arr.append(obj.jsonb)
  return arr

""" DEPRECATED
demoteParents(demoted, winner_id, pid)
makes each of demoted[] (and its children if any)
a child of winner_id

# """
# def demoteParents(demoted, winner_id, pid):
#   #demoted = ['14156468']
#   #newparent_id = winner_id
#   print('demoteParents()',demoted, winner_id, pid)
#   #qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""
#
#   # updates 'winner' with children & names from demoted
#   def q_updatewinner(kids, names):
#     pass
#   return {"script":{
#     "source": """ctx._source.children.addAll(params.newkids);
#       ctx._source.suggest.input.addAll(params.names);
#       ctx._source.searchy.addAll(params.names);""",
#     "lang": "painless",
#     "params":{
#       "newkids": kids,
#       "names": names }
#   }}
#
#   for d in demoted:
#     # get the demoted doc, its names and kids if any
#     #d = demoted[0]
#     #d = '14156468'
#     #winner_id = '14156467'
#     qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""
#     try:
#       qget = qget % (d)
#       doc = es.search(index='whg', body=qget)['hits']['hits'][0]
#     except:
#       print('failed getting winner; winner_id, pid',winner_id, pid)
#       sys.exit(sys.exc_info())
#     srcd = doc['_source']
#     kids = srcd['children']
#     # add this doc b/c it's now a kid
#     kids.append(doc['_id'])
#     names = list(set(srcd['suggest']['input']))
#
#     # first update the 'winner' parent
#     q=q_updatewinner(kids, names)
#     try:
#       es.update(idx,winner_id,body=q)
#     except:
#       print('q_updatewinner failed (pid, winner_id)',pid,winner_id)
#       sys.exit(sys.exc_info())
#
#     # then modify copy of demoted,
#     # delete the old, index the new
#     # --------------
#     newsrcd = deepcopy(srcd)
#     newsrcd['relation'] = {"name":"child","parent":winner_id}
#     newsrcd['children'] = []
#     if 'whg_id' in newsrcd:
#       newsrcd.pop('whg_id')
#     # zap the old demoted, index the modified
#     try:
#       es.delete('whg', d)
#       es.index(index='whg',id=d,body=newsrcd,routing=1)
#     except:
#       print('reindex failed (pid, demoted)',pid,d)
#       sys.exit(sys.exc_info())
