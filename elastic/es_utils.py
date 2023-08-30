# es_utils.py; created 2019-02-07;
# misc elasticsearch supporting tasks
# revs: 2021-03; 2020-03; 2019-10-01; 2019-03-05;

from django.conf import settings
from django.http import JsonResponse
from places.models import Place
from datasets.static.hashes.parents import ccodes as cchash
from elasticsearch7 import Elasticsearch
es = settings.ES_CONN
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
build query object qobj for ES
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
  # if no aat mappings (srcLabel only)
  for t in place.types.all():
    if t.jsonb['identifier'] not in ['', None]:
      types.append(t.jsonb['identifier'])
    else:
      # no type? use inhabited place, cultural group, site
      types.extend(['aat:300008347','aat:300387171','aat:300000809'])
      # add fclasses
      # qobj['fclasses'] = ['P','S']

      # hot fix 2 Apr 2023:
      # if no types, add all fclasses ('X' appears in some)
      qobj['fclasses'] = ['P', 'S', 'A', 'T', 'H', 'L', 'R', 'X']
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
  _id = hit['_id']
  src = hit['_source']
  pid = src['place_id']
  print('profileHit() src', src)

  relation = src['relation']
  profile = {
    '_id':_id,'pid':pid,'title':src['title'],
    'pass': hit['pass'], 'role':relation['name'],
    'dataset':src['dataset'],
    'score':hit['_score']
  }
  types = src['types']

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
    # add its own names to the searchy field
    # suggest['input] field no longer in use
    for n in parent_obj['names']:
      parent_obj['searchy'].append(n['toponym'])
      # parent_obj['suggest']['input'].append(n['toponym'])
    # add its title
    if place.title not in parent_obj['searchy']:
      parent_obj['searchy'].append(place.title)
    # print('parent_obj',parent_obj)
    #index it
    try:
      res = es.index(index=idx, id=str(whg_id), body=json.dumps(parent_obj))
    except:
      print('failed indexing (as parent)'+str(pid),sys.exc_info())
      pass
    place.indexed = True
    place.review_whg = True
    place.save()
    # print('created parent:',idx, pid, place.title)

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
        es.delete_by_query(index=idx, body=esq_id(doc['_id']))
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
  print('args, kwargs', args, kwargs)
  from datasets.models import Dataset
  ds = Dataset.objects.get(id = args[0] if args else kwargs['dsid'])
  es = settings.ES_CONN
  # q_pids = {"match": {"dataset": 'sample7h'}}
  q_pids = {"match": {"dataset": ds.label}}
  res = es.search(index='whg', query=q_pids, _source=["title", "place_id"], size=ds.places.count())
  pids = [h['_source']['place_id'] for h in res['hits']['hits']]
  print('pids in remove...()', pids)
  removePlacesFromIndex(es, 'whg', pids)
  ds.ds_status = 'wd-complete'
  ds.save()
  # remove indexed flag in places

  # delete latest idx task (its hits were removed already)
  latest = ds.tasks.filter(task_name='align_idx',status="SUCCESS").order_by('-date_done')[0]
  latest.delete()

  # for browser console
  return JsonResponse({ 'msg':'pids passed to removePlacesFromIndex('+str(ds.id)+')',
                        'ids': pids})

#
# delete docs in given pid list
# if parent, promotes a child if any
# if child, removes references to it in parent (children[], suggest.input[])
# called from ds_update() and removeDatasetFromIndex() above
# TODO: why populate delthese[]? pids to delete are provided
def removePlacesFromIndex(es, idx, pids):
  delthese=[]
  print('pids in removePlacesFromIndex()', pids)
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
      # searchy[] for children is typically empty
      searchy = list(set([item for item in src['searchy'] if type(item) != list]))
      # role-dependent action
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
          # > 0 eligible children? pick winner from confirmed children
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
          # TODO: better logical choice of winner?
          newparent = winner
          newkids = eligible.pop(eligible.index(winner))
          # update winner
          # make it a parent: give it a whg_id; update children with newkids; update searchy[]
          q_update = {"script":{"source": "ctx._source.whg_id = params._id; \
              ctx._source.relation.name = 'parent'; \
              ctx._source.relation.remove('parent'); \
              ctx._source.children.addAll(params.newkids); \
              ctx._source.searchy.addAll(params.searchy);",
              "lang": "painless",
              "params":{"_id": newparent, "newkids": newkids, "searchy": searchy }
            },
              "query": {"match":{"place_id": newparent }}}
          try:
            es.update_by_query(index=idx, body=q_update)
            delthese.append(pid)
          except:
            print('update of new parent failed',sys.exit(sys.exc_info()))
          # parent status transfered to 'eligible' child, add to list
          print('parent w/kids, '+pid +' transferred resp to: '+newparent+' & was tagged for deletion')
      elif role == 'child':
        # get its parent and remove its id from parent's children
        # parent's searchy can't be reliably edited
        parent = src['relation']['parent']
        qget = {"bool": {"must": [{"match":{"_id": parent }}]}}
        res = es.search(index=idx, query=qget)
        # parent _source, suggest, searchy
        psrc = res['hits']['hits'][0]['_source']
        print('a child; parent src:', pid, psrc)
        # is parent slated for deletion? (walking dead)
        zombie = psrc['place_id'] in pids
        # skip zombies; picked up above with if role == 'parent':
        # sometimes docs named as parent don't have the id of the child
        # in that case, don't look for the child id, it'll break ES
        if not zombie and len(psrc['children']) > 0:
          # remove this id from parent's children
          q_update = {"script":{
            "lang": "painless",
            "source": "ctx._source.children.remove(ctx._source.children.indexOf(params.val))",
            "params":{"val": str(pid) }
            },
              "query": {"match":{"_id": parent }}
            }
          try:
            es.update_by_query(index=idx, body=q_update)
            # child id removed, add to delthese[]
            delthese.append(pid)
            print('child ' + psrc['title'], str(pid) + ' excised from parent: ' + parent + '; tagged for deletion')
          except:
            print('update of parent losing child failed', sys.exit(sys.exc_info()))
            pass
          # TODO: should excise names from searchy, but can't yet
          print('q_update initial:', q_update)
        else:
          # child thought this was parent but parent.children[] doesn't have it
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
      print(str(pid) + ' not in index for some reason, passed')
      pass
  es.delete_by_query(index=idx, body={"query": {"terms": {"place_id": delthese}}})
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
  es = settings.ES_CONN
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
  idx='wd_text'
  import os, codecs
  from django.conf import settings
  os.chdir('elastic/mappings/')
  es = settings.ES_CONN
  file = codecs.open('wd2023_multi.json', 'r', 'utf8').read()
  mappings = ''.join(line for line in file if not '/*' in line)
  # zap existing if exists, re-create
  if confirm(prompt='Zap index '+idx+'?', resp=False):
    try:
      es.indices.delete(index=idx)
    except Exception as ex:
      print(ex)
    try:
      es.indices.create(index=idx, ignore=400, body=mappings)
      es.indices.put_alias('wd_text', 'wd')
    except Exception as ex:
      print(ex)
    print ('index "'+idx+'" created')
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

def repair2():
  from django.db import connection
  cursor1 = connection.cursor()
  cursor1.execute("SELECT aat_id FROM types where fclass is not null")
  raw=cursor1.fetchall()
  aatlist = [x[0] for x in raw]
  with connection.cursor() as cursor2:
    # cursor2 = connection.cursor()
    for a in aatlist:
      cursor2.execute("SELECT fclass FROM types where aat_id = %s", [a])
      fc = [cursor2.fetchone()[0]]
      q_update = {"script": {
        "source": "ctx._source.fclasses = params.fc",
        "lang": "painless",
        "params": {"fc": fc}
      },
        "query": {"bool": {
          "must": [
            {"match": {"types.identifier": 'aat:'+str(a)}},
            {"exists": {"field": "whg_id"}}
          ]
        }}
      }
      print(fc[0])
      es.update_by_query(index=idx, body=q_update, conflicts='proceed')


def repair_fclasses():
  from places.models import Place
  # indexed = Place.objects.filter(indexed=True)
  indexed = Place.objects.filter(indexed=True, dataset='wri_lakes')
  idx = 'restored_whg07'
  for p in indexed:
    # new_fc = p.fclasses.append('X')
    q_update = {"script": {
      "source": "ctx._source.fclasses = params.fc",
      "lang": "painless",
      "params": {"fc": p.fclasses}
    },
      "query": {"bool": {
        "must": [
          {"match": {"fclasses": "X"}},
          {"exists": {"field": "whg_id"}}
        ]
      }}
    }
    es.update_by_query(index=idx, body=q_update, conflicts='proceed')


"""
  find perform match for hits matching variant(s) and geoms <= ~3k apart
"""
def match_close_idx(dsid, n=50000, test=True, dscoll=None):
  from django.conf import settings
  from datasets.models import Dataset, Hit
  from elastic.es_utils import makeDoc
  from places.models import Place, PlaceGeom
  from django.contrib.gis.geos import GEOSGeometry
  import json, sys
  es = settings. ES_CONN
  ds=Dataset.objects.get(id=dsid)
  idx='whg'

  # latest successful align_idx task
  task = ds.tasks.filter(task_name='align_idx', task_args='['+str(dsid)+']', status='SUCCESS')[0]

  # all unreviewed hits for the task
  hits = Hit.objects.filter(task_id = task.task_id,
                            reviewed = False,
                            ).order_by('place_id')

  [count_add, geom_add, link_add, still_queued, skipped, to_match] = [0, 0, 0, 0, 0, 0]
  matches = []

  for h in hits[:n]:
    # incoming record
    place = Place.objects.get(id=h.place_id)
    p_geoms = [pg.geom for pg in place.geoms.all()]

    # prospective match
    hitplace = Place.objects.get(id=h.json['pid'])
    h_geoms = h.geom

    # both must have geometry or don't bother
    if len(p_geoms) > 0 and len(h_geoms) > 0:
      # print('geoms to compare, proceed')

        # partial db record
      hobj = {'pid': h.json['pid'],
              'links':h.json['links'],
              'geoms':h.json['geoms']}

      # first hit geometry
      ghit = [GEOSGeometry(json.dumps(g)) for g in hobj['geoms']][0]

      # first place geometry
      gpl = p_geoms[0] # first geom of db place

      try:
        dist = ghit.distance(gpl) * 100
      except:
        print('dist failed', sys.exc_info())

      """
      perform match, i.e.
        create ES place doc for place
          all hits are parents
          add place as child; add its names to searchy[]
        FUTURE gaz-builder scenario:
          if hit in ds collection:
            write db place_link
      """
      if dist <= 5:
        count_add +=1
        if not test:
          # perform match
          new_obj = makeDoc(place)
          # new_obj['relation'] = {"name": "child", "parent": parent_whgid}
          new_obj['relation'] = {"name":"child", "parent": h.json['whg_id']}
          # index the child
          try:
            es.index(index=idx, id=place.id, routing=1, body=json.dumps(new_obj))
          except:
            print('es.index() failed', sys.exc_info())
          # add child to parent children[]
          place_names = [p.toponym for p in place.names.all()]
          hit_names = [elem for sublist in [s['variants'] for s in h.json['sources']] for elem in sublist]
          new_names = list(set(place_names) - set(hit_names))
          q_update = {"script": {
            "source": "ctx._source.children.add(params.id)",
            # "source": "ctx._source.searchy.input.addAll(params.names); ctx._source.children.add(params.id)",
            "lang": "painless",
            "params": {"names": new_names, "id": str(place.id)}
          },
            "query": {"match": {"_id": h.json['whg_id']}}}
          try:
            es.update_by_query(index=idx, body=q_update, conflicts='proceed')
          except:
            print('es.update_by_query() failed', sys.exc_info())

          # place was reviewed
          place.review_whg = 1
          place.save()
          h.matched = True
          h.reviewed = True
          h.save()

          """ not creating db link records, YET! """
          # jsonb = {
          #   "type": gidx0.geom_type,
          #   "citation": {"id": 'wd:' + hit_pid, "label":'Wikidata' },
          #   "coordinates": hobj['geoms'][0]['coordinates']
          # }

          # place, src_id, jsonb, task_id, geom
          # pgobj = PlaceGeom.objects.create(
          #   place = place,
          #   geom = gidx0,
          #   jsonb = jsonb,
          #   src_id = place.src_id,
          #   task_id = task.task_id
          # )
          # pgobj.save()
          # geom_add +=1
          # # place, src_id, jsonb, task_id
          # # {"type": "closeMatch", "identifier": "tgn:7011198"}
          # for i, l in enumerate(hobj['links']):
          #   # print('links to add:', i, l)
          #   link_add +=1
          #   plobj = PlaceLink.objects.create(
          #     place_id = place.id,
          #     src_id = place.src_id,
          #     task_id = task.task_id,
          #     jsonb={'type': 'closeMatch', 'identifier': l}
          #   )
          #   plobj.save()
          #
          # # wrote geoms/links
          # h.matched = True
          # h.reviewed = True
          # h.save()
          # # place was reviewed
          # place.review_wd = 1
          # place.save()
        else:
          # count or log this match for inspection
          to_match +=1
          # matches.append({'ds_pid':place.id, 'hit_pid':h.json['pid']})
      else:
        # no match, leave in queue
        still_queued +=1
    else:
      skipped +=1
  print("wrote match records" if not test else "only tested...")
  print("counts:", {"matched": count_add,
                    "to_match": to_match,
                    "no geom": skipped,
                    "geoms": geom_add,
                    "links": link_add,
                    "remaining": still_queued,
                    "matches": matches
                    })

