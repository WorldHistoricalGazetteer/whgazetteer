# es_utils.py rev. Mar 2020; rev. 02 Oct 2019; rev 5 Mar 2019; created 7 Feb 2019;
# misc elasticsearch supporting tasks 

# ***
# index docs given place_id list
# ***
# TODO:
def indexSomeParents(es,idx,pids):
  from datasets.tasks import maxID
  from django.shortcuts import get_object_or_404
  from places.models import Place
  import sys,json
  whg_id=maxID(es,idx)
  for pid in pids:
    place=get_object_or_404(Place,id=pid)
    whg_id = whg_id+1
    print('new whg_id',whg_id)
    parent_obj = makeDoc(place,'none')
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
      res = es.index(index=idx, doc_type='place', id=str(whg_id), body=json.dumps(parent_obj))
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
        newchild = makeDoc(place, 'none')
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
        es.index(index=idx,doc_type='place',id=doc['_id'],
          routing=1,body=json.dumps(newchild))
        repl_count +=1
      elif role == 'parent':
        # get id, children, sugs from existing index doc
        kids_e = src['children']
        sugs_e = list(set(src['suggest']['input'])) # distinct only
                
        # new doc from db place; fill from existing
        newparent = makeDoc(place, None)
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
        es.index(index=idx,doc_type='place',id=doc['_id'],
          routing=1,body=json.dumps(newparent))
        
        repl_count +=1
        print('replaced parent', doc['_id'])
    else:
      print(str(pid)+' not in index, misplaced in pids[]')
      pass
  print('replaceInIndex() count:',repl_count)
      
      
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
  # 
  for pid in pids:
    # get its index document
    res = es.search(index=idx, body=esq_pid(pid))
    hits=res['hits']['hits']
    # is it in the index?
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
          # TODO: this is meaningful only for tests
          # ensure the prospective parent exists
          if len(res['hits']['hits']) > 0:
            hit = res['hits']['hits'][0]
            _id = hit['_id']
            # elevate to parent
            q_update = {"script":{"source": "ctx._source.whg_id = params._id; \
                ctx._source.relation.name = 'parent'; \
                ctx._source.relation.remove('parent'); \
                ctx._source.children.addAll(params.newkids); \
                ctx._source.suggest.input.addAll(params.sugs); \
                ctx._source.searchy.addAll(params.sugs);",
                "lang": "painless",
              "params":{"_id": _id, "newkids": newkids, "sugs": sugs }
              },
              "query": {"match":{"place_id": newparent }}}
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
    #elif len(hits) == 0:
      #print('not indexed, skipping...')
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
def esq_pid(pid):
  q = {"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
  return q

# ***
# query to get a document by _id
# ***
def esq_id(_id):
  q = {"query": {"bool": {"must": [{"match":{"_id": _id }}]}}}
  return q

# ***
# count of dataset docs in index
# ***
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


# ***
# create an index
# ***
def esInit(idx):
  import os, codecs
  os.chdir('/Users/karlg/Documents/Repos/_whgazetteer')

  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  mappings = codecs.open('es/mappings_whg03.json', 'r', 'utf8').read()

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
def makeDoc(place,parentid):
  # TODO: remove parentid; used in early tests
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
    elif attr == 'whens':
      when_ts = obj.jsonb['timespans']
      # TODO: index wants numbers, spec says strings
      # expect strings, including operators
      for t in when_ts:
        x={"start": int(t['start'][list(t['start'])[0]]), \
           "end": int(t['end'][list(t['end'])[0]]) }
        arr.append(x)
    else:
      arr.append(obj.jsonb)
  return arr

