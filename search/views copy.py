# various search.views
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import View
import simplejson as json, sys
from areas.models import Area
from datasets.tasks import normalize, get_bounds_filter
from datasets.models import Dataset, Hit
from elasticsearch import Elasticsearch
from django.db.models import Count

    
class LookupView(View):
  @staticmethod
  def get(request):
    print('in LookupView, GET =',request.GET)
    """
      args in request.GET:
        [string] idx: latest name for whg index
        [string] place_id: from a trace body
    """
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    idx = request.GET.get('idx')
    pid = request.GET.get('place_id')
    q={"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
    res = es.search(index=idx, doc_type='place', body=q)
    hit = res['hits']['hits'][0]
    print('hit[_id] from search/lookup',hit['_id'])
    #print('LookupView pid',pid)
    print({"whg_id":hit['_id']})
    return JsonResponse({"whg_id":hit['_id']}, safe=False)
    #return {"whg_id":hit['_id']}
  
def fetchArea(request):
  aid = request.GET.get('pk')
  area = Area.objects.filter(id=aid)
  return JsonResponse(area)

def makeGeom(pid,geom):
  # TODO: account for non-point
  geomset = []
  if len(geom) > 0:    
    for g in geom:
      geomset.append(
        {"type":g['location']['type'],"coordinates":g['location']['coordinates'],"properties":{"pid": pid}}
      )
  return geomset

"""
  format search result items (places or traces)
"""
def suggestionItem(s,doctype,scope):
  #print('sug geom',s['geometries'])
  if doctype == 'place':
    if scope == 'suggest':
      item = { 
        "name":s['title'],
        "type":s['types'][0]['label'],
        "whg_id":s['whg_id'],
        "pid":s['place_id'],
        "variants":[n for n in s['suggest']['input'] if n != s['title']],
        "dataset":s['dataset'],
        "ccodes":s['ccodes'],
        #"geom": makeGeom(s['place_id'],s['geoms'])
      }
      #print('place sug item', item)
    else:
      h = s['hit']
      #print('place search "s":',s)
      #print('place search hit:',h)
      item = {
        "whg_id": h['whg_id'] if 'whg_id' in h else '',
        "pid":h['place_id'],
        "linkcount":s['linkcount'],
        "name": h['title'],
        "variants":[n for n in h['suggest']['input'] if n != h['title']],
        "ccodes": h['ccodes'],
        "types": [t['label'] for t in h['types'] ],
        "geom": makeGeom(h['place_id'],h['geoms'])
        #"snippet": s['snippet']['descriptions.value'][0] if s['snippet'] != '' else []
      }
  elif doctype == 'trace':
    # now with place_id, not whg_id (aka _id; they're transient)
    # TODO: targets are list in latest spec, but example data has only one
    target = s['hit']['target'] if type(s['hit']['target']) == dict else s['hit']['target'][0]
    item = {
      "_id": s['_id'],
      "id": target['id'],
      "type": target['type'],
      "title": target['title'],
      "depiction": target['depiction'] if 'depiction' in target.keys() else '',
      "bodies":s['hit']['body']
    }
  #print('place search item:',item)
  return item


"""
  actually performs es search, places (whg) or traces
"""
def suggester(doctype,q,scope,idx):
  # returns only parents; children retrieved into place portal
  #print('suggester',doctype,q)
  es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout':30, 'max_retries':10, 'retry_on_timeout':True}])
  suggestions = []
  
  if doctype=='place':
    print('suggester/place q:',q)
    #res = es.search(index=idx, doc_type='place', body=q)
    res = es.search(body=q)
    if scope == 'suggest':
      sugs = res['suggest']['suggest'][0]['options']
      #print('suggester()/place sugs',sugs)
      if len(sugs) > 0:
        for s in sugs:
          if 'parent' not in s['_source']['relation'].keys():
            # it's a parent, add to suggestions[]
            suggestions.append(s['_source'])
      return suggestions      
    elif scope == 'search':
      hits = res['hits']['hits']
      #print('suggester()/place hits',hits)
      # identify distinct parents
      #parents = [h for h in hits if 'whg_id' in h['_source']]
      #children = [h for h in hits if 'whg_id' not in h['_source']]
      #parent_ids = [p['_source']['whg_id'] for p in parents]
      #kid_parents = [int(c['_source']['relation']['parent']) for c in children]
      if len(hits) > 0:
        for h in hits:
          #snippet = h['highlight'] if 'highlight' in h else ''
          suggestions.append(
            {"_id": h['_id'],
             "linkcount":len(h['_source']['links']),
             "hit": h['_source'],
             #"snippet":snippet
            }
          )
      sortedsugs = sorted(suggestions, key=lambda x: x['linkcount'], reverse=True)
      # TODO: there may be parents and children
      #print('SUGGESTIONS from suggester()',type(suggestions), sortedsugs)
      return sortedsugs
    
  elif doctype == 'trace':
    print('suggester()/trace q:',q)
    res = es.search(index='traces',doc_type='trace',body=q)
    hits = res['hits']['hits']
    #print('suggester()/trace hits',hits)
    if len(hits) > 0:
      for h in hits:
        suggestions.append({"_id":h['_id'],"hit":h['_source']})
    #print('suggestions',suggestions)
    return suggestions 

""" 
  executes place:search/suggest or trace:search 
  via suggester(), formatted by suggestionItem()
"""
class SearchView(View):
  @staticmethod
  def get(request):
    print('SearchView() request',request.GET)
    print('SearchView() bounds',request.GET.get('bounds'))
    """
      args in request.GET:
          [string] qstr: query string
          [string] doc_type: place or trace
          [string] scope: suggest or search
          [string] idx: index to be queried
          [int] year: filter for timespans including this
          [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
    """
    qstr = request.GET.get('qstr')
    doctype = request.GET.get('doc_type')
    scope = request.GET.get('scope')
    idx = request.GET.get('idx')
    fclasses = request.GET.get('fclasses')
    start = request.GET.get('start')
    end = request.GET.get('end')
    bounds = request.GET.get('bounds')
    #print('bounds',fclasses,start,end)
    
    if doctype == 'place':
      if scope == 'suggest':
        q = { "suggest":{"suggest":{"prefix":qstr,"completion":{"field":"suggest"}} } }
        print('suggest query:',q)
      elif scope == 'search':
        q = { "size": 100,
              "query": {"bool": {
              "must": [
                {"exists": {"field": "whg_id"}},
                {"multi_match": {
                  "query": qstr, 
                  "fields": ["title","names.toponym","searchy"],
                  "type": "phrase"
                }}
              ]
            }}
        }
        if fclasses:
          q['query']['bool']['must'].append({"terms": {"fclasses": fclasses.split(',')}})
        if start:
          q['query']['bool']['must'].append({"range":{"timespans":{"gte" :start,"lte":end if end else 2005}}})
        if bounds:
          bounds=json.loads(bounds)
          #q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg') if bounds['id'] != ['0'] else []
          q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg')
          
        # truncate, may include polygon coordinates
        print('search must[]:',q['query']['bool']['must'])
        #if 'filter' in q['query']['bool']:
          #print('search filter[]:',q['query']['bool']['filter'])

    elif doctype == 'trace':
      q={ 
        "size": 100,
        "query": { "bool": {
          "must": [
            {"multi_match": {
              "query": qstr,
              "fields": ["target.title","tags"],
              "type": "phrase_prefix"
          }}]
        }}
      }      
      #q = { "query": {"match": {"target.title": {"query": qstr,"operator": "and"}}} }
      print('trace query:',q)
      
    suggestions = suggester(doctype, q, scope, idx)
    #print('raw suggestions (new style):',suggestions)
    suggestions = [ suggestionItem(s, doctype, scope) for s in suggestions]
    return JsonResponse(suggestions, safe=False)
  
'''
  returns 300 index docs in current map viewport
'''
def contextSearch(idx,doctype,q):
  #print('context query',q)
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  count_hits=0
  result_obj = {"hits":[]}
  res = es.search(index=idx, doc_type=doctype, body=q, size=300)
  hits = res['hits']['hits']
  # TODO: refactor this bit
  #print('hits',hits)
  if len(hits) > 0:
    #print('contextSearch() hit0 _source: ',hits[0]["_source"])
    for hit in hits:
      count_hits +=1
      if idx.startswith("whg"):
        # why normalize here?
        # result_obj["hits"].append(hit["_source"])
        result_obj["hits"].append(normalize(hit["_source"],idx))
      else:
        # this is traces
        result_obj["hits"].append(hit["_source"]['body'])
  result_obj["count"] = count_hits
  # returns 'bodies' for TraceGeomView()
  # {id, title, relation, when, whg_id}
  return result_obj
class FeatureContextView(View):
  @staticmethod
  def get(request):
    print('FeatureContextView GET:',request.GET)
    """
    args in request.GET:
        [string] idx: index to be queried
        [string] search: geometry to intersect
        [string] doc_type: 'place' in this case
    """
    idx = request.GET.get('idx')
    bbox = request.GET.get('search')
    doctype = request.GET.get('doc_type')
    print('context request',idx,doctype)
    q_context_all = {"query": { 
      "bool": {
        "must": [{"match_all":{}}],
        "filter": { "geo_shape": {
          "geoms.location": {
            "shape": {
              "type": "polygon",
              "coordinates": json.loads(bbox)
            },
            "relation": "within"
          }
        }}        
      }    
    }}
    features = contextSearch(idx, doctype, q_context_all)
    return JsonResponse(features, safe=False)


''' 
  Returns places in a trace body
'''
def getGeomCollection(idx,doctype,q):
  # q includes list of place_ids from a trace record
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  #try:
  res = es.search(index='whg', doc_type='place', body=q, size=300)
  #except:
    #print(sys.exc_info()[0])
  hits = res['hits']['hits']
  print(len(hits),'hits from getGeomCollection()')
  #geoms=[]
  collection={"type":"FeatureCollection","feature_count":len(hits),"features":[]}
  for h in hits:
    if len(h['_source']['geoms'])>0:
      #print('hit _source from getGeomCollection',h['_source'])
      collection['features'].append(
        {"type":"Feature",
         "geometry":h['_source']['geoms'][0]['location'],
         "properties":{
           "title":h['_source']['title']
           ,"place_id":h['_source']['place_id']
           ,"whg_id":h['_id']
         }
        }
      )
  #print(str(len(collection['features']))+' features')  
  return collection

class TraceGeomView(View):
  @staticmethod
  def get(request):
    #print('TraceGeomView GET:',request.GET)
    """
    args in request.GET:
        [string] idx: index to be queried
        [string] search: whg_id
        [string] doc_type: 'trace' in this case
    """
    idx = request.GET.get('idx')
    trace_id = request.GET.get('search')
    doctype = request.GET.get('doc_type')
    q_trace = {"query": {"bool": {"must": [{"match":{"_id": trace_id}}]}}}
    
    # using contextSearch() to get bodyids (i.e. place_ids)
    bodies = contextSearch(idx, doctype, q_trace)['hits'][0]
    print('a body from TraceGeomView->contextSearch',bodies[0])
    
    bodyids = [b['place_id'] for b in bodies if b['place_id']]
    print('len(bodyids)',len(bodyids))
    q_geom={"query": {"bool": {"must": [{"terms":{"place_id": bodyids}}]}}}
    geoms = getGeomCollection(idx,doctype,q_geom)
    print('len(geoms["features"])',len(geoms['features']))
    geoms['bodies'] = bodies
    
    return JsonResponse(geoms, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})
    #return JsonResponse(geoms, safe=False)      


def home(request):
  return render(request, 'search/home.html')

