# various search.views
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import View
import simplejson as json, sys
from areas.models import Area
from datasets.tasks import normalize
from datasets.models import Dataset, Hit
from elasticsearch import Elasticsearch
from django.db.models import Count

class UpdateCountsView(View):
  """ Returns counts of unreviewed hist per pass """
  @staticmethod
  def get(request):
    print('UpdateCountsView GET:',request.GET)
    """
    args in request.GET:
        [integer] ds_id: dataset id
    """
    ds = get_object_or_404(Dataset, id=request.GET.get('ds_id'))
    updates = {}
    for tid in [t.task_id for t in ds.tasks.all()]:
      updates[tid] = {
        'pass1':len(Hit.objects.raw('select distinct on (place_id_id) place_id_id, id from hits where task_id = %s and query_pass=%s and reviewed=false',[tid,'pass1'])),
        'pass2':len(Hit.objects.raw('select distinct on (place_id_id) place_id_id, id from hits where task_id = %s and query_pass=%s and reviewed=false',[tid,'pass2'])),
        'pass3':len(Hit.objects.raw('select distinct on (place_id_id) place_id_id, id from hits where task_id = %s and query_pass=%s and reviewed=false',[tid,'pass3']))
      }    
    return JsonResponse(updates, safe=False)
    
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

# make stuff available in autocomplete dropdown
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
        "whg_id": h['whg_id'],
        "name": h['title'],
        "variants":[n for n in h['suggest']['input'] if n != h['title']],
        "ccodes": h['ccodes'],
        "types": [t['label'] for t in h['types'] ],
        "snippet": s['snippet']['descriptions.value'][0] if s['snippet'] != '' else []
        ,"geom": makeGeom(h['place_id'],h['geoms'])
      }
      #if 'snippet' in s:
        #print('snippet',s['snippet'])
        #item["snippet"] = s['snippet']['descriptions.value'][0]
  elif doctype == 'trace':
    item = {
      "_id":s['_id'],
      "id":s['hit']['target']['id'],
      "type":s['hit']['target']['type'],
      "title":s['hit']['target']['title'],
      "depiction":s['hit']['target']['depiction'] if 'depiction' in s['hit']['target'].keys() else '',
      "bodies":s['hit']['body']
    }
    #print('trace search item:',item)
  return item

def suggester(doctype,q,scope):
  # returns only parents; children retrieved in place portal
  #print('suggester',doctype,q)
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  suggestions = []
  
  if doctype=='place':
    print('suggester/place q:',q)
    res = es.search(index='whg',doc_type='place',body=q)
    if scope == 'suggest':
      sugs = res['suggest']['suggest'][0]['options']
      #print('suggester()/place sugs',sugs)
      if len(sugs) > 0:
        for s in sugs:
          hit_id = s['_id']
          if 'parent' not in s['_source']['relation'].keys():
            # it's a parent, add to suggestions[]
            suggestions.append(s['_source'])
    elif scope == 'search':
      hits = res['hits']['hits']
      #print('suggester()/place hits',hits)
      if len(hits) > 0:
        for h in hits:
          snippet = h['highlight'] if 'highlight' in h else ''
          suggestions.append({"_id":h['_id'],"hit":h['_source'],"snippet":snippet})
    
  elif doctype == 'trace':
    #print('suggester()/trace q:',q)
    res = es.search(index='traces',doc_type='trace',body=q)
    hits = res['hits']['hits']
    #print('suggester()/trace hits',hits)
    if len(hits) > 0:
      for h in hits:
        suggestions.append({"_id":h['_id'],"hit":h['_source']})
  return suggestions


""" Returns place:search/suggest or trace:search """
class SearchView(View):
  @staticmethod
  def get(request):
    print('in SearchView',request.GET)
    """
      args in request.GET:
          [string] qstr: query string
          [string] doc_type: place or trace
          [string] scope: suggest or search
    """
    qstr = request.GET.get('qstr')
    doctype = request.GET.get('doc_type')
    scope = request.GET.get('scope')
    if doctype == 'place':
      if scope == 'suggest':
        q = { "suggest":{"suggest":{"prefix":qstr,"completion":{"field":"suggest"}}} }  
      elif scope == 'search':
        q = { "size": 200,
              "query": {"bool": {
              "must": [
                {"exists": {"field": "whg_id"}},
                {"match": {"title": qstr}}
                #,{"match": {"descriptions.value": qstr}}
              ]
              ,"should":[
                {"match": {"descriptions.value": qstr}}
              ]
            }},
            "highlight": {"fields" : {"descriptions.value" : {}}}
          }
    elif doctype == 'trace': 
      q = { "query": {"match": {"target.title": {"query": qstr,"operator": "and"}}} }
      #print('trace query:',q)      
    suggestions = suggester(doctype, q, scope)
    #print('raw suggestions',suggestions)
    suggestions = [ suggestionItem(s, doctype, scope) for s in suggestions]
    print('SUGGESTIONS:',suggestions)
    return JsonResponse(suggestions, safe=False)
  
""" Returns place or trace suggestions """
class SuggestView(View):
  @staticmethod
  def get(request):
    print('in SuggestView',request.GET)
    """
      args in request.GET:
          [string] qstr: chars to be queried for the suggest field search
          [string] doc_type: context needed to filter suggestion searches
    """
    qstr = request.GET.get('qstr')
    doctype = request.GET.get('doc_type')
    if doctype == 'place':
      q = { "suggest":{"suggest":{"prefix":qstr,"completion":{"field":"suggest"}}} }  
    elif doctype == 'trace': 
      q = { "query": {"match": {"target.title": {"query": qstr,"operator": "and"}}} }
    suggestions = suggester(doctype, q, "suggest" )
    suggestions = [ suggestionItem(s,doctype) for s in suggestions]
    return JsonResponse(suggestions, safe=False)
  
## ***
##    get features in current map viewport
## ***
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
    #print('hit0 _source: ',hits[0]["_source"])
    for hit in hits:
      count_hits +=1
      if idx=="whg":
        #print('hit _source: ',hit["_source"])
        result_obj["hits"].append(normalize(hit["_source"],'whg'))
      else:
        result_obj["hits"].append(hit["_source"]['body'])
  result_obj["count"] = count_hits
  return result_obj

class FeatureContextView(View):
  """ Returns places in a bounding box """
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

def getGeomCollection(idx,doctype,q):
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  #try:
  res = es.search(index='whg', doc_type='place', body=q, size=300)
  #except:
    #print(sys.exc_info()[0])
  hits = res['hits']['hits']
  #geoms=[]
  collection={"type":"FeatureCollection","features":[]}
  for h in hits:
    if len(h['_source']['geoms'])>0:
      collection['features'].append(
        {"type":"Feature",
         "geometry":h['_source']['geoms'][0]['location'],
         "properties":{
           "title":h['_source']['title']
           ,"whg_id":h['_source']['whg_id']
         }
        }
      )
  #print(str(len(collection['features']))+' features')  
  return collection

class TraceGeomView(View):
  """ Returns places in a trace body """
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
    bodies = contextSearch(idx, doctype, q_trace)['hits'][0]
    #print('bodies from TraceGeomView->contextSearch',bodies)
    bodyids = [b['whg_id'] for b in bodies if b['whg_id']]
    q_geom={"query": {"bool": {"must": [{"terms":{"_id": bodyids}}]}}}
    geoms = getGeomCollection(idx,doctype,q_geom)
    geoms['bodies'] = bodies
    return JsonResponse(geoms, safe=False)      


def home(request):
  return render(request, 'search/home.html')

def advanced(request):
  print('in search/advanced() view')
  return render(request, 'search/advanced.html')

