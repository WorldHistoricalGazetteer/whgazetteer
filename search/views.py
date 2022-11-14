# various search.views
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Q, Count
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import View
from django.views.generic.base import TemplateView
import simplejson as json, sys
from api.serializers import SearchDatabaseSerializer
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset, Hit
from datasets.tasks import normalize, get_bounds_filter
from elasticsearch7 import Elasticsearch
from places.models import Place, PlaceGeom

    
class SearchPageView(TemplateView):
  template_name = 'search/search.html'
  
  def get_context_data(self, *args, **kwargs):
    # return list of datasets
    dslist = Dataset.objects.filter(public=True)

    #bboxes = [
      #{"type":"Feature",
       #"properties": {"id":ds.id, "label": ds.label, "title": ds.title},
       #"geometry":ds.bounds} for ds in dslist if ds.label not in ['tgn_filtered_01']]

    context = super(SearchPageView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['media_url'] = settings.MEDIA_URL
    context['dslist'] = dslist
    context['search_params'] = self.request.session.get('search_params')
    #context['bboxes'] = bboxes
    return context
  
class LookupView(View):
  @staticmethod
  def get(request):
    print('in LookupView, GET =',request.GET)
    """
      args in request.GET:
        [string] idx: latest name for whg index
        [string] place_id: from a trace body
    """
    es = Elasticsearch([{'host': 'localhost',
                         'port': 9200,
                         'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                         'timeout': 30,
                         'max_retries': 10,
                         'retry_on_timeout': True}])
    idx = request.GET.get('idx')
    pid = request.GET.get('place_id')
    q={"query": {"bool": {"must": [{"match":{"place_id": pid }}]}}}
    res = es.search(index=idx, body=q)
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
        {"type":g['location']['type'],
         "coordinates":g['location']['coordinates'],
         "properties":{"pid": pid}}
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
        "type": s['types'][0]['sourceLabel'] if 'sourceLabel' in s['types'][0] else s['types'][0]['label'],
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
      item = {
        "whg_id": h['whg_id'] if 'whg_id' in h else '',
        "pid":h['place_id'],
        "linkcount":s['linkcount'],
        "name": h['title'],
        "variants":[n for n in h['suggest']['input'] if n != h['title']],
        "ccodes": h['ccodes'],
        "fclasses": h['fclasses'],
        # "types": [t['sourceLabel'] or t['label'] for t in h['types'] ],
        # "types": [t['src_label'] or t['label'] for t in h['types'] ],
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
  print('key', settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY)
  # returns only parents; children retrieved into place portal
  print('suggester',doctype,q)
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       'timeout':30,
                       'max_retries':10,
                       'retry_on_timeout':True}])
  print('suggester es connector',es)

  suggestions = []
  
  if doctype=='place':
    #print('suggester/place q:',q)
    # res = es.search(index=idx, body=q)
    # doc_type not present from 7.17 on
    # body to be deprecated 'in 9.0'
    res = es.search(index=idx, body=q)
    #res = es.search(index='whg,tgn', body=q)
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
      if len(hits) > 0:
        for h in hits:
          suggestions.append(
            {"_id": h['_id'],
             "linkcount":len(set(h['_source']['children'])),
             "hit": h['_source'],
            }
          )
      sortedsugs = sorted(suggestions, key=lambda x: x['linkcount'], reverse=True)
      # TODO: there may be parents and children
      # print('SUGGESTIONS from suggester()',type(suggestions), sortedsugs)
      return sortedsugs
    
  elif doctype == 'trace':
    print('suggester()/trace q:',q)
    res = es.search(index='traces', body=q)
    hits = res['hits']['hits']
    #print('suggester()/trace hits',hits)
    if len(hits) > 0:
      for h in hits:
        suggestions.append({"_id":h['_id'],"hit":h['_source']})
    #print('suggestions',suggestions)
    return suggestions 

""" 
  /search/index/?
  from search.html
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
        [string] bounds: text of JSON geometry
    """
    qstr = request.GET.get('qstr')
    doctype = request.GET.get('doc_type')
    scope = request.GET.get('scope')
    idx = request.GET.get('idx')
    fclasses = request.GET.get('fclasses')
    start = request.GET.get('start')
    end = request.GET.get('end')
    bounds = request.GET.get('bounds')
    #ds = request.GET.get('ds')
    
    params = {
      "qstr":qstr,
      "doctype": doctype,
      "scope": scope,
      "idx": idx,
      "fclasses": fclasses,
      "start": start,
      "end": end,
      "bounds": bounds,
    }
    request.session["search_params"] = params 
    print('search_params set', params)

    # TODO: fuzzy search; results ranked for closeness
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
                    "fields": ["title^3", "names.toponym", "searchy"],
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
          q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg')

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
      print('trace query:',q)
    
    suggestions = suggester(doctype, q, scope, idx)
    suggestions = [suggestionItem(s, doctype, scope) for s in suggestions]
    
    # return query params for ??
    result = suggestions if doctype=='trace' else \
      {'get': request.GET, 'suggestions': suggestions, 'session': params }

    return JsonResponse(result, safe=False)
      
  
"""
  executes search on db.places /search/db
"""
class SearchDatabaseView(View):
  @staticmethod
  def get(request):
    pagesize = 200
    print('SearchDatabaseView() request',request.GET)
    """
      args in request.GET:
        [string] name: query string
        [string] fclasses: geonames class (A,H,L,P,S,T)
        [int] year: within place.minmax timespan
        [string] bounds: text of JSON geometry
        [int] dsid: dataset.id
        
    """
    name = request.GET.get('name')
    name_contains = request.GET.get('name_contains') or None
    fclasses = request.GET.get('fclasses').split(',')
    year = request.GET.get('year')
    bounds = request.GET.get('bounds')
    dsid = request.GET.get('dsid')
    ds = Dataset.objects.get(pk=int(dsid)) if dsid else None
    print('dsid, ds',dsid, ds)

    from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
    if bounds:
      bounds=json.loads(bounds)
      area = Area.objects.get(id = bounds['id'][0])
      print('bounds area', area)
      ga = GEOSGeometry(json.dumps(area.geojson))
    
    print('seaech db params:', {'name':name,'name_contains':name_contains,'fclasses':fclasses,'bounds':bounds,'ds':ds})
    # africanports1887 mystery
    # {'name': 'Ambriz', 'name_contains': None, 'fclasses': ['A', 'P', 'S', 'R', 'L', 'T', 'H'], 'bounds': '', 'ds': None}
    # returns 0
    # {'name': 'Abydos', 'name_contains': None, 'fclasses': ['A', 'P', 'S', 'R', 'L', 'T', 'H'], 'bounds': '', 'ds': None}
    # returns 4
    # Abitibiwinni Aki
    qs = Place.objects.filter(dataset__public=True)
    
    if bounds:
      print('bounds geometry', ga[:200])
      qs = qs.filter(geoms__geom__within=ga)      
    else:
      print('no bounds, or empty string')

    if fclasses and len(fclasses) < 7:
      qs.filter(fclasses__overlap=fclasses)
    #   qs.filter(Q(fclasses__isnull=True) | Q(fclasses__overlap=fclasses) | Q(fclasses=[])).count()
    # else:
    #   # filter in effect
    #   qs.filter(fclasses__overlap=fclasses)
    
    if name_contains:
      print( 'name_contains exists',name_contains)
      qs = qs.filter(title__icontains=name_contains)
    elif name and name != '':
      #qs = qs.filter(title__istartswith=name)
      qs = qs.filter(names__jsonb__toponym__istartswith=name).distinct()

    qs = qs.filter(dataset=ds.label) if ds else qs
    #qs = qs.filter(ccodes__overlap=cc) if cc else qs
    count = len(qs)

    #filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]
    filtered = qs[:pagesize]

    # needed for suglister
    #pid    #name    #variants[]    #ccodes[]    #types[]    #geom[]
    # adding dataset
    
    # normalizes place.geoms objects for results display
    def dbsug_geoms(pobjs):
      suglist = []
      for p in pobjs:
        g = p.jsonb
        if 'citation' in g: del g['citation']
        g['src'] = 'db'
        g["properties"] = {"pid":p.place_id, "title": p.title}
        suglist.append(g)
      return suglist
      
    # mimics suggestion items from SearchView (index)
    suggestions = []
    print('qs length', count)
    print('filtered qs length', len(filtered))
    for place in filtered:
      ds=place.dataset
      try:        
        suggestions.append({
          "pid": place.id,
          "ds": {"id":ds.id, "label": ds.label, "title": ds.title},
          "name": place.title,
          "variants": [n.jsonb['toponym'] for n in place.names.all()],
          "ccodes": place.ccodes,
          "fclasses": place.fclasses,
          "types": [t.jsonb['sourceLabel'] or t.jsonb['src_label'] for t in place.types.all()],
          "geom": dbsug_geoms(place.geoms.all())
        })
      except:
        print("db sugbuilder error:", place.id, sys.exc_info())
      
    result = {'get': request.GET, 'count': count, 'suggestions': suggestions}
    return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii':False})
  
'''
  returns 8000 index docs in current map viewport
  OR if task == 'count': count of features in area
'''
def contextSearch(idx, doctype, q, task):
  print('context query', q)
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       'timeout':30,
                       'max_retries':10,
                       'retry_on_timeout':True}])
  count_hits=0
  result_obj = {"hits":[]}
  # TODO: convert calling map(s) to MapLibre.js to handle large datasets
  if task == 'count':
    res = es.count(index=idx, body=q)
    return {'count': res['count']}
  elif task == 'features':
    res = es.search(index=idx, body=q, size=8000)
  hits = res['hits']['hits']
  # TODO: refactor this bit
  print('hits', len(hits))
  if len(hits) > 0:
    #print('contextSearch() hit0 _source: ',hits[0]["_source"])
    for hit in hits:
      count_hits +=1
      if idx.startswith("whg"):
        # why normalize here?
        result_obj["hits"].append(hit['_source'])
      else:
        # this is traces
        result_obj["hits"].append(hit["_source"]['body'])
  result_obj["count"] = count_hits
  return result_obj

class FeatureContextView(View):
  @staticmethod
  def get(request):
    print('FeatureContextView GET:',request.GET)
    """
    args in request.GET:
        [string] idx: index to be queried
        [string] extent: geometry to intersect
        [string] doc_type: 'place' in this case
        [string] task: 'features' or 'count'
    """
    idx = request.GET.get('idx')
    extent = request.GET.get('extent') # coordinates string
    doctype = request.GET.get('doc_type')
    task = request.GET.get('task')
    q_context_all = {"query": {
      "bool": {
        "must": [{"match_all":{}}],
        "filter": { "geo_shape": {
          "geoms.location": {
            "shape": {
              "type": "polygon",
              "coordinates": json.loads(extent)
            },
            "relation": "within"
          }
        }}        
      }    
    }}
    response = contextSearch(idx, doctype, q_context_all, task)
    return JsonResponse(response, safe=False)

''' 
  Returns places in a trace body
'''
def getGeomCollection(idx,doctype,q):
  # q includes list of place_ids from a trace record
  es = Elasticsearch([{'host': 'localhost',
                       'port': 9200,
                       'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                       'timeout':30,
                       'max_retries':10,
                       'retry_on_timeout':True}])
  #try:
  res = es.search(index='whg', body=q, size=300)
  # res = es.search(index='whg', body=q, size=300)
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

class CollectionGeomView(View):
  @staticmethod
  def get(request):
    #print('CollectionGeomView GET:',request.GET)
    """
    args in request.GET:
        [string] coll_id: collection to be queried
    """
    coll_id = request.GET.get('coll_id')
    coll = Collection.objects.get(id=coll_id)
    pids = [p.id for p in coll.places_all]
    placegeoms = PlaceGeom.objects.filter(place_id__in=pids)
    features = [{"type":"Feature",
                 "geometry":pg.jsonb,
                 "properties":{"pid":pg.place_id, "title":pg.title}
                 } for pg in placegeoms]
    fcoll = {"type":"FeatureCollection", "features": features}
    print('len(fc["features"])',len(fcoll['features']))
    
    return JsonResponse(fcoll, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

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
    bodies = contextSearch(idx, doctype, q_trace, 'features')['hits'][0]
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

