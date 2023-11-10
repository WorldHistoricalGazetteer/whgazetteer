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
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
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
    es = settings.ES_CONN
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
  format search result items
"""
def suggestionItem(s):
  h = s['hit']
  item = {
    "whg_id": h['whg_id'] if 'whg_id' in h else '',
    "pid":h['place_id'],
    "linkcount":s['linkcount'],
    "name": h['title'],
    "variants":[n for n in h['suggest']['input'] if n != h['title']],
    "ccodes": h['ccodes'],
    "fclasses": h['fclasses'],
    "types": [t['label'] for t in h['types'] ],
    "geom": makeGeom(h['place_id'],h['geoms'])
  }
  return item

# chatgpt replacement for multiple indexes
"""
  performs es search against 'whg' and 'pub'
"""
def suggester(q, indices):
  es = settings.ES_CONN
  print('indices in suggester()', indices)
  suggestions = []

  # Search across multiple indices
  res = es.search(index=','.join(indices), body=q)  # indices is a list of index names
  hits = res['hits']['hits']
  if len(hits) > 0:
    for h in hits:
      # Initialize linkcount based on whether 'children' field is present
      linkcount = len(set(h['_source']['children'])) if 'children' in h['_source'] else 0
      suggestion = {
        "_id": h['_id'],
        "_index": h['_index'],  # Include the index name
        "linkcount": linkcount,
        "hit": h['_source'],
      }
      suggestions.append(suggestion)

  # Sort suggestions by linkcount, if applicable
  sortedsugs = sorted(suggestions, key=lambda x: x['linkcount'], reverse=True)
  return sortedsugs


"""
  performs es search in index aliased 'whg'
"""
# def suggester(q, idx):
#   # print('key', settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY)
#   # returns only parents; children retrieved into place portal
#   print('suggester q',q)
#   es = settings.ES_CONN
#   # print('suggester es connector',es)
#
#   suggestions = []
#
#   res = es.search(index=idx, body=q)
#   hits = res['hits']['hits']
#   if len(hits) > 0:
#     for h in hits:
#       suggestions.append(
#         {"_id": h['_id'],
#          "linkcount":len(set(h['_source']['children'])),
#          "hit": h['_source'],
#         }
#       )
#
#   sortedsugs = sorted(suggestions, key=lambda x: x['linkcount'], reverse=True)
#   # TODO: there may be parents and children
#   return sortedsugs
#
"""
  /search/index/?
  from search.html
"""
class SearchView(View):
  @staticmethod
  def get(request):
    print('SearchView() request',request.GET)
    print('SearchView() bounds',request.GET.get('fclasses'))
    """
      args in request.GET:
        [string] qstr: query string
        # [string] doc_type: place or trace
        # [string] scope: suggest or search
        [string] idx: index to be queried
        [int] year: filter for timespans including this
        [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
        [string] bounds: text of JSON geometry
    """
    qstr = request.GET.get('qstr')
    idx = request.GET.get('idx')
    fclasses = request.GET.get('fclasses')
    start = request.GET.get('start')
    end = request.GET.get('end')
    bounds = request.GET.get('bounds')

    params = {
      "qstr":qstr,
      "idx": idx,
      "fclasses": fclasses,
      "start": start,
      "end": end,
      "bounds": bounds,
    }
    request.session["search_params"] = params
    print('search_params set', params)

    # TODO: fuzzy search; results ranked for closeness
    # always include fclass-less records in results (i.e. ['X']
    fclist = ['X']
    if fclasses:
      fclist.extend(fclasses.split(','))

    q = {
      "size": 100,
      "query": {
        "bool": {
          "must": [
            {"exists": {"field": "whg_id"}},
            {"multi_match": {
                "query": qstr,
                "fields": ["title^3", "names.toponym", "searchy"]
            }},
            {"terms": {"fclasses": fclist}}
          ]
        }
      }
    }

    if start:
      q['query']['bool']['must'].append({"range":{"timespans":{"gte" :start,"lte":end if end else 2005}}})
    if bounds:
      bounds=json.loads(bounds)
      q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg')

    print('query q in search', q)

    # suggestions = suggester(q, idx)
    suggestions = suggester(q, [idx, 'pub'])
    suggestions = [suggestionItem(s) for s in suggestions]
    # print('suggestions', suggestions)
    # return query params for ??
    result = {'get': request.GET, 'suggestions': suggestions, 'session': params }

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
    qs = Place.objects.filter(dataset__public=True)

    if bounds:
      print('bounds geometry', ga[:200])
      qs = qs.filter(geoms__geom__within=ga)
    else:
      print('no bounds, or empty string')

    if fclasses and len(fclasses) < 7:
      qs.filter(fclasses__overlap=fclasses)

    if name_contains:
      print( 'name_contains exists',name_contains)
      qs = qs.filter(title__icontains=name_contains)
    elif name and name != '':
      #qs = qs.filter(title__istartswith=name)
      qs = qs.filter(names__jsonb__toponym__istartswith=name).distinct()

    qs = qs.filter(dataset=ds.label) if ds else qs
    count = len(qs)

    filtered = qs[:pagesize]

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
  es = settings.ES_CONN
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
  es = settings.ES_CONN
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
