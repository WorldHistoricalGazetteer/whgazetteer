# api.views
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User, Group
#from django.contrib.postgres import search
from django.core import serializers
from django.db.models import Q
from django.http import JsonResponse, HttpResponse#, FileResponse
from django.shortcuts import get_object_or_404
from django.views.generic import View
#from django.views.decorators.csrf import csrf_exempt
from django_filters.rest_framework import DjangoFilterBackend
from elasticsearch import Elasticsearch
from rest_framework import filters
from rest_framework import generics
from rest_framework import permissions
#from rest_framework import status
from rest_framework import viewsets
from rest_framework.authentication import SessionAuthentication
from rest_framework.decorators import api_view
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from rest_framework.reverse import reverse
from rest_framework.views import APIView
import simplejson as json
from accounts.permissions import IsOwnerOrReadOnly
from api.serializers import (UserSerializer, DatasetSerializer, PlaceSerializer, PlaceTableSerializer, PlaceGeomSerializer, AreaSerializer, FeatureSerializer, LPFSerializer)#, SearchDatabaseSerializer)
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from datasets.tasks import get_bounds_filter
from places.models import Place, PlaceGeom
from search.views import getGeomCollection

class StandardResultsSetPagination(PageNumberPagination):
  page_size = 10
  page_size_query_param = 'page_size'
  max_page_size = 20000


#
# External API
# 
#

"""
nearby and bbox spatial db queries
"""
class SpatialAPIView(generics.ListAPIView):
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  # search_fields = ['@title']

  def get(self, format=None, *args, **kwargs):
    params = self.request.query_params
    print('SpatialAPIView() params', params)

    qtype = params.get('type', None)
    bbox = params.get('bbox', None)
    nearby = params.get('nearby', None)
    lng = params.get('lng', None)
    lat = params.get('lat', None)
    sw = params.get('sw', None)
    ne = params.get('ne', None)
    fc = params.get('fc', None)
    fclasses = list(set([x.upper() for x in ','.join(fc)])) if fc else None
    ds = params.get('dataset', None)
    pagesize = params.get('pagesize', None)
    year = params.get('year', None)
    # ?
    err_note = None

    qs = Place.objects.filter(dataset__public=True)

    # right combo of params?
    if not qtype:
    # if all(v is None for v in [bbox, nearby]):
      return HttpResponse(content=b'<div style="margin:3rem; font-size: 1.2rem; border:1px solid gainsboro; padding:.5rem;">'+
        b'<p>Spatial query parameters must include either either <ul><li><b>?type=nearby</b> (with <b>&lng=</b> and <b>&lat=</b>) <i>or</i></li>'+
        b'<li><b>?type=bbox</b> (with <b>&sw=</b> and <b>&ne+</b>).</p></div')
    if qtype == 'nearby':
      if not all(v for v in [lng, lat]):
        return HttpResponse(content=b'<h3>A "nearby" spatial query requires "lng" and "lat" parameters</h3>')
      else:
        # do nearby query
        print("do nearby query (lng, lat) "+lng+", "+lat)
        return HttpResponse(content='nearby '+json.dumps(params))
        # return HttpResponse(content=b'nearby')
    elif qtype == 'bbox':
      if not all(v for v in [sw, ne]):
        return HttpResponse(content=b'<h3>A "bbox" spatial query requires "sw" (southwest) and "ne" (northeast) parameters</h3>')
      else:
        # do bbox query
        bbox = [[float(sw.split(',')[0]), float(sw.split(',')[1])],
                  [float(ne.split(',')[0]), float(ne.split(',')[1])]]
        msg="do bbox query (sw, ne) "+str(bbox)
        return HttpResponse(content='bbox '+msg)

    if ds:
      print("limiting to dataset:", ds)
    if fclasses:
      print("limiting to feature classes:",fclasses)
      # if id_:
      #   qs = qs.filter(id=id_)
      #   err_note = 'id given, other parameters ignored' if len(
      #       params.keys()) > 1 else None
      # else:
      #   qs = qs.filter(minmax__0__lte=year,
      #                  minmax__1__gte=year) if year else qs
      #   qs = qs.filter(fclasses__overlap=fclasses) if fc else qs

      #   #res=qs.filter(names__jsonb__toponym__icontains=name)

      #   if name_contains:
      #     qs = qs.filter(title__icontains=name_contains)
      #   elif name and name != '':
      #     #qs = qs.filter(title__istartswith=name)
      #     qs = qs.filter(names__jsonb__toponym__icontains=name)

        # qs = qs.filter(dataset=ds) if ds else qs
        # qs = qs.filter(ccodes__overlap=cc) if cc else qs

      filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]

      #serial = LPFSerializer if context else SearchDatabaseSerializer
      serial = LPFSerializer
      serializer = serial(filtered, many=True, context={
                          'request': self.request})

      serialized_data = serializer.data
      result = {"count": qs.count(),
                "pagesize": len(filtered),
                "parameters": params,
                "note": err_note,
                "type": "FeatureCollection",
                "features": serialized_data
                }
      #print('place result',result)
      return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  makeGeom(); called by collectionItem()
  format index locations as geojson
"""
def makeGeom(geom):
  #print('geom',geom)
  # TODO: account for non-point
  if len(geom) > 1:
    geomobj = {"type":"GeometryCollection", "geometries": []}  
    for g in geom:
      geomobj['geometries'].append(g['location'])
        #{"type":g['location']['type'],
         #"coordinates":g['location']['coordinates']
  elif len(geom) == 1:
    geomobj=geom[0]['location']
  else:
    geomobj=None
  return geomobj

"""
  collectionItem(); called by collector();
  formats api search hits 
"""
def collectionItem(i,datatype,format):
  #print('collectionItem i',i)
  _id = i['_id']
  if datatype == 'place':
    # serialize as geojson
    i=i['hit']
    item = {
      "type":"Feature",
      "properties": {
        "title":i['title'],
        "index_id":_id,
        "index_role":i['relation']['name'],
        "place_id":i['place_id'],
        "child_place_ids":[int(c) for c in i['children']],
        "dataset":i['dataset'],
        "placetypes":[t['label'] for t in i['types']],
        "variants":[n for n in i['suggest']['input'] if n != i['title']],
        'links':i['links'],
        "timespans":i['timespans'],
        "minmax":i['minmax'] if 'minmax' in i.keys() else [],
        "ccodes":i['ccodes']
      },
      "geometry": makeGeom(i['geoms'])
    }
    #print('place sug item', item)
  elif datatype == 'trace':
    hit = i['hit']
    pids = [b['place_id'] for b in hit['body'] if b['place_id']]
    q_geom={"query": {"bool": {"must": [{"terms":{"place_id": pids}}]}}}
    print('q_geom',q_geom)
    geoms = getGeomCollection('whg','place',q_geom)
    if format == 'geojson':
      item = {
        "type":"FeatureCollection",
        "trace_id": 'http://whgazetteer.org/traces/'+i['_id'],
        "target_id":hit['target']['id'],
        "target_title":hit['target']['title'],
        "features":geoms}
    else: # W3C anno format
      item = {
        "@context": ["http://www.w3.org/ns/anno.jsonld", 
                     {"lpo:": "http://linkedpasts.org/ontology/lpo.jsonld"}], 
        "_id":'http://whgazetteer.org/traces/'+i['_id'],
        "type": "Annotation",
        "created": hit['created'], 
        "creator": hit['creator'], 
        "motivation": hit['motivation'], 
        "keywords": hit['tags'],
        "target":{
          "id":hit['target']['id'],
          "type":hit['target']['type'],
          "title":hit['target']['title'],
          "depiction":hit['target']['depiction'] if 'depiction' in hit['target'].keys() else '',
          "format": "text/html", 
          "language": "en"
        },
        "bodies":hit['body']
      }
  #print('place search item:',item)
  return item
"""
  collector(); called by IndexAPIView; 
  execute es.search, return results post-processed by suggestionItem()
"""
def collector(q,datatype,idx):
  # returns only parents
  #print('collector',doctype,q)
  es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout':30, 'max_retries':10, 'retry_on_timeout':True}])
  items = []
  
  if datatype=='place':
    #print('collector/place q:',q)
    # TODO: trap errors
    res = es.search(index=idx, doc_type='place', body=q)
    hits = res['hits']['hits']
    #print('collector()/place hits',hits)
    if len(hits) > 0:
      for h in hits:
        items.append(
          {"_id": h['_id'],
           "linkcount":len(h['_source']['links']),
           "childcount":len(h['_source']['children']),
           "hit": h['_source'],
          }
        )
    sorteditems = sorted(items, key=lambda x: x['childcount'], reverse=True)
    #print('sorteditems from collector()',sorteditems)
    return sorteditems
    
  elif datatype == 'trace':
    print('collector()/trace q:',q)
    res = es.search(index='traces',doc_type='trace',body=q)
    hits = res['hits']['hits']
    #print('collector()/trace hits',hits)
    if len(hits) > 0:
      for h in hits:
        items.append({"_id":h['_id'],"hit":h['_source']})
    return items 


"""
  bundler();  called by IndexAPIView, case api/index?whgid=
  execute es.search, return post-processed results 
"""
def bundler(q,whgid,idx):
  es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout':30, 'max_retries':10, 'retry_on_timeout':True}])
  res = es.search(index=idx, doc_type='place', body=q)
  hits = res['hits']['hits']
  bundle = []
  if len(hits) > 0:
    for h in hits:
      bundle.append(
        {"_id": h['_id'],
         "linkcount":len(h['_source']['links']),
         "childcount":len(h['_source']['children']),
         "hit": h['_source'],
        }
      )
  #return bundle
  stuff = [ collectionItem(i, 'place', None ) for i in bundle]
  return stuff

""" 
  /api/traces?
  search trace target titles and tags
"""
class TracesAPIView(View):
  @staticmethod
  def get(request):
    params=request.GET
    print('TracesAPIView request params',params)
    qstr = params.get('q',None)
    format = params.get('format',None)
    
    """ 
      args q = query string
    """
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
    
    # run query
    collection = collector(q,'trace','traces')
    # format results    
    collection = [ collectionItem(item,'trace',format) for item in collection]
  
    # result object
    result = {"trace_count":len(collection),"results":collection}
    
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})
    
""" 
  /api/index?
  search place index (always whg) parent records
  based on search.views.SearchView(View)
"""
class IndexAPIView(View):
  @staticmethod
  def get(request):
    params=request.GET
    print('IndexAPIView request params',params)
    """
      args in params: whgid, pid, name, name_startswith, fclass, dataset, ccode, year, area

    """
    whgid = request.GET.get('whgid')
    pid = request.GET.get('pid')
    name = request.GET.get('name')
    name_startswith = request.GET.get('name_startswith')
    fc = params.get('fclass',None)
    fclasses=[x.upper() for x in fc.split(',')] if fc else None
    dataset = request.GET.get('dataset')
    cc = request.GET.get('ccode')
    ccodes=[x.upper() for x in cc.split(',')] if cc else None
    year = request.GET.get('year')
    area = request.GET.get('area')
    idx = 'whg'
    
    if all(v is None for v in [name, name_startswith, whgid,pid]):
      # TODO: format better
      return HttpResponse(content='<h3>Query requires either name, name_startswith, pid, or whgid</h3>'+'<p><a href="/usingapi/">API instructions</a>')
    else:
      if whgid and whgid !='':
        print('fetching whg_id', whgid)
        q = {"query":{"bool":{"should": [
            {"parent_id": {"type": "child","id":whgid}},
            {"match":{"_id":whgid}}
        ]}}}
        bundle = bundler(q, whgid, idx)
        #print('bundler', bundle)
        print('bundler q', q)
        #result=[b for b in bundle]
        result={"index_id":whgid,
                "note":str(len(bundle)) + " records asserted as skos:closeMatch",
                "type":"FeatureCollection",
                "features":[b for b in bundle]}
      else:
        q = { 
          "size": 100,
          "query": { "bool": {
            "must": [
              {"exists": {"field": "whg_id"}},
              {"multi_match": {
                "query": name if name else name_startswith, 
                "fields": ["title","names.toponym","searchy"],
                "type": "phrase" if name else "phrase_prefix"
            }}]
          }}
        }
        if fc:
          q['query']['bool']['must'].append({"terms": {"fclasses": fclasses}})
        if dataset:
          q['query']['bool']['must'].append({"match": {"dataset": dataset}})
        if ccodes:
          q['query']['bool']['must'].append({"terms": {"ccodes": ccodes}})
        if year:
          q['query']['bool']['must'].append({"term":{"timespans":{"value": year}}})
        #if area:
          #TODO: 
        if area:
          a = get_object_or_404(Area,pk=area)
          bounds={"id":[str(a.id)],"type":[a.type]} # nec. b/c some are polygons, some are multipolygons
          q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg')

        #print('the api query was:',q)
        
        # run query
        collection = collector(q,'place','whg')
        # format hits
        collection = [ collectionItem(s,'place', None) for s in collection]
      
        # result object
        result = {'type':'FeatureCollection','count': len(collection), 'features':collection}

    
    # to client
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

""" 
  /api/db?
  SearchAPIView()
  return lpf results from database search 
"""
class SearchAPIView(generics.ListAPIView):
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  search_fields = ['@title']

  #def get_queryset(self, format=None):
  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params
    print('SearchAPIView() params',params)

    id_ = params.get('id',None)
    name = params.get('name',None)
    name_contains = params.get('name_contains',None)
    cc = map(str.upper, params.get('ccode').split(',')) if params.get('ccode') else None
    ds = params.get('dataset',None)
    fc = params.get('fc',None)
    fclasses=list(set([x.upper() for x in ','.join(fc)])) if fc else None
    year = params.get('year',None)
    pagesize = params.get('pagesize',None)
    err_note = None
    context = params.get('context',None)
    # params
    print({"cc":cc,"fclasses":fclasses})
    
    qs = Place.objects.filter(dataset__public=True)

    if all(v is None for v in [name,name_contains,id_]):
      # TODO: return a template with API instructions
      return HttpResponse(content=b'<h3>Needs either a "name", a "name_contains", or "id" parameter at \
          minimum <br/>(e.g. ?name=myplacename or ?name_contains=astring or ?id=integer)</h3>')
    else:
      if id_:
        qs=qs.filter(id=id_)
        err_note = 'id given, other parameters ignored' if len(params.keys())>1 else None
      else:
        qs = qs.filter(minmax__0__lte=year,minmax__1__gte=year) if year else qs
        qs = qs.filter(fclasses__overlap=fclasses) if fc else qs
        
        #res=qs.filter(names__jsonb__toponym__icontains=name)
        
        if name_contains:
          qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
          #qs = qs.filter(title__istartswith=name)
          qs = qs.filter(names__jsonb__toponym__icontains=name)
  
        qs = qs.filter(dataset=ds) if ds else qs
        qs = qs.filter(ccodes__overlap=cc) if cc else qs
        
      filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]

      #serial = LPFSerializer if context else SearchDatabaseSerializer
      serial = LPFSerializer
      serializer = serial(filtered, many=True, context={'request': self.request})
      
      serialized_data = serializer.data
      result = {"count":qs.count(),
                "pagesize": len(filtered),
                "parameters": params,
                "note": err_note,
                "type": "FeatureCollection",
                "features":serialized_data
                }
      #print('place result',result)
      return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})



""" *** """
""" TODO: the next two attempt the same and are WAY TOO SLOW """
""" 
    api/places/<str:dslabel>/[?q={string}]
    Paged list of places in dataset. 
"""
class PlaceAPIView(generics.ListAPIView):
  serializer_class = PlaceSerializer
  pagination_class = StandardResultsSetPagination

  def get_queryset(self, format=None, *args, **kwargs):
    print('kwargs',self.kwargs)
    print('self.request.GET',self.request.GET)
    dslabel=self.kwargs['dslabel']
    qs = Place.objects.all().filter(dataset=dslabel).order_by('title')
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(title__icontains=query)
    return qs

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]

  
""" 
    api/dataset/<str:dslabel>/lpf/
    all places in a dataset, for download
"""
class DownloadDatasetAPIView(generics.ListAPIView):
  """  Dataset as LPF FeatureCollection  """
  #serializer_class = PlaceSerializer
  #pagination_class = StandardResultsSetPagination

  def get(self, format=None):
    print('self.request.GET',self.request.GET)
    dslabel=self.request.GET.get('dataset')
    ds=get_object_or_404(Dataset,label=dslabel)
    features = []
    qs = ds.places.all()
    for p in qs:
      rec = {"type":"Feature",
             "properties":{"id":p.id,"src_id":p.src_id,"title":p.title,"ccodes":p.ccodes},
             "geometry":{"type":"GeometryCollection",
                         "features":[g.jsonb for g in p.geoms.all()]},
             "names": [n.jsonb for n in p.names.all()],
             "types": [t.jsonb for t in p.types.all()],
             "links": [l.jsonb for l in p.links.all()],
             "whens": [w.jsonb for w in p.whens.all()],
      }
      #print('rec',rec)
      features.append(rec)
    
    result={"type":"FeatureCollection", "features": features}
    print('result',result)
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

  #permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
 
"""
  /api/datasets? > query public datasets by id, label, term
"""
class DatasetAPIView(LoginRequiredMixin, generics.ListAPIView):
  """    List public datasets    """
  serializer_class = DatasetSerializer
  renderer_classes = [JSONRenderer]

  def get_queryset(self, format=None, *args, **kwargs):
    params=self.request.query_params  
    print('api/datasets params',params)
    id_ = params.get('id', None)
    dslabel = params.get('label', None)
    query = params.get('q', None)
    
    qs = Dataset.objects.filter(public=True).order_by('label')
    
    if id_:
      qs = qs.filter(id = id_)
    elif dslabel:
      qs = qs.filter(label = dslabel)
    elif query:
      qs = qs.filter(Q(description__icontains=query) | Q(title__icontains=query))

    print('qs',qs)
    result = {
              "count":qs.count(),
              "parameters": params,
              #"features":serialized_data
              "features":qs
              }
    print('ds result',result,type(result))
    return qs

"""
  /api/area_features
"""
# geojson feature for api
class AreaFeaturesView(generics.ListAPIView):
  #@staticmethod
  
  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params  
    user = self.request.user
    print('params', params)
    print('api/areas request',self.request)
    
    id_ = params.get('id', None)
    query = params.get('q', None)
    filter = params.get('filter', None)
    
    areas = []
    # qs = Area.objects.all().filter((Q(type='predefined') | Q(owner=user))).values('id','title','type','description','geojson')
    qs = Area.objects.all().filter((Q(type='predefined'))).values('id','title','type','description','geojson')
    
    # filter for parameters
    if id_:
      qs=qs.filter(id=id_)
    if query:
      qs = qs.filter(title__icontains=query)
    if filter and filter == 'un':
      qs = qs.filter(description="UN Statistical Division Sub-Region")

      
    for a in qs:
      #area = {"id":a['id'],"title":a['title'],"type":a['type']}
      feat = {
        "type":"Feature",
        "properties":{"id":a['id'],"title":a['title'],"type":a['type'],"description":a['description']},
        "geometry":a['geojson']
      }
      areas.append(feat)
      
    return JsonResponse(areas, safe=False)  
  
class UserList(generics.ListAPIView):
  queryset = User.objects.all()
  serializer_class = UserSerializer

class UserDetail(generics.RetrieveAPIView):
  queryset = User.objects.all()
  serializer_class = UserSerializer

"""
  API 'home page' (not implemented)
"""
@api_view(['GET'])
def api_root(request, format=None):
  return Response({
      #'users': reverse('user-list', request=request, format=format),
        'datasets': reverse('dataset-list', request=request, format=format)
    })


class PrettyJsonRenderer(JSONRenderer):    
  def get_indent(self, accepted_media_type, renderer_context):
    return 2
  
#

# IN USE May 2020

#
"""
    place/<int:pk>/
    in dataset.html#browse
    "published record by place_id"
"""
class PlaceDetailAPIView(generics.RetrieveAPIView):
  """  single database place record by id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer
  renderer_classes = [PrettyJsonRenderer]

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]

"""
    place/<str:dslabel>/<str:src_id>/
    published record by dataset label and src_id
"""
class PlaceDetailSourceAPIView(generics.RetrieveAPIView):
  """  single database place record by src_id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer
  renderer_classes = [PrettyJsonRenderer]

  lookup_field = 'src_id'
  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]


""" 
    /api/geoms?ds={{ ds.label }}} 
    /api/geoms?coll={{ coll.id }}} 
    in ds_browse and ds_places for all geoms if < 15k
    TODO: this needs refactor (make collection.geometries @property?)
"""
class GeomViewSet(viewsets.ModelViewSet):
  queryset = PlaceGeom.objects.all()
  serializer_class = PlaceGeomSerializer
  #pagination_class = None
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

  def get_queryset(self):
    # PlaceGeom objects do not have dataset id or label :^(
    if 'ds' in self.request.GET:
      dslabel = self.request.GET.get('ds')
      dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
      qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
    elif 'coll' in self.request.GET:
      cid = self.request.GET.get('coll')
      coll = Collection.objects.get(id=cid)
      collPlaceIds = [p.id for p in coll.places.all()]
      # leaves out polygons and linestrings
      qs = PlaceGeom.objects.filter(
        place_id__in=collPlaceIds,
        jsonb__type__icontains='Point')
    return qs

""" 
    /api/geojson/{{ ds.id }}
"""
#class GeoJSONViewSet(viewsets.ModelViewSet):
class GeoJSONAPIView(generics.ListAPIView):
  # use: api/geojson
  #queryset = PlaceGeom.objects.all()
  #serializer_class = GeoJsonSerializer
  serializer_class = FeatureSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
  
  def get_queryset(self, format=None, *args, **kwargs):
    print('GeoJSONViewSet request.GET',self.request.GET)
    print('GeoJSONViewSet args, kwargs',args, kwargs)
    if 'id' in self.request.GET:
      dsid = self.request.GET.get('id')
      dslabel = get_object_or_404(Dataset, pk=dsid).label
      dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
      qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
    elif 'coll' in self.request.GET:
      cid = self.request.GET.get('coll')
      coll = Collection.objects.get(id=cid)
      collPlaceIds = [p.id for p in coll.places.all()]
      qs = PlaceGeom.objects.filter(place_id__in=collPlaceIds,jsonb__type='Point')    
    #print('qs',qs)
    return qs

"""
    populates drf table in ds_browse.html
"""
class PlaceTableViewSet(viewsets.ModelViewSet):
  #queryset = Place.objects.all().order_by('title')
  serializer_class = PlaceTableSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly)

  """
    q: query string
    ds: dataset
  """
  def get_queryset(self):
    #print('PlaceTableViewSet.get_queryset()',self.request.GET)
    ds = get_object_or_404(Dataset, label=self.request.GET.get('ds'))
    qs = ds.places.all().order_by('place_id')
    # qs = ds.places.all()
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(title__istartswith=query)
    return qs

  def get_permissions(self):
    """
    Instantiates and returns the list of permissions that this view requires.
    """
    if self.action in ['list','retrieve']:
      print(self.action)
      permission_classes = [permissions.AllowAny]
    else:
      permission_classes = [permissions.IsAdminUser]
    return [permission() for permission in permission_classes]

"""
    populates drf table in collection.collection_places.html
"""
class PlaceTableCollViewSet(viewsets.ModelViewSet):
  serializer_class = PlaceTableSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly)

  """
    q: query string
    coll: collection
  """
  def get_queryset(self):
    print('PlaceTableCollViewSet request[id]',self.request.GET['id'])
    coll = get_object_or_404(Collection, id=self.request.GET.get('id'))
    qs = coll.places.all()
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(title__istartswith=query)
    return qs

  def get_permissions(self):
    """
    Instantiates and returns the list of permissions that this view requires.
    """
    if self.action in ['list','retrieve']:
      print(self.action)
      permission_classes = [permissions.AllowAny]
    else:
      permission_classes = [permissions.IsAdminUser]
    return [permission() for permission in permission_classes]


"""
  areas/

"""
# simple objects for dropdown
class AreaListView(View):
  @staticmethod
  def get(request):
    print('area_list() request.user',request.user, type(request.user))
    print('area_list() request.user',str(request.user))
    userstr = str(request.user)
    if userstr == 'AnonymousUser':
      qs = Area.objects.all().filter(Q(type__in=('predefined','country'))).values('id','title','type')
    else:
      user = request.user
      qs = Area.objects.all().filter(Q(type__in=('predefined','country'))| Q(owner=user)).values('id','title','type')
    area_list = []
    for a in qs:
      area = {"id":a['id'],"title":a['title'],"type":a['type']}
      area_list.append(area)
      
    return JsonResponse(area_list, safe=False)
  
"""
  areas/

"""
# simple objects for dropdown
class AreaListAllView(View):
  @staticmethod
  def get(request):
    print('area_list() request',request)
    user = request.user
    area_list = []
    #qs = Area.objects.all().filter(Q(type='predefined')| Q(owner=request.user)).values('id','title','type')
    qs = Area.objects.all().filter(Q(type__in=('predefined','country'))| Q(owner=request.user)).values('id','title','type')
    for a in qs:
      area = {"id":a['id'],"title":a['title'],"type":a['type']}
      area_list.append(area)
      
    return JsonResponse(area_list, safe=False)

  
"""
    area/<int:pk>/
    in dataset.html#addtask
"""
class AreaViewSet(viewsets.ModelViewSet):
  queryset = Area.objects.all().order_by('title')
  serializer_class = AreaSerializer

"""
    regions/
    in dataset.html#addtask
"""
class RegionViewSet(View):
  queryset = Area.objects.filter(
      description='UN Statistical Division Sub-Region').order_by('title')
  serializer_class = AreaSerializer

