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
from rest_framework import status
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
from api.serializers import (UserSerializer, DatasetSerializer, PlaceSerializer, 
                             PlaceGeomSerializer, AreaSerializer, FeatureSerializer, LPFSerializer)
from areas.models import Area
from datasets.models import Dataset
from datasets.tasks import get_bounds_filter
from places.models import Place, PlaceGeom
from search.views import getGeomCollection

class StandardResultsSetPagination(PageNumberPagination):
  page_size = 10
  page_size_query_param = 'page_size'
  max_page_size = 15000


#
# External API
# 
#

"""
  makeGeom(); called by collectionItem()
  format index locations as geojson
"""
def makeGeom(geom):
  print('geom',geom)
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
  print('collectionItem i',i)
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
  stuff = [ collectionItem(i, 'place') for i in bundle]
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
    
    if all(v is None for v in [name,name_startswith,whgid,pid]):
      # TODO: format better
      return HttpResponse(content='<h3>Query requires either name, name_startswith, pid, or whgid</h3>'+
                          '<p><a href="/usingapi/">API instructions</a>')
    else:
      if whgid and whgid !='':
        print('fetching whg_id',whgid)
        q = {"query":{"bool":{"should": [
            {"parent_id": {"type": "child","id":whgid}},
            {"match":{"_id":whgid}}
        ]}}}
        bundle = bundler(q,whgid,idx)
        print('bundle',bundle)
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

        print('the api query was:',q)
        
        # run query
        collection = collector(q,'place','whg')
        # format hits
        collection = [ collectionItem(s,'place',None) for s in collection]
      
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
    id_ = params.get('id',None)
    name = params.get('name',None)
    name_contains = params.get('name_contains',None)
    cc = map(str.upper,params.get('ccode').split(',')) if params.get('ccode') else None
    ds = params.get('dataset',None)
    fc = params.get('fclass',None)
    fclasses=[x.upper() for x in fc.split(',')] if fc else None
    year = params.get('year',None)
    pagesize = params.get('pagesize',None)
    err_note = None
    print('SearchAPIView() params',params)
    qs = Place.objects.filter(dataset__public=True)
    #qs = Place.objects.all()

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
  
        if name_contains:
          qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
          qs = qs.filter(title__istartswith=name)
  
        qs = qs.filter(dataset=ds) if ds else qs
        qs = qs.filter(ccodes__overlap=cc) if cc else qs
        
      filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]

      serializer = LPFSerializer(filtered, many=True, context={'request': self.request})
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
  /api/areas?
"""
# geojson feature for api
class AreaFeaturesView(generics.ListAPIView):
  #@staticmethod
  
  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params  
    user = self.request.user
    print('api/areas request',self.request)
    
    id_ = params.get('id', None)
    query = params.get('q', None)
    
    areas = []
    qs = Area.objects.all().filter((Q(type='predefined') | Q(owner=user))).values('id','title','type','description','geojson')
    
    # filter for parameters
    if id_:
      qs=qs.filter(id=id_)
    if query:
      qs = qs.filter(title__icontains=query)
      
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
    /api/dataset/<int:ds>/geom/ 
"""
class DownloadGeomsAPIView(generics.ListAPIView):
  # use: dataset/<int:ds>/geom/
  queryset = PlaceGeom.objects.all()
  serializer_class = FeatureSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

  def get_queryset(self, format=None, *args, **kwargs):
    ds = get_object_or_404(Dataset,pk=self.kwargs['ds'])
    qs = PlaceGeom.objects.all().filter(place_id__in=ds.placeids)
    #print('qs',qs)
    return qs

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
    /api/geoms {ds:{{ ds.label }}} 
    in dataset.html#browse for all geoms if < 15k
"""
class GeomViewSet(viewsets.ModelViewSet):
  queryset = PlaceGeom.objects.all()
  serializer_class = PlaceGeomSerializer
  #pagination_class = None
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

  def get_queryset(self):
    dslabel = self.request.GET.get('ds')
    dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
    qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
    return qs

"""
    populates drf table in dataset.detail#browse
"""
class PlaceTableViewSet(viewsets.ModelViewSet):
  queryset = Place.objects.all().order_by('title')
  serializer_class = PlaceSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly)

  """
    q: query string
    ds: dataset
  """
  def get_queryset(self):
    #print('PlaceViewSet.get_queryset()',self.request.GET)
    qs = Place.objects.all()
    query = self.request.GET.get('q')
    ds = self.request.GET.get('ds')
    if ds is not None:
      qs = qs.filter(dataset = ds)
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

