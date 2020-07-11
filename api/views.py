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
from places.models import Place, PlaceGeom

class StandardResultsSetPagination(PageNumberPagination):
  page_size = 10
  page_size_query_param = 'page_size'
  max_page_size = 15000


#
# External API
# 
#

"""
  suggestionItem(); called by suggester();
  formats search hits 
"""
def suggestionItem(s,datatype):
  #print('suggestionItem s',s)
  _id = s['_id']
  s=s['hit']
  if datatype == 'place':
    item = { 
      #"index_id":s['whg_id'],
      "index_id":_id,
      "title":s['title'],
      "place_ids":[int(c) for c in s['children']]+[s['place_id']],
      "types":[t['label'] for t in s['types']],
      "variants":[n for n in s['suggest']['input'] if n != s['title']],
      "timespans":s['timespans'],
      "minmax":s['minmax'] if 'minmax' in s.keys() else [],
      "ccodes":s['ccodes'],
      #"geom": makeGeom(s['place_id'],s['geoms'])
    }
    #print('place sug item', item)
  elif datatype == 'trace':
    # now with place_id, not whg_id (aka _id; they're transient)
    item = {
      "_id":s['_id'],
      "id":s['hit']['target']['id'],
      "type":s['hit']['target']['type'],
      "title":s['hit']['target']['title'],
      "depiction":s['hit']['target']['depiction'] if 'depiction' in s['hit']['target'].keys() else '',
      "bodies":s['hit']['body']
    }
  #print('place search item:',item)
  return item

"""
  suggester(); called by IndexAPIView; 
  execute es.search, return results post-processed by suggestionItem()
"""
def suggester(datatype,q,idx):
  # returns only parents; children retrieved into place portal
  #print('suggester',doctype,q)
  es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout':30, 'max_retries':10, 'retry_on_timeout':True}])
  suggestions = []
  
  if datatype=='place':
    #print('suggester/place q:',q)
    # TODO: trap errors
    res = es.search(index=idx, doc_type='place', body=q)
    hits = res['hits']['hits']
    #print('suggester()/place hits',hits)
    if len(hits) > 0:
      for h in hits:
        suggestions.append(
          {"_id": h['_id'],
           "linkcount":len(h['_source']['links']),
           "childcount":len(h['_source']['children']),
           "hit": h['_source'],
          }
        )
    sortedsugs = sorted(suggestions, key=lambda x: x['childcount'], reverse=True)
    print('sortedsugs from suggester()',sortedsugs)
    return sortedsugs
    
  elif datatype == 'trace':
    print('suggester()/trace q:',q)
    res = es.search(index='traces',doc_type='trace',body=q)
    hits = res['hits']['hits']
    #print('suggester()/trace hits',hits)
    if len(hits) > 0:
      for h in hits:
        suggestions.append({"_id":h['_id'],"hit":h['_source']})
    return suggestions 


"""
  bundler();  called by IndexAPIView
  execute es.search, return post-processed results 
"""
def bundler(q,whgid,idx):
  es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout':30, 'max_retries':10, 'retry_on_timeout':True}])
  res = es.search(index=idx, doc_type='place', body=q)
  hits = res['hits']['hits']
  bundle = []
  #bundle = {"index_id":whgid, 
            #"count":res['hits']['total'],
            ##"result": [h['_source'] for h in hits]
            #"result": res['hits']
            #}
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
  stuff = [ suggestionItem(i, 'place') for i in bundle]
  return stuff
"""
    search index parent records, given name string
    based on search.views.SearchView(View)

"""
class IndexAPIView(View):
  @staticmethod
  def get(request):
    idx='whg'
    datatype='place'
    params=request.GET
    print('IndexAPIView request params',params)
    """
      args in params:
          [str] name: exact
          [str] name_startswith
          [int] whgid: whg_id
          [int] pid: place_id
          [str] class: geonames fclass
    """
    whgid = request.GET.get('whgid')
    pid = request.GET.get('pid')
    name = request.GET.get('name')
    name_startswith = request.GET.get('name_startswith')
    fc = params.get('fclass',None)
    fclasses=[x.upper() for x in fc.split(',')] if fc else None
    
    if all(v is None for v in [name,name_startswith,whgid,pid]):
      # TODO: format better
      return HttpResponse(content='<h3>Query requires either name, name_startswith, pid, or whgid</h3>'+
                          '<p><a href="http://localhost:8000/usingapi/">API instructions</a>')
    else:
      if whgid and whgid !='':
        print('fetching whg_id',whgid)
        q = {"query":{"bool":{"should": [
            {"parent_id": {"type": "child","id":whgid}},
            {"match":{"_id":whgid}}
        ]}}}
        bundle = bundler(q,whgid,idx)
        print('bundle',bundle)
        result=[b for b in bundle]
        #result={"index_id":whgid,"count":bundle['count'],"result":bundle['result']}
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
          print('query run:',q)
        
        # run query
        suggestions = suggester(datatype, q, idx)
        # format hits
        suggestions = [ suggestionItem(s, datatype) for s in suggestions]
      
        # result object
        result = {'count': len(suggestions), 'result':suggestions}
    
    # to client
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

""" 
  SearchAPIView()
  /api/db?
  return lpf results from database search 
  
"""
class SearchAPIView(generics.ListAPIView):
  #serializer_class = LPFSerializer
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
    maxrows = params.get('maxrows',None)
    err_note = None
    print('SearchAPIView() params',params)
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
  
        if name_contains:
          qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
          qs = qs.filter(title__istartswith=name)
  
        qs = qs.filter(dataset=ds) if ds else qs
        qs = qs.filter(ccodes__overlap=cc) if cc else qs
        
      filtered = qs[:maxrows] if maxrows and maxrows < 200 else qs[:20]
      serializer = LPFSerializer(filtered, many=True, context={'request': self.request})
      serialized_data = serializer.data
      result = {"count":qs.count(),
                "parameters": params,
                "note": err_note,
                "features":serialized_data
                }
      #print('place result',result)
      return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})


""" 
    Paged list of places in dataset. 
    api/places/<str:dslabel>/[?q={string}]
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
  /api/datasets?
  identical to places
    
"""
class DatasetSearchView(generics.ListAPIView):
  """    List public datasets    """
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  
  def get_queryset(self, format=None, *args, **kwargs):
    params=self.request.query_params  
    print('api/datasets request',self.request)
    
    id_ = params.get('id', None)
    dslabel = params.get('label', None)
    query = params.get('q', None)
    
    qs = Dataset.objects.filter(public=True).order_by('label')
    
    if id_:
      qs = qs.filter(id = id_)
    elif dslabel:
      qs = qs.filter(label = dslabel)
    elif query is not None:
      qs = qs.filter(Q(description__icontains=query) | Q(title__icontains=query))

    print('qs',qs)
    #serializer = DatasetSerializer(qs, many=True, context={'request': self.request})
    serializer = DatasetSerializer(qs, context={'request': self.request})
    serialized_data = serializer.data
    #print('serialized_data',serialized_data)
    result = {
              "count":qs.count(),
              "parameters": params,
              "features":serialized_data
              #"features":qs
              }
    print('ds result type, value',type(result),result)
    #return qs
    #return result
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})
    
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
    api/dataset/<str:dslabel>/lpf/
"""
class DownloadDatasetAPIView(generics.ListAPIView):
  """  Dataset as LPF FeatureCollection  """
  serializer_class = PlaceSerializer
  #pagination_class = StandardResultsSetPagination

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
  API 'home page' (not implemented)
"""
@api_view(['GET'])
def api_root(request, format=None):
  return Response({
      #'users': reverse('user-list', request=request, format=format),
        'datasets': reverse('dataset-list', request=request, format=format)
    })


""" cf. https://www.django-rest-framework.org/api-guide/filtering/#djangofilterbackend """
class FilteredSearchAPIView(generics.ListAPIView):
  queryset = Place.objects.all()
  serializer_class = LPFSerializer
  renderer_classes = [JSONRenderer]
  filter_backends = [DjangoFilterBackend]    
  #filter_backends = [filters.SearchFilter]    
  filterset_fields = ['title']

class PrettyJsonRenderer(JSONRenderer):    
  def get_indent(self, accepted_media_type, renderer_context):
    return 2
#

# Internal IN USE May 2020

#
"""
    place/<int:pk>/
    in dataset.html#browse
"""
class PlaceDetailAPIView(generics.RetrieveAPIView):
  """  single database place record by id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer
  renderer_classes = [PrettyJsonRenderer]

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]


""" 
    /api/geoms {ds:{{ ds.label }}} 
    in dataset.html#browse
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
    qs = Area.objects.all().filter(Q(type='predefined') | Q(owner=request.user)).values('id','title','type')
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
    dataset/<int:pk>/
    in usingapi.html example
"""
class DatasetDetailAPIView(LoginRequiredMixin, generics.RetrieveAPIView):
  """    dataset record by id   """
  queryset = Dataset.objects.all().filter(public=True)
  serializer_class = DatasetSerializer

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]


#""" 
  #Paged list of places w/title matching a query. 
  #api/[?q={str}, ?ds={str}]
#"""
#class BoogeredSearchAPIView(generics.ListAPIView):
  #serializer_class = PlaceSerializer
  #pagination_class = StandardResultsSetPagination
  #renderer_class = JSONRenderer

  #def get_queryset(self, format=None, *args, **kwargs):
    #req=self.request.GET
    #if req.get('q') is None:
      #result = {"errors":['Missing a query term, e.g. ?q=myplacename']}
      #return Response(result)
    ##print('kwargs',self.kwargs)
    #print('self.request.GET',self.request.GET)
    #dslabel = self.request.GET.get('ds')
    #query = self.request.GET.get('q')
    #qs = Place.objects.all()
    #if dslabel is not None:
      #qs = qs.filter(dataset=dslabel).order_by('title')
    #if query is not None:
      #qs = qs.filter(title__icontains=query)
    #return qs

  #permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]

"""
    search index e.g. union/?idx=whg&_id=12345979
    in usingapi.html example
"""
#class indexAPIView(View):
  #@staticmethod
  #def get(request):
    #print('in indexAPIView, GET =',request.GET)
    #"""
        #args in request.GET:
        #[string] idx: latest name for whg index
        #[string] _id: same as whg_id in index
        #"""
    #es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    #idx = request.GET.get('idx')
    #_id = request.GET.get('_id')
    #q={"query": {"bool": {"must": [{"match":{"_id": _id }}]}}}
    #res = es.search(index=idx, doc_type='place', body=q)
    ## single hit (it's a unique id after all)
    #hit = res['hits']['hits'][0]
    #print('hit[_id] from indexAPIView()',hit['_id'])
    ## now get traces
    ## does hit have children?

    ##qt={"query": {"bool": {"must": [{"match":{"_id": _id }}]}}}
    ##res_t = es.search(index="traces", doc_type='trace', body=q)
    ##print('indexAPIView _id',_id)
    #print('indexAPIView hit',hit)
    #return JsonResponse(hit, safe=True)

