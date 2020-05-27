# api.views
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User, Group
from django.contrib.postgres import search
from django.core import serializers
from django.db.models import Count
from django.http import JsonResponse, Http404, HttpResponse, FileResponse
from django.shortcuts import get_object_or_404
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
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
# API
# 
#

"""
  suggestionItem(); called by suggester();
  formats search hits 
"""
def suggestionItem(s,datatype):
  print('suggestionItem s',s)
  s=s['hit']
  if datatype == 'place':
    item = { 
      "index_id":s['whg_id'],
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
    #print('SUGGESTIONS from suggester()',type(suggestions), sortedsugs)
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
    search index given name string
    based on search.views.SearchView(View)

"""
class IndexAPIView(View):
  @staticmethod
  def get(request):
    print('IndexAPIView request',request.GET)
    """
      args in request.GET:
          [string] q: query string
          [string] datatype: place or trace
    """
    qstr = request.GET.get('q')
    datatype = 'trace' if request.GET.get('datatype') == 'trace' else 'place'
    idx = 'whg03'
    if datatype == 'place':
      q = { 
        "size": 100,
        "query": { "bool": {
          "must": [
            {"exists": {"field": "whg_id"}},
            {"multi_match": {
              "query": qstr, 
              "fields": ["title","names.toponym","searchy"],
              "type": "phrase"
          }}]
        }},
        "highlight": {"fields" : {"descriptions.value" : {}}}
      }
      print('search query:',q)
    elif datatype == 'trace':
      q = { "query": {"match": {"target.title": {"query": qstr, "operator": "and"}}} }
      print('trace query:',q)

    suggestions = suggester(datatype, q, idx)
    #
    suggestions = [ suggestionItem(s, datatype) for s in suggestions]
    resut = {'count': len(suggestions), 'result':suggestions}
    return JsonResponse(suggestions, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

""" 
    return lpf results from database search 
    q <str>, contains <str>, dataset <str>, ccodes (xx[|,..]),
    class [A,P,T,L,H], year <int>, range <int>,<int>

"""
#class SearchAPIView(APIView):
class SearchAPIView(generics.ListAPIView):
  #serializer_class = LPFSerializer
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  search_fields = ['@title']

  #def get_queryset(self, format=None):
  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params
    q = params.get('q',None)
    contains = params.get('contains')
    cc = map(str.upper,params.get('ccodes').split(',')) if params.get('ccodes') else None
    dslabel = params.get('dataset',None)
    fc = params.get('class',None)
    fclasses=[x.upper() for x in fc.split(',')] if fc else None
    year = params.get('year',None)
    count = params.get('count',None)

    print('SearchAPIView() params (q,contains,cc,dslabel,fc,count',q,contains,cc,dslabel,fc,count)
    qs = Place.objects.all()

    if q is None and contains is None:
      # TODO: return a template with API instructions
      return HttpResponse(content=b'<h3>Needs either a "q" or "contains" parameter at minimum <br/>(e.g. ?q=myplacename or ?contains=astring)</h3>')
    else:
      qs = qs.filter(minmax__0__lte=year,minmax__1__gte=year) if year else qs
      qs = qs.filter(fclasses__overlap=fclasses) if fc else qs
      if contains and contains != '':
        qs = qs.filter(title__icontains=contains)
      elif q and q != '':
        qs.filter(title__istartswith=q)
      #qs = qs.filter(title__icontains=q) if contains is not None else qs.filter(title__istartswith=q)
      qs = qs.filter(dataset=dslabel) if dslabel else qs
      qs = qs.filter(ccodes__overlap=cc) if cc else qs
      filtered = qs[:count] if count else qs[:10]
      serializer = LPFSerializer(filtered, many=True, context={'request': self.request})
      serialized_data = serializer.data
      result = {"count":qs.count(),"parameters": params,"features":serialized_data}
      return Response(result)


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

# API 'home page' (not implemented)
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

#

# IN USE Apr 2020

#
"""
    place/<int:pk>/
    in dataset.html#browse
"""
class PlaceDetailAPIView(generics.RetrieveAPIView):
  """  single database place record by id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer

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
    area/<int:pk>/
    in dataset.html#addtask
"""
class AreaViewSet(viewsets.ModelViewSet):
  queryset = Area.objects.all().order_by('title')
  serializer_class = AreaSerializer

"""
    search index e.g. union/?idx=whg&_id=12345979
    in usingapi.html example
"""
class indexAPIView(View):
  @staticmethod
  def get(request):
    print('in indexAPIView, GET =',request.GET)
    """
        args in request.GET:
        [string] idx: latest name for whg index
        [string] _id: same as whg_id in index
        """
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    idx = request.GET.get('idx')
    _id = request.GET.get('_id')
    q={"query": {"bool": {"must": [{"match":{"_id": _id }}]}}}
    res = es.search(index=idx, doc_type='place', body=q)
    # single hit (it's a unique id after all)
    hit = res['hits']['hits'][0]
    print('hit[_id] from indexAPIView()',hit['_id'])
    # now get traces
    # does hit have children?

    #qt={"query": {"bool": {"must": [{"match":{"_id": _id }}]}}}
    #res_t = es.search(index="traces", doc_type='trace', body=q)
    #print('indexAPIView _id',_id)
    print('indexAPIView hit',hit)
    return JsonResponse(hit, safe=True)



"""
    datasets/
    in usingapi.html example
"""
class DatasetAPIView(LoginRequiredMixin, generics.ListAPIView):
  """    List public datasets    """
  serializer_class = DatasetSerializer
  #pagination_class = StandardResultsSetPagination

  def get_queryset(self, format=None):
    qs = Dataset.objects.all().filter(public=True).order_by('label')
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(description__icontains=query)
    return qs

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]

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

