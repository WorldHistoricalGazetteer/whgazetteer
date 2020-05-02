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

#class SearchAPIView(generics.ListAPIView):
    #serializer_class = PlaceSerializer
    ## q <str>, scope [db}index], ccode <str>, mode [exact|fuzzy]; 
    ## type {oneof}, cat [settlement|site|feature], dataset {oneof}
    #def get(self, format=None, *args, **kwargs):
    ##def get_queryset(self, format=None, *args, **kwargs):
        #req=self.request.GET
        #print('search request:',req)
        #params = list(req.keys())
        #if req.get('q') is None:
            #result = {"errors":['Missing a query term, e.g. ?q=myplacename']}
            #return JsonResponse(result, safe=True)
        #f = 'json' if 'format' not in params else req['format']
        #scope = 'db' if 'scope' not in params else req['scope']
        #if scope == 'db':
            #qs = Place.objects.all()
            #if req.get('ds') is not None:
                #qs=qs.filter(dataset=req.get('ds'))
            #qs = qs.filter(title__icontains=req.get('q'))
        #result = {"q":req,
                  #"scope":scope,
                  #"format":f,
                  #"result":serializers.serialize("json", serializers.serialize("json", qs))}
        ##return result
        #return JsonResponse(result, safe=True)

""" cf. https://www.django-rest-framework.org/api-guide/filtering/#djangofilterbackend """
class FilteredSearchAPIView(generics.ListAPIView):
    queryset = Place.objects.all()
    serializer_class = LPFSerializer
    renderer_classes = [JSONRenderer]
    filter_backends = [DjangoFilterBackend]    
    #filter_backends = [filters.SearchFilter]    
    filterset_fields = ['title']
    
    
""" 
    return lpf results from search 
    q <str>, dataset {oneof}, ccode <str>, mode [exact|fuzzy], 
    type {oneof}, category [settlement|site|feature], scope [db}index]

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
        fclass = params.get('class',None)
        
        print('SearchAPIView() params',params)
        qs = Place.objects.all()
        
        if q is None and contains is None:
            return HttpResponse(content=b'<h3>Needs either a "q" or "contains" parameter at minimum <br/>(e.g. ?q=myplacename or ?contains=astring)</h3>')
        else:
            qs = qs.filter(title__icontains=q) if contains is not None else qs.filter(title__istartswith=q)
            qs = qs.filter(dataset=dslabel) if dslabel else qs
            qs = qs.filter(ccodes__overlap=cc) if cc else qs
            filtered = qs[:20]
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
    search index e.g. union/?idx=whg02&_id=12345979
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

