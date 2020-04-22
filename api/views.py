# api.views
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User, Group
from django.db.models import Count
from django.http import JsonResponse, Http404, HttpResponse, FileResponse
from django.shortcuts import get_object_or_404
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from elasticsearch import Elasticsearch
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

from accounts.permissions import IsOwnerOrReadOnly
from api.serializers import (UserSerializer, DatasetSerializer,
    PlaceSerializer, PlaceGeomSerializer, AreaSerializer, FeatureSerializer)
from areas.models import Area
from datasets.models import Dataset
from places.models import Place, PlaceGeom

#class StandardResultsSetPagination(PageNumberPagination):
    #page_size = 10
    #page_size_query_param = 'page_size'
    #max_page_size = 15000
    
@api_view(['GET'])
def api_root(request, format=None):
    return Response({
        #'users': reverse('user-list', request=request, format=format),
        'datasets': reverse('dataset-list', request=request, format=format)
    })

#
# API

class PlaceAPIView(generics.ListAPIView):
    """    Paged list of places in dataset. Optionally filtered on title with ?q={string}  """
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

class PlaceDetailAPIView(generics.RetrieveAPIView):
    """  single place record by id  (database)  """
    queryset = Place.objects.all()
    serializer_class = PlaceSerializer
    
    permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
    authentication_classes = [SessionAuthentication]

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

class DatasetDetailAPIView(LoginRequiredMixin, generics.RetrieveAPIView):
#class DatasetDetailAPIView(generics.RetrieveAPIView):
    """    dataset record by id   """
    queryset = Dataset.objects.all().filter(public=True)
    serializer_class = DatasetSerializer
    
    permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
    authentication_classes = [SessionAuthentication]
    

class UserList(generics.ListAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    
class UserDetail(generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer

class FeatureAPIView(generics.ListAPIView):
    """  GeoJSON FeatureCollection """
    serializer_class = FeatureSerializer
    pagination_class = None
    
    def get_queryset(self, format=None, *args, **kwargs):
        #print('kwargs',self.kwargs)
        dslabel=self.kwargs['dslabel']
        qs = Place.objects.all().filter(dataset=dslabel).order_by('title')
        query = self.request.GET.get('q')
        if query is not None:
            qs = qs.filter(title__icontains=query)
        #return {"type":"FeatureCollection","features":list(qs)}
        return qs
    
    permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]

class DownloadGeomViewSet(generics.ListAPIView):
    queryset = PlaceGeom.objects.all()
    serializer_class = FeatureSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
    
    def get_queryset(self, format=None, *args, **kwargs):
        #fn='mygeom.json'
        ds = get_object_or_404(Dataset,pk=self.kwargs['ds'])
        qs = PlaceGeom.objects.all().filter(place_id__in=ds.placeids)
        #print('qs',qs)
        return qs
        #response = HttpResponse(qs,content_type='text/json')
        #response['Content-Disposition'] = 'attachment; filename="'+fn+'"'    
    
        #return response
#
# in use pre-Apr 2020    
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
    
#
# populates drf table in dataset.detail#browse
class PlaceTableViewSet(viewsets.ModelViewSet):
    queryset = Place.objects.all().order_by('title')
    serializer_class = PlaceSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly)

    def get_queryset(self):
        print('PlaceViewSet.get_queryset()',self.request.GET)
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

class AreaViewSet(viewsets.ModelViewSet):
    queryset = Area.objects.all().order_by('title')
    serializer_class = AreaSerializer
#
# search index
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
        
# DREK
#class UserViewSet(viewsets.ModelViewSet):
    #queryset = User.objects.all().order_by('-date_joined')
    #serializer_class = UserSerializer

    #def get_permissions(self):
        #"""
        #Instantiates and returns the list of permissions that this view requires.
        #"""
        #if self.action in ['list','retrieve']:
            #print(self.action)
            #permission_classes = [permissions.AllowAny]
        #else:
            #permission_classes = [permissions.IsAdminUser]
        #return [permission() for permission in permission_classes]



#class DatasetViewSet(viewsets.ModelViewSet):
    ## print('in DatasetViewSet()')
    #queryset = Dataset.objects.all().filter(public=True).order_by('label')
    #serializer_class = DatasetSerializer

    #def get_permissions(self):
        #"""
        #Instantiates and returns the list of permissions that this view requires.
        #"""
        #if self.action in ['list','retrieve']:
            #print(self.action)
            #permission_classes = [permissions.AllowAny]
        #else:
            #permission_classes = [permissions.IsAdminUser]
        #return [permission() for permission in permission_classes]
