# api.views
from django.contrib.auth.models import User, Group
from django.db.models import Count
from django.shortcuts import get_object_or_404
from rest_framework import generics
from rest_framework import permissions
from rest_framework import viewsets

from .serializers import UserSerializer, GroupSerializer, DatasetSerializer, \
    PlaceSerializer, PlaceDRFSerializer, PlaceGeomSerializer, AreaSerializer


from accounts.permissions import IsOwnerOrReadOnly, IsOwner
from datasets.models import Dataset
from areas.models import Area
from places.models import Place, PlaceGeom

class GeomViewSet(viewsets.ModelViewSet):
    queryset = PlaceGeom.objects.all()
    serializer_class = PlaceGeomSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
    
    def get_queryset(self):
        dslabel = self.request.GET.get('ds')
        #ds = get_object_or_404(Dataset,label=dslabel)
        dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
        qs = PlaceGeom.objects.filter(place_id_id__in=dsPlaceIds)
        #print('qs count',qs.count())
        return qs
    
class PlaceViewSet(viewsets.ModelViewSet):
    queryset = Place.objects.all().order_by('title')
    serializer_class = PlaceSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly)

    def get_queryset(self):
        #qs = Place.objects.annotate(num_g=Count('geoms'))
        qs = Place.objects.order_by('geom_count')
        query = self.request.GET.get('q')
        ds = self.request.GET.get('ds')
        print('GET.get from PlaceViewSet()',self.request.GET)
        #f = self.request.GET.get('f')
        for key, value in self.request.GET.items():
            print('foo',key, value)
        if ds is not None:
            qs = qs.filter(dataset = ds)
        #if f == 'nogeom':
            #qs = qs.filter(num_g__lt=1)
            #print('nogeom;count',qs.count())
        if query is not None:
            qs = qs.filter(title__istartswith=query)
            #qs = qs.filter(title__icontains=query)
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

class DatasetViewSet(viewsets.ModelViewSet):
    # print('in DatasetViewSet()')
    #queryset = Dataset.objects.all().order_by('label')
    queryset = Dataset.objects.all().filter(spine=True).order_by('label')
    # TODO: public list only accepted datasets
    # queryset = Dataset.objects.exclude(accepted_date__isnull=True).order_by('label')
    serializer_class = DatasetSerializer

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

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer

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


class GroupViewSet(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer
