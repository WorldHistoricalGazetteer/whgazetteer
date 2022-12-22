#
# API for external app integrations
# (Sep 2022)
#
import codecs
import datasets.utils
from api.views import collector, collectionItem
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset, DatasetFile
from datasets.tasks import get_bounds_filter
from places.models import Type
from django.http import JsonResponse, HttpResponse#
from django.shortcuts import get_object_or_404
from django.views.generic import View

from rest_framework import (
    viewsets,
    mixins,
    status,
)
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated

from remote.serializers import *

class DatasetViewSet(viewsets.ModelViewSet):
  """View for managing dataset APIs."""
  serializer_class = DatasetRemoteDetailSerializer
  queryset = Dataset.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get_queryset(self):
    print('self.request', self.request)
    """Retrieve recipes for authenticated user."""
    queryset = self.queryset
    result = queryset.filter(
      # user=self.request.user
      owner=self.request.user
    ).order_by('-id').distinct()
    return result

  def get_serializer_class(self):
      """Return the serializer class for request."""
      if self.action == 'list':
          return DatasetRemoteSerializer

      return self.serializer_class

  def perform_create(self, serializer):
    # Create a new dataset
    user = self.request.user
    if 'label' not in serializer.validated_data:
      label_out = 'ds_'+ str(Dataset.objects.last().id+1)
    else:
      label_out = serializer.validated_data['label']
    # serializer.save(owner=user, label=label_out)
    serializer.save(owner=user)
    # label_out = 'ds_' + str(Dataset.objects.last().id)
    serializer.save(label=label_out)

    # needs a new dummy DatasetFile too
    # will be deleted with cascade
    dsid = Dataset.objects.last()
    filename = 'user_'+user.username+'/'+str(dsid)+'-dummy.txt'
    filepath = 'media/'+filename
    dummyfile=codecs.open(filepath, mode='w', encoding='utf8')
    dummyfile.write("# nothing to see here, it's a dummy file")
    dummyfile.close()
    DatasetFile.objects.create(
      dataset_id=Dataset.objects.last(),
      file=filename,
      format='delimited',
      df_status='dummy',
      upload_date=None,
    )

class PlaceViewSet(viewsets.ModelViewSet):
  """View for managing place APIs."""
  serializer_class = PlaceRemoteDetailSerializer
  queryset = Place.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def _params_to_ints(self, qs):
    """Convert a list of strings to integers."""
    return [int(str_id) for str_id in qs.split(',')]

  def get_queryset(self):
    """Retrieve places for authenticated user."""
    links = self.request.query_params.get('links')
    queryset = self.queryset
    if links:
      link_ids = self._params_to_ints(links)
      queryset = queryset.filter(links__id__in=link_ids)

    return queryset.filter(
      user=self.request.user
    ).order_by('-id').distinct()

  def get_serializer_class(self):
      """return basic serializer for list, else detailed one"""
      """NB we never list places"""
      if self.action == 'list':
          return PlaceRemoteSerializer
      return self.serializer_class

  def perform_create(self, serializer):
    """Create a new place..."""
    ds = serializer.validated_data['dataset']
    # ds = Dataset.objects.get(label=dslabel)
    max_srcid = 999 if ds.places.count() == 0 else ds.places.last().src_id
    # srcids=list(ds.places.values_list('src_id', flat=True))
    # max_srcid = max([int(x[len(dslabel)+1:]) for x in srcids])

    if 'src_id' not in serializer.validated_data:
      # create one
      srcid = ds.label+'_'+str(max_srcid+1)
    cc_in = serializer.validated_data['ccodes']
    geoms = serializer.validated_data['geoms']
    if len(cc_in) == 0 and len(geoms) > 0:
      cc_out = datasets.utils.ccodesFromGeom(geoms[0]['jsonb'])
    else:
      cc_out = cc_in
    serializer.save(ccodes=cc_out, src_id=srcid)

class CollectionViewSet(viewsets.ModelViewSet):
  """View for managing collection APIs."""
  serializer_class = CollectionRemoteSerializer
  queryset = Collection.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get_queryset(self):
    """Retrieve collections for authenticated user."""
    queryset = self.queryset
    return queryset.filter(
      owner=self.request.user
    ).order_by('-id').distinct()

  def perform_create(self, serializer):
    """Create a new collection."""
    serializer.save(owner=self.request.user)

class TypeViewSet(viewsets.ModelViewSet):
  """View for managing Type (place type) APIs."""
  serializer_class = TypeRemoteSerializerSlim
  queryset = Type.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  # def get_queryset(self):
  #   """Retrieve collections for authenticated user."""
  #   queryset = self.queryset
  #   return queryset.filter(
  #     owner=self.request.user
  #   ).order_by('-id').distinct()

