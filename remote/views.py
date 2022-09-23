#
# External API for external app integrations
# (Sep 2022)
#
from collection.models import Collection
from datasets.models import Dataset, DatasetFile
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
    serializer.save(owner=user)

    # needs a new dummy DatasetFile too
    DatasetFile.objects.create(
      dataset_id=Dataset.objects.last(),
      file='data/dummy.txt',
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
    serializer.save()

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
