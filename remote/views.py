#
# External API for external app integrations
# (Sep 2022)
#
from collection.models import Collection

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
    """Create a new dataset."""
    serializer.save(owner=self.request.user)


class PlaceViewSet(viewsets.ModelViewSet):
  """View for managing place APIs."""
  serializer_class = PlaceRemoteSerializer
  queryset = Place.objects.all() # ???
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get_serializer_class(self):
      """Return the serializer class for request."""
      if self.action == 'list':
          return PlaceRemoteSerializer

      return self.serializer_class

  def perform_create(self, serializer):
    """Create a new place."""
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
