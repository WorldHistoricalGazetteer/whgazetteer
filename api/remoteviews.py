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

from api.serializers import *
from api.remoteserializers import *


class DatasetViewSet(viewsets.ModelViewSet):
  """View for manage recipe APIs."""
  serializer_class = DatasetRemoteSerializer
  queryset = Dataset.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get_queryset(self):
    """Retrieve recipes for authenticated user."""
    queryset = self.queryset
    return queryset.filter(
      user=self.request.user
    ).order_by('-id').distinct()

  def get_serializer_class(self):
      """Return the serializer class for request."""
      if self.action == 'list':
          return DatasetRemoteSerializer

      return self.serializer_class

  def perform_create(self, serializer):
    """Create a new dataset."""
    serializer.save(owner=self.request.user)
    # serializer.save(user=self.request.user)


class PlaceViewSet(viewsets.ModelViewSet):
  """View for manage recipe APIs."""
  serializer_class = PlaceRemoteSerializer
  queryset = Place.objects.all() # ???
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]


class CollectionViewSet(viewsets.ModelViewSet):
  """View for manage recipe APIs."""
  serializer_class = CollectionRemoteSerializer
  queryset = Collection.objects.all()
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get_queryset(self):
    """Retrieve collections for authenticated user."""
    queryset = self.queryset
    return queryset.filter(
      user=self.request.user
    ).order_by('title').distinct()
