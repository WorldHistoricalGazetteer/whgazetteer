from rest_framework import serializers

# ******************
# NEW for remoteapi
# ******************
class DatasetRemoteSerializer(serializers.ModelSerializer):
  # list an authenticated user's datasets
  # create new Dataset
  owner = serializers.ReadOnlyField(source='owner.username')

class PlaceRemoteSerializer(serializers.ModelSerializer):
  # create new Place
  owner = serializers.ReadOnlyField(source='owner.username')

class CollectionRemoteSerializer(serializers.ModelSerializer):
  # list an authenticated user's Place Collections
  owner = serializers.ReadOnlyField(source='owner.username')