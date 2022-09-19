from rest_framework import serializers

from datasets.models import Dataset
from places.models import Place
from collection.models import Collection

# ******************
# NEW for remoteapi
# ******************
class DatasetRemoteSerializer(serializers.ModelSerializer):
	""" for lists """
	owner = serializers.ReadOnlyField(source='owner.username')

	class Meta:
		model = Dataset
		fields = [
			'id', 'owner', 'title', 'label', 'description', 'ds_status'
		]
		read_only_fields = ['id']


class DatasetRemoteDetailSerializer(DatasetRemoteSerializer):
	""" subclasses the simpler list above"""
	""" detail adds a couple fields"""

	class Meta(DatasetRemoteSerializer.Meta):
		fields = DatasetRemoteSerializer.Meta.fields + ['public', 'numrows']

	def create(self, validated_data):
		"""Create a dataset."""
		dataset = Dataset.objects.create(**validated_data)

		return dataset


class CollectionRemoteSerializer(serializers.ModelSerializer):
	# for list, add (place)
	owner = serializers.ReadOnlyField(source='owner.username')

	class Meta:
		model = Collection
		fields = [
			'id', 'owner', 'collection_class', 'title', 'description',
		]
		read_only_fields = ['id']


class PlaceRemoteSerializer(serializers.ModelSerializer):
	# for create only

	class Meta:
		model = Place
		fields = [
			'id', 'dataset', 'title', 'src_id', 'ccodes',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a dataset."""
		place = Place.objects.create(**validated_data)

		return place