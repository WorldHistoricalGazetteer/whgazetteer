from rest_framework import serializers

from datasets.models import Dataset
from places.models import Place, PlaceLink, PlaceGeom, PlaceDescription
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
			'id', 'owner', 'title', 'label', 'description', 'ds_status', 'uri_base'
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


class PlaceLinkRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceLink
		fields = [
			'id', 'place_id', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a dataset."""
		place = PlaceLink.objects.create(**validated_data)

		return place

class PlaceGeomRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceGeom
		fields = [
			'id', 'place_id', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a dataset."""
		place = PlaceGeom.objects.create(**validated_data)

		return place

class PlaceDescriptionRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceDescription
		fields = [
			'id', 'place_id', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a dataset."""
		place = PlaceDescription.objects.create(**validated_data)

		return place

class PlaceRemoteSerializer(serializers.ModelSerializer):
	# for create only
	links = PlaceLinkRemoteSerializer(many=True, required=False)
	# geoms = PlaceGeomRemoteSerializer(many=True, required=False)
	# descriptions = PlaceDescriptionRemoteSerializer(many=True, required=False)

	class Meta:
		model = Place
		fields = [
			'id', 'dataset', 'title', 'src_id', 'ccodes',	'links',
			# 'geoms', 'decriptions'
		]
		read_only_fields = ['id']

class PlaceRemoteDetailSerializer(PlaceRemoteSerializer):

	def _get_or_create_links(self, links, place):
		"""Handle getting or creating links as needed."""
		auth_user = self.context['request'].user
		for link in links:
			link_obj, created = PlaceLink.objects.get_or_create(
				place_id = place.id,
				# user=auth_user,
				**link,
			)
			place.links.add(link_obj)

	def create(self, validated_data):
		"""Create a place and related"""
		links = validated_data.pop('links', [])
		place = Place.objects.create(**validated_data)
		self._get_or_create_links(links, place)

		return place
