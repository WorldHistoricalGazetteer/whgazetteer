from rest_framework import serializers

from datasets.models import Dataset
from places.models import Place, PlaceName, PlaceLink, PlaceGeom, PlaceWhen, PlaceDescription
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

class PlaceNameRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceName
		fields = [
			'id', 'place_id', 'toponym', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a PlaceName."""
		pn = PlaceName.objects.create(**validated_data)

		return pn

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
		"""Create a PlaceGeom."""
		pg = PlaceGeom.objects.create(**validated_data)

		return pg

class PlaceWhenRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceWhen
		fields = [
			'id', 'place_id', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a PlaceDescription."""
		pw = PlaceWhen.objects.create(**validated_data)

		return pw

class PlaceDescriptionRemoteSerializer(serializers.ModelSerializer):
	# for create only
	class Meta:
		model = PlaceDescription
		fields = [
			'id', 'place_id', 'jsonb',
		]
		read_only_fields = ['id']

	def create(self, validated_data):
		"""Create a PlaceDescription."""
		pd = PlaceDescription.objects.create(**validated_data)

		return pd

class PlaceRemoteSerializer(serializers.ModelSerializer):
	"""basic place record for lists"""
	names = PlaceNameRemoteSerializer(many=True, required=False)
	links = PlaceLinkRemoteSerializer(many=True, required=False)
	geoms = PlaceGeomRemoteSerializer(many=True, required=False)
	whens = PlaceWhenRemoteSerializer(required=False)
	descriptions = PlaceDescriptionRemoteSerializer(many=True, required=False)

	class Meta:
		model = Place
		fields = [
			'id', 'dataset', 'title', 'src_id', 'ccodes',
			'names', 'links', 'geoms', 'whens',
			'descriptions'
		]
		read_only_fields = ['id']

class PlaceRemoteDetailSerializer(PlaceRemoteSerializer):
	"""create Place and related records"""
	def _get_or_create_names(self, names, place):
		"""Handle getting or creating names as needed."""
		auth_user = self.context['request'].user
		for name in names:
			name_obj, created = PlaceName.objects.get_or_create(
				place_id = place.id,
				src_id=place.src_id,
				**name,
			)
			place.names.add(name_obj)

	def _get_or_create_links(self, links, place):
		"""Handle getting or creating links as needed."""
		auth_user = self.context['request'].user
		for link in links:
			link_obj, created = PlaceLink.objects.get_or_create(
				place_id = place.id,
				src_id=place.src_id,
				**link,
			)
			place.links.add(link_obj)

	def _get_or_create_geoms(self, geoms, place):
		"""Handle getting or creating geometries as needed."""
		auth_user = self.context['request'].user
		for geom in geoms:
			pg_obj, created = PlaceGeom.objects.get_or_create(
				place_id = place.id,
				src_id=place.src_id,
				**geom,
			)
			place.geoms.add(pg_obj)

	def _get_or_create_whens(self, when, place):
		"""Handle getting or creating a single when as needed."""
		auth_user = self.context['request'].user
		#
		pw_obj, created = PlaceWhen.objects.get_or_create(
			place_id = place.id,
			src_id = place.src_id,
			**when,
		)
		place.whens.add(pw_obj)

	def _get_or_create_descriptions(self, descriptions, place):
		"""Handle getting or creating descriptions as needed."""
		for descrip in descriptions:
			pd_obj, created = PlaceDescription.objects.get_or_create(
				place_id = place.id,
				src_id=place.src_id,
				**descrip,
			)
			place.descriptions.add(pd_obj)

	def create(self, validated_data):
		"""Create a place and related"""
		# pull attributes because they will need the new place_id
		names = validated_data.pop('names', [])
		links = validated_data.pop('links', [])
		geoms = validated_data.pop('geoms',[])
		whens = validated_data.pop('whens',[])
		descriptions = validated_data.pop('descriptions',[])

		place = Place.objects.create(**validated_data)
		self._get_or_create_names(names, place)
		self._get_or_create_links(links, place)
		self._get_or_create_geoms(geoms, place)
		self._get_or_create_whens(whens, place)
		self._get_or_create_descriptions(descriptions, place)

		return place
