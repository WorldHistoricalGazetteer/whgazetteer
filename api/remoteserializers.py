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
			'id', 'owner', 'title', 'label', 'description', 'public', 'numrows'
			# 'id', 'owner', 'title', 'label', 'description',
		]
		read_only_fields = ['id']

class DatasetRemoteDetailSerializer(serializers.ModelSerializer):
	""" for detail """
	owner = serializers.ReadOnlyField(source='owner.username')

	class Meta:
		model = Dataset
		fields = [
			'id', 'owner', 'title', 'label', 'description', 'public', 'numrows'
		]
		read_only_fields = ['id']


class PlaceRemoteSerializer(serializers.ModelSerializer):
	# for create

	class Meta:
		model = Place
		fields = [
			'id', 'title', 'src_id', 'dataset',
		]
		read_only_fields = ['id']

class CollectionRemoteSerializer(serializers.ModelSerializer):
	# for list, add (place)

	owner = serializers.ReadOnlyField(source='owner.username')

	class Meta:
		model = Collection
		fields = [
			'id', 'owner', 'title', 'keywords', 'description',
		]
		read_only_fields = ['id']
