# api.serializers.py

from django.contrib.auth.models import User, Group
from rest_framework import serializers
from datasets.models import Dataset
from areas.models import Area
from places.models import *

#class DatasetSerializer(serializers.HyperlinkedModelSerializer):
class DatasetSerializer(serializers.ModelSerializer):
    # don't list all places in a dataset API record
    # places = serializers.PrimaryKeyRelatedField(many=True, queryset=Place.objects.all())
    owner = serializers.ReadOnlyField(source='owner.username')

    class Meta:
        model = Dataset
        fields = ('id', 'url', 'owner', 'label', 'name', 'numrows', 'description','format',
            'datatype', 'delimiter', 'status', 'upload_date',
            'accepted_date', 'mapbox_id','uri_base','spine')

class PlaceDepictionSerializer(serializers.ModelSerializer):
    # json: @id, title, license
    identifier = serializers.ReadOnlyField(source='jsonb.@id')
    title = serializers.ReadOnlyField(source='jsonb.title')
    license = serializers.ReadOnlyField(source='jsonb.license')

    class Meta:
        model = PlaceDepiction
        fields = ('identifier','title','license')

class PlaceDescriptionSerializer(serializers.ModelSerializer):
    # json: @id, value, lang
    identifier = serializers.ReadOnlyField(source='jsonb.id')
    value = serializers.ReadOnlyField(source='jsonb.value')
    lang = serializers.ReadOnlyField(source='jsonb.lang')

    class Meta:
        model = PlaceDescription
        fields = ('identifier','value','lang')

class PlaceWhenSerializer(serializers.ModelSerializer):
    # json: timespans, periods, label, duration
    timespans = serializers.ReadOnlyField(source='jsonb.timespans')
    periods = serializers.ReadOnlyField(source='jsonb.periods')
    label = serializers.ReadOnlyField(source='jsonb.label')
    duration = serializers.ReadOnlyField(source='jsonb.duration')

    class Meta:
        model = PlaceWhen
        fields = ('timespans', 'periods', 'label', 'duration')

class PlaceRelatedSerializer(serializers.ModelSerializer):
    # json: relation_type, relation_to, label, when, citation, certainty
    relation_type = serializers.ReadOnlyField(source='jsonb.relationType')
    relation_to = serializers.ReadOnlyField(source='jsonb.relationTo')
    label = serializers.ReadOnlyField(source='jsonb.label')
    when = serializers.ReadOnlyField(source='jsonb.when')
    citation = serializers.ReadOnlyField(source='jsonb.citation')
    certainty = serializers.ReadOnlyField(source='jsonb.certainty')

    class Meta:
        model = PlaceRelated
        fields = ('relation_type', 'relation_to', 'label', 'when',
            'citation', 'certainty')

class PlaceLinkSerializer(serializers.ModelSerializer):
    # json: type, identifier
    type = serializers.ReadOnlyField(source='jsonb.type')
    identifier = serializers.ReadOnlyField(source='jsonb.identifier')

    class Meta:
        model = PlaceLink
        fields = ('type', 'identifier')

class PlaceGeomSerializer(serializers.ModelSerializer):
    # json: type, geowkt, coordinates, when{}
    type = serializers.ReadOnlyField(source='jsonb.type')
    geowkt = serializers.ReadOnlyField(source='jsonb.geowkt')
    coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
    citation = serializers.ReadOnlyField(source='jsonb.citation')
    when = serializers.ReadOnlyField(source='jsonb.when')

    class Meta:
        model = PlaceGeom
        #fields = ('type', 'geowkt', 'coordinates', 'geom_src', 'citation', 'when')
        fields = ('place_id_id','src_id','type', 'geowkt', 'coordinates', 'geom_src', 'citation', 'when')

class PlaceTypeSerializer(serializers.ModelSerializer):
    # json: identifier, label, source_label, when{}
    identifier = serializers.ReadOnlyField(source='jsonb.identifier')
    label = serializers.ReadOnlyField(source='jsonb.label')
    sourceLabel = serializers.ReadOnlyField(source='jsonb.sourceLabel')
    when = serializers.ReadOnlyField(source='jsonb.when')

    class Meta:
        model = PlaceType
        fields = ('label', 'sourceLabel', 'when', 'identifier')

class PlaceNameSerializer(serializers.ModelSerializer):
    # json: toponym, citation{}
    toponym = serializers.ReadOnlyField(source='jsonb.toponym')
    citation = serializers.ReadOnlyField(source='jsonb.citation')

    class Meta:
        model = PlaceName
        fields = ('toponym', 'citation')

class PlaceSerializer(serializers.HyperlinkedModelSerializer):
    dataset = serializers.ReadOnlyField(source='dataset.label')
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    geoms = PlaceGeomSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    class Meta:
        model = Place
        fields = ('url','id', 'title', 'src_id', 'dataset','ccodes',
            'names','types','geoms','links',
            'related','whens', 'descriptions', 'depictions','geom_count'
            )

# for dataset_recon.html queries
class PlaceDRFSerializer(serializers.ModelSerializer):
    class Meta:
        model = Place
        fields = ('id','src_id','title','ccodes')

class AreaSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Area
        fields = ('title', 'type', 'geojson')

# class UserSerializer(serializers.HyperlinkedModelSerializer):
class UserSerializer(serializers.ModelSerializer):
    datasets = serializers.PrimaryKeyRelatedField(
        many=True,
        read_only=True
    )
    class Meta:
        model = User
        fields = ('url', 'username', 'email', 'groups', 'datasets')

class GroupSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Group
        fields = ('url', 'name')
