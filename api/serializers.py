# api.serializers.py
from django.contrib.auth.models import User, Group
from django.contrib.gis.geos import GEOSGeometry, Point, Polygon, MultiPolygon, LineString, MultiLineString
from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer, GeometrySerializerMethodField
from datasets.models import Dataset
from areas.models import Area
from places.models import *

import json, geojson
from shapely.geometry import shape

# TODO: these are updated in both Dataset & DatasetFile  (??)
datatype = models.CharField(max_length=12, null=False,choices=DATATYPES,
    default='place')
numrows = models.IntegerField(null=True, blank=True)

# these are back-filled
numlinked = models.IntegerField(null=True, blank=True)
total_links = models.IntegerField(null=True, blank=True)

# ***
# IN USE Apr 2020
# ***
class DatasetSerializer(serializers.HyperlinkedModelSerializer):
#class DatasetSerializer(serializers.ModelSerializer):
    # don't list all places in a dataset API record
    owner = serializers.ReadOnlyField(source='owner.username')
    #files = serializers.HyperlinkedRelatedField(
        #many=True, view_name='datasetfile-detail', read_only=True)

    class Meta:
        model = Dataset
        fields = ('id', 'owner', 'label', 'title', 'description',
            'datatype', 'ds_status', 'create_date', 'public','core')

class UserSerializer(serializers.HyperlinkedModelSerializer):
    datasets = serializers.HyperlinkedRelatedField(
        many=True, view_name='dataset-detail', read_only=True)

    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'url', 'datasets']
        #fields = ('id','url', 'username', 'email', 'groups', 'datasets')

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
        fields = ('place_id','src_id','type', 'geowkt', 'coordinates', 'geom_src', 'citation', 'when')

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
    when = serializers.ReadOnlyField(source='jsonb.when')

    class Meta:
        model = PlaceName
        fields = ('toponym','when','citation')

#class PlaceSerializer(serializers.HyperlinkedModelSerializer):
""" 
    direct representation of normalized records in database 
    used in multiple views
"""
class PlaceSerializer(serializers.ModelSerializer):
    dataset = serializers.ReadOnlyField(source='dataset.label')
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    geoms = PlaceGeomSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    geo = serializers.SerializerMethodField('has_geom')    
    def has_geom(self,place):
        return '<i class="fa fa-globe"></i>' if place.geom_count > 0 else "-"
    
    # 6365611 (pleiades_20200402); 6367873 (euratlas_cities); 6365630 (owtrad10)
    minmax = serializers.SerializerMethodField('get_minmax')
    def get_minmax(self,place):
        tsarr=[n.jsonb['timespans'][0] for n in place.whens.all() if place.whens.count()>0]
        tsarr=tsarr+[n.jsonb['when']['timespans'][0] for n in place.names.all() if 'when' in n.jsonb]
        tsarr=tsarr+[t.jsonb['when']['timespans'][0] for t in place.types.all() if 'when' in t.jsonb]
        tsarr=tsarr+[g.jsonb['when']['timespans'][0] for g in place.geoms.all() if 'when' in g.jsonb]
        starts=[];ends=[];years=[];nullset=set([None]);
        for ts in tsarr:
            years.append(int(list(ts['start'].values())[0]))
            years.append(int(list(ts['end'].values())[0]) if 'end' in ts.keys() else None)
            years = list(set(years)-nullset)
        return [min(years),max(years)] if len(years)>0 else []
    
    class Meta:
        model = Place
        fields = ('url','id', 'title', 'src_id', 'dataset','ccodes',
            'names','types','geoms','links','related',
            'whens', 'descriptions', 'depictions',
            'geo','minmax'
        )
        



""" uses: DownloadGeomsAPIView() """
class FeatureSerializer(GeoFeatureModelSerializer):
    geom = GeometrySerializerMethodField()
    def get_geom(self, obj):
        print('obj',obj.__dict__)
        s=json.dumps(obj.jsonb)
        g1 = geojson.loads(s)
        g2 = shape(g1)
        djgeo = GEOSGeometry(g2.wkt)
        print('geom', djgeo.geojson)
        return GEOSGeometry(g2.wkt)
    
    #
    title = serializers.SerializerMethodField('get_title')    
    def get_title(self, obj):
        return obj.place.title

    class Meta:
        model = PlaceGeom
        geo_field = 'geom'
        id_field = False
        fields = ('place_id','src_id','title')

""" uses: AreaViewset()"""
class AreaSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Area
        fields = ('title', 'type', 'geojson')


class LPFSerializer(serializers.HyperlinkedModelSerializer):
    #dataset = serializers.ReadOnlyField(source='dataset.label')
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    # custom fields for LPF transform
    properties = serializers.SerializerMethodField('get_properties')
    def get_properties(self,place):
        props = {
            "src_id":place.src_id,
            "title":place.title,
            "whg_placeid":place.id,
            "dataset":place.dataset.label,
            "ccodes":place.ccodes
        }
        return props
    geometry = serializers.SerializerMethodField('get_geometry')
    def get_geometry(self,place):
        collection = {"type":"FeatureCollection","features":[]}
        geoms = [g.jsonb for g in place.geoms.all()]
        for g in geoms:
            collection['features'].append(g)
        return collection
    minmax = serializers.SerializerMethodField('get_minmax')
    def get_minmax(self,place):
        tsarr=[n.jsonb['timespans'][0] for n in place.whens.all() if place.whens.count()>0]
        tsarr=tsarr+[n.jsonb['when']['timespans'][0] for n in place.names.all() if 'when' in n.jsonb]
        tsarr=tsarr+[t.jsonb['when']['timespans'][0] for t in place.types.all() if 'when' in t.jsonb]
        tsarr=tsarr+[g.jsonb['when']['timespans'][0] for g in place.geoms.all() if 'when' in g.jsonb]
        starts=[];ends=[];years=[];nullset=set([None]);
        for ts in tsarr:
            years.append(int(list(ts['start'].values())[0]))
            years.append(int(list(ts['end'].values())[0]) if 'end' in ts.keys() else None)
            years = list(set(years)-nullset)
        return [min(years),max(years)] if len(years)>0 else []
    

    class Meta:
        model = Place
        fields = ('url','properties','geometry',
            'names','types','links','related',
            'whens', 'descriptions', 'depictions','minmax'
        )
