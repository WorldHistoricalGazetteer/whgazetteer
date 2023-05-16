# api.serializers.py
from django.contrib.auth.models import User, Group
from django.contrib.gis.geos import GEOSGeometry, Point, Polygon, MultiPolygon, LineString, MultiLineString
from rest_framework import serializers
from django.core import serializers as coreserializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer, GeometrySerializerMethodField
from datasets.models import Dataset
from areas.models import Area
from main.choices import DATATYPES
from places.models import *
from traces.models import TraceAnnotation

import json, geojson
#from edtf import parse_edtf
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
  # don't list all places in a dataset API record
  owner = serializers.ReadOnlyField(source='owner.username')

  place_count = serializers.SerializerMethodField('get_count')
  def get_count(self,ds):
    return ds.places.count()

  class Meta:
    model = Dataset
    fields = ('id', 'place_count', 'owner', 'label', 'title', 'description','datatype', 'ds_status', 'create_date', 'public', 'core','creator','webpage')
    extra_kwargs = {
          'created_by': { 'read_only': True }}

class UserSerializer(serializers.HyperlinkedModelSerializer):
  datasets = serializers.HyperlinkedRelatedField(
      #many=True, view_name='dataset-detail', read_only=True)
      many=True, view_name='ds_summary', read_only=True)

  class Meta:
    model = User
    fields = ['id', 'username', 'email', 'url', 'datasets']
    #fields = ('id','url', 'username', 'email', 'groups', 'datasets')

class TraceAnnotationSerializer(serializers.ModelSerializer):
  class Meta:
    model = TraceAnnotation
    fields = ('id', 'src_id', 'collection', 'anno_type', 'motivation',
              'when', 'sequence', 'creator', 'created')

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
  aug = serializers.SerializerMethodField('augmented')
  def augmented(self, obj):
    return True if obj.task_id is not None else False

  type = serializers.ReadOnlyField(source='jsonb.type')
  identifier = serializers.ReadOnlyField(source='jsonb.identifier')

  class Meta:
    model = PlaceLink
    fields = ('type', 'identifier', 'aug')

# returns single (first) geom for a place
class PlaceGeomSerializer(serializers.ModelSerializer):
  # json: type, geowkt, coordinates, when{}
  #title = serializers.ReadOnlyField(source='title')
  ds = serializers.SerializerMethodField()
  def get_ds(self, obj):
    return obj.place.dataset.id
  #title = serializers.SerializerMethodField()
  #def get_title(self, obj):
    #return obj.place.title

  type = serializers.ReadOnlyField(source='jsonb.type')
  geowkt = serializers.ReadOnlyField(source='jsonb.geowkt')
  coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
  citation = serializers.ReadOnlyField(source='jsonb.citation')
  when = serializers.ReadOnlyField(source='jsonb.when')
  certainty = serializers.ReadOnlyField(source='jsonb.certainty')

  class Meta:
    model = PlaceGeom
    fields = ('place_id','src_id','type', 'geowkt', 'coordinates',
              'geom_src', 'citation', 'when', 'title', 'ds',
              'certainty')

# return list of normalized coordinates for a place
class PlaceGeomsSerializer(serializers.ModelSerializer):
  # ds = serializers.SerializerMethodField()
  # def get_ds(self, obj):
  #   return obj.place.dataset.id

  type = serializers.ReadOnlyField(source='jsonb.type')
  geowkt = serializers.ReadOnlyField(source='jsonb.geowkt')
  coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
  citation = serializers.ReadOnlyField(source='jsonb.citation')
  # citation = serializers.ReadOnlyField(source='jsonb.citation') or None
  # when = serializers.ReadOnlyField(source='jsonb.when')
  # certainty = serializers.ReadOnlyField(source='jsonb.certainty')

  class Meta:
    model = PlaceGeom
    fields = ('place_id','src_id','type', 'geowkt', 'coordinates',
              'citation'
              #, 'ds'
              #'title', 'geom_src', 'when', 'certainty'
    )

class PlaceTypeSerializer(serializers.ModelSerializer):
  # json: identifier, label, sourceLabel OR sourceLabels[{}], when{}
  identifier = serializers.ReadOnlyField(source='jsonb.identifier')
  label = serializers.ReadOnlyField(source='jsonb.label')
  sourceLabel = serializers.ReadOnlyField(source='jsonb.sourceLabel')
  sourceLabels = serializers.ReadOnlyField(source='jsonb.sourceLabels')
  when = serializers.ReadOnlyField(source='jsonb.when')
  gn_class = serializers.ReadOnlyField(source='fclass')

  class Meta:
    model = PlaceType
    fields = ('label', 'sourceLabel', 'sourceLabels', 'when', 'identifier','gn_class')

class PlaceNameSerializer(serializers.ModelSerializer):
  # jsonb: toponym, citation{}
  toponym = serializers.ReadOnlyField(source='jsonb.toponym')
  citations = serializers.ReadOnlyField(source='jsonb.citations')
  # id = serializers.ReadOnlyField(source='jsonb.citation["id"]')
  # label = serializers.ReadOnlyField(source='jsonb.citation.label')
  when = serializers.ReadOnlyField(source='jsonb.when')

  class Meta:
    model = PlaceName
    fields = ('toponym','when','citations')

"""
    direct representation of normalized records in database
    used in multiple views
"""
class PlaceSerializer(serializers.ModelSerializer):
  dataset = serializers.ReadOnlyField(source='dataset.title')
  names = PlaceNameSerializer(many=True, read_only=True)
  types = PlaceTypeSerializer(many=True, read_only=True)
  geoms = PlaceGeomSerializer(many=True, read_only=True)
  links = PlaceLinkSerializer(many=True, read_only=True)
  related = PlaceRelatedSerializer(many=True, read_only=True)
  whens = PlaceWhenSerializer(many=True, read_only=True)
  descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
  depictions = PlaceDepictionSerializer(many=True, read_only=True)

  # traces = serializers.serialize("json", TraceAnnotation.objects.filter())
  traces = serializers.SerializerMethodField('trace_anno')
  def trace_anno(self, place):
    return coreserializers.serialize("json", TraceAnnotation.objects.filter(place=place.id))

  geo = serializers.SerializerMethodField('has_geom')

  def has_geom(self,place):
    return '<i class="fa fa-globe"></i>' if place.geom_count > 0 else "-"

  class Meta:
    model = Place
    fields = ('url', 'id', 'title', 'src_id', 'dataset', 'ccodes', 'fclasses',
              'names', 'types', 'geoms', 'links', 'related', 'whens',
              'descriptions', 'depictions', 'geo', 'minmax', 'traces', 'attestation_year'
            )

"""
    partial direct representation of normalized place-related records in database
    used for dataset updates
"""
class PlaceCompareSerializer(serializers.ModelSerializer):
  dataset = serializers.ReadOnlyField(source='dataset.title')
  names = PlaceNameSerializer(many=True, read_only=True)
  types = PlaceTypeSerializer(many=True, read_only=True)
  geoms = PlaceGeomsSerializer(many=True, read_only=True)
  links = PlaceLinkSerializer(many=True, read_only=True)
  related = PlaceRelatedSerializer(many=True, read_only=True)
  whens = PlaceWhenSerializer(many=True, read_only=True)

  class Meta:
    model = Place
    fields = ('url', 'id', 'title', 'src_id', 'dataset', 'ccodes', 'fclasses',
              'names', 'types', 'geoms', 'links', 'related', 'whens',
              'minmax' )

# returns default and computed columns for owner ds browse table (ds_browse.html)
class PlaceTableSerializer(serializers.ModelSerializer):
  dataset = DatasetSerializer()
  ds = serializers.SerializerMethodField()

  # user = serializers.SerializerMethodField()
  # def get_user(self, place):
  #   request = self.context.get('request', None)
  #   if request:
  #     return request.user

  def get_ds(self, place):
    cell_value = '<a class="pop-link pop-dataset" data-id='+str(place.dataset.id)+\
          ' data-toggle="popover" title="Dataset Profile" data-content=""'+\
          ' tabindex="0" rel="clickover">'+place.dataset.label+'</a>'
    return cell_value

  id = serializers.SerializerMethodField()
  def get_id(self, place):
    user = self.context['request'].user
    if place.dataset.public or place.dataset.core or place.dataset.owner == user:
      pid = '<a href="'+settings.URL_FRONT+'api/place/'+str(place.id)+'" target="_blank">'+str(place.id)+'</a>'
    else:
      pid = place.id
    return pid

  src_id = serializers.SerializerMethodField()
  def get_src_id(self, place):
    uri_base=place.dataset.uri_base
    if (place.dataset.public or place.dataset.core) and 'whg' not in uri_base:
      srcid = '<a href="'+uri_base+str(place.src_id)+'" target="_blank">'+str(place.src_id)+'</a>'
    else:
       srcid=place.src_id
    return srcid

  geo = serializers.SerializerMethodField()
  def get_geo(self, place):
    if place.geom_count > 0:
      gtype = place.geoms.all()[0].jsonb['type'].lower()
      fn="point" if 'point' in gtype else "polygon" if 'poly' in gtype else "linestring"
      return '<img src="/static/images/geo_'+fn+'.svg" width=12/>'
    else:
      return '&mdash;'

  chk = serializers.SerializerMethodField()
  def get_chk(self, place):
    return '<input type="checkbox" name="addme" class="table-chk" data-id="'+str(place.id)+'"/>'

  revwd = serializers.SerializerMethodField('rev_wd')
  def rev_wd(self, place):
    tasks_wd = place.dataset.tasks.filter(task_name='align_wdlocal', status='SUCCESS')
    if place.review_wd == 1:
      val = '<i class="fa fa-check-square-o"></i>'
    elif not place.hashits_wd:
        val = '<i>no hits</i>'
    elif place.review_wd == 0:
      val = '&#9744;'
    elif place.flag == True:
      val = 'altered'
    else:
      # direct link to deferred record
      val = '<a href="/datasets/'+str(place.dataset.id)+'/review/'+\
            tasks_wd[0].task_id+'/def?pid='+str(place.id)+'"><i>deferred</i></a>' \
            if len(tasks_wd) > 0 else '<i>deferred</i>'
    return val

  revwhg = serializers.SerializerMethodField('rev_whg')
  def rev_whg(self, place):
    tasks_whg = place.dataset.tasks.filter(task_name='align_idx', status='SUCCESS')
    if place.review_whg == 1:
      val = '<i class="fa fa-check-square-o"></i>'
    elif not place.hashits_whg:
      val = '<i>no hits</i>'
    elif place.review_whg == 0:
      val = '&#9744;'
    else:
      # direct link to deferred record
      val = '<a href="/datasets/' + str(place.dataset.id) + '/review/' + \
          tasks_whg[0].task_id + '/def?pid=' + str(place.id) + '"><i>deferred</i></a>' \
      if len(tasks_whg) > 0 else '<i>deferred</i>'
    return val

  revtgn = serializers.SerializerMethodField('rev_tgn')
  def rev_tgn(self, place):
    if place.review_tgn == 1:
      val = '<i class="fa fa-check-square-o"></i>'
    elif not place.hashits_tgn:
      val = '<i>no hits</i>'
    elif place.review_tgn == 0:
      val = '&#9744;'
    else:
      val = '<i>deferred</i>'
    return val

  class Meta:
    model = Place
    fields =  ('url','id', 'title', 'src_id',
                  'ccodes', 'geo', 'minmax',
                  'revwhg', 'revwd', 'revtgn',
                  'review_whg', 'review_wd', 'review_tgn'
                  ,'ds', 'dataset', 'dataset_id', 'chk'
                  )


""" used by: api.views.GeoJSONViewSet() """
class FeatureSerializer(GeoFeatureModelSerializer):
  geom = GeometrySerializerMethodField()
  def get_geom(self, obj):
    #print('obj',obj.__dict__)
    s=json.dumps(obj.jsonb)
    g1 = geojson.loads(s)
    g2 = shape(g1)
    djgeo = GEOSGeometry(g2.wkt)
    #print('geom', djgeo.geojson)
    return GEOSGeometry(g2.wkt)

  #
  title = serializers.SerializerMethodField('get_title')
  def get_title(self, obj):
    return obj.place.title

  class Meta:
    model = PlaceGeom
    geo_field = 'geom'
    id_field = False
    #fields = ('place_id','src_id','title')
    fields = ('place_id','src_id','title','geom')

""" uses: AreaViewset()"""
class AreaSerializer(serializers.HyperlinkedModelSerializer):
  class Meta:
    model = Area
    fields = ('title', 'type', 'geojson')


# TODO: what is this???
class SearchDatabaseSerializer(serializers.HyperlinkedModelSerializer):

  class Meta:
    model = Place
    #depth = 1
    fields = ('url','type','properties','geometry','names', 'types','links'
                  ,'related','whens', 'descriptions', 'depictions','minmax'
            )

""" TEST TEST TEST TEST TEST TEST
  used for nearby queries in SpatialAPIView() from /api/spatial? 
  returns Place records via PlaceGeom
"""
# class PlaceGeomSerializer(serializers.ModelSerializer):
#   names = serializers.SerializerMethodField('get_names')
#   def get_names(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.names.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     print('name data', data)
#     return data
#
#   types = serializers.SerializerMethodField('get_types')
#   def get_types(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.types.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     return data
#
#   links = serializers.SerializerMethodField('get_links')
#   def get_links(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.links.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     return data
#
#   whens = serializers.SerializerMethodField('get_whens')
#   def get_whens(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.whens.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     return data
#
#   related = serializers.SerializerMethodField('get_related')
#   def get_related(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.related.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     return data
#
#   descriptions = serializers.SerializerMethodField('get_descriptions')
#   def get_descriptions(self, placegeom):
#     full = json.loads(coreserializers.serialize(
#         "json", placegeom.place.descriptions.all()))
#     data = [n['fields']['jsonb'] for n in full]
#     return data
#
#   # custom fields for LPF transform
#   type = serializers.SerializerMethodField('get_type')
#   def get_type(self, placegeom):
#     return "Feature"
#
#   uri = serializers.SerializerMethodField('get_uri')
#   def get_uri(self, placegeom):
#     return "https://whgazetteer.org/api/place/" + str(placegeom.place.id)
#
#   properties = serializers.SerializerMethodField('get_properties')
#   def get_properties(self, placegeom):
#     props = {
#       "place_id":placegeom.place.id,
#       "dataset_label": placegeom.place.dataset.label,
#       "src_id": placegeom.place.src_id,
#       "title": placegeom.place.title,
#       "ccodes": placegeom.place.ccodes,
#       "fclasses": placegeom.place.fclasses,
#       "minmax": placegeom.place.minmax,
#       "timespans": placegeom.place.timespans
#     }
#     return props
#
#   geometry = serializers.SerializerMethodField('get_geometry')
#   def get_geometry(self, placegeom):
#     return placegeom.jsonb
#     # gcoll = {"type":"GeometryCollection","geometries":[]}
#     # geoms = [g.jsonb for g in placegeom.geoms.all()]
#     # for g in geoms:
#     #   gcoll["geometries"].append(g)
#     # return gcoll
#
#   class Meta:
#     model = PlaceGeom
#     fields = ('uri', 'type', 'properties', 'geometry', 'place'
#                 ,'names','types','links'
#                 ,'related','whens', 'descriptions'
#                 # 'depictions', 'minmax'
#             )

""" used by 
    SearchAPIView() from /api/db? 
    bbox queries in SpatialAPIView() from /api/spatial?
"""
class LPFSerializer(serializers.Serializer):
  names = PlaceNameSerializer(source="placename_set", many=True, read_only=True)
  types = PlaceTypeSerializer(many=True, read_only=True)
  links = PlaceLinkSerializer(many=True, read_only=True)
  related = PlaceRelatedSerializer(many=True, read_only=True)
  whens = PlaceWhenSerializer(many=True, read_only=True)
  descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
  depictions = PlaceDepictionSerializer(many=True, read_only=True)

  # custom fields for LPF transform
  type = serializers.SerializerMethodField('get_type')
  def get_type(self, place):
    return "Feature"

  uri = serializers.SerializerMethodField('get_uri')
  def get_uri(self, place):
    return "https://whgazetteer.org/api/place/" + str(place.id)

  properties = serializers.SerializerMethodField('get_properties')
  def get_properties(self,place):
    props = {
      "place_id":place.id,
      "dataset_label":place.dataset.label,
      "src_id":place.src_id,
      "title":place.title,
      "ccodes":place.ccodes,
      "fclasses":place.fclasses,
      "minmax":place.minmax,
      "timespans":place.timespans
    }
    return props

  geometry = serializers.SerializerMethodField('get_geometry')
  # {"type": "Point", "geowkt": "POINT(110.6 0.13333)", "coordinates": [110.6, 0.13333]}
  def get_geometry(self, place):
    gcoll = {"type":"GeometryCollection","geometries":[]}
    geoms = [g.jsonb for g in place.geoms.all()]
    for g in geoms:
      gcoll["geometries"].append(g)
    return gcoll

  class Meta:
    model = Place
    #depth = 1
    # fields = ('type','properties','geometry','names', 'types','links'
    #               ,'related','whens', 'descriptions', 'depictions', 'minmax'
    #         )
    fields = ('uri','type','properties','geometry','names', 'types','links'
                  ,'related','whens', 'descriptions', 'depictions', 'minmax'
            )
