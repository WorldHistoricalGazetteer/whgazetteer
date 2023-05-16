# api.views

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User, Group
from django.contrib.gis.geos import Polygon, Point
# from django.contrib.postgres import search
from django.contrib.gis.measure import D
from django.contrib.gis.db.models.functions import Distance
from django.db.models import Q
from django.http import JsonResponse, HttpResponse#, FileResponse
from django.shortcuts import get_object_or_404
from django.views.generic import View
#from django.views.decorators.csrf import csrf_exempt
from django_filters.rest_framework import DjangoFilterBackend
from elasticsearch7 import Elasticsearch
from rest_framework import filters
from rest_framework import generics
from rest_framework import permissions
#from rest_framework import status
from rest_framework import viewsets
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from rest_framework.reverse import reverse
from rest_framework.views import APIView
from accounts.permissions import IsOwnerOrReadOnly
from api.serializers import (UserSerializer, DatasetSerializer, PlaceSerializer,
                             PlaceTableSerializer, PlaceGeomSerializer, AreaSerializer,
                             FeatureSerializer, LPFSerializer, PlaceCompareSerializer)
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from datasets.tasks import get_bounds_filter
from places.models import Place, PlaceGeom
from search.views import getGeomCollection

class StandardResultsSetPagination(PageNumberPagination):
  page_size = 10
  page_size_query_param = 'page_size'
  max_page_size = 20000

#
# External API
# 
#
""" 
  /remote/
  search place index (always whg) parent records
  params: name, name_startswith, fclass, ccode, area, dataset, collection, pagesize, fuzzy
"""
class RemoteIndexAPIView(View):
  authentication_classes = [TokenAuthentication]
  permission_classes = [IsAuthenticated]

  def get(self, request):
    idx = 'whg'
    params = request.GET
    # print('RemoteSearchIndexView request params', params)

    name = params.get('name')
    name_startswith = params.get('name_startswith')
    fc = params.get('fclass', None)
    fclasses = [x.upper() for x in fc.split(',')] if fc else None
    cc = params.get('ccode', None)
    ccodes = [x.upper() for x in cc.split(',')] if cc else None
    area = params.get('area', None)
    dataset = params.get('dataset', None)
    collection = params.get('collection', None)
    pagesize = params.get('pagesize', None)
    offset = params.get('offset', None)
    fuzzy = params.get('fuzzy', None)

    print('offset', offset)

    if all(v is None for v in [name, name_startswith]):
      return HttpResponse(
        content='<h3>Query requires either name or name_startswith</h3>')
    else:
      q = {
        "size": pagesize if pagesize else 10,
        "from": offset if offset else 0,
        "query": {"bool": {
          "must": [
            {"exists": {"field": "whg_id"}},
            {"multi_match": {
              "query": name if name else name_startswith,
              "fields": ["title^3", "names.toponym", "searchy"],
            }}],
          "filter": []
        }}
      }
      if fc:
        q['query']['bool']['must'].append({"terms": {"fclasses": fclasses}})
      if dataset:
        q['query']['bool']['must'].append({"match": {"dataset": dataset}})
      if ccodes:
        q['query']['bool']['must'].append({"terms": {"ccodes": ccodes}})
      if area:
        a = get_object_or_404(Area, pk=area)
        bounds = {"id": [str(a.id)], "type": [a.type]}  # nec. b/c some are polygons, some are multipolygons
        # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
        q['query']['bool']["filter"].append(get_bounds_filter(bounds, 'whg'))
      if collection:
        c = get_object_or_404(Collection, pk=collection)
        ds_list = [d.label for d in c.datasets.all()]
        q['query']['bool']["filter"].append({"terms": {"dataset": ds_list}})
      if fuzzy and fuzzy.lower() == 'true':
        q['query']['bool']['must'][1]['multi_match']['fuzziness'] = 'AUTO'
        # up the count of results for fuzze search
        q['size'] = 20 if not pagesize else pagesize
        q['from'] = 20 if not offset else offset

      print('q', q)

      # run query
      index_set = collector(q, 'whg')
      print('index_set (collector() result)', index_set)

      # format hit items
      items = [collectionItem(i) for i in index_set['items']]
      # items = [collectionItem(s, 'place', None) for s in index_set['items']]

      # result object
      result = {'type': 'FeatureCollection',
                'count': index_set['count'],
                'offset': q['from'],
                'pagesize': q['size'],
                'features': items[:int(pagesize)] if pagesize else items}

    # to client
    return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  nearby and bbox spatial db queries
"""
class SpatialAPIView(generics.ListAPIView):
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  # search_fields = ['@title']

  def get(self, format=None, *args, **kwargs):
    params = self.request.query_params
    print('SpatialAPIView() params', params)

    qtype = params.get('type', None)
    lon = params.get('lon', None)
    lat = params.get('lat', None)
    dist = params.get('km', None)
    sw = params.get('sw', None)
    ne = params.get('ne', None)
    fc = params.get('fc', None)
    fclasses = list(set([x.upper() for x in ','.join(fc)])) if fc else None
    ds = params.get('dataset', None)
    coll = params.get('collection', None)
    pagesize = params.get('pagesize', None)
    # year = params.get('year', None)

    err_note = None

    if not qtype:
      return HttpResponse(content=b'<div style="margin:3rem; font-size: 1.2rem; border:1px solid gainsboro; padding:.5rem;">' +
        b'<p>Spatial query parameters must include either either <ul><li><b>?type=nearby</b> (with <b>&lng=</b> and <b>&lat=</b>) <i>or</i></li>' +
        b'<li><b>?type=bbox</b> (with <b>&sw=</b> and <b>&ne+</b>).</p></div')

    # uses PlaceGeom records and PlaceGeomSerializer
    if qtype == 'nearby':
      if not all(v for v in [lon, lat, dist]):
        return HttpResponse(content=b'<div style="margin:3rem; font-size: 1.2rem; border:1px solid gainsboro; padding:.5rem;">' +
          b'<p>A <b>nearby</b> spatial query requires <b>lng</b>, <b>lat</b>, and <b>km</b> parameters</p></div>')
      pnt = Point(float(lon), float(lat), srid=4326)
      # PlaceGeom records, only points
      qs = PlaceGeom.objects.extra(
          where=["geometrytype(geom) LIKE 'POINT'"]). \
            annotate(distance=Distance('geom', pnt)). \
              filter(geom__distance_lte=(pnt, D(km=dist))).order_by('distance')
      # filter on params
      if coll:
        collids = Collection.objects.get(id=coll).places.all().values_list('id',flat=True)
        qs = qs.filter(place_id__in=collids)
      qs = qs.filter(place__dataset=ds) if ds else qs
      qs = qs.filter(place__fclasses__overlap=fclasses) if fclasses else qs
      # qs = qs.filter(place__minmax__0__lte=year, place__minmax__1__gte=year) if year else qs

      msg = "nearby query (lon, lat): "+str(pnt.coords)+' w/'+str(dist)+'km buffer'
      print(msg)

    # uses Place records and LPFSerializer
    elif qtype == 'bbox':
      if not all(v for v in [sw, ne]):
        return HttpResponse(content=b'<div style="margin:3rem; font-size: 1.2rem; border:1px solid gainsboro; padding:.5rem;">' +
          b'<p>A <b>bbox</b> spatial query requires both <b>sw</b> and <b>ne</b> parameters</p></div>')
      else:
        qs = Place.objects.filter(dataset__public=True, geoms__jsonb__type='Point')
        bb = [float(sw.split(',')[0]), float(sw.split(',')[1]),
                  float(ne.split(',')[0]), float(ne.split(',')[1])]
        bbox = Polygon.from_bbox(bb) # [xmin, ymin, xmax, ymax]
        placeids = PlaceGeom.objects.filter(geom__within=bbox).values_list('place_id')
        qs = qs.filter(id__in=placeids)
        # filter on params
        if coll:
          collids = Collection.objects.get(
              id=coll).places.all().values_list('id', flat=True)
          qs = qs.filter(id__in=collids)
        qs = qs.filter(dataset=ds) if ds else qs
        qs = qs.filter(fclasses__overlap=fclasses) if fclasses else qs

        msg="bbox query (sw, ne): "+str(bbox)
        print(msg)

    filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]
    serial = LPFSerializer if qtype == 'bbox' else PlaceGeomSerializer
    serializer = serial(filtered, many=True, context={'request': self.request})
    serialized_data = serializer.data
    result = {
              "count": qs.count(),
              "pagesize": filtered.count(),
              "parameters": params,
              "errors": err_note,
              "type": "FeatureCollection",
              "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
              "features": serialized_data
              }
    #print('place result',result)
    return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})

"""
  makeGeom(); called by collectionItem()
  format index locations as geojson
"""
def makeGeom(geom):
  #print('geom',geom)
  # TODO: account for non-point
  if len(geom) > 1:
    geomobj = {"type":"GeometryCollection", "geometries": []}  
    for g in geom:
      geomobj['geometries'].append(g['location'])
        #{"type":g['location']['type'],
         #"coordinates":g['location']['coordinates']
  elif len(geom) == 1:
    geomobj=geom[0]['location']
  else:
    geomobj=None
  return geomobj

"""
  collectionItem(); called by collector();
  formats api search hits 
  TODO: rename
"""
def collectionItem(i):
  print('collectionItem i',i)
  _id = i['_id']
  score = i['score']
  # serialize as geojson
  i = i['hit']
  item = {
    "type":"Feature",
    "score": score,
    "properties": {
      "title":i['title'],
      "index_id":_id,
      "index_role":i['relation']['name'],
      "place_id":i['place_id'],
      "child_place_ids":[int(c) for c in i['children']],
      "dataset":i['dataset'],
      "fclasses": [c for c in i['fclasses']],
      "placetypes":[t['sourceLabel'] for t in i['types']],
      "variants":[n for n in i['suggest']['input'] if n != i['title']],
      'links':i['links'],
      "timespans":i['timespans'],
      "minmax":i['minmax'] if 'minmax' in i.keys() else [],
      "ccodes":i['ccodes']
    },
    "geometry": makeGeom(i['geoms'])
  }
  return item

"""
  collector(); called by IndexAPIView(), RemoteIndexAPIView()
  execute es.search, return results post-processed by suggestionItem()
"""
def collector(q, idx):
  es = settings.ES_CONN
  items = []

  # TODO: trap errors
  res = es.search(index=idx, body=q)
  # print('res', res)
  hits = res['hits']['hits']
  count = res['hits']['total']['value']
  if len(hits) > 0:
    for h in hits:
      # print('h in collector()', h)
      items.append(
        {"_id": h['_id'],
         "linkcount": len(h['_source']['links']),
         "childcount": len(h['_source']['children']),
         "score": h['_score'],
         "hit": h['_source'],
         }
      )
  result = {"count": count,
            "items": sorted(items, key=lambda x: x['score'], reverse=True)}
  return result

"""
  bundler();  called by IndexAPIView, case api/index?whgid=
  execute es.search, return post-processed results 
"""
def bundler(q, whgid, idx):
  es = settings.ES_CONN
  print('bundler es connector', es)
  res = es.search(index=idx, body=q)
  hits = res['hits']['hits']
  bundle = []
  if len(hits) > 0:
    for h in hits:
      bundle.append(
        {"_id": h['_id'],
         "linkcount":len(h['_source']['links']),
         "childcount":len(h['_source']['children']),
         "score": h['_score'],
         "hit": h['_source'],
        }
      )
  stuff = [ collectionItem(i) for i in bundle]
  return stuff

""" 
  /api/index?
  search place index (always whg) parent records
  based on search.views.SearchView(View)
"""
class IndexAPIView(View):
  # @staticmethod
  def get(self, request):
    params=request.GET
    print('IndexAPIView request params',params)
    """
      args in params: whgid, pid, name, name_startswith, fclass, dataset, ccode, year, area
    """
    whgid = request.GET.get('whgid')
    pid = request.GET.get('pid')
    name = request.GET.get('name')
    name_startswith = request.GET.get('name_startswith')
    fc = params.get('fclass',None)
    fclasses=[x.upper() for x in fc.split(',')] if fc else None
    dataset = request.GET.get('dataset')
    cc = request.GET.get('ccode')
    ccodes=[x.upper() for x in cc.split(',')] if cc else None
    year = request.GET.get('year')
    pagesize = params.get('pagesize', None)
    area = request.GET.get('area')
    idx = 'whg'
    
    if all(v is None for v in [name, name_startswith, whgid,pid]):
      # TODO: format better
      return HttpResponse(content='<h3>Query requires either name, pid, or whgid</h3>'+'<p><a href="/usingapi/">API instructions</a>')
    else:
      if whgid and whgid !='':
        print('fetching whg_id', whgid)
        q = {"query":{"bool":{"should": [
            {"parent_id": {"type": "child","id":whgid}},
            {"match":{"_id":whgid}}
        ]}}}
        bundle = bundler(q, whgid, idx)
        print('bundler q', q)
        result={"index_id":whgid,
                "note":str(len(bundle)) + " records asserted as skos:closeMatch",
                "type":"FeatureCollection",
                "features":[b for b in bundle]}
      else:
        # if not name_startswith:
        q = {
          "size": 100,
          "query": { "bool": {
            "must": [
              {"exists": {"field": "whg_id"}},
              {"multi_match": {
                "query": name if name else name_startswith,
                "fields": ["title^3", "names.toponym", "searchy"],
              }}
            ]
          }}
        }
        if fc:
          q['query']['bool']['must'].append({"terms": {"fclasses": fclasses}})
        if dataset:
          q['query']['bool']['must'].append({"match": {"dataset": dataset}})
        if ccodes:
          q['query']['bool']['must'].append({"terms": {"ccodes": ccodes}})
        if year:
          q['query']['bool']['must'].append({"term":{"timespans":{"value": year}}})
        #if area:
          #TODO: 
        if area:
          a = get_object_or_404(Area,pk=area)
          bounds={"id":[str(a.id)],"type":[a.type]} # nec. b/c some are polygons, some are multipolygons
          q['query']['bool']["filter"]=get_bounds_filter(bounds,'whg')

        print('the api query was:',q)
        
        # run query
        # TODO; rename from collection to avoid confusing with Collection
        collection = collector(q, 'whg')
        # collection = collector(q,'place','whg')
        # format hits
        print('collection in IndexAPIView()', collection)
        # print('collection["items"] in IndexAPIView()', collection['items'])
        collection = [collectionItem(i) for i in collection['items']]

        # result object
        result = {'type':'FeatureCollection',
                  'count': len(collection),
                  'pagesize': pagesize,
                  'features':collection[:int(pagesize)] if pagesize else collection}

    # to client
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

""" 
  /api/db?
  SearchAPIView()
  return lpf results from database search 
"""
class SearchAPIView(generics.ListAPIView):
  renderer_classes = [JSONRenderer]
  filter_backends = [filters.SearchFilter]
  search_fields = ['@title']

  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params
    print('SearchAPIView() params', params)

    id_ = params.get('id',None)
    name = params.get('name',None)
    name_contains = params.get('name_contains',None)
    cc = map(str.upper, params.get('ccode').split(',')) if params.get('ccode') else None
    ds = params.get('dataset',None)
    fc = params.get('fc',None)
    fclasses=list(set([x.upper() for x in ','.join(fc)])) if fc else None
    year = params.get('year',None)
    pagesize = params.get('pagesize',None)
    err_note = None
    context = params.get('context',None)
    # params
    print({"id_":id_, "fclasses":fclasses})
    
    qs = Place.objects.filter(Q(dataset__public=True) | Q(dataset__core=True))

    if all(v is None for v in [name,name_contains,id_]):
      # TODO: return a template with API instructions
      return HttpResponse(content=b'<h3>Needs either a "name", a "name_contains", or "id" parameter at \
          minimum <br/>(e.g. ?name=myplacename or ?name_contains=astring or ?id=integer)</h3>')
    else:
      if id_:
        qs=qs.filter(id=id_)
        err_note = 'id given, other parameters ignored' if len(params.keys())>1 else None
        print('qs', qs)
      else:
        qs = qs.filter(minmax__0__lte=year,minmax__1__gte=year) if year else qs
        qs = qs.filter(fclasses__overlap=fclasses) if fc else qs
        
        if name_contains:
          qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
          #qs = qs.filter(title__istartswith=name)
          qs = qs.filter(names__jsonb__toponym__icontains=name)
  
        qs = qs.filter(dataset=ds) if ds else qs
        qs = qs.filter(ccodes__overlap=cc) if cc else qs
        
      filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]

      #serial = LPFSerializer if context else SearchDatabaseSerializer
      serial = LPFSerializer
      serializer = serial(filtered, many=True, context={'request': self.request})
      
      serialized_data = serializer.data
      result = {"count":qs.count(),
                "pagesize": len(filtered),
                "parameters": params,
                "note": err_note,
                "type": "FeatureCollection",
                "features":serialized_data
                }
      #print('place result',result)
      return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})


""" *** """
""" TODO: the next two attempt the same and are WAY TOO SLOW """
""" 
    api/places/<str:dslabel>/[?q={string}]
    Paged list of places in dataset. 
"""
class PlaceAPIView(generics.ListAPIView):
  serializer_class = PlaceSerializer
  pagination_class = StandardResultsSetPagination

  def get_queryset(self, format=None, *args, **kwargs):
    print('kwargs',self.kwargs)
    print('self.request.GET',self.request.GET)
    dslabel=self.kwargs['dslabel']
    qs = Place.objects.all().filter(dataset=dslabel).order_by('title')
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(title__icontains=query)
    return qs

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]

  
""" 
    api/dataset/<str:dslabel>/lpf/
    all places in a dataset, for download
"""
class DownloadDatasetAPIView(generics.ListAPIView):
  """  Dataset as LPF FeatureCollection  """
  #serializer_class = PlaceSerializer
  #pagination_class = StandardResultsSetPagination

  def get(self, format=None):
    print('self.request.GET',self.request.GET)
    dslabel=self.request.GET.get('dataset')
    ds=get_object_or_404(Dataset,label=dslabel)
    features = []
    qs = ds.places.all()
    for p in qs:
      rec = {"type":"Feature",
             "properties":{"id":p.id,"src_id":p.src_id,"title":p.title,"ccodes":p.ccodes},
             "geometry":{"type":"GeometryCollection",
                         "features":[g.jsonb for g in p.geoms.all()]},
             "names": [n.jsonb for n in p.names.all()],
             "types": [t.jsonb for t in p.types.all()],
             "links": [l.jsonb for l in p.links.all()],
             "whens": [w.jsonb for w in p.whens.all()],
      }
      #print('rec',rec)
      features.append(rec)
    
    result={"type":"FeatureCollection", "features": features}
    print('result',result)
    return JsonResponse(result, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

  #permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
 
"""
  /api/datasets? > query public datasets by id, label, term
"""
class DatasetAPIView(LoginRequiredMixin, generics.ListAPIView):
  """    List public datasets    """
  serializer_class = DatasetSerializer
  renderer_classes = [JSONRenderer]

  def get_queryset(self, format=None, *args, **kwargs):
    params=self.request.query_params
    print('api/datasets params',params)
    id_ = params.get('id', None)
    dslabel = params.get('label', None)
    query = params.get('q', None)
    
    qs = Dataset.objects.filter(Q(public=True) | Q(core=True)).order_by('label')
    
    if id_:
      qs = qs.filter(id = id_)
    elif dslabel:
      qs = qs.filter(label = dslabel)
    elif query:
      qs = qs.filter(Q(description__icontains=query) | Q(title__icontains=query))

    print('qs',qs)
    result = {
              "count":qs.count(),
              "parameters": params,
              #"features":serialized_data
              "features":qs
              }
    print('ds result', result,type(result))
    return qs

"""
  /api/area_features
"""
# geojson feature for api
class AreaFeaturesView(generics.ListAPIView):
  #@staticmethod
  
  def get(self, format=None, *args, **kwargs):
    params=self.request.query_params  
    user = self.request.user
    print('params', params)
    print('api/areas request',self.request)
    
    id_ = params.get('id', None)
    query = params.get('q', None)
    filter = params.get('filter', None)
    
    areas = []
    qs = Area.objects.all().filter((Q(type='predefined'))).values('id','title','type','description','geojson')
    
    # filter for parameters
    if id_:
      qs=qs.filter(id=id_)
    if query:
      qs = qs.filter(title__icontains=query)
    if filter and filter == 'un':
      qs = qs.filter(description="UN Statistical Division Sub-Region")

      
    for a in qs:
      feat = {
        "type":"Feature",
        "properties":{"id":a['id'],"title":a['title'],"type":a['type'],"description":a['description']},
        "geometry":a['geojson']
      }
      areas.append(feat)
      
    return JsonResponse(areas, safe=False)  
  
class UserList(generics.ListAPIView):
  queryset = User.objects.all()
  serializer_class = UserSerializer

class UserDetail(generics.RetrieveAPIView):
  queryset = User.objects.all()
  serializer_class = UserSerializer

"""
  API 'home page' (not implemented)
"""
@api_view(['GET'])
def api_root(request, format=None):
  return Response({
    # 'datasets': reverse('dataset-list', request=request, format=format)
    'datasets': reverse('api:ds-list', request=request, format=format)
  })


class PrettyJsonRenderer(JSONRenderer):    
  def get_indent(self, accepted_media_type, renderer_context):
    return 2
  
#

# IN USE May 2020

#
"""
    place/<int:pk>/
    uses: ds_browse.html; place_collection_browse.html
    "published record by place_id"
"""
class PlaceDetailAPIView(generics.RetrieveAPIView):
  """  returns single serialized database place record by id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer
  renderer_classes = [PrettyJsonRenderer]

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]

"""
    place_compare/<int:pk>/
    uses: ds_update()
    "partial database record by place_id for update comparisons"
"""
class PlaceCompareAPIView(generics.RetrieveAPIView):
  """  returns single serialized database place record by id  """
  queryset = Place.objects.all()
  serializer_class = PlaceCompareSerializer
  renderer_classes = [PrettyJsonRenderer]

  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]

"""
    place/<str:dslabel>/<str:src_id>/
    published record by dataset label and src_id
"""
class PlaceDetailSourceAPIView(generics.RetrieveAPIView):
  """  single database place record by src_id  """
  queryset = Place.objects.all()
  serializer_class = PlaceSerializer
  renderer_classes = [PrettyJsonRenderer]

  lookup_field = 'src_id'
  permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
  authentication_classes = [SessionAuthentication]


""" 
    /api/geoms?ds={{ ds.label }}} 
    /api/geoms?coll={{ coll.id }}} 
    in ds_browse and ds_places for all geoms if < 15k
    TODO: this needs refactor (make collection.geometries @property?)
"""
class GeomViewSet(viewsets.ModelViewSet):
  queryset = PlaceGeom.objects.all()
  serializer_class = PlaceGeomSerializer
  #pagination_class = None
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

  def get_queryset(self):
    # PlaceGeom objects do not have dataset id or label :^(
    if 'ds' in self.request.GET:
      dslabel = self.request.GET.get('ds')
      dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
      qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
    elif 'coll' in self.request.GET:
      cid = self.request.GET.get('coll')
      coll = Collection.objects.get(id=cid)
      collPlaceIds = [p.id for p in coll.places.all()]
      # leaves out polygons and linestrings
      qs = PlaceGeom.objects.filter(
        place_id__in=collPlaceIds,
        jsonb__type__icontains='Point')
    return qs

""" 
    /api/geojson/{{ ds.id }}
"""
#class GeoJSONViewSet(viewsets.ModelViewSet):
class GeoJSONAPIView(generics.ListAPIView):
  # use: api/geojson
  #queryset = PlaceGeom.objects.all()
  #serializer_class = GeoJsonSerializer
  serializer_class = FeatureSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
  
  def get_queryset(self, format=None, *args, **kwargs):
    print('GeoJSONViewSet request.GET',self.request.GET)
    print('GeoJSONViewSet args, kwargs',args, kwargs)
    if 'id' in self.request.GET:
      dsid = self.request.GET.get('id')
      dslabel = get_object_or_404(Dataset, pk=dsid).label
      dsPlaceIds = Place.objects.values('id').filter(dataset = dslabel)
      qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
    elif 'coll' in self.request.GET:
      cid = self.request.GET.get('coll')
      coll = Collection.objects.get(id=cid)
      collPlaceIds = [p.id for p in coll.places.all()]
      qs = PlaceGeom.objects.filter(place_id__in=collPlaceIds,jsonb__type='Point')    
    #print('qs',qs)
    return qs

"""
    populates drf table in ds_browse.html
"""
class PlaceTableViewSet(viewsets.ModelViewSet):
  # queryset = Place.objects.all()
  serializer_class = PlaceTableSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly)

  """
    q: query string
    ds: dataset
  """
  def get_queryset(self):
    ds = get_object_or_404(Dataset, label=self.request.GET.get('ds'))
    # qs = ds.places.all().order_by('place_id')
    qs = ds.places.all().order_by('id')
    # qs = ds.places.all()
    query = self.request.GET.get('q')
    if query is not None:
      qs = qs.filter(title__istartswith=query)
    return qs

  def get_permissions(self):
    """
    Instantiates and returns the list of permissions that this view requires.
    """
    if self.action in ['list','retrieve']:
      print(self.action)
      permission_classes = [permissions.AllowAny]
    else:
      permission_classes = [permissions.IsAdminUser]
    return [permission() for permission in permission_classes]

"""
    populates drf table in collection.collection_places.html
"""
class PlaceTableCollViewSet(viewsets.ModelViewSet):
  # queryset = Place.objects.all()
  serializer_class = PlaceTableSerializer
  permission_classes = (permissions.IsAuthenticatedOrReadOnly)

  """
    q: query string
    coll: collection
  """
  def get_queryset(self):
    print('user', self.request.user)
    coll = get_object_or_404(Collection, id=self.request.GET.get('id'))
    qs = coll.places_all.order_by('title')
    query = self.request.GET.get('q')
    # print('queryset', qs)
    if query is not None:
      qs = qs.filter(title__istartswith=query)
    return qs

  def get_permissions(self):
    """
    Instantiates and returns the list of permissions that this view requires.
    """
    if self.action in ['list','retrieve']:
      print(self.action)
      permission_classes = [permissions.AllowAny]
    else:
      permission_classes = [permissions.IsAdminUser]
    return [permission() for permission in permission_classes]


"""
  areas/

"""
# simple objects for dropdown
class AreaListView(View):
  @staticmethod
  def get(request):
    print('area_list() request.user',request.user, type(request.user))
    print('area_list() request.user',str(request.user))
    userstr = str(request.user)
    if userstr == 'AnonymousUser':
      qs = Area.objects.all().filter(Q(type__in=('predefined','country'))).values('id','title','type')
    else:
      user = request.user
      qs = Area.objects.all().filter(Q(type__in=('predefined','country'))| Q(owner=user)).values('id','title','type')
    area_list = []
    for a in qs:
      area = {"id":a['id'],"title":a['title'],"type":a['type']}
      area_list.append(area)
      
    return JsonResponse(area_list, safe=False)
  
"""
  areas/

"""
# simple objects for dropdown
class AreaListAllView(View):
  @staticmethod
  def get(request):
    print('area_list() request',request)
    user = request.user
    area_list = []
    #qs = Area.objects.all().filter(Q(type='predefined')| Q(owner=request.user)).values('id','title','type')
    qs = Area.objects.all().filter(Q(type__in=('predefined','country'))| Q(owner=request.user)).values('id','title','type')
    for a in qs:
      area = {"id":a['id'],"title":a['title'],"type":a['type']}
      area_list.append(area)
      
    return JsonResponse(area_list, safe=False)

  
"""
    area/<int:pk>/
    in dataset.html#addtask
"""
class AreaViewSet(viewsets.ModelViewSet):
  queryset = Area.objects.all().order_by('title')
  serializer_class = AreaSerializer

"""
    regions/
    in dataset.html#addtask
"""
class RegionViewSet(View):
  queryset = Area.objects.filter(
      description='UN Statistical Division Sub-Region').order_by('title')
  serializer_class = AreaSerializer

