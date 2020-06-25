# celery reconciliation tasks [align_tgn(), align_whg()] and related functions
from __future__ import absolute_import, unicode_literals
from celery.decorators import task
from django.conf import settings
from django.shortcuts import get_object_or_404, render, redirect
from django.contrib.gis.geos import Polygon, Point, LineString
import logging
##
import sys, os, re, json, codecs, datetime, time, csv, random
from copy import deepcopy
from pprint import pprint
from areas.models import Area
from es.es_utils import makeDoc
from datasets.models import Dataset, Hit
from datasets.static.regions import regions as region_hash
from datasets.static.hashes.parents import ccodes
from datasets.utils import *
from places.models import Place, PlaceLink
##
#import shapely.geometry as sgeo
from geopy import distance
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
##

# test task for uptimerobot
@task(name="testAdd")
def testAdd(n1,n2):
  sum = n1+n2
  return sum

def types(hit):
  type_array = []
  for t in hit["_source"]['types']:
    if bool(t['placetype'] != None):
      type_array.append(t['placetype']+', '+str(t['display']))
  return type_array

def names(hit):
  name_array = []
  for t in hit["_source"]['names']:
    if bool(t['name'] != None):
      name_array.append(t['name']+', '+str(t['display']))
  return name_array

def toGeoJSON(hit):
  src = hit['_source']
  feat = {"type": "Feature", "geometry": src['location'],
            "aatid": hit['_id'], "tgnid": src['tgnid'],
            "properties": {"title": src['title'], "parents": src['parents'], "names": names(hit), "types": types(hit) } }
  return feat

def reverse(coords):
  fubar = [coords[1],coords[0]]
  return fubar

def parseWhen(when):
  print('when to parse',when)
  timespan = 'parse me now'
  return timespan

def maxID(es,idx):
  q={"query": {"bool": {"must" : {"match_all" : {}} }},
       "sort": [{"whg_id": {"order": "desc"}}],
       "size": 1  
       }
  try:
    res = es.search(index=idx, body=q)
    #maxy = int(res['hits']['hits'][0]['_id'])
    maxy = int(res['hits']['hits'][0]['_source']['whg_id'])
  except:
      maxy = 12345677
  return maxy 

def parseDateTime(string):
  year = re.search("(\d{4})-",string).group(1)
  if string[0] == '-':
    year = year + ' BCE' 
  return year.lstrip('0')

def ccDecode(codes):
  countries=[]
  print('codes in ccDecode',codes)
  for c in codes:
    countries.append(ccodes[0][c]['gnlabel'])
  return countries
  
# normalize hits json from any authority
# 
def normalize(h,auth):
  if auth.startswith('whg'):
    rec = HitRecord(h['place_id'], h['dataset'], h['src_id'], h['title'])
    print('normalize(): hit',h)
    print('normalize(): HitRecord',rec)
    rec.whg_id = h['whg_id'] if 'whg_id' in h.keys() else h['relation']['parent']
    # add elements if non-empty in index record
    rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
    # TODO: grungy hack b/c index has both src_label and sourceLabel
    key = 'src_label' if 'src_label' in h['types'][0] else 'sourceLabel'      
    rec.types = [t['label']+' ('+t[key]  +')' if t['label']!=None else t[key] \
                for t in h['types']] if len(h['types']) > 0 else []
    # TODO: rewrite ccDecode to handle all conditions coming from index
    # ccodes might be [] or [''] or ['ZZ', ...]
    rec.ccodes = ccDecode(h['ccodes']) if ('ccodes' in h.keys() and (len(h['ccodes']) > 0 and h['ccodes'][0] !='')) else []
    rec.parents = ['partOf: '+r.label+' ('+parseWhen(r['when']['timespans'])+')' for r in h['relations']] \
                if 'relations' in h.keys() and len(h['relations']) > 0 else []
    rec.descriptions = h['descriptions'] if len(h['descriptions']) > 0 else []
    
    rec.geoms = [{
      "type":h['geoms'][0]['location']['type'],
      "coordinates":h['geoms'][0]['location']['coordinates'],
      "id":h['place_id'], \
      "ds":"whg"}] \
      if len(h['geoms'])>0 else []   
    
    rec.minmax = dict(sorted(h['minmax'].items(),reverse=True)) if len(h['minmax']) > 0 else []
    # TODO: deal with whens
    #rec.whens = [parseWhen(t) for t in h['timespans']] \
                #if len(h['timespans']) > 0 else []
    rec.links = [l['type']+': '+l['identifier'] for l in h['links']] \
                if len(h['links']) > 0 else []
  
  elif auth == 'wd':
    try:
      # locations and links may be multiple, comma-delimited
      locs=[]; links = []
      if 'locations' in h.keys():
        for l in h['locations']['value'].split(', '):
          loc = parse_wkt(l)
          loc["id"]=h['place']['value'][31:]
          loc['ds']='wd'
          locs.append(loc)
      if 'links' in h.keys():
        for l in h['links']:
          links.append('closeMatch: '+l)
      #  place_id, dataset, src_id, title
      rec = HitRecord(-1, 'wd', h['place']['value'][31:], h['placeLabel']['value'])
      rec.variants = []
      rec.types = h['types']['value'] if 'types' in h.keys() else []
      rec.ccodes = [h['countryLabel']['value']]
      rec.parents =h['parents']['value'] if 'parents' in h.keys() else []
      rec.geoms = locs if len(locs)>0 else []
      rec.links = links if len(links)>0 else []
      rec.minmax = []
      rec.inception = parseDateTime(h['inception']['value']) if 'inception' in h.keys() else ''
      # {'datatype': 'http://www.w3.org/2001/XMLSchema#dateTime', 'type': 'literal', 'value': '1858-11-22T00:00:00Z'}
    except:
      print("normalize(wd) error:", h['place']['value'][31:], sys.exc_info())    
  elif auth == 'tgn':
    # h=hit['_source']; ['tgnid', 'title', 'names', 'suggest', 'types', 'parents', 'note', 'location']
    # whg_id, place_id, dataset, src_id, title
    # h['location'] = {'type': 'point', 'coordinates': [105.041, 26.398]}
    #try:
      #rec = HitRecord(-1, -1, 'tgn', h['tgnid'], h['title'])
    print('hit (h) in normalize()',h)
    rec = HitRecord(-1, 'tgn', h['tgnid'], h['title'])
    print('normalize rec, tgn',rec)
    rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
    rec.types = [(t['placetype'] if 'placetype' in t and t['placetype'] != None else 'unspecified') + \
                (' ('+t['id']  +')' if 'id' in t and t['id'] != None else '') for t in h['types']] \
                if len(h['types']) > 0 else []
    rec.ccodes = []
    rec.parents = ' > '.join(h['parents']) if len(h['parents']) > 0 else []
    rec.descriptions = [h['note']] if h['note'] != None else []
    if 'location' in h.keys():
      rec.geoms = [{
        "type":"Point",
        "coordinates":h['location']['coordinates'],
        "id":h['tgnid'], \
        "ds":"tgn"}]
    else: 
      rec.geoms=[]
    #rec.geoms = [h['location']] if h['location'] != None else []
    rec.minmax = []
    #rec.whens =[]
    rec.links = []
    print(rec)
    #except:
      #print("normalize(tgn) error:", h['tgnid'], sys.exc_info())
  return rec.toJSON()

# user-supplied spatial bounds
def get_bounds_filter(bounds,idx):
  print('bounds in get_bounds_filter()',bounds)
  id = bounds['id'][0]
  areatype = bounds['type'][0]
  area = Area.objects.get(id = id)
  # TODO: area always a hull polygon now; test MultiPolygon
  # NOTE: 'whg' is generic, references current index for WHG, vs. 'tgn' for example
  geofield = "geoms.location" if idx == 'whg' else "location"
  filter = { "geo_shape": {
    geofield: {
        "shape": {
          "type": "polygon" if areatype == 'userarea' else "multipolygon",
          "coordinates": area.geojson['coordinates']
        },
        "relation": "intersects" if idx=='whg' else 'within' # within | intersects | contains
      }
  }} 
  return filter


# from align_wd (wikidata)
# b: wikidata binding; passnum: 1 or 2; ds (obj); pid,srcid,title > whg Place instance
def writeHit(b,passnum,ds,pid,srcid,title):
  # gather any links
  authkeys=['tgnids','gnids','viafids','locids']
  linkkeys = list(set(list(b.keys())).intersection(authkeys))
  linklist = []
  # TODO: yuk
  for l in linkkeys:
    for v in b[l]['value'].split(', '):
      if v != '':
        linklist.append(l[:-3]+':'+v)
  b['links'] = linklist
  print('writeHit() linklist',linklist) # ['viaf:124330404', 'tgn:1003084', 'tgn:7004799', 'gn:6553047', 'loc:n80046295']
  if b['placeLabel']['value'] != b['place']['value'][31:]: # ??
    from datasets.models import Hit
    new = Hit(
      authority = 'wd',
      authrecord_id = b['place']['value'][31:],
      dataset = ds,
      place_id = get_object_or_404(Place, id=pid),
      #task_id = 'wd_20200517',
      task_id = align_wd.request.id,
      query_pass = passnum,
      # consistent json for review display
      json = normalize(b,'wd'),
      src_id = srcid,
      #score = hit['_score'],
      geom = parse_wkt(b['locations']['value']) if 'locations' in b.keys() else [],
      reviewed = False,
    )
    new.save()          
    hit = str(pid)+'\t'+ \
          title+'\t'+ \
          b['placeLabel']['value']+'\t'+ \
          b['place']['value']+'\t'
    print('wrote hit: '+hit + '\n')

# 
@task(name="align_wd")
def align_wd(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  #bounds = kwargs['bounds']
  
  from SPARQLWrapper import SPARQLWrapper, JSON
  import sys, os, re, json, codecs, time, datetime, geojson
  from datasets.align_utils import classy, roundy, fixName
  from shapely.geometry import shape
  
  #endpoint = "http://dbpedia.org/sparql"
  endpoint = "https://query.wikidata.org/sparql"
  sparql = SPARQLWrapper(endpoint)
  
  start = time.time()
  timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M"); print(timestamp)

  hit_parade = {"summary": {}}
  
  #def toWKT(coords):
    #wkt = 'POINT('+str(coords[0])+' '+str(coords[1])+')'
    #return wkt
  
  [count,count_skipped] = [0,0]
  global count_hit, count_nohit, total_hits, count_p1, count_p2
  [count_hit, count_nohit, total_hits, count_p1, count_p2] = [0,0,0,0,0]
  
  #for place in ds.places.filter(flag=True):
  for place in ds.places.all(): #.order_by('id')[:10]: #.filter(id__lt=224265):
    #place=get_object_or_404(Place, id=6369031) # Aachen
    #place=get_object_or_404(Place, id=6369589) # Abrantes
    #place=get_object_or_404(Place, id=6453302) # Tourlaville
    count +=1
    place_id = place.id
    src_id = place.src_id
    title = fixName(place.title)
    qobj = {"place_id":place_id,"src_id":place.src_id,"title":fixName(place.title)}
    [variants,geoms,types,ccodes,parents]=[[],[],[],[],[]]

    # ccodes (2-letter i  so codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = ccodes

    # types (Getty AAT identifiers)
    for t in place.types.all():
      try:
        id = t.jsonb['identifier']
        if id !=None:
          #types.append(int(id[4:]) if id.startswith('aat:') else '')      
          types.append(id[4:] if id.startswith('aat:') else int(id))      
      except:
        print('int error in types',t.jsonb['identifier'])
    qobj['placetypes'] = types

    # names
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = variants
    qobj['variants'].append(title)

    # parents
    # TODO: other relations
    for rel in place.related.all():
      if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
        parents.append(rel.jsonb['label'])
    qobj['parents'] = parents

    # geoms
    if len(place.geoms.all()) > 0:
      g_types = [g.jsonb['type'] for g in place.geoms.all()]
      g_list = [g.jsonb for g in place.geoms.all()]
      if len(set(g_types)) > 1 and len(set(g_types) & set(['Polygon','MultiPolygon'])) >0:
        g_list=list(filter(lambda d: d['type'] in ['Polygon','MultiPolygon'], g_list))
      # make everything a simple polygon hull for spatial filter
      # TODO: hully() assumes list is all one type
      qobj['geom'] = hully(g_list)

    print('qobj before modifications for Wikidata',qobj)
    # wikidata sparql needs this form for lists
    variants = ' '.join(['"'+n+'"' for n in qobj['variants']])
    
    # countries, placetypes if they're there
    countries = ', '.join([c for c in getQ(qobj['countries'],'ccodes')]) \
      if len(qobj['countries'])>0 and qobj['countries'] != [''] else ''
    placetype = getQ(qobj['placetypes'],'types')[0] if len(qobj['placetypes'])>0 else ''
    print('variants,countries,placetype',variants,countries,placetype)
    
    # belongs?          OPTIONAL {?place wdt:P31 ?placeType .}  
    # TODO admin parent P131, retrieve wiki article name, country P17, ??
    #q='''SELECT ?place ?placeLabel ?countryLabel ?inception ?tgnids ?gnids ?viafids ?locids
    q='''SELECT ?place ?placeLabel ?countryLabel ?inception 
        (group_concat(distinct ?parentLabel; SEPARATOR=", ") as ?parents)
        (group_concat(distinct ?placeTypeLabel; SEPARATOR=", ") as ?types)
        (group_concat(distinct ?location; SEPARATOR=", ") as ?locations)
        (group_concat(distinct ?tgnid; SEPARATOR=", ") as ?tgnids)
        (group_concat(distinct ?gnid; SEPARATOR=", ") as ?gnids)
        (group_concat(distinct ?viafid; SEPARATOR=", ") as ?viafids)
        (group_concat(distinct ?locid; SEPARATOR=", ") as ?locids)
        WHERE {
          VALUES ?plabel { %s } .
          SERVICE wikibase:mwapi {
            bd:serviceParam wikibase:api "EntitySearch" .
            bd:serviceParam wikibase:endpoint "www.wikidata.org" .
            bd:serviceParam mwapi:search ?plabel .
            bd:serviceParam mwapi:language "en" .
            ?place wikibase:apiOutputItem mwapi:item .
            ?num wikibase:apiOrdinal true .
          }         
          OPTIONAL {?place wdt:P17 ?country .}
          OPTIONAL {?place wdt:P131 ?parent .}
          OPTIONAL {?place wdt:P571 ?inception .}
  
          OPTIONAL {?place wdt:P1667 ?tgnid .} 
          OPTIONAL {?place wdt:P1566 ?gnid .}
          OPTIONAL {?place wdt:P214 ?viafid .}
          OPTIONAL {?place wdt:P244 ?locid .}
  
          SERVICE wikibase:label { 
            bd:serviceParam wikibase:language "en".
            ?place rdfs:label ?placeLabel .
            ?parent rdfs:label ?parentLabel . 
            ?country rdfs:label ?countryLabel .
            ?placeType rdfs:label ?placeTypeLabel .
          }
      '''% (variants)
      
    if 'geom' in qobj.keys():
      loc=shape(geojson.loads(json.dumps(qobj['geom'])))
      loc_sw='POINT('+str(loc.bounds[0])+' '+str(loc.bounds[1])+')'
      loc_ne='POINT('+str(loc.bounds[2])+' '+str(loc.bounds[3])+')'
      q+='''
          SERVICE wikibase:box {
            ?place wdt:P625 ?location .
              bd:serviceParam wikibase:cornerWest "%s"^^geo:wktLiteral .
              bd:serviceParam wikibase:cornerEast "%s"^^geo:wktLiteral .
          }
        '''% (loc_sw, loc_ne)
    else:
      q+='''
        ?place wdt:P625 ?location .
      '''
    qtype = q+'''
      ?place wdt:P31/wdt:P279* ?placeType .
      FILTER (?placeType in (%s)) . 
    '''%(placetype)
        
    if countries != '':
      q+='FILTER (?country in (%s)) . }'% (countries)
      qtype+='FILTER (?country in (%s)) . }'% (countries)
      
    # qbase is pass1: names, types, geometry, countries
    qbase = qtype+'''
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnids ?gnids ?viafids ?locids
      ORDER BY ASC(?num) LIMIT 5
    '''

    # qbare is pass2, omitting type filter
    qbare = q+'''
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnid ?gnid ?viafid ?locid
      ORDER BY ASC(?num) LIMIT 10'''

    def runQuery():
      global count_hit, count_nohit, total_hits, count_p1, count_p2
      sparql.setQuery(qbase)
      sparql.setReturnFormat(JSON)
      sparql.addCustomHttpHeader('User-Agent','WHGazetteer/0.2 (http://dev.whgazetteer.org; karl@kgeographer.org)')
      #sparql.addCustomHttpHeader('Retry-After','')
  
      # pass1 (qbase)
      try:
        bindings = sparql.query().convert()["results"]["bindings"]
      except ConnectionError as exc:
        print('429',sys.exc_info())
        if exc.status_code == 429:
          self.retry(exc=exc, countdown=61)
        
      # test, output results
      if len(bindings) > 0:
        #print(str(len(bindings))+' bindings for pass1: '+str(place_id),qbase)
        # TODO: this counts hits, written or not
        count_hit +=1 # got at least 1
        count_p1 +=1 # it's pass1
        for b in bindings:
          # write hit only if there's geometry
          if b['locations']['value'] != '': 
            total_hits+=1 # add to total
            # if type is empty, insert from query
            if b['types']['value'] == '':
              b['types']['value'] = placetype 
            writeHit(b,'pass1',ds,place_id,src_id,title)
            print('pass1 hit binding:',b)
      elif len(bindings) == 0:
        # no hits, pass2(qbare) drops type
        sparql.setQuery(qbare)
        sparql.setReturnFormat(JSON)
        sparql.addCustomHttpHeader('User-Agent','WHGazetteer/0.2 (http://dev.whgazetteer.org; karl@kgeographer.org)')
        # pass2 (qbare)
        try:
          bindings = sparql.query().convert()["results"]["bindings"]
        except ConnectionError as exc:
          print('pass2 error',sys.exc_info())
          print('qbare',qbare)
          if exc.status_code == 429:
            self.retry(exc=exc, countdown=61)
        if len(bindings) == 0:
          count_nohit +=1 # tried 2 passes, nothing
          #fout2.write(str(place_id)+' ('+title+'), pass2 \n')
        else:
          count_hit+=1 # got at least 1
          count_p2+=1 # it's pass2
          print(str(len(bindings))+' bindings, pass2: '+str(place_id),qbare)
          for b in bindings:
            # could be anything, see if it has a location
            if b['locations']['value'] != '':
              total_hits+=1 # add to total
              writeHit(b,'pass2',ds,place_id,src_id,title)
              #fout1.write(str(place_id)+'\tpass2:'+' '+str(b)+'\n')   
              print('pass2 hit binding:',b)
    # any exception, go on to the next
    try:
      runQuery()
    except:
      print('runQuery() failed, place#',place_id)
      print('runQuery() error:',sys.exc_info())
      count_skipped +=1
      continue
  
  print(str(count)+' rows; >=1 hit:'+str(count_hit)+'; '+str(total_hits)+' in total; ', str(count_nohit) + \
        ' misses; '+str(count_skipped)+' skipped')
  
  end = time.time()
  print('elapsed time in minutes:',int((end - start)/60))
  #   [count, count_hit, count_nohit, total_hits, count_p1, count_p2] = [0,0,0,0,0,0]
  hit_parade['summary'] = {
      'count':count,
      'got_hits':count_hit,
      'total': total_hits, 
      'pass1': count_p1, 
      'pass2': count_p2, 
      'pass3': 'n/a', 
      'no_hits': {'count': count_nohit },
      'elapsed': int((end - start)/60)
    }
  print("summary returned",hit_parade['summary'])
  return hit_parade['summary']
  
  
# ***
# performs elasticsearch queries
# ***
def es_lookup_tgn(qobj, *args, **kwargs):
  #print('qobj',qobj)
  bounds = kwargs['bounds']
  hit_count = 0

  # empty result object
  result_obj = {
      'place_id': qobj['place_id'], 'hits':[],
        'missed':-1, 'total_hits':-1
    }  

  # array (includes title)
  variants = list(set(qobj['variants']))

  # bestParent() coalesces mod. country and region; countries.json
  parent = bestParent(qobj)

  # pre-computed in sql
  # minmax = row['minmax']

  # getty aat numerical identifiers
  placetypes = list(set(qobj['placetypes']))

  # base query: name, type, parent, bounds if specified
  # geo_polygon filter added later for pass1; used as-is for pass2
  qbase = {"query": { 
    "bool": {
      "must": [
        {"terms": {"names.name":variants}},
        {"terms": {"types.id":placetypes}}
        ],
      "should":[
        {"terms": {"parents":parent}}
        #,{"terms": {"types.id":placetypes}}
        ],
      "filter": [get_bounds_filter(bounds,'tgn')] if bounds['id'] != ['0'] else []
    }
  }}

  qbare = {"query": { 
    "bool": {
      "must": [
        {"terms": {"names.name":variants}}
        ],
      "should":[
        {"terms": {"parents":parent}}                
        ],
      "filter": [get_bounds_filter(bounds,'tgn')] if bounds['id'] != ['0'] else []
    }
  }}

  # grab deep copy of qbase, add w/geo filter if 'geom'
  q1 = deepcopy(qbase)

  # create 'within polygon' filter and add to q1
  if 'geom' in qobj.keys():
    location = qobj['geom']
    # always polygon returned from hully(g_list)
    filter_within = { "geo_shape": {
      "location": {
        "shape": {
          "type": location['type'],
          "coordinates" : location['coordinates']
        },
        "relation": "within" # within | intersects | contains
      }
    }}    
    q1['query']['bool']['filter'].append(filter_within)

  # /\/\/\/\/\/
  # pass1: must[name]; should[type,parent]; filter[bounds,geom]
  # /\/\/\/\/\/
  #print('q1',q1)
  try:
    res1 = es.search(index="tgn_shape", body = q1)
    hits1 = res1['hits']['hits']
  except:
    print('pass1 error:',sys.exc_info())
  if len(hits1) > 0:
    for hit in hits1:
      hit_count +=1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
  elif len(hits1) == 0:
    # /\/\/\/\/\/
    # pass2: revert to qbase{} (drops geom)
    # /\/\/\/\/\/  
    q2 = qbase
    #print('q2 (base)',q2)
    try:
      res2 = es.search(index="tgn_shape", body = q2)
      hits2 = res2['hits']['hits']
    except:
      print('pass2 error:',sys.exc_info()) 
    if len(hits2) > 0:
      for hit in hits2:
        hit_count +=1
        hit['pass'] = 'pass2'
        result_obj['hits'].append(hit)
    elif len(hits2) == 0:
      # /\/\/\/\/\/
      # pass3: revert to qbare{} (drops placetype)
      # /\/\/\/\/\/  
      q3 = qbare
      #print('q3 (bare)',q3)
      try:
        res3 = es.search(index="tgn_shape", body = q3)
        hits3 = res3['hits']['hits']
      except:
        print('pass3 error:',sys.exc_info())        
      if len(hits3) > 0:
        for hit in hits3:
          hit_count +=1
          hit['pass'] = 'pass3'
          result_obj['hits'].append(hit)
      else:
        # no hit at all, name & bounds only
        result_obj['missed'] = qobj['place_id']
  result_obj['hit_count'] = hit_count
  return result_obj

# ***
# manage reconcile to tgn
# ***
@task(name="align_tgn")
def align_tgn(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  print('ds',ds.__dict__)
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  print('kwargs from align_tgn() task',kwargs)
  hit_parade = {"summary": {}, "hits": []}
  [nohits,tgn_es_errors,features] = [[],[],[]]
  [count, count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3] = [0,0,0,0,0,0,0]
  start = datetime.datetime.now()

  # queryset depends on choice of scope in addtask form
  #qs = ds.places.all().filter(flag=True)
  qs = ds.places.all() if scope == 'all' else ds.places.all().filter(indexed=False)

  for place in qs:
    # build query object
    #place=get_object_or_404(Place,id=131735) # Caledonian Canal (ne)
    count +=1
    qobj = {"place_id":place.id,"src_id":place.src_id,"title":place.title}
    [variants,geoms,types,ccodes,parents]=[[],[],[],[],[]]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = place.ccodes

    # types (Getty AAT identifiers)
    # tgn_shape index has 'aat:' prefix
    for t in place.types.all():
      types.append('aat:'+t.jsonb['identifier'])
    qobj['placetypes'] = types

    # names
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = variants

    # parents
    # TODO: other relations
    if len(place.related.all()) > 0:
      for rel in place.related.all():
        if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
          parents.append(rel.jsonb['label'])
      qobj['parents'] = parents
    else:
      qobj['parents'] = []

    # align_whg geoms
    if len(place.geoms.all()) > 0:
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter
      qobj['geom'] = hully(g_list)

    ## run pass1-pass3 ES queries
    # don't trap here - it masks errors in es_lookup_tgn()
    #try:
    print('qobj in align_tgn()',qobj)
    result_obj = es_lookup_tgn(qobj, bounds=bounds)
    #except:
      #print('es_lookup_tgn failed on ',place.id, sys.exc_info())
      
    if result_obj['hit_count'] == 0:
      count_nohit +=1
      nohits.append(result_obj['missed'])
    else:
      count_hit +=1
      total_hits += len(result_obj['hits'])
      #print("hit[0]: ",result_obj['hits'][0]['_source'])  
      print('hits from align_tgn',result_obj['hits'])
      for hit in result_obj['hits']:
        if hit['pass'] == 'pass1': 
          count_p1+=1 
        elif hit['pass'] == 'pass2': 
          count_p2+=1
        elif hit['pass'] == 'pass3': 
          count_p3+=1
        hit_parade["hits"].append(hit)
        # correct lower case 'point' in tgn index
        # TODO: reindex properly
        if 'location' in hit['_source'].keys():
          loc = hit['_source']['location'] 
          loc['type'] = "Point"
        else:
          loc={}
        new = Hit(
          authority = 'tgn',
          authrecord_id = hit['_id'],
          dataset = ds,
          place_id = get_object_or_404(Place, id=qobj['place_id']),
          task_id = align_tgn.request.id,
          query_pass = hit['pass'],
          # consistent, for review display
          json = normalize(hit['_source'],'tgn'),
          src_id = qobj['src_id'],
          score = hit['_score'],
          geom = loc,
          reviewed = False,
        )
        new.save()
  end = datetime.datetime.now()

  print('tgn ES errors:',tgn_es_errors)
  hit_parade['summary'] = {
      'count':count,
      'got_hits':count_hit,
      'total': total_hits, 
      'pass1': count_p1, 
      'pass2': count_p2, 
      'pass3': count_p3,
      'pass1remains': -1,
      'pass2remains': -1,
      'pass3remains': -1,
      'no_hits': {'count': count_nohit },
      'elapsed': elapsed(end-start)
    }
  print("summary returned",hit_parade['summary'])
  return hit_parade['summary']

# ***
# performs elasticsearch queries
# ***
def es_lookup_whg(qobj, *args, **kwargs):
  global whg_id
  idx = kwargs['index']
  bounds = kwargs['bounds']
  #ds = kwargs['dataset'] 
  place = kwargs['place']
  hit_count, err_count = [0,0]

  # create empty result object
  result_obj = {
    'place_id': qobj['place_id'], 'title': qobj['title'], 
      'hits':[], 'missed':-1, 'total_hits':-1
  }  

  # prepare queries
  # initial for pass1: common link?
  qlinks = {"query": { 
     "bool": {
       "must": [
          {"terms": {"links.identifier": qobj['links'] }}
        ]
       ,"must_not": [
          {"terms": {"links.type": ['related'] }}
        ]
     }
  }}
  
  # base query: name, type, bounds if specified
  qbase = {"query": { 
    "bool": {
      "must": [
        {"terms": {"names.toponym": qobj['variants']}},
        {"terms": {"types.identifier": qobj['placetypes']}}
        ],
      "filter": [get_bounds_filter(bounds,'whg')] if bounds['id'] != ['0'] else []
    }
  }}
  
  # suggest w/spatial experiment: can't do type AND geom contexts
  qsugg = {
    "suggest": {
      "suggest" : {
        "prefix" : qobj['title'],
        "completion" : {
          "field" : "suggest",
          "size": 10,
          "contexts": 
            {"place_type": qobj['placetypes']}
        }
      }
  }}
  
  # last gasp: only name(s) and bounds
  qbare = {"query": { 
    "bool": {
      "must": [
        {"terms": {"names.toponym":qobj['variants']}}
        ]
      ,"filter": [get_bounds_filter(bounds,'whg')] if bounds['id'] != ['0'] else []
    }
  }}

  # if geom, define intersect filter & apply to qbase (q2)
  if 'geom' in qobj.keys():
    # call it location
    location = qobj['geom']
    #print('location for filter_intersects_area:',location)
    filter_intersects_area = { "geo_shape": {
      "geoms.location": {
        "shape": {
          # always a polygon, from hully(g_list)
          "type": location['type'],
            "coordinates" : location['coordinates']
        },
        "relation": "intersects" # within | intersects | contains
      }
    }}
    qbase['query']['bool']['filter'].append(filter_intersects_area)
    
    repr_point=list(Polygon(location['coordinates'][0]).centroid.coords) \
                    if location['type'].lower() == 'polygon' else \
                    list(LineString(location['coordinates']).centroid.coords) \
                    if location['type'].lower() == 'linestring' else \
                    list(Point(location['coordinates']).coords)
    
    qsugg['suggest']['suggest']['completion']['contexts']={"place_type": qobj['placetypes']}, \
      {"representative_point": {"lon":repr_point[0] , "lat":repr_point[1], "precision": "100km"}}
  
  # grab a copy of each
  q1 = qlinks
  q2 = qbase
  q3 = qbare
  print('q1',q1)
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  # pass1: must[links]; should[names->variants]
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  try:
    res1 = es.search(index=idx, body = q1)
    hits1 = res1['hits']['hits']
  except:
    print("q1, ES error:", q1, sys.exc_info())
  if len(hits1) > 0:
    # shared link(s); return for immed. indexinf
    for hit in hits1:
      hit_count +=1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
      result_obj['hit_count'] = hit_count
    return result_obj
  elif len(hits1) == 0:
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  # pass2: must[name, type]; should[parent]; filter[geom, bounds]
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    try:
      #print("q2:", q2)
      res2 = es.search(index=idx, body = q2)
      hits2 = res2['hits']['hits']
    except:
      print("q2, error:", q2, sys.exc_info())
    if len(hits2) > 0:
      # pass2 hit(s); return them
      for hit in hits2:
        hit_count +=1
        hit['pass'] = 'pass2'
        result_obj['hits'].append(hit)
        result_obj['hit_count'] = hit_count
        return result_obj
    elif len(hits2) == 0:
      # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
      # pass3: must[name]; should[parent]; filter[bounds]
      # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
      try:
        #print("q3:", q3)
        res3 = es.search(index=idx, body = q3)
        hits3 = res3['hits']['hits']
      except:
        print("q2, error:", q3, sys.exc_info())
      if len(hits3) > 0:
        for hit in hits3:
          hit_count +=1
          hit['pass'] = 'pass3'
          result_obj['hits'].append(hit)
      else:
        # no hit at all
        result_obj['missed'] = qobj['place_id']
  result_obj['hit_count'] = hit_count
  return result_obj


# ***
# reconciling to whg
# pass1 finds shared links, auto-indexes
# pass2, pass3 hits queued for review
# ***
@task(name="align_whg")
def align_whg(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  # set index
  idx='whg'
  
  # get last identifier (used for whg_id & _id)
  whg_id = maxID(es,idx)
    
  #dummy for testing
  #bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  
  # TODO: system for region creation
  hit_parade = {"summary": {}, "hits": []}
  [count,count_hit,count_nohit,total_hits,count_p1,count_p2,count_p3] = [0,0,0,0,0,0,0]
  [count_errors,count_seeds,count_kids,count_fail] = [0,0,0,0]

  start = datetime.datetime.now()
  print('kwargs in align_whg()',kwargs)
    
  # queryset depends on choice of scope in addtask form
  qs = ds.places.all() if scope == 'all' else ds.places.all().filter(indexed=False)
  
  """
  build query object 'qobj'
  then result_obj = es_lookup_whg(qobj)
  """
  for place in qs:
    #place=get_object_or_404(Place,id=6369031) # Aachen
    # (2 index docs with tgn:7004799 link)
    count +=1
    qobj = {"place_id":place.id, "src_id":place.src_id, "title":place.title}
    links=[]; ccodes=[]; types=[]; variants=[]; parents=[]; geoms=[]; 

    # links
    for l in place.links.all():
      links.append(l.jsonb['identifier'])
    qobj['links'] = links

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = list(set(place.ccodes))

    # types (Getty AAT identifiers)
    # accounts for 'null' in 97 black records
    for t in place.types.all():
      if t.jsonb['identifier'] != None:
        types.append(t.jsonb['identifier'])
      else:
        # no type? use inhabited place, cultural group, site
        types.extend(['aat:300008347','aat:300387171','aat:300000809'])
    qobj['placetypes'] = types

    # names
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = [v.lower() for v in variants]

    # parents
    for rel in place.related.all():
      if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
        parents.append(rel.jsonb['label'])
    qobj['parents'] = parents

    # geoms
    if len(place.geoms.all()) > 0:
      # any geoms at all...
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter purposes
      qobj['geom'] = hully(g_list)

    # ***
    # run es_lookup_whg(qobj): 3 query passes
    # ***
    #result_obj = es_lookup_whg(qobj, index=idx, bounds=bounds, dataset=ds.label, place=place)
    result_obj = es_lookup_whg(qobj, index=idx, bounds=bounds, place=place)

    # PARSE RESULTS
    # no hits on any pass, create parent record now
    if result_obj['hit_count'] == 0:
      count_nohit += 1
      # increment whg_id (max at start computed earlier)
      whg_id += 1
      #print('need new parent, whg_id:',whg_id)
      
      # es_utils.makeDoc() -> new ES format document object
      parent_obj = makeDoc(place,'none')
      
      # add more elements...
      # make it a parent
      parent_obj['relation']={"name":"parent"}
      # give it the new, incremented whg_id
      parent_obj['whg_id']=whg_id
      # add its own names to the suggest field
      for n in parent_obj['names']:
        parent_obj['suggest']['input'].append(n['toponym'])
      # add its title
      if place.title not in parent_obj['suggest']['input']:
        parent_obj['suggest']['input'].append(place.title)
      # temp hack: using searchy field duplicates suggest.input
      # (autocomplete disabled for poor performance)
      parent_obj['searchy'] =  parent_obj['suggest']['input']
      # index it
      # and flag its db record
      try:
        es.index(index=idx, doc_type='place', id=str(whg_id), body=json.dumps(parent_obj))
        count_seeds +=1
        place.indexed = True
        place.save()
        print('new parent: '+str(whg_id)+' from place: '+str(place.id)+' ('+place.title+')')
      except:
        print('failed indexing '+str(place.id)+' as parent',sys.exc_info[0])
        pass
        #print(sys.exc_info[0])
        
    # got some hits
    elif result_obj['hit_count'] > 0:
      hits = result_obj['hits']
      count_hit +=1
      [count_kids,count_errors] = [0,0]
      total_hits += result_obj['hit_count']
      # extract pass1 if any
      pass1hits = [hit for hit in hits if hit['pass']=='pass1']
      # 0 or >1 pass1 hit -> write each to db for review
      # (pass1 are never mixed with others)
      if len(pass1hits) == 0 or len(pass1hits) >1:
        for hit in hits:
          if hit['pass'] == 'pass1': 
            count_p1+=1
          if hit['pass'] == 'pass2': 
            count_p2+=1
          if hit['pass'] == 'pass3': 
            count_p3+=1          
          hit_parade["hits"].append(hit)
          loc = hit['_source']['geoms'] if 'geoms' in hit['_source'].keys() else None
          # creates hit record for review process
          try:
            new = Hit(
              authority = 'whg',
              authrecord_id = hit['_id'],
              dataset = ds,
              place_id = get_object_or_404(Place, id=qobj['place_id']),
              task_id = align_whg.request.id,
              query_pass = hit['pass'],
              # consistent json for review display
              json = normalize(hit['_source'],'whg'),
              src_id = qobj['src_id'],
              score = hit['_score'],
              geom = loc,
              reviewed = False,
            )
            new.save()
          except:
            count_errors +=1
            print("hit _source, error:",hit, sys.exc_info())
      # single pass1 hit, index it 
      elif len(pass1hits) == 1:
        hit = hits[0]
        # instantiate a child doc
        newchild = makeDoc(place, 'none')      
        # is hit a parent or child?
        role = hit['_source']['relation']['name']
        # get correct parent _id (aka whg_id)
        if role == 'child':
          # becomes hit's sibling; grab parent _id
          parent_whgid = hit['_source']['relation']['parent']
        elif role == 'parent':
          # becomes hit's child
          parent_whgid = hit['_id'] 
        newchild['relation']={"name":"child","parent":parent_whgid}
        # ***
        # index it
        # ***
        try:
          # add it
          es.index(index=idx,doc_type='place',id=place.id,
                         routing=1,body=json.dumps(newchild))
          count_kids +=1                
          print('added '+str(place.id) + ' as child of '+ parent_whgid + ' (_id)')
          
          # then update its parent: 
          #  add new child place_id to _source.children[], 
          #  add variants to suggest.input[] (also to searchy[], a temp hack for slow autocomplete)
          # title isn't always a variant but needs to be added to _source.suggest
          if place.title not in qobj['variants']:
            qobj['variants'].append(place.title)
          q_update = { "script": {
              "source": """
                ctx._source.suggest.input.addAll(params.names); 
                ctx._source.children.add(params.id); 
                ctx._source.searchy.addAll(params.names)
              """,
              "lang": "painless",
              "params":{"names": qobj['variants'], "id": str(place.id)}
            },
            "query": {"match":{"_id": parent_whgid}}}
          es.update_by_query(index=idx, doc_type='place', body=q_update, conflicts='proceed')
          # flag place as indexed in db
          place.indexed = True
          place.save()
        except:
          print('failed indexing '+str(place.id)+' as child of '+str(parent_whgid))
          count_fail += 1
          pass
          
  end = datetime.datetime.now()
  hit_parade['summary'] = {
    'count':count,
    'got_hits':count_hit,
    'total': total_hits, 
    'seeds': count_seeds,
    'kids': count_kids,
    'pass1': count_p1, 
    'pass2': count_p2, 
    'pass3': count_p3,
    'no_hits': {'count': count_nohit },
    'elapsed': elapsed(end-start),
    'skipped': count_fail
  }
  print("hit_parade['summary']",hit_parade['summary'])
  return hit_parade['summary']
