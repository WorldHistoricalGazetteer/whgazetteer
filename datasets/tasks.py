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
from datasets.es_utils import makeDoc, esInit
from datasets.models import Dataset, Hit
from datasets.regions import regions as region_hash
from datasets.static.hashes.parents import ccodes
from datasets.utils import *
from places.models import Place
##
#import shapely.geometry as sgeo
from geopy import distance
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
##

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
def maxID(es):
  q={"query": {"bool": {"must" : {"match_all" : {}} }},
       "sort": [{"whg_id": {"order": "desc"}}],
       "size": 1  
       }
  try:
    res = es.search(index='whg', body=q)
    #maxy = int(res['hits']['hits'][0]['_id'])
    maxy = int(res['hits']['hits'][0]['_source']['whg_id'])
  except:
      maxy = 12345677
  return maxy 

def ccDecode(codes):
  countries=[]
  #print('codes in ccDecode',codes)
  for c in codes:
    countries.append(ccodes[0][c]['gnlabel'])
  return countries

def parseDateTime(string):
  year = re.search("(\d{4})-",string).group(1)
  if string[0] == '-':
    year = year + ' BCE' 
  return year.lstrip('0')

  
# normalize hits json from any authority
def normalize(h,auth):
  if auth == 'whg':
    #try:
    rec = HitRecord(h['place_id'], h['dataset'], h['src_id'], h['title'])
    rec.whg_id = h['whg_id'] if 'whg_id' in h.keys() else h['relation']['parent']
    #print('rec',rec)
    # add elements if non-empty in index record
    rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
    rec.types = [t['label']+' ('+t['src_label']  +')' if t['label']!=None else t['src_label'] \
                for t in h['types']] if len(h['types']) > 0 else []
    rec.ccodes = ccDecode(h['ccodes']) if 'ccodes' in h.keys() else []
    rec.parents = ['partOf: '+r.label+' ('+parseWhen(r['when']['timespans'])+')' for r in h['relations']] \
                if 'relations' in h.keys() and len(h['relations']) > 0 else []
    rec.descriptions = h['descriptions'] if len(h['descriptions']) > 0 else []
    rec.geoms = [{
      "type":h['geoms'][0]['location']['type'], 
      "coordinates":h['geoms'][0]['location']['coordinates'],
      }] if len(h['geoms'])>0 else []
    rec.minmax = dict(sorted(h['minmax'].items(),reverse=True)) if len(h['minmax']) > 0 else []
    #rec.whens = [parseWhen(t) for t in h['timespans']] \
                #if len(h['timespans']) > 0 else []
    rec.links = [l['type']+': '+l['identifier'] for l in h['links']] \
                if len(h['links']) > 0 else []
    #except:
      #print("normalize(whg) error:", h['place_id'], sys.exc_info()[0])    
  
  elif auth == 'wd':
    try:
      #
      locs=[]
      if 'locations' in h.keys():
        for l in h['locations']['value'].split(', '):
          loc = parse_wkt(l)
          loc["id"]=h['place']['value'][31:]
          loc['ds']='wd'
          locs.append(loc)
      #  place_id, dataset, src_id, title
      rec = HitRecord(-1, 'wd', h['place']['value'][31:], h['placeLabel']['value'])
      rec.variants = []
      rec.types = h['types']['value'] if 'types' in h.keys() else []
      rec.ccodes = [h['countryLabel']['value']]
      rec.parents =h['parents']['value'] if 'parents' in h.keys() else []
      rec.geoms = locs if len(locs)>0 else []
      rec.minmax = []
      rec.links = ['closeMatch: '+l+':'+h['links'][l] for l in h['links']] \
                  if len(h['links']) > 0 else []
      rec.inception = parseDateTime(h['inception']['value']) if 'inception' in h.keys() else ''
      # {'datatype': 'http://www.w3.org/2001/XMLSchema#dateTime', 'type': 'literal', 'value': '1858-11-22T00:00:00Z'}
    except:
      print("normalize(wd) error:", h['place']['value'][31:], sys.exc_info())    
  elif auth == 'tgn':
    # h=hit['_source']; ['tgnid', 'title', 'names', 'suggest', 'types', 'parents', 'note', 'location']
    # whg_id, place_id, dataset, src_id, title
    # h['location'] = {'type': 'point', 'coordinates': [105.041, 26.398]}
    try:
      #rec = HitRecord(-1, -1, 'tgn', h['tgnid'], h['title'])
      rec = HitRecord(-1, 'tgn', h['tgnid'], h['title'])
      print('normalize rec - tgn',rec)
      rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
      rec.types = [t['placetype']+' ('+t['id']  +')' for t in h['types'] ] if len(h['types']) > 0 else []
      rec.ccodes = []
      rec.parents = ' > '.join(h['parents']) if len(h['parents']) > 0 else []
      rec.descriptions = [h['note']] if h['note'] != None else []
      rec.geoms = [{
        "type":h['location']['type'], 
        "coordinates":h['location']['coordinates'],
        "id":h['tgnid'], \
        "ds":"tgn"}] \
        if h['location'] != None else []
      #rec.geoms = [h['location']] if h['location'] != None else []
      rec.minmax = []
      #rec.whens =[]
      rec.links = []
    except:
      print("normalize(tgn) error:", h['tgnid'], sys.exc_info())
  return rec.toJSON()

# user-supplied spatial bounds
def get_bounds_filter(bounds,idx):
  #print('bounds',bounds)
  id = bounds['id'][0]
  areatype = bounds['type'][0]
  area = Area.objects.get(id = id)
  # TODO: area always a hull polygon now; test MultiPolygon
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


# wikidata: b,'pass1',ds,place_id,src_id,title
def writeHit(b,passnum,ds,pid,srcid,title):
  # gather any links
  authkeys=['tgnid','gnid','viafid','locid']
  linklist = list(set(list(b.keys())).intersection(authkeys))
  linkobj = {}
  for l in linklist:
    linkobj[l[:-2]] = b[l]['value']
  b['links'] = linkobj
  if b['placeLabel']['value'] != b['place']['value'][31:]:
    from datasets.models import Hit
    new = Hit(
      authority = 'wd',
      authrecord_id = b['place']['value'][31:],
      dataset = ds,
      place_id = get_object_or_404(Place, id=pid),
      #task_id = 'wd_20190517',
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

# TODO: uncomment after tested
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
  outdir='/Users/karlg/Documents/Repos/_whgdata/pyout/align_wd/'
  
  # missed, skipped
  fout1 = codecs.open(outdir+ds.label+'/es-hits_'+timestamp+'.txt', 'w', 'utf8')
  fout2 = codecs.open(outdir+ds.label+'/es-missed_'+timestamp+'.txt', 'w', 'utf8')
  fout3 = codecs.open(outdir+ds.label+'/es-skipped_'+timestamp+'.txt', 'w', 'utf8')
  
  def toWKT(coords):
    wkt = 'POINT('+str(coords[0])+' '+str(coords[1])+')'
    return wkt
  
  [count,count_skipped] = [0,0]
  global count_hit, count_nohit, total_hits, count_p1, count_p2
  [count_hit, count_nohit, total_hits, count_p1, count_p2] = [0,0,0,0,0]
  
  #for place in ds.places.filter(flag=True):
  for place in ds.places.all().order_by('id'):
    #place=get_object_or_404(Place, id=176861) # Abancay
    #place=get_object_or_404(Place, id=83495) # Denver
    #place=get_object_or_404(Place, id=179689) # Eusebio Ayala
    count +=1
    place_id = place.id
    src_id = place.src_id
    title = fixName(place.title)
    qobj = {"place_id":place.id,"src_id":place.src_id,"title":fixName(place.title)}
    [variants,geoms,types,ccodes,parents]=[[],[],[],[],[]]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = place.ccodes

    # types (Getty AAT identifiers)
    for t in place.types.all():
      types.append(int(t.jsonb['identifier'][4:]) if t.jsonb['identifier'] !=None else '')
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
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter
      qobj['geom'] = hully(g_list)

    # wikidata sparql needs this form for lists
    variants = ' '.join(['"'+n+'"' for n in qobj['variants']])
    countries = ', '.join([c for c in getQ(qobj['countries'],'ccodes')])
    types = ', '.join([c for c in getQ(qobj['placetypes'],'types')])
    
    # TODO admin parent P131, retrieve wiki article name, country P17, ??
    q='''SELECT ?place ?placeLabel ?countryLabel ?inception ?tgnid ?gnid ?viafid ?locid
        (group_concat(distinct ?parentLabel; SEPARATOR=", ") as ?parents)
        (group_concat(distinct ?placeTypeLabel; SEPARATOR=", ") as ?types)
        (group_concat(distinct ?location; SEPARATOR=", ") as ?locations)
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

          ?place (wdt:P279|wdt:P31) ?placeType .
  
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

    if countries != '':
      q+='FILTER (?country in (%s)) .'% (countries)
    # qbase is pass1: names, types, geometry, countries
    qbase = q+'''
      FILTER (?placeType in (%s)) . }
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnid ?gnid ?viafid ?locid
      ORDER BY ASC(?num) LIMIT 5
    '''% (types)
    
    # qbare is pass2, omitting type filter
    qbare = q+''' }
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnid ?gnid ?viafid ?locid
      ORDER BY ASC(?num) LIMIT 5'''

    def runQuery():
      global count_hit, count_nohit, total_hits, count_p1, count_p2
      sparql.setQuery(qbase)
      sparql.setReturnFormat(JSON)
  
      # pass1 (qbase)
      bindings = sparql.query().convert()["results"]["bindings"]
  
      # test, output results
      if len(bindings) > 0:
        print(str(len(bindings))+' bindings for pass1: '+str(place_id),qbase)
        count_hit +=1 # got at least 1
        count_p1+=1 # it's pass1
        for b in bindings:
          if b['locations']['value'] != '':
            total_hits+=1 # add to total
            writeHit(b,'pass1',ds,place_id,src_id,title)
            fout1.write(str(place_id)+'\tpass1:'+' '+str(b)+'\n')   
            print('pass1 hit binding:',b)   
      elif len(bindings) == 0:
        # no hits, pass2(qbare) drops type
        sparql.setQuery(qbare)
        sparql.setReturnFormat(JSON)
        # pass2 (qbare)
        bindings = sparql.query().convert()["results"]["bindings"]
        if len(bindings) == 0:
          count_nohit +=1 # tried 2 passes, nothing
          fout2.write(str(place_id)+' ('+title+'), pass2 \n')
        else:
          count_hit+=1 # got at least 1
          count_p2+=1 # it's pass2
          print(str(len(bindings))+' bindings, pass2: '+str(place_id),qbare)
          for b in bindings:
            # could be anything, see if it has a location
            if b['locations']['value'] != '':
              total_hits+=1 # add to total
              writeHit(b,'pass2',ds,place_id,src_id,title)
              fout1.write(str(place_id)+'\tpass2:'+' '+str(b)+'\n')   
              print('pass1 hit binding:',b)
    try:
      runQuery()
    except:
      count_skipped +=1
      fout3.write(str(place_id) + '\t' + title + '\t' + str(sys.exc_info()[0]) + '\n')
      continue
  
  print(str(count)+' rows; >=1 hit:'+str(count_hit)+'; '+str(total_hits)+' in total; ', str(count_nohit) + \
        ' misses; '+str(count_skipped)+' skipped')
  fout1.close()
  fout2.close()
  fout3.close()
  
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
  
  
# queries > result_obj
# TODO: count 'got hit' per pass not hit per pass
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
  print('q1',q1)
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
    print('q2 (base)',q2)
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
      print('q3 (bare)',q3)
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

# manage ES queries to tgn
@task(name="align_tgn")
def align_tgn(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  bounds = kwargs['bounds']
  #bounds = {'type': ['userarea'], 'id': ['65']} # Alcedo 
  #bounds = {'type': ['region'], 'id': ['76']}  # C. America
  print('bounds:',bounds,type(bounds))
  hit_parade = {"summary": {}, "hits": []}
  [nohits,tgn_es_errors,features] = [[],[],[]]
  [count, count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3] = [0,0,0,0,0,0,0]
  start = datetime.datetime.now()

  # build query object
  #for place in ds.places.all()[:1]:
  for place in ds.places.all():
    #place=get_object_or_404(Place,id=131735) # Caledonian Canal (ne)
    #place=get_object_or_404(Place,id=131648) # Atengo river (ne)
    #place=get_object_or_404(Place,id=81655) # Atlas Mountains
    #place=get_object_or_404(Place,id=124653) # !Kung (dplace)
    #place=get_object_or_404(Place,id=124925) # Abenaki (dplace)
    #place=get_object_or_404(Place, id=125681) # Chukchi (dplace)
    count +=1
    qobj = {"place_id":place.id,"src_id":place.src_id,"title":place.title}
    [variants,geoms,types,ccodes,parents]=[[],[],[],[],[]]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = place.ccodes

    # types (Getty AAT identifiers)
    for t in place.types.all():
      types.append(t.jsonb['identifier'])
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

    # align_whg geoms
    if len(place.geoms.all()) > 0:
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter
      qobj['geom'] = hully(g_list)

    ## run pass1-pass3 ES queries
    try:
      result_obj = es_lookup_tgn(qobj, bounds=bounds)
    except:
      print('es_lookup_tgn failed:',sys.exc_info())
      
    if result_obj['hit_count'] == 0:
      count_nohit +=1
      nohits.append(result_obj['missed'])
    else:
      count_hit +=1
      total_hits += len(result_obj['hits'])
      print("hit[0]: ",result_obj['hits'][0]['_source'])      
      for hit in result_obj['hits']:
        if hit['pass'] == 'pass1': 
          count_p1+=1 
        elif hit['pass'] == 'pass2': 
          count_p2+=1
        elif hit['pass'] == 'pass3': 
          count_p3+=1
        hit_parade["hits"].append(hit)
        # print('creating hit:',hit)
        loc = hit['_source']['location'] if 'location' in hit['_source'].keys() else None
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

# queries > result_obj
def es_lookup_whg(qobj, *args, **kwargs):
  global whg_id
  idx='whg'
  bounds = kwargs['bounds']
  ds = kwargs['dataset'] 
  place = kwargs['place']
  #bounds = {'type': ['region'], 'id': ['87']}
  #bounds = {'type': ['userarea'], 'id': ['0']}
  hit_count, err_count = [0,0]

  # empty result object
  result_obj = {
    'place_id': qobj['place_id'], 'title': qobj['title'], 
      'hits':[], 'missed':-1, 'total_hits':-1
  }  

  # initial for pass1
  qlinks = {"query": { 
     "bool": {
       "must": [
          {"terms": {"links.identifier": qobj['links'] }}
        ],
        "should": [
          {"terms": {"names.toponym": qobj['variants']}}
        ]
     }
  }}
  
  # base query: name, type, bounds if specified
  qbase = {"query": { 
    "bool": {
      "must": [
        {"terms": {"names.toponym": qobj['variants']}},
        #{"match": {"names.toponym": qobj['title']}},
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
        ],
      "should":[
        {"terms": {"parents":bestParent(qobj)}}                
        ],
      "filter": [get_bounds_filter(bounds,'whg')] if bounds['id'] != ['0'] else []
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
  
  # grab copies
  q1 = qlinks
  q2 = qbase #qsugg
  q3 = qbare
  count_seeds=0
  
  # /\/\/\/\/\/
  # pass1: must[links]; should[names->variants]
  # /\/\/\/\/\/
  try:
    #print("q1:", q1)
    res1 = es.search(index=idx, body = q1)
    hits1 = res1['hits']['hits']
  except:
    print("q1, error:", q1, sys.exc_info())
  if len(hits1) > 0:
    for hit in hits1:
      hit_count +=1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
  elif len(hits1) == 0:
    # no shared links; for black, index place as a parent immediately
    # for any other dataset, continue to passes 2 & 3
    if ds == 'black':
      result_obj['hit_count'] = hit_count
      return result_obj
  # /\/\/\/\/\/
  # pass2: must[name, type]; should[parent]; filter[geom, bounds]
  # /\/\/\/\/\/
    try:
      #print("q2:", q2)
      res2 = es.search(index=idx, body = q2)
      hits2 = res2['hits']['hits']
    except:
      print("q2, error:", q2, sys.exc_info())
    if len(hits2) > 0:
      for hit in hits2:
        hit_count +=1
        hit['pass'] = 'pass2'
        result_obj['hits'].append(hit)
    elif len(hits2) == 0:
      # /\/\/\/\/\/
      # pass3: must[name]; should[parent]; filter[bounds]
      # /\/\/\/\/\/
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


# accessioning and/or queueing hits to whg
@task(name="align_whg")
def align_whg(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  # get last identifier (used for whg_id & _id)
  whg_id=maxID(es)
  
  # DANGER: this zaps whg index, creates new one
  #if ds.id==1:
    #errors_black = codecs.open('err_black-whg.txt', mode='w', encoding='utf8')
    #esInit('whg')
  
  #dummies for testing
  #bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']

  # TODO: system for region creation
  hit_parade = {"summary": {}, "hits": []}
  [nohits, errors] = [[],[]] # 
  [count, count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3, count_errors, count_seeds] = [0,0,0,0,0,0,0,0,0]

  start = datetime.datetime.now()

  """
  build qobj query object
  result_obj = es_lookup_whg(qobj)
  if 'black' && no hits on pass1, index immediately
  else, write all hits to db 
  """
  qs=ds.places.all()
  for place in qs:
    #place=get_object_or_404(Place,id=81034) # Acragas
    #place=get_object_or_404(Place,id=81104) # Agrigentum
    count +=1
    qobj = {"place_id":place.id, "src_id":place.src_id, "title":place.title}
    links=[]; ccodes=[]; types=[]; variants=[]; parents=[]; geoms=[]; 

    ## links
    for l in place.links.all():
      links.append(l.jsonb['identifier'])
    qobj['links'] = links

    ## ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = list(set(place.ccodes))

    ## types (Getty AAT identifiers)
    ## account for 'null' in 97 black records
    for t in place.types.all():
      if t.jsonb['identifier'] != None:
        types.append(t.jsonb['identifier'])
      else:
        # inhabited place, cultural group, site
        types.extend(['aat:300008347','aat:300387171','aat:300000809'])
    qobj['placetypes'] = types

    ## names
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = variants

    ## parents
    for rel in place.related.all():
      if rel.jsonb['relation_type'] == 'gvp:broaderPartitive':
        parents.append(rel.jsonb['label'])
    qobj['parents'] = parents

    ## geoms
    if len(place.geoms.all()) > 0:
      # any geoms at all...
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter purposes
      qobj['geom'] = hully(g_list)

    #
    ## run pass1-pass3 ES queries
    result_obj = es_lookup_whg(qobj, bounds=bounds, dataset=ds.label, place=place)

    if result_obj['hit_count'] == 0:
      count_nohit +=1
      # no hits, create parent record immediately      
      # for now, only if 'black'
      # TODO
      if ds.label == 'black':
        whg_id+=1
        place=get_object_or_404(Place,id=result_obj['place_id'])
        print('new whg_id',whg_id)
        parent_obj = makeDoc(place,'none')
        parent_obj['relation']={"name":"parent"}
        parent_obj['whg_id']=whg_id
        # add its own names to the suggest field
        for n in parent_obj['names']:
          parent_obj['suggest']['input'].append(n['toponym']) 
        #index it
        try:
          res = es.index(index='whg', doc_type='place', id=str(whg_id), body=json.dumps(parent_obj))
          count_seeds +=1
        except:
          print('failed indexing '+str(place.id), parent_obj)
          print(sys.exc_info[0])
          #errors_black.write(str({"pid":place.id, "title":place.title})+'\n')
        print('created parent:',result_obj['place_id'],result_obj['title'])
      #nohits.append(result_obj['missed'])
    elif result_obj['hit_count'] > 0:
      # create hit record for review process
      count_hit +=1
      [count_kids,count_errors] = [0,0]
      total_hits += result_obj['hit_count']
      #print("hit['_source']: ",result_obj['hits'][0]['_source'])
      for hit in result_obj['hits']:
        parentid=hit['_source']['place_id']
        #one-time filter for black atlas children        
        #print('hit parentid:',parentid)
        #if hit['pass'] == 'pass1':
          #count_p1+=1
          ## one-time black repair 20190420
          ## if ds='black' and place has a 'whg*' match link, make it child of that id
          ## got a whg match in place_link?
          #if ds.label == 'black':
            #print('it is black')
            #qs_links=get_object_or_404(Place,id=place.id).links.filter(black_parent__isnull=False)
            #if len(qs_links)>0:
              #print('match type:',qs_links[0].jsonb['type'])
              
              ## leave 'related' and unlinked out of it
              #if 'Match' in qs_links[0].jsonb['type']:
                
                ## get _id (whg_id) of parent
                #parentid=qs_links[0].black_parent
                #q_parent={"query": {"bool": {"must": [{"match":{"place_id": parentid}}]}}}
                #res = es.search(index='whg', body=q_parent)
                #parent_whgid = res['hits']['hits'][0]['_id'] #; print(parent_whgid)
                
                ## gather names, make an index doc
                #match_names = [p.toponym for p in place.names.all()]
                #child_obj = makeDoc(place,'none')
                #child_obj['relation']={"name":"child","parent":parent_whgid}
                
                ## index it
                #try:
                  #res = es.index(index='whg',doc_type='place',id=place.id,
                                 #routing=1,body=json.dumps(child_obj))
                  #count_kids +=1                
                  #print('added '+str(place.id) + ' as child of '+ str(parentid))
                #except:
                  #print('failed indexing '+str(place.id), child_obj)
                  #sys.exit(sys.exc_info())
                #q_update = { "script": {
                    #"source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id)",
                    #"lang": "painless",
                    #"params":{"names": match_names, "id": str(place.id)}
                  #},
                  #"query": {"match":{"_id": parent_whgid}}}
                #try:
                  #es.update_by_query(index='whg', doc_type='place', body=q_update)
                #except:
                  #print('failed updating '+str(parentid)+' from child '+str(place.id))
                  #print(count_kids-1)
                  #sys.exit(sys.exc_info())
              #else:
                ## it's 'related' or not linked at all...make it a parent
                #whg_id+=1
                #place=get_object_or_404(Place,id=result_obj['place_id'])
                #print('new whg_id',whg_id)
                #parent_obj = makeDoc(place,'none')
                #parent_obj['relation']={"name":"parent"}
                #parent_obj['whg_id']=whg_id
                
                ## add its names to its suggest field
                #for n in parent_obj['names']:
                  #parent_obj['suggest']['input'].append(n['toponym']) 
                  
                ##index it with fresh whg_id
                #try:
                  #res = es.index(index='whg', doc_type='place', id=str(whg_id), body=json.dumps(parent_obj))
                  #count_seeds +=1
                #except:
                  #print('failed indexing '+str(place.id), parent_obj)
                  #print(sys.exc_info[0])

        #elif hit['pass'] == 'pass2': 
          #count_p2+=1
        #elif hit['pass'] == 'pass3': 
          #count_p3+=1
        hit_parade["hits"].append(hit)
        loc = hit['_source']['geoms'] if 'geoms' in hit['_source'].keys() else None
        try:
          new = Hit(
            authority = 'whg',
            authrecord_id = hit['_id'],
            dataset = ds,
            place_id = get_object_or_404(Place, id=qobj['place_id']),
            task_id = align_whg.request.id,
            #task_id = 'abcxxyyzz',
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
  end = datetime.datetime.now()
  # ds.status = 'recon_whg'
  hit_parade['summary'] = {
    'count':count,
    'got_hits':count_hit,
    'total': total_hits, 
    'pass1': count_p1, 
    'pass2': count_p2, 
    'pass3': count_p3,
    'no_hits': {'count': count_nohit },
    'elapsed': elapsed(end-start)
    #'skipped': count_errors
  }
  #if ds.label == 'black': errors_black.close()
  print("hit_parade['summary']",hit_parade['summary'])
  return hit_parade['summary']

#q_around='''SELECT distinct ?place ?location ?distance ?placeLabel ?tgnid ?viafid ?locid ?bnfid ?gnid WHERE {
        #SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        #SERVICE wikibase:around { 
          #?place wdt:P625 ?location . 
          #bd:serviceParam wikibase:center "%s"^^geo:wktLiteral .
          #bd:serviceParam wikibase:radius "500" . 
          #bd:serviceParam wikibase:distance ?distance .
        #} 

        #?place rdfs:label ?placeLabel ;
            #(wdt:P31/wdt:P279*) ?placeType .

        #FILTER (?placeType = wd:Q486972 || ?placeType = wd:Q839954) .
        #FILTER (STR(?placeLabel) in (%s)) .

        ## external IDs
        #OPTIONAL {?place wdt:P1667 ?tgnid .}
        #OPTIONAL {?place wdt:P1566 ?gnid .}
        #OPTIONAL {?place wdt:P214 ?viafid .}
        #OPTIONAL {?place wdt:P268 ?bnfid .}
        #OPTIONAL {?place wdt:P244 ?locid .}
    #} ORDER BY ?placeLabel'''% (loc_wkt, variants)       
