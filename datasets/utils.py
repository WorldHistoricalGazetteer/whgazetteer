import codecs, datetime, sys, csv, os, pprint, chardet
import simplejson as json
from goodtables import validate as gvalidate
from shapely import wkt
from datasets.models import DatasetUser
from datasets.static.hashes import aat, parents
from jsonschema import Draft7Validator, draft7_format_checker, validate
pp = pprint.PrettyPrinter(indent=1)

# use jsonschema to validate Linked Places json-ld
# format ['coll' (FeatureCollection) | 'lines' (json-lines)]
def validate_lpf(infile,format):
  # TODO: handle json-lines
  # TODO: create v1.1 schema; phase out v1.0
  schema = json.loads(codecs.open('datasets/static/validate/schema_lpf_v1.0.json','r','utf8').read())
  fout = codecs.open('validate-lpf-result.txt','w','utf8')
  #infile=codecs.open('tests/whg/lugares_10_citations.jsonld','r','utf-8')
  #infile=codecs.open('tests/whg/lugares_10_citations_errors.jsonld','r','utf-8')
  #infile=open('tests/whg/alcedo_200errors.tsv')
  result = {"format":"lpf_"+format,"errors":[]}
  [countrows,count_ok] = [0,0]
  
  jdata = json.loads(infile.read())
  if ['type', '@context', 'features'] != list(jdata.keys()) \
     or jdata['type'] != 'FeatureCollection' \
     or len(jdata['features']) == 0:
    print('not valid GeoJSON-LD')
  else:
    for feat in jdata['features']:
      countrows +=1
      print(feat['properties']['title'])
      try:
        validate(
          instance=feat,
          schema=schema,
          format_checker=draft7_format_checker
        )
        count_ok +=1
      except:
        err = sys.exc_info()
        print('some kinda error',err)
        result["errors"].append({"feat":countrows,'error':err[1].args[0]})

  fout.write(json.dumps(result["errors"]))
  fout.close()
  result['count'] = countrows
  return result

# validate LP-TSV file
def goodtable(tempfn,filename,user):
  #user='whgadmin'
  result = {"errors":[],"format":"delimited"}
  # TODO: detect encoding
  #enc = chardet.detect(open(tempfn,'rb').read())
  #print('encoding is',enc)
  #if enc['encoding'] != 'utf-8':
    #result['errors'].append({'message':'File is not utf-8 encoded'})
    #return result
  newfn = tempfn+'.tsv'
  os.rename(tempfn,newfn)
  #print('tempfn,filename,user,dir',tempfn,filename,user,os.getcwd())
  schema_lptsv = json.loads(codecs.open('datasets/static/validate/schema_tsv.json', 'r', 'utf8').read())
  report = gvalidate(newfn,schema=schema_lptsv,order_fields=True)
  #pp.pprint(report)  
  #print('error count',report['error-count'])
  result['count'] = report['tables'][0]['row-count']
  result['columns'] = report['tables'][0]['headers']
  for e in report['tables'][0]['errors']:
    if e['code'] not in ["blank-header","missing-header"]:
      result["errors"].append(e)
  print('result',result)
  return result

class HitRecord(object):
  #def __init__(self, whg_id, place_id, dataset, src_id, title):
  def __init__(self, place_id, dataset, src_id, title):
    #self.whg_id = whg_id
    self.place_id = place_id
    self.src_id = src_id
    self.title = title
    self.dataset = dataset

  def __str__(self):
    import json
    return json.dumps(str(self.__dict__))    
    #return json.dumps(self.__dict__)

  def toJSON(self):
    import json
    return json.loads(json.dumps(self.__dict__,indent=2))

def aat_lookup(id):
  try:
    label = aat.types[id]['term_full']
    return label
  except:
    print(id,' broke it')
    print("error:", sys.exc_info())        

def hully(g_list):
  from django.contrib.gis.geos import GEOSGeometry
  from django.contrib.gis.geos import MultiPoint
  from django.contrib.gis.geos import GeometryCollection
  if g_list[0]['type'] == 'Point':
    # 1 or more points >> make hull; if not near 180 deg., add buffer(1) (~200km @ 20deg lat)
    hull=MultiPoint([GEOSGeometry(json.dumps(g)) for g in g_list]).convex_hull
    l = list(set([g_list[0]['coordinates'][0] for c in g_list[0]]))
    if len([i for i in l if i >= 175]) == 0:
      hull = hull.buffer(1)
    else:
      hull = hull.buffer(0.1)
  elif g_list[0]['type'] == 'MultiLineString':
    hull=GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).convex_hull
  else:
    # now only linestrings and multiple multipolygons -> simple convex_hull (unions are precise but bigger)
    hull=GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).convex_hull
  return json.loads(hull.geojson)

def parse_wkt(g):
  print('wkt',g)
  from shapely.geometry import mapping
  gw = wkt.loads(g)
  feature = mapping(gw)
  #print('wkt, feature',g, feature)
  return feature

# TODO: not truly implemented; cf. datasets.views.DashboardView
def myteam(me):
  myteam=[]
  for g in me.groups.all():
    for u in g.user_set.all():
      myteam.append(u)
  return myteam
def myprojects(me):
  return DatasetUser.objects.filter(user_id_id=me.id).values_list('dataset_id_id')
  
  
def parsejson(value,key):
  """returns value for given key"""
  print('parsejson() value',value)
  obj = json.loads(value.replace("'",'"'))
  return obj[key]

def elapsed(delta):
  minutes, seconds = divmod(delta.seconds, 60)
  return '{:02}:{:02}'.format(int(minutes), int(seconds))

def bestParent(qobj, flag=False):
  # applicable for tgn only 
  best = []
  print('qobj in bestParent',qobj)
  # merge parent country/ies & parents
  if len(qobj['countries']) > 0 and qobj['countries'][0] != '':
    for c in qobj['countries']:
      best.append(parents.ccodes[0][c]['tgnlabel'])
  if len(qobj['parents']) > 0:
    for p in qobj['parents']:
      best.append(p)
  if len(best) == 0:
    best = ['World']
  return best

# wikidata Qs from ccodes
#TODO: consolidate hashes
def getQ(arr,what):
  qids=[]
  if what == 'ccodes':
    from datasets.static.hashes.parents import ccodes
    for c in arr:
      qids.append('wd:'+ccodes[0][c]['wdid'])
  elif what == 'types':
    from datasets.static.hashes.aat_q import qnums
    for t in arr:
      if t in qnums:
        for q in qnums[t]:
          qids.append('wd:'+q)
      else:
        qids.append('wd:Q486972')
  return qids

def roundy(x, direct="up", base=10):
  import math
  if direct == "down":
    return int(math.ceil(x / 10.0)) * 10 - base
  else:
    return int(math.ceil(x / 10.0)) * 10

def fixName(toponym):
  import re
  search_name = toponym
  r1 = re.compile(r"(.*?), Gulf of")
  r2 = re.compile(r"(.*?), Sea of")
  r3 = re.compile(r"(.*?), Cape")
  r4 = re.compile(r"^'")
  if bool(re.search(r1,toponym)):
    search_name = "Gulf of " + re.search(r1,toponym).group(1)
  if bool(re.search(r2,toponym)):
    search_name = "Sea of " + re.search(r2,toponym).group(1)
  if bool(re.search(r3,toponym)):
    search_name = "Cape " + re.search(r3,toponym).group(1)
  if bool(re.search(r4,toponym)):
    search_name = toponym[1:]
  return search_name if search_name != toponym else toponym

# in: list of Black atlas place types
# returns list of equivalent classes or types for {gaz}
def classy(gaz, typeArray):
  import codecs, json
  #print(typeArray)
  types = []
  finhash = codecs.open('../data/feature-classes.json', 'r', 'utf8')
  classes = json.loads(finhash.read())
  finhash.close()
  if gaz == 'gn':
    t = classes['geonames']
    default = 'P'
    for k,v in t.items():
      if not set(typeArray).isdisjoint(t[k]):
        types.append(k)
      else:
        types.append(default)
  elif gaz == 'tgn':
    t = classes['tgn']
    default = 'inhabited places' # inhabited places
    # if 'settlement' exclude others
    typeArray = ['settlement'] if 'settlement' in typeArray else typeArray
    # if 'admin1' (US states) exclude others
    typeArray = ['admin1'] if 'admin1' in typeArray else typeArray
    for k,v in t.items():
      if not set(typeArray).isdisjoint(t[k]):
        types.append(k)
      else:
        types.append(default)
  elif gaz == "dbp":
    t = classes['dbpedia']
    default = 'Place'
    for k,v in t.items():
      # is any Black type in dbp array?
      # TOD: this is crap logic, fix it
      if not set(typeArray).isdisjoint(t[k]):
        types.append(k)
      #else:
        #types.append(default)
  if len(types) == 0:
    types.append(default)
  return list(set(types))
