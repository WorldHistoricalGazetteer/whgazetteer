import codecs, datetime, sys, csv, os
import simplejson as json
from shapely import wkt
from datasets.static.hashes import aat, parents
from jsonschema import validate, Draft7Validator, draft7_format_checker

def validate_lpf(infile,format):
  # format ['coll' (FeatureCollection) | 'lines' (json-lines)]
  # TODO: handle json-lines
  schema = json.loads(codecs.open('datasets/static/validate/lpf-schema.json', 'r', 'utf8').read())
  fout = codecs.open('validate-lpf-result.txt', 'w', 'utf8')
  #print()
  #infile=codecs.open('datasets/static/validate/lugares_10_citations.jsonld','r','utf-8')
  #infile=codecs.open('example_data/alcedo_200errors.tsv','r','utf-8')
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
        result["errors"].append({"feat":countrows-1,'error':err[1].args[0]})

  fout.write(json.dumps(result["errors"]))
  fout.close()
  result['count'] = countrows
  return result

def validate_csv(infile):
  #infile=codecs.open('example_data/alcedo_200errors.tsv','r','utf-8')
  infile=codecs.open('example_data/epirus_60errors.tsv','r','utf-8')
  # TODO: Pandas?
  # some WKT is big
  csv.field_size_limit(100000000)
  result = {'format':'delimited','errors':[]}
  
  # allowed fields
  allowed = set(['id','title','title_source','title_uri','ccodes','matches','variants','types','aat_types',
                'parent_name','parent_id','lon','lat','geowkt','geo_source','geo_id','start','end'])
  # required fields
  required = set(['id', 'title', 'title_source'])

  # learn delimiter ['\t','|']
  # TODO: falling back to tab if it fails; need more stable approach
  try:
    dialect = csv.Sniffer().sniff(infile.read(16000),['\t','|'])
    result['delimiter'] = 'tab' if dialect.delimiter == '\t' else dialect.delimiter
  except:
    result['errors'] = "delimiter"
    print("can't tell delimiter")
    # break out immediately
    #return result
    #dialect = '\t'
    #result['delimiter'] = 'tab'

  reader = csv.reader(infile, dialect)
  infile.seek(0)
  result['count'] = sum(1 for row in reader)

  # get header
  infile.seek(0)
  header = next(reader, None) # ordered
  first = next(reader, None)
  result['columns'] = header
  headerset = set(header) # set for comparison
  
  # are req columns present?
  if not required <= headerset:
    result['errors'].append({'req':'required column(s) missing: '+str(list(required-headerset))})
  
  # are all column names valid (allowed)?
  if not headerset <= allowed:
    result['errors'].append({'req':'invalid column name(s): '+str(list(headerset-allowed))})

  # row by row
  infile.seek(0)
  next(reader) # skip header row
  latlon_errors = []
  delim_errors = []
  for i,row in enumerate(reader):
    
    # lon and lat must be a pair and decimal degrees
    empties=['' for n in [row[header.index('lat')],row[header.index('lon')]] if n=='']
    if len(empties) ==1: # missing an x or y
      latlon_errors.append(str(i+2))
      #print('line',i,'missing either a lat or lon')
      
    # multiple value fields semicolon-delimited
    multis = set(['ccodes', 'variants', 'types', 'aat_types', 'matches'])
    # which multi-val fields are present?
    multis = list(multis & headerset)
    for field in multis:
      val = row[header.index(field)]
      if len(val.split(',')) > 1 or len(val.split(', ')) > 1 or \
         len(val.split('|')) > 1 or len(val.split('| ')) > 1:
        # comma or pipe delimited, no-no!
        delim_errors.append([i,field])
    
  if len(latlon_errors) > 0:
    result['errors'].append({"latlon":"Row(s) missing lat OR lon: "+', '.join(latlon_errors)})
  if len(delim_errors) > 0:
    error_fields = []
    for field in delim_errors:
      result['errors'].append( {"delim":"Invalid delimiter for field "+field[1]+' in row '+str(field[0]+2)} )
  
  if len(result['errors']) == 0:
    print('validate_csv(): no errors')
  else:
    print('validate_csv() got errors')
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
  from shapely.geometry import mapping
  gw = wkt.loads(g)
  feature = mapping(gw)
  print('wkt, feature',g, feature)
  return feature

def myteam(me):
  myteam=[]
  for g in me.groups.all():
    for u in g.user_set.all():
      myteam.append(u)
  return myteam

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
  # merge parent country/ies & parents
  if len(qobj['countries']) > 0:
    for c in qobj['countries']:
      best.append(parents.ccodes[0][c]['tgnlabel'])
  if len(qobj['parents']) > 0:
    for p in qobj['parents']:
      best.append(p)
  if len(best) == 0:
    best = ['World']
  return best

# wikidata Qs from ccodes
# TODO: consolidate hashes
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
