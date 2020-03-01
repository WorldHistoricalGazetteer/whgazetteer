from django.shortcuts import get_object_or_404

from pathlib import Path
import codecs, datetime, sys, csv, os, pprint, chardet
import simplejson as json
from goodtables import validate as gvalidate
from shapely import wkt
from datasets.models import DatasetFile, DatasetUser
from datasets.static.hashes import aat, parents
from jsonschema import Draft7Validator, draft7_format_checker, validate
pp = pprint.PrettyPrinter(indent=1)
import pandas as pd

# use jsonschema to validate Linked Places json-ld
# format ['coll' (FeatureCollection) | 'lines' (json-lines)]
#def validate_lpf(infile,format):
def validate_lpf(tempfn,format):
  # TODO: handle json-lines
  # TODO: create v1.1 schema; phase out v1.0
  #wd = '/Users/karlg/Documents/Repos/_whgazetteer/'
  schema = json.loads(codecs.open('datasets/static/validate/schema_lpf_v1.0.json','r','utf8').read())
  # rename tempfn
  newfn = tempfn+'.jsonld'
  os.rename(tempfn,newfn)
  #infile = codecs.open(tempfn, 'r', 'utf8')
  infile = codecs.open(newfn, 'r', 'utf8')
  #fout = codecs.open('validate-lpf-result.txt','w','utf8')
  result = {"format":"lpf","errors":[]}
  [countrows,count_ok] = [0,0]
  
  jdata = json.loads(infile.read())
  if ['type', '@context', 'features'] != list(jdata.keys()) \
     or jdata['type'] != 'FeatureCollection' \
     or len(jdata['features']) == 0:
    print('not valid GeoJSON-LD')
  else:
    for feat in jdata['features']:
      countrows +=1
      #print(feat['properties']['title'])
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

  #fout.write(json.dumps(result["errors"]))
  #fout.close()
  result['count'] = countrows
  print('validate_lpf() result',result)
  return result

# validate LP-TSV file
def validate_tsv(tempfn):
  result = {"errors":[],"format":"delimited"}
  # TODO: detect encoding
  #enc = chardet.detect(open(tempfn,'rb').read())
  #print('encoding is',enc)
  #if enc['encoding'] != 'utf-8':
    #result['errors'].append({'message':'File is not utf-8 encoded'})
    #return result
  newfn = tempfn+'.tsv'
  os.rename(tempfn,newfn)
  print('tempfn,newfn',tempfn,newfn)
  schema_lptsv = json.loads(codecs.open('datasets/static/validate/schema_tsv.json', 'r', 'utf8').read())
  report = gvalidate(newfn,schema=schema_lptsv,order_fields=True)
  #pp.pprint(report)  
  #print('error count',report['error-count'])
  result['count'] = report['tables'][0]['row-count']
  result['columns'] = report['tables'][0]['headers']
  for e in report['tables'][0]['errors']:
    if e['code'] not in ["blank-header","missing-header"]:
      result["errors"].append(e)
  print('validate_tsv() result',result)
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
def makeCoords(lonstr,latstr):
  print(type(lonstr),latstr)
  lon = float(lonstr) if lonstr not in ['','nan'] else ''
  lat = float(latstr) if latstr not in ['','nan'] else ''
  #lon = float(lonstr) if lonstr != '' else ''
  #lat = float(latstr) if latstr != '' else ''
  coords = [] if (lonstr == ''  or latstr == '') else [lon,lat]
  return coords
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
  print('arr,what',arr, what)
  qids=[]
  if what == 'ccodes':
    from datasets.static.hashes.parents import ccodes
    for c in arr:
      qids.append('wd:'+ccodes[0][c]['wdid'])
  elif what == 'types':
    if len(arr) == 0:
      qids.append('wd:Q486972')
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

# ABANDONED for views.ds_compare
#def compare(dsid):
  ## get last two uploaded DatasetFile instances for this dataset
  ## a is the current data, b is newly uploaded
  ##file_a = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-rev')[1]
  ## TODO: for testing, a is always the initial
  #file_a = get_object_or_404(DatasetFile,dataset_id_id=dsid,rev=1)
  #file_b = DatasetFile.objects.filter(dataset_id_id=dsid).order_by('-rev')[0]
  #fn_a = file_a.file.name
  #fn_b = file_b.file.name
  #print('dsid; files to compare():',dsid,fn_a,fn_b)
  #print('files exist?',Path('media/'+fn_a).exists(),Path('media/'+fn_b).exists())
  #adf = pd.read_csv('media/'+fn_a,delimiter='\t')
  ##print('adf',adf)
  #bdf = pd.read_csv('/Users/karlg/Documents/Repos/_whgazetteer/media/'+fn_b,delimiter='\t')
  ##try:
    ##bdf = pd.read_csv('media/'+fn_b,delimiter='\t')
  ##except:
    ##sys.exit(sys.exc_info())
  #ids_a = adf['id'].tolist()
  #ids_b = bdf['id'].tolist()
  #resobj={"count_new":len(ids_b),'count_diff':len(ids_b)-len(ids_a)}
  ## new or removed columns?
  #col_del = list(set(adf.columns)-set(bdf.columns))
  #col_add = list(set(bdf.columns)-set(adf.columns))
  #resobj['col_add']=col_add
  #resobj['col_del']=col_del
  #resobj['rows_add']=list(set(ids_b)-set(ids_a))
  #resobj['rows_del']=list(set(ids_a)-set(ids_b))
  ## build a little blurb
  #text='The revised dataset has '+str(resobj['count_new'])+' records, a difference of '+str(resobj['count_diff'])+". Columns "
  #text += 'to add: '+str(resobj['col_add']) + '. 'if len(resobj['col_add']) > 0 else \
    #'to remove: '+ str(resobj['col_del'])+'. ' if len(resobj['col_del']) > 0 \
              #else "remain the same. "
  #text += 'Records to be added: '+str(resobj['rows_add'])+'. ' if len(resobj['rows_add'])>0 else ''
  #text += 'Records to be removed: '+str(resobj['rows_del'])+'. ' if len(resobj['rows_del'])>0 else ''
  #text += 'All records with an ID matching one in the existing dataset will be replaced.'  
  #resobj['text'] = text
  
  #return resobj
