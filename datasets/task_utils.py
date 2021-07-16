# THESE ARE DUPLICATED FROM utils.py
# b/c of an import looping problems
# TODO: called only from tasks.py?
import time

def TestScheduler():
  import time
  for i in range(0,29):
    starttime = time()
    print("test")
    time.sleep(1.0 - ((time() - starttime) % 1.0))

#makeNow, HitRecord, bestParent, post_recon_update, getQ, parse_wkt, hully, elapsed
def makeNow():
  ts = time.time()
  sttime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
  return sttime

class HitRecord(object):
  def __init__(self, place_id, dataset, src_id, title):
    self.place_id = place_id
    self.src_id = src_id
    self.title = title
    self.dataset = dataset

  def __str__(self):
    import json
    return json.dumps(str(self.__dict__))    

  def toJSON(self):
    import json
    return json.loads(json.dumps(self.__dict__,indent=2))

def bestParent(qobj, flag=False):
  # applicable for tgn only
  best = []
  #print('qobj in bestParent',qobj)
  # merge parent country/ies & parents
  if len(qobj['countries']) > 0 and qobj['countries'][0] != '':
    for c in qobj['countries']:
      best.append(parents.ccodes[0][c.upper()]['tgnlabel'])
  if len(qobj['parents']) > 0:
    for p in qobj['parents']:
      best.append(p)
  if len(best) == 0:
    best = ['World']
  return best

# log recon action & update status
def post_recon_update(ds, user, task):
  if task == 'idx':
    ds.ds_status = 'indexed' if ds.unindexed == 0 else 'accessioning'  
  else:
    ds.ds_status = 'reconciling'
  ds.save()
  
  # recon task has completed, log it
  logobj = Log.objects.create(
    category = 'dataset',
    logtype = 'ds_recon',
    subtype = 'align_'+task,
    dataset_id = ds.id,
    user_id = user.id
  )
  logobj.save()
  print('post_recon_update() logobj',logobj)

# wikidata Qs from ccodes
#TODO: consolidate hashes
def getQ(arr,what):
  #print('arr,what',arr, what)
  qids=[]
  if what == 'ccodes':
    from datasets.static.hashes.parents import ccodes
    for c in arr:
      if c.upper() in ccodes[0]:
        qids.append('wd:'+ccodes[0][c.upper()]['wdid'].upper())
  elif what == 'types':
    if len(arr) == 0:
      qids.append('wd:Q486972')
    for t in arr:
      if t in aat_q.qnums:
        for q in aat_q.qnums[t]:
          qids.append('wd:'+q)
      else:
        qids.append('wd:Q486972')
  return list(set(qids))


def parse_wkt(g):
  #print('wkt',g)
  from shapely.geometry import mapping
  gw = wkt.loads(g)
  feature = json.loads(json.dumps(mapping(gw)))
  #print('wkt, feature',g, feature)
  return feature


#*# test loads
#from django.shortcuts import get_object_or_404
#from places.models import Place
#place=get_object_or_404(Place,pk=6591626)
#g_list =[g.jsonb for g in place.geoms.all()]
#*#
def hully(g_list):
  from django.contrib.gis.geos import GeometryCollection, GEOSGeometry, MultiPoint

  # maybe mixed bag
  #types = list(set([g['type'] for g in g_list]))

      
  # make a hull from any geometry
  # 1 point -> Point; 2 points -> LineString; >2 -> Polygon
  try:
    hull=GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).convex_hull
    #hull=GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list_b]).convex_hull
  except:
    print('hully() failed on g_list', g_list)
    
  if hull.geom_type in ['Point', 'LineString', 'Polygon']:
    # buffer hull, but only a little if near meridian
    coll = GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).simplify()
    #longs = list(c[0] for c in coll.coords)
    longs = list(c[0] for c in flatten(coll.coords))
    try:
      if len([i for i in longs if i >= 175]) == 0:
        hull = hull.buffer(1.4) # ~100km radius
      else:
        hull = hull.buffer(0.1)
    except:
      print('hully buffer error longs:', longs )
  #print(hull.geojson)    
  return json.loads(hull.geojson) if hull.geojson !=None else []

def elapsed(delta):
  minutes, seconds = divmod(delta.seconds, 60)
  return '{:02}:{:02}'.format(int(minutes), int(seconds))


