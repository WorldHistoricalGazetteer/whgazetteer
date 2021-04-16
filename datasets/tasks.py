# celery reconciliation tasks [align_tgn(), align_wdlocal(), align_idx(), align_whg] and related functions
from __future__ import absolute_import, unicode_literals
from celery.decorators import task # this is @task decorator
from django_celery_results.models import TaskResult
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
#from django.forms.models import model_to_dict
from django.shortcuts import get_object_or_404
from django.contrib.auth.models import User
from django.contrib.gis.geos import Polygon, Point, LineString
#import logging
##
import  datetime, json, os, re, sys #, codecs, time, csv, random
from copy import deepcopy
#from pprint import pprint
from areas.models import Area
from elastic.es_utils import makeDoc
from datasets.models import Dataset, Hit
#from datasets.static.regions import regions as region_hash
from datasets.static.hashes.parents import ccodes as cchash
from datasets.static.hashes.qtypes import qtypes
from datasets.utils import *
from places.models import Place
##
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
##

@task(name="task_emailer")
def task_emailer(tid, dslabel, username, email, counthit, totalhits):
  print('emailer tid, dslabel, username, email, counthit, totalhits',tid, dslabel, username, email, counthit, totalhits)
  # TODO: sometimes a valid tid is not recognized (race?)
  time.sleep(5)
  try:
    task = get_object_or_404(TaskResult, task_id=tid) or False
    tasklabel = 'Wikidata' if task.task_name[6:8]=='wd' else \
      'Getty TGN' if task.task_name.endswith('tgn') else 'WHGazetteer'
    if task.status == "FAILURE":
      fail_msg = task.result['exc_message']
      text_content="Greetings "+username+"! Unfortunately, your "+tasklabel+" reconciliation task has completed with status: "+ \
        task.status+". \nError: "+fail_msg+"\nWHG staff have been notified. We will troubleshoot the issue and get back to you."
      html_content_fail="<h3>Greetings, "+username+"</h3> <p>Unfortunately, your <b>"+tasklabel+"</b> reconciliation task for the <b>"+ds.label+"</b> dataset has completed with status: "+ task.status+".</p><p>Error: "+fail_msg+". WHG staff have been notified. We will troubleshoot the issue and get back to you soon.</p>"
    else:
      text_content="Greetings "+username+"! Your "+tasklabel+" reconciliation task has completed with status: "+ \
        task.status+". \n"+str(counthit)+" records got a total of "+str(totalhits)+" hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
      html_content_success="<h3>Greetings, "+username+"</h3> <p>Your <b>"+tasklabel+"</b> reconciliation task for the <b>"+dslabel+"</b> dataset has completed with status: "+ task.status+". "+str(counthit)+" records got a total of "+str(totalhits)+" hits.</p>" + \
        "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"
  except:
    print('task lookup in task_emailer() failed on tid', tid, 'how come?')
    text_content="Greetings "+username+"! Your reconciliation task for the <b>"+dslabel+"</b> dataset has completed.\n"+ \
      str(counthit)+" records got a total of "+str(totalhits)+" hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
    html_content_success="<h3>Greetings, "+username+"</h3> <p>Your reconciliation task for the <b>"+dslabel+"</b> dataset has completed. "+str(counthit)+" records got a total of "+str(totalhits)+" hits.</p>" + \
      "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"
    
  subject, from_email = 'WHG reconciliation result', 'whgazetteer@gmail.com'
  msg = EmailMultiAlternatives(
    subject, 
    text_content, 
    from_email, 
    #[email,'karl.geog@gmail.com'] if task.status=='SUCCESS' else [email,'karl.geog@gmail.com'])
    [email])
  msg.bcc = ['karl@kgeographer.com']
  msg.attach_alternative(html_content_success if task and task.status == 'SUCCESS' else html_content_fail, "text/html")
  msg.send(fail_silently=False)
  
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
  #print('codes in ccDecode',codes)
  for c in codes:
    countries.append(cchash[0][c]['gnlabel'])
  return countries
  
# generate a language-dependent {name} ({en}) from wikidata variants
def wdTitle(variants, lang):
  if len(variants) == 0:
    return 'unnamed'
  else:
    vl_en=next( (v for v in variants if v['lang'] == 'en'), None)#; print(vl_en)
    vl_pref=next( (v for v in variants if v['lang'] == lang), None)#; print(vl_pref)
    vl_first=next( (v for v in variants ), None); print(vl_first)
  
    title = vl_pref['names'][0] + (' (' + vl_en['names'][0] + ')' if vl_en else '') \
      if vl_pref and lang != 'en' else vl_en['names'][0] if vl_en else vl_first['names'][0]
    return title

def wdDescriptions(descrips, lang):
  dpref=next( (v for v in descrips if v['lang'] == lang), None)
  dstd=next( (v for v in descrips if v['lang'] == 'en'), None)

  result = [dstd, dpref] if lang != 'en' else [dstd] \
    if dstd else []
  return result

def profileHit(hit):
  #print('_source keys',hit['_source'].keys())
  _id = hit['_id']
  src = hit['_source']
  pid = src['place_id']
  
  relation = src['relation']
  profile = {
    '_id':_id,'pid':pid,'title':src['title'],
    'pass': hit['pass'], 'role':relation['name'],
    'dataset':src['dataset'],
    'score':hit['_score']
  }
  profile['parent'] = relation['parent'] if \
    relation['name']=='child' else None
  profile['children'] = src['children'] if \
    relation['name']=='parent' else None
  profile['minmax'] = [src['minmax']['gte'],src['minmax']['lte']] if type(src['minmax']) == dict else None
  profile['links'] = [l['identifier'] for l in src['links']]\
    if len(src['links'])>0 else None
  profile['countries'] = ccDecode(src['ccodes'])
  profile['variants'] = [n['toponym'] for n in src['names']]
  profile['types'] = [t['sourceLabel'] for t in src['types']]
  if len(src['descriptions']) > 0:
    profile['descriptions'] = [d['value'] for d in src['descriptions']]
  profile['geoms'] = [{'id':pid, 'ds':src['dataset'], 'coordinates':g['location']['coordinates'], 'type':g['location']['type']} for g in src['geoms']]
  
  return profile

# create cluster payload from set of hits for a place
def normalize_whg(hits):
  result = []
  src = [h['_source'] for h in hits]
  parents = [h for h in hits if 'whg_id' in h['_source']]
  children = [h for h in hits if 'whg_id' not in h['_source']]
  titles=list(set([h['_source']['title'] for h in hits]))
  [links, countries] = [[],[]]
  for h in src:
    countries.append(ccDecode(h['ccodes']))
    for l in h['links']:
      links.append(l['identifier'])
  # each parent seeds cluster of >=1 hit
  for par in parents:
    kid_ids = par['_source']['children'] or None
    kids = [c['_source'] for c in children if c['_id'] in kid_ids]
    cluster={
      "whg_id": par["_id"],
      "titles": titles,
      "countries":list(set(countries)), 
      "links":list(set(links)), 
      "geoms":[],
      "sources":[]
    }
    result.append(cluster)
  return result.toJSON()
    
    
# normalize hit json from any authority
# language relevant only for wikidata local)
def normalize(h, auth, language=None):
  if auth.startswith('whg'):
    # for whg h is full hit, not only _source
    hit = deepcopy(h)
    h = hit['_source']
    #_id = hit['_id']
    # build a json object, for Hit.json field
    rec = HitRecord(
      h['place_id'], 
      h['dataset'],
      h['src_id'], 
      h['title']
    )
    print('"rec" HitRecord',rec)
    rec.score = hit['_score']
    rec.passnum = hit['pass'][:5]
    
    # only parents have whg_id
    if 'whg_id' in h:
      rec.whg_id = h['whg_id']
    
    # add elements if non-empty in index record
    rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
    # TODO: fix grungy hack (index has both src_label and sourceLabel)
    key = 'src_label' if 'src_label' in h['types'][0] else 'sourceLabel'      
    rec.types = [t['label']+' ('+t[key]  +')' if t['label']!=None else t[key] \
                for t in h['types']] if len(h['types']) > 0 else []
    # TODO: rewrite ccDecode to handle all conditions coming from index
    # ccodes might be [] or [''] or ['ZZ', ...]
    rec.countries = ccDecode(h['ccodes']) if ('ccodes' in h.keys() and (len(h['ccodes']) > 0 and h['ccodes'][0] !='')) else []
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
    rec.links = [l['identifier'] for l in h['links']] \
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
      #if 'links' in h.keys():
        #for l in h['links']:
          #links.append('closeMatch: '+l)
      #  place_id, dataset, src_id, title
      rec = HitRecord(-1, 'wd', h['place']['value'][31:], h['placeLabel']['value'])
      print('"rec" HitRecord',rec)      
      rec.variants = []
      rec.types = h['types']['value'] if 'types' in h.keys() else []
      rec.ccodes = [h['countryLabel']['value']]
      rec.parents =h['parents']['value'] if 'parents' in h.keys() else []
      rec.geoms = locs if len(locs)>0 else []
      rec.links = links if len(links)>0 else []
      rec.minmax = []
      rec.inception = parseDateTime(h['inception']['value']) if 'inception' in h.keys() else ''
    except:
      print("normalize(wd) error:", h['place']['value'][31:], sys.exc_info())    
  elif auth == 'wdlocal':
    # hit['_source'] keys(): ['id', 'type', 'modified', 'descriptions', 'claims', 'sitelinks', 'variants', 'minmax', 'types', 'location'] 
    try:
      print('h in normalize',h)
      # TODO: do it in index?
      variants=h['variants']
      title = wdTitle(variants, language)

      #  place_id, dataset, src_id, title
      rec = HitRecord(-1, 'wd', h['id'], title)
      print('"rec" HitRecord',rec)
      
      # list of variant@lang (excldes chosen title)
      #variants= [{'lang': 'ru', 'names': ['Toamasina', 'Туамасина']},{'lang': 'ja', 'names': ['タマタヴ', 'トゥアマシナ']}]
      v_array=[]
      for v in variants:
        for n in v['names']:
          if n != title:
            v_array.append(n+'@'+v['lang'])
      rec.variants = v_array
            
      if 'location' in h.keys():
        # single MultiPoint geometry
        loc = h['location']
        loc['id'] = h['id']
        loc['ds'] = 'wd'        
        # single MultiPoint geom if exists
        rec.geoms = [loc]

      # turn these identifier claims into links
      qlinks = {'P1566':'gn', 'P1584':'pl', 'P244':'loc', 'P1667':'tgn', 'P214':'viaf', 'P268':'bnf', 'P1667':'tgn', 'P2503':'gov', 'P1871':'cerl', 'P227':'gnd'}
      links=[]
      hlinks = list(
        set(h['claims'].keys()) & set(qlinks.keys()))
      if len(hlinks) > 0:
        for l in hlinks:
          #links.append('closeMatch: '+qlinks[l]+':'+str(h['claims'][l][0]))
          links.append(qlinks[l]+':'+str(h['claims'][l][0]))

      # add en and FIRST {language} wikipedia sitelink OR first sitelink
      wplinks = []
      wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == 'en']
      if language != 'en':
        wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == language]
      if len(wplinks) == 0 and len(h['sitelinks']) > 0:
        wplinks += [h['sitelinks'][0]['title']]
        
      links += ['primaryTopicOf: wp:'+l for l in set(wplinks)]

      rec.links = links
      #print('rec.links',rec.links)

      # look up Q class labels
      htypes = set(h['claims']['P31'])
      qtypekeys = set([t[0] for t in qtypes.items()])
      rec.types = [qtypes[t] for t in list(set(htypes & qtypekeys))]

      # countries
      rec.ccodes = [
        ccodes[0][c]['gnlabel'] for c in ccodes[0] \
          if ccodes[0][c]['wdid'] in h['claims']['P17']
      ]
      
      # include en + native lang if not en
      #print('h["descriptions"]',h['descriptions'])
      rec.descriptions = wdDescriptions(h['descriptions'], language) if 'descriptions' in h.keys() else []
      
      # not applicable
      rec.parents = []
      
      # no minmax in hit if no inception value(s)
      rec.minmax = [h['minmax']['gte'],h['minmax']['lte']] if 'minmax' in h else []
    except:
      # TODO: log error
      print("normalize(wdlocal) error:", h['id'], sys.exc_info())
      print('h in normalize', h)

  elif auth == 'tgn':
    rec = HitRecord(-1, 'tgn', h['tgnid'], h['title'])
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
    rec.minmax = []
    rec.links = []
    print(rec)
  print('normalized hit record',rec.toJSON())
  # TODO: raise any errors
  return rec.toJSON()

# ***
# elasticsearch filter from Area (types: predefined, ccodes, drawn)
# e.g. {'type': ['drawn'], 'id': ['128']}
# called from: es_lookup_tgn(), es_lookup_whg(), search.SearchView(), 
# FUTURE: parse multiple areas
# ***
def get_bounds_filter(bounds,idx):
  #print('bounds in get_bounds_filter()',bounds)
  id = bounds['id'][0]
  #areatype = bounds['type'][0]
  area = Area.objects.get(id = id)
  # 
  # NOTE: 'whg' is generic, references current index for WHG, vs. 'tgn' for example
  geofield = "geoms.location" if idx == 'whg' else "location"
  filter = { "geo_shape": {
    geofield: {
        "shape": {
          #"type": "polygon" if areatype in ['ccodes','drawn'] else "multipolygon",
          "type": area.geojson['type'],
          "coordinates": area.geojson['coordinates']
        },
        "relation": "intersects" if idx=='whg' else 'within' # within | intersects | contains
      }
  }} 
  return filter


# from align_wd (wikidata)
# b: wikidata binding; passnum: 1 or 2; ds (obj); pid,srcid,title > whg Place instance
def writeHit(b, passnum, ds, pid, srcid, title):
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

# ***
# manage, perform SPARQL reconcile to remote wikidata
# ***
@task(name="align_wd")
def align_wd(pk, *args, **kwargs):
  task_id = align_wd.request.id
  ds = get_object_or_404(Dataset, id=pk)
  #bounds = kwargs['bounds']
  
  from SPARQLWrapper import SPARQLWrapper, JSON
  #import sys, os, re, json, codecs, time, datetime, geojson
  #from datasets.align_utils import classy, roundy, fixName
  import sys, json, time, datetime, geojson
  from datasets.align_utils import fixName
  from shapely.geometry import shape
  
  #endpoint = "http://dbpedia.org/sparql"
  endpoint = "https://query.wikidata.org/sparql"
  sparql = SPARQLWrapper(endpoint)
  
  start = time.time()
  timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M"); print(timestamp)

  hit_parade = {"summary": {}}
    
  [count,count_skipped] = [0,0]
  global count_hit, count_nohit, total_hits, count_p1, count_p2
  [count_hit, count_nohit, total_hits, count_p1, count_p2] = [0,0,0,0,0]
  
  for place in ds.places.all():
    # e.g. place=get_object_or_404(Place, id=6369031) # Aachen
    count +=1
    place_id = place.id
    src_id = place.src_id
    title = fixName(place.title)
    qobj = {"place_id":place_id,"src_id":place.src_id,"title":fixName(place.title)}
    [variants,geoms,types,ccodes,parents]=[[],[],[],[],[]]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c.upper())
    qobj['countries'] = ccodes

    # types (pull aat_ids from types if there)
    for t in place.types.all():
      try:
        id = t.jsonb['identifier']
        if id !=None:
          types.append(int(id[4:]) if id.startswith('aat:') else int(id))      
      except:
        print('no aat_id for', t.jsonb['sourceLabel'])
    qobj['placetypes'] = types

    # names
    for name in place.names.all():
      #variants.append(name.toponym)
      variants.append(name.toponym.strip())
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

    print('qobj input for Wikidata sparql query',qobj)

    # lists for wikidata sparql
    variants = ' '.join(['"'+n+'"' for n in qobj['variants']])
    # countries, placetypes if they're there
    countries = ', '.join([c for c in getQ(qobj['countries'],'ccodes')]) \
      if len(qobj['countries'])>0 and qobj['countries'] != [''] else ''
    # types
    placetypes = ', '.join([t for t in getQ(qobj['placetypes'],'types')])

    print('variants,countries,placetype',variants,countries,placetypes)
    
    # TODO admin parent P131, retrieve wiki article name, country P17, ??
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
    #
    # geom here is a hull
    # area constraint is not used!
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
      q += 'FILTER (?country in (%s)) .'% (countries)
    # 
    # placetype is 1st aat_id if any
    # separate pass1 and pass2 queries here
    qpass1 = q+'''
      ?place wdt:P31/wdt:P279* ?placeType .
      FILTER (?placeType in (%s)) . }
    '''%(placetypes)
    
    qpass2 = q+'''
      ?place wdt:P31 ?placeType . }
    '''
      
    # 
    qpass1 += '''
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnids ?gnids ?viafids ?locids
      ORDER BY ASC(?num) LIMIT 5
    '''

    # return type, but no type filter 
    qpass2 += '''
      GROUP BY ?place ?placeLabel ?countryLabel ?inception ?tgnid ?gnid ?viafid ?locid
      ORDER BY ASC(?num) LIMIT 10'''

    def runQuery():
      global count_hit, count_nohit, total_hits, count_p1, count_p2
      sparql.setQuery(qpass1)
      sparql.setReturnFormat(JSON)
      sparql.addCustomHttpHeader('User-Agent','WHGazetteer/1.1 (http://whgazetteer.org; karl@kgeographer.org)')
  
      # pass1
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
        # no hits, pass2 drops type filter
        sparql.setQuery(qpass2)
        sparql.setReturnFormat(JSON)
        sparql.addCustomHttpHeader('User-Agent','WHGazetteer/1.0 (http://whgazetteer.org; karl@kgeographer.org)')
        try:
          bindings = sparql.query().convert()["results"]["bindings"]
        except ConnectionError as exc:
          print('pass2 error',sys.exc_info())
          print('qpass2', qpass2)
          if exc.status_code == 429:
            self.retry(exc=exc, countdown=61)
        if len(bindings) == 0:
          count_nohit +=1 # tried 2 passes, nothing
        else:
          count_hit+=1 # got at least 1
          count_p2+=1 # it's pass2
          print(str(len(bindings))+' bindings, pass2: '+str(place_id), qpass2)
          for b in bindings:
            # could be anything, see if it has a location
            if b['locations']['value'] != '':
              total_hits+=1 # add to total
              writeHit(b, 'pass2', ds,place_id, src_id,title)
              #fout1.write(str(place_id)+'\tpass2:'+' '+str(b)+'\n')   
              print('pass2 hit binding:', b)
    # any exception, go on to the next
    try:
      runQuery()
    except:
      print('runQuery() failed, place#', place_id)
      print('runQuery() error:', sys.exc_info())
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

  # email owner when complete
  task_emailer.delay(
    task_id,
    ds.label, 
    ds.owner.username, 
    ds.owner.email, 
    count_hit, 
    total_hits)
  
  return hit_parade['summary']
# ***
# performs elasticsearch > tgn queries
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
          #"coordinates" : location['coordinates']
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
    #res1 = es.search(index="tgn", body = q1)
    hits1 = res1['hits']['hits']
  except:
    print('pass1 error:',sys.exc_info())
  if len(hits1) > 0:
    for hit in hits1:
      hit_count +=1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
  elif len(hits1) == 0:
    print('q1 no result:)',q1)
    # /\/\/\/\/\/
    # pass2: revert to qbase{} (drops geom)
    # /\/\/\/\/\/  
    q2 = qbase
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
      print('q2 no result:)',q2)
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
        print('q3 no result:)',q3)
        result_obj['missed'] = qobj['place_id']
  result_obj['hit_count'] = hit_count
  return result_obj

# ***
# manage reconcile to tgn
# ***
@task(name="align_tgn")
def align_tgn(pk, *args, **kwargs):
  task_id = align_tgn.request.id
  print('task_id', task_id)
  ds = get_object_or_404(Dataset, id=pk)
  user = get_object_or_404(User, pk=kwargs['user'])
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  print('args, kwargs from align_tgn() task',args,kwargs)
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
      ccodes.append(c.upper())
    qobj['countries'] = place.ccodes

    # types (Getty AAT identifiers)
    # all have 'aat:' prefixes
    for t in place.types.all():
      #types.append('aat:'+t.jsonb['identifier'])
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
    else:
      qobj['parents'] = []

    # geoms
    if len(place.geoms.all()) > 0:
      g_list =[g.jsonb for g in place.geoms.all()]
      # make everything a simple polygon hull for spatial filter
      qobj['geom'] = hully(g_list)

    ## run pass1-pass3 ES queries
    #print('qobj in align_tgn()',qobj)
    result_obj = es_lookup_tgn(qobj, bounds=bounds)
      
    if result_obj['hit_count'] == 0:
      count_nohit +=1
      nohits.append(result_obj['missed'])
    else:
      count_hit +=1
      total_hits += len(result_obj['hits'])
      #print("hit[0]: ",result_obj['hits'][0]['_source'])  
      #print('hits from align_tgn',result_obj['hits'])
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
          # prepare for consistent display in review screen
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
  
  # create log entry and update ds status
  post_recon_update(ds, user, 'tgn')

  # email owner when complete
  task_emailer.delay(
    task_id,
    ds.label,
    user.username,
    user.email,
    count_hit,
    total_hits  
  )  
  
  return hit_parade['summary']




#
# test stuff
#bounds={'type': ['userarea'], 'id': ['0']}
#from datasets.static.hashes import aat, parents, aat_q
#from datasets.utils import getQ
#from areas.models import Area
#from places.models import Place
#from datasets.tasks import get_bounds_filter
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# ***
# performs elasticsearch > wdlocal queries
# ***
def es_lookup_wdlocal(qobj, *args, **kwargs):
  #bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']
  hit_count = 0

  # empty result object
  result_obj = {
    'place_id': qobj['place_id'], 
    'hits':[],'missed':-1, 'total_hits':-1}  

  # names (distinct, w/o language)
  variants = list(set(qobj['variants']))

  # types
  # wikidata Q ids for aat_ids, ccodes; strip wd: prefix
  # if no aatids, returns ['Q486972'] (human settlement)
  qtypes = [t[3:] for t in getQ(qobj['placetypes'],'types')]

  # prep spatial 
  
  # if no ccodes, returns []
  countries = [t[3:] for t in getQ(qobj['countries'],'ccodes')]
  
  has_bounds = bounds['id'] != ['0']
  has_geom = 'geom' in qobj.keys()
  has_countries = len(countries) > 0
  if has_bounds:
    area_filter = get_bounds_filter(bounds,'wd')
  if has_geom:
    # qobj['geom'] always a polygon hull
    shape_filter = { "geo_shape": {
      "location": {
        "shape": {
          "type": qobj['geom']['type'],
          "coordinates" : qobj['geom']['coordinates']},
        "relation": "intersects" }
    }}
  if has_countries:
    countries_match = {"terms": {"claims.P17":countries}}
  
  # prelim query: any authid matches?
  # can be accepted without review
  # incoming qobj['authids'] might include
  # a wikidata identifier matching an index _id (Qnnnnnnn)
  # OR an id match in wikidata authids[] e.g. gn:, tgn:, pl:, bnf:, viaf:
  # 
  q0 = {"query": { 
    "bool": {
      "must": [
        #{"terms": {"authids":qobj['authids']}},
        {"bool": {
          "should": [
            {"terms": {"authids": qobj['authids']}},
            # capture any wd: Q ids
            {"terms": {"_id": [i[3:] for i in qobj['authids']] }}
          ],
          "minimum_should_match": 1
        }}
      ]
  }}}

  # base query
  qbase = {"query": { 
    "bool": {
      "must": [
        {"terms": {"variants.names":variants}}
      ],
      # boosts score if matched
      "should":[
        {"terms": {"authids": qobj['authids']}}
      ],
      "filter": []
    }
  }}
  
  # add spatial filter as available in qobj
  if has_geom:
    # shape_filter is polygon hull ~100km diameter
    qbase['query']['bool']['filter'].append(shape_filter)
    if has_countries:
      qbase['query']['bool']['should'].append(countries_match)
  elif has_countries:
    # matches ccodes
    qbase['query']['bool']['must'].append(countries_match)
  elif has_bounds:
    # area_filter (predefined region or study area)
    qbase['query']['bool']['filter'].append(area_filter)
    if has_countries:
      qbase['query']['bool']['should'].append(countries_match)

  
  # q1 = qbase + types
  q1 = deepcopy(qbase)
  q1['query']['bool']['must'].append(    
    {"terms": {"types.id":qtypes}})

  # add fclasses if any, drop types; geom if any remains
  q2 = deepcopy(qbase)
  if len(qobj['fclasses']) > 0:
    q2['query']['bool']['must'].append(
      {"terms": {"fclasses":qobj['fclasses']}})

  # /\/\/\/\/\/
  # pass0 (q0): 
  # must[authid]; match any link
  # /\/\/\/\/\/
  try:
    res0 = es.search(index="wd", body = q0)
    hits0 = res0['hits']['hits']
  except:
    print('pid; pass0 error:', qobj, sys.exc_info())

  if len(hits0) > 0:
    for hit in hits0:
      hit_count +=1
      hit['pass'] = 'pass0'
      result_obj['hits'].append(hit)
  elif len(hits0) == 0:
    print('q0 (no hits)', qobj)
    # /\/\/\/\/\/
    # pass1 (q1): 
    # must[name, placetype]; spatial filter
    # /\/\/\/\/\/
    #print('q1',q1)
    try:
      res1 = es.search(index="wd", body = q1)
      hits1 = res1['hits']['hits']
    except:
      print('pass1 error qobj:', qobj, sys.exc_info())
      print('pass1 error q1:', q1)
      sys.exit()
    if len(hits1) > 0:
      for hit in hits1:
        hit_count +=1
        hit['pass'] = 'pass1'
        result_obj['hits'].append(hit)
    elif len(hits1) == 0:
      # /\/\/\/\/\/
      # pass2: remove type, add fclasses
      # /\/\/\/\/\/  
      print('q1: no hits',q1)
      try:
        res2 = es.search(index="wd", body = q2)
        hits2 = res2['hits']['hits']
      except:
        print('pass2 error qobj', qobj, sys.exc_info())
        print('pass2 error q2', q2)
        sys.exit()
      if len(hits2) > 0:
        for hit in hits2:
          hit_count +=1
          hit['pass'] = 'pass2'
          result_obj['hits'].append(hit)
      elif len(hits2) == 0:
        result_obj['missed'] = str(qobj['place_id']) + ': ' + qobj['title']
        print('q2: no hits',q2)
  result_obj['hit_count'] = hit_count
  return result_obj


# ***
# manage reconcile to local wikidata
# ***
# test stuff
#from django.shortcuts import get_object_or_404
#from datasets.models import Dataset
#from datasets.static.hashes import aat, parents, aat_q
#from datasets.utils import getQ, hully
#from places.models import Place
#place=get_object_or_404(Place, pk = 6591148) #6587009 Cherkasy, UA
#bounds = {'type': ['userarea'], 'id': ['369']}
#from copy import deepcopy
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#
#def is_beta_or_better(user):
  #return user.groups.filter(name__in=['beta', 'admin']).exists()

@task(name="align_wdlocal")
def align_wdlocal(pk, **kwargs):
  task_id = align_wdlocal.request.id
  ds = get_object_or_404(Dataset, id=pk)
  print('kwargs from align_wdlocal() task', kwargs)
  #bounds = {'type': ['userarea'], 'id': ['0']}
  user = get_object_or_404(User, pk=kwargs['user'])
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  language = kwargs['lang']
  start = datetime.datetime.now()
  hit_parade = {"summary": {}, "hits": []}
  [nohits,wdlocal_es_errors,features] = [[],[],[]]
  [count, count_hit, count_nohit, total_hits, count_p0, count_p1, count_p2] = [0,0,0,0,0,0,0]

  # queryset depends on choice of scope in addtask form
  # 
  pids = Hit.objects.filter(dataset_id=pk, authority ='wd',reviewed=False).values_list('place_id',flat=True)
  if scope == 'unreviewed':
    qs = ds.places.filter(id__in=pids)
  elif scope == 'all':
    qs = ds.places.all()

  for place in qs:
    # build query object
    count +=1
    qobj = {"place_id":place.id,
            "src_id":place.src_id,
            "title":place.title,
            "fclasses":place.fclasses or []}

    [variants,geoms,types,ccodes,parents,links]=[[],[],[],[],[],[]]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c.upper())
    qobj['countries'] = place.ccodes

    # types (Getty AAT integer ids if available)
    for t in place.types.all():
      if t.jsonb['identifier'].startswith('aat:'):
        types.append(int(t.jsonb['identifier'].replace('aat:','')) )
    qobj['placetypes'] = types

    # variants
    variants.append(place.title)
    for name in place.names.all():
      variants.append(name.toponym)
    qobj['variants'] = list(set(variants))

    # parents
    # TODO: other relations
    if len(place.related.all()) > 0:
      for rel in place.related.all():
        if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
          parents.append(rel.jsonb['label'])
      qobj['parents'] = parents
    else:
      qobj['parents'] = []

    # geoms
    if len(place.geoms.all()) > 0:
      g_list =[g.jsonb for g in place.geoms.all()]
      # make simple polygon hull for ES shape filter
      qobj['geom'] = hully(g_list)
      # make a representative_point for ES distance
      #qobj['repr_point'] = pointy(g_list)
      

    # 'P1566':'gn', 'P1584':'pleiades', 'P244':'loc', 'P214':'viaf', 'P268':'bnf', 'P1667':'tgn', 'P2503':'gov', 'P1871':'cerl', 'P227':'gnd'
    # links
    if len(place.links.all()) > 0:
      l_list = [l.jsonb['identifier'] for l in place.links.all()]
      qobj['authids'] = l_list
    else:
      qobj['authids'] = []
      
    #
    # run pass0-pass2 ES queries
    #print('qobj in align_wd_local()',qobj)
    result_obj = es_lookup_wdlocal(qobj, bounds=bounds)
      
    if result_obj['hit_count'] == 0:
      count_nohit +=1
      nohits.append(result_obj['missed'])
    else:
      count_hit +=1
      total_hits += len(result_obj['hits'])
      for hit in result_obj['hits']:
        print('pre-write hit', hit)
        if hit['pass'] == 'pass0': 
          count_p0+=1 
        if hit['pass'] == 'pass1': 
          count_p1+=1 
        elif hit['pass'] == 'pass2': 
          count_p2+=1
        hit_parade["hits"].append(hit)
        new = Hit(
          authority = 'wd',
          authrecord_id = hit['_id'],
          dataset = ds,
          place_id = get_object_or_404(Place, id=qobj['place_id']),
          task_id = task_id,
          query_pass = hit['pass'],
          # prepare for consistent display in review screen
          json = normalize(hit['_source'],'wdlocal',language),
          src_id = qobj['src_id'],
          score = hit['_score'],
          #geom = loc,
          reviewed = False,
        )
        new.save()
  end = datetime.datetime.now()

  print('wdlocal ES errors:',wdlocal_es_errors)
  hit_parade['summary'] = {
      'count':count,
      'got_hits':count_hit,
      'total': total_hits, 
      'pass0': count_p0, 
      'pass1': count_p1, 
      'pass2': count_p2, 
      'no_hits': {'count': count_nohit },
      'elapsed': elapsed(end-start)
    }
  print("summary returned",hit_parade['summary'])

  # create log entry and update ds status
  post_recon_update(ds, user, 'wdlocal')
    
  # email owner when complete
  task_emailer.delay(
    task_id,
    ds.label,
    user.username,
    user.email,
    count_hit,
    total_hits
  )
  
  return hit_parade['summary']

"""
# performs elasticsearch queries on whg
"""
def es_lookup_whg(qobj, *args, **kwargs):
  #print('kwargs from es_lookup_whg',kwargs)
  global whg_id
  idx = 'whg'
  #bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']
  hit_count, err_count = [0,0]

  # empty result object
  result_obj = {
    'place_id': qobj['place_id'], 
    'title': qobj['title'], 
    'hits':[], 'missed':-1, 'total_hits':-1
  }  

  # distinct names, w/o language)
  variants = list(set(qobj['variants']))

  # PREP SPATIAL CONSTRAINTS
  has_bounds = bounds['id'] != ['0']
  has_geom = 'geom' in qobj.keys()
  has_countries = len(qobj['countries']) > 0
  if has_bounds:
    area_filter = get_bounds_filter(bounds,'wd')
    print('area_filter', area_filter)
  if has_geom:
    # qobj['geom'] is always a polygon hull
    shape_filter = { "geo_shape": {
      "geoms.location": {
        "shape": {
          "type": qobj['geom']['type'],
          "coordinates" : qobj['geom']['coordinates']},
        "relation": "intersects" }
    }}
    print('shape_filter', shape_filter)
  if has_countries:
    countries_match = {"terms": {"ccodes":qobj['countries']}}
    print('countries_match', countries_match)

  """
  prepare queries from qobj
  """  
  # NEW
  qbase = {"query": { 
    "bool": {
      "must": [
        # must share a variant (strict match)
        {"terms": {"names.toponym": variants}},
      ],
      "should": [
        # weights for shared links, type matches
        {"terms": {"links.identifier": qobj['links'] }},
        {"terms": {"types.identifier": qobj['placetypes']}}
      ],
      # spatial filters added according to what's available
      "filter": []
    }
  }}

  # if fclasses, use broadly as a 'must'
  if len(qobj['fclasses']) > 0:
    # if A, P, or S, use all three
    if len(set(qobj['fclasses']) & set(['A','P','S'])) > 0:
      class_grp = ['A','P','S']
    else: 
      class_grp = qobj['fclasses']
    qbase['query']['bool']['must'].append(
      {"terms": {"fclasses": class_grp}})
    
    
  # if geom, does target intersect ~100km diam polygon hull 
  if has_geom:
    qbase['query']['bool']['filter'].append(shape_filter)
    if has_countries:
      # add weight for country match
      qbase['query']['bool']['should'].append(countries_match)

  # no geom, use country codes if there
  if not has_geom and has_countries:
    qbase['query']['bool']['must'].append(countries_match)
    
  # has no geom but has bounds (region or user study area)
  if not has_geom and has_bounds:
    # area_filter (predefined region or study area)
    qbase['query']['bool']['filter'].append(area_filter)
    if has_countries:
      # add weight for country match
      qbase['query']['bool']['should'].append(countries_match)


  # grab a copy
  q1 = qbase
  print('q1', q1)
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  # pass1 (only one)
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  try:
    res1 = es.search(index=idx, body=q1)
    hits1 = res1['hits']['hits']
  except:
    print("q1, ES error:", q0, sys.exc_info())
  if len(hits1) > 0:
    # shared link(s); return for immed. indexing
    for hit in hits1:
      hit_count +=1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
      result_obj['hit_count'] = hit_count
    return result_obj
  elif len(hits1) == 0:
    # no matches, this will be an index seed
    print('congrats, a seed')
  result_obj['hit_count'] = hit_count
  return result_obj


"""
# reconcile to whg w/no indexing
# returns all potential matches
# (does not stop at shared link)
"""
@task(name="align_whg")
def align_whg(pk, *args, **kwargs):
  print('kwargs from align_whg() task', kwargs)
  print('align_whg.request', align_whg.request)
  
  task_id = align_whg.request.id
  ds = get_object_or_404(Dataset, id=pk)
  user = get_object_or_404(User, id=kwargs['user'])
  idx='whg'
  
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  
  # empty vessels
  hit_parade = {"summary": {}, "hits": []}
  [nohits,whg_es_errors,features] = [[],[],[]]
  [count,count_hit,count_nohit,total_hits,count_p0,count_p1,count_p2] = [0,0,0,0,0,0,0]

  start = datetime.datetime.now()
    
  # queryset depends on choice of scope in addtask form
  qs = ds.places.all() if scope == 'all' else ds.places.all().filter(indexed=False)

  #from places.models import Place
  #from django.shortcuts import get_object_or_404
  #from datasets.utils import hully
  #from datasets.tasks import es_lookup_whg
  #bounds = {'type': ['userarea'], 'id': ['0']}    
  #idx='whg'
  #from pprint import pprint as pp
  #place=get_object_or_404(Place,id=6594341) # Al Madain
 
  for place in qs:    
    print('building qobj for ' + str(place.id) + ': ' + place.title)
    count +=1
    
    """ 
      build query object 'qobj' 
    """
    qobj = {"place_id":place.id, 
            "src_id":place.src_id, 
            "title":place.title,
            "fclasses":place.fclasses or []}
    [links,ccodes,types,variants,parents,geoms]=[[],[],[],[],[],[]]

    # links
    for l in place.links.all():
      links.append(l.jsonb['identifier'])
    qobj['links'] = links

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c)
    qobj['countries'] = list(set(place.ccodes))

    # types (Getty AAT identifiers)
    for t in place.types.all():
      if t.jsonb['identifier'] != None:
        types.append(t.jsonb['identifier'])
      else:
        # no type? use inhabited place, cultural group, site
        types.extend(['aat:300008347','aat:300387171','aat:300000809'])
    qobj['placetypes'] = types

    # variants
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
        

    """
      run es_lookup_whg(qobj): query passes 0, 1, 2
    """
    result_obj = es_lookup_whg(qobj, index=idx, bounds=bounds, place=place)

    # WRITE HITS
    if result_obj['hit_count'] == 0:
      count_nohit +=1
    else:
      count_hit +=1
      total_hits += len(result_obj['hits'])
      #print("hit[0]: ",result_obj['hits'][0]['_source'])  
      #print('hits from align_whg',result_obj['hits'])
      for hit in result_obj['hits']:
        # they are ALL pass1 for now
        print('align_whg hit', hit)
        if hit['pass'] == 'pass1': 
          count_p1+=1
        hit_parade["hits"].append(hit)
        
        loc = hit['_source']['geoms'] if 'geoms' in hit['_source'].keys() else None
        
        new = Hit(
          authority = 'whg',
          authrecord_id = hit['_id'],
          dataset = ds,
          place_id = get_object_or_404(Place, id=qobj['place_id']),
          
          task_id = align_whg.request.id,
          
          query_pass = hit['pass'],
          
          # for review display
          # TODO; tailor for whg case
          json = normalize(hit,'whg'),
          
          src_id = qobj['src_id'],
          score = hit['_score'],
          geom = loc,
          reviewed = False,
        )
        new.save()
  end = datetime.datetime.now()

  print('align_whg ES errors:',whg_es_errors)
  hit_parade['summary'] = {
      'count':count,
      'got_hits':count_hit,
      'total': total_hits, 
      'pass1': count_p1, 
      'no_hits': {'count': count_nohit },
      'elapsed': elapsed(end-start)
    }
  print("align_whg summary returned",hit_parade['summary'])

  # create log entry and update ds status
  post_recon_update(ds, user, 'whg')

  task_emailer.delay(
    task_id,
    ds.label,
    user.username,
    user.email,
    count_hit,
    total_hits
  )
  
  return hit_parade['summary']


"""
# accession to whg
#
#
"""
@task(name="align_idx")
def align_idx(pk, *args, **kwargs):
  ds = get_object_or_404(Dataset, id=pk)
  # set index
  idx='whg'
  user = get_object_or_404(User, id=kwargs['user'])
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
  print('kwargs in align_idx()',kwargs)
    
  # queryset depends on choice of scope in addtask form
  qs = ds.places.all() if scope == 'all' else ds.places.all().filter(indexed=False)

  
  """
  build query object 'qobj'
  then result_obj = es_lookup_whg(qobj)
  """
  for place in qs:
    #place=get_object_or_404(Place,id=6369031) # Aachen
    print('building qobj for place.id',place.id, place.title)
    count +=1
    qobj = {"place_id":place.id, "src_id":place.src_id, "title":place.title}
    [links,ccodes,types,variants,parents,geoms] = [[],[],[],[],[],[]] 

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
    result_obj = es_lookup_whg(qobj, index=idx, bounds=bounds, place=place)

    # PARSE RESULTS
    # no hits on any pass, create parent index doc now
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
              task_id = align_idx.request.id,
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
  
  # create log entry and update ds status
  post_recon_update(ds, user, 'idx')
    
  return hit_parade['summary']

"""
original if 'whg' from normalize 

"""
#if auth.startswith('whg'):
  #rec = HitRecord(h['place_id'], h['dataset'], h['src_id'], h['title'])
  #print('"rec" HitRecord',rec)
  #rec.whg_id = h['whg_id'] if 'whg_id' in h.keys() else h['relation']['parent']
  ## add elements if non-empty in index record
  #rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
  ## TODO: grungy hack b/c index has both src_label and sourceLabel
  #key = 'src_label' if 'src_label' in h['types'][0] else 'sourceLabel'      
  #rec.types = [t['label']+' ('+t[key]  +')' if t['label']!=None else t[key] \
              #for t in h['types']] if len(h['types']) > 0 else []
  ## TODO: rewrite ccDecode to handle all conditions coming from index
  ## ccodes might be [] or [''] or ['ZZ', ...]
  #rec.ccodes = ccDecode(h['ccodes']) if ('ccodes' in h.keys() and (len(h['ccodes']) > 0 and h['ccodes'][0] !='')) else []
  #rec.parents = ['partOf: '+r.label+' ('+parseWhen(r['when']['timespans'])+')' for r in h['relations']] \
              #if 'relations' in h.keys() and len(h['relations']) > 0 else []
  #rec.descriptions = h['descriptions'] if len(h['descriptions']) > 0 else []
  
  #rec.geoms = [{
    #"type":h['geoms'][0]['location']['type'],
    #"coordinates":h['geoms'][0]['location']['coordinates'],
    #"id":h['place_id'], \
    #"ds":"whg"}] \
    #if len(h['geoms'])>0 else []   
  
  #rec.minmax = dict(sorted(h['minmax'].items(),reverse=True)) if len(h['minmax']) > 0 else []
  ## TODO: deal with whens
  ##rec.whens = [parseWhen(t) for t in h['timespans']] \
              ##if len(h['timespans']) > 0 else []
  #rec.links = [l['type']+': '+l['identifier'] for l in h['links']] \
              #if len(h['links']) > 0 else []
