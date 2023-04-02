# celery tasks for reconciliation and downloads
# align_tgn(), align_wdlocal(), align_idx(), align_whg, make_download
from __future__ import absolute_import, unicode_literals
from celery import task, shared_task # these are @task decorators
#from celery_progress.backend import ProgressRecorder
from django_celery_results.models import TaskResult
from django.conf import settings
from django.core import mail
from django.core.mail import EmailMultiAlternatives, EmailMessage
from django.db import connection
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.contrib.auth.models import User

# from django.contrib.gis.geos import Polygon, Point, LineString

import codecs, csv, datetime, itertools, re, sys, time
import pandas as pd
import simplejson as json
from copy import deepcopy
from itertools import chain

from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset, Hit
from datasets.static.hashes.parents import ccodes as cchash
from datasets.static.hashes.qtypes import qtypes
from elastic.es_utils import makeDoc, build_qobj, profileHit
#from datasets.task_utils import *
from datasets.utils import bestParent, elapsed, getQ, \
  HitRecord, hully, makeNow, parse_wkt, post_recon_update

from main.models import Log

#from places.models import Place
##
from elasticsearch7 import Elasticsearch

## global for all es connections in this file?
es = settings.ES_CONN

@shared_task(name="testy")
def testy():
  print("I'm testy...who wouldn't be?")

""" 
  called by utils.downloader()
  builds download file, retrieved via ajax JS in ds_summary.html, ds_meta.html,
  collection_detail.html (modal), place_collection_browse.html (modal)
"""
@shared_task(name="make_download")
def make_download(request, *args, **kwargs):
  # TODO: integrate progress_recorder for better progress bar in GUI
  # progress_recorder = ProgressRecorder(self) #accessed?
  username = request['username'] or "AnonymousUser"
  userid = request['userid'] or User.objects.get(username="AnonymousUser").id
  req_format = kwargs['format']
  dsid = kwargs['dsid'] or None
  collid = kwargs['collid'] or None
  print('make_download() dsid, collid', dsid, collid)
  # test values
  # username = 'whgadmin'
  # userid=1
  # req_format = 'tsv'
  # dsid = 1423

  date = makeNow()

  if collid and not dsid:
    print('entire collection', collid)
    coll=Collection.objects.get(id=collid)
    colltitle = coll.title
    qs = coll.places.all()
    req_format = 'lpf'
    fn = 'media/downloads/'+username+'_'+collid+'_'+date+'.json'
    outfile= open(fn, 'w', encoding='utf-8')
    features = []
    for p in qs:
      rec = {"type":"Feature",
             "properties":{"id":p.id,"src_id":p.src_id,"title":p.title,"ccodes":p.ccodes},
             "geometry":{"type":"GeometryCollection",
                         "geometries":[g.jsonb for g in p.geoms.all()]},
             "names": [n.jsonb for n in p.names.all()],
             "types": [t.jsonb for t in p.types.all()],
             "links": [l.jsonb for l in p.links.all()],
             "whens": [w.jsonb for w in p.whens.all()],
      }
      features.append(rec)

    count = str(len(qs))
    print('download file for '+count+' places in '+colltitle)
    result = {"type": "FeatureCollection", "features": features,
              "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
              "filename": "/" + fn}
    outfile.write(json.dumps(result,indent=2).replace('null','""'))
    # TODO: Log object has dataset_id, no collection_id
  elif dsid:
    ds=Dataset.objects.get(pk=dsid)
    dslabel = ds.label
    if collid:
      coll = Collection.objects.get(id=collid)
      qs=coll.places.filter(dataset=ds)
      print('collection places from dataset', collid, dsid)
    else:
      qs=ds.places.all()
    count = str(len(qs))

    print("tasks.make_download()", {"format": req_format, "ds": dsid})

    if ds.format == 'delimited' and req_format in ['tsv', 'delimited']:
      print('making an augmented tsv file')

      # get header as uploaded and create newheader w/any "missing" columns
      # get latest dataset file
      dsf = ds.file
      # make pandas dataframe
      df = pd.read_csv('media/'+dsf.file.name,
                       delimiter=dsf.delimiter,
                       # delimiter='\t',
                       dtype={'id':'str','aat_types':'str'})
      print('df', df)
      # copy existing header to newheader for write
      header = list(df)
      # header = list(df)[0].split(',')
      newheader = deepcopy(header)

      # all exports should have these, empty or not
      newheader = list(set(newheader+['lon','lat','matches','geo_id','geo_source','geowkt']))


      # name and open csv file for writer
      fn = 'media/downloads/'+username+'_'+dslabel+'_'+date+'.tsv'
      csvfile = open(fn, 'w', newline='', encoding='utf-8')
      writer = csv.writer(csvfile, delimiter='\t', quotechar='', quoting=csv.QUOTE_NONE)

      # TODO: better order?
      writer.writerow(newheader)
      # missing columns (were added to newheader)
      missing=list(set(newheader)-set(list(df)))
      print('missing',missing)

      for i, row in df.iterrows():
        dfrow = df.loc[i,:]
        # get db record
        # src_id is NOT distinct amongst all places!!
        p = qs.get(src_id = dfrow['id'], dataset = ds.label)

        # df row to newrow json object
        rowjs = json.loads(dfrow.to_json())
        newrow = deepcopy(rowjs)

        # add missing keys from newheader, if any
        for m in missing:
          newrow[m] = ''
        # newrow now has all keys -> fill with db values as req.

        # LINKS (matches)
        # get all distinct matches in db as string
        links = (';').join(list(set([l.jsonb['identifier'] for l in p.links.all()])))
        # replace whatever was in file
        newrow['matches'] = links

        # GEOMETRY
        # if db has >0 geom and row has none, add lon/lat and geowkt
        # otherwise, don't bother
        geoms = p.geoms.all()
        if geoms.count() > 0:
          geowkt= newrow['geowkt'] if 'geowkt' in newrow else None

          lonlat= [newrow['lon'],newrow['lat']] if \
            len(set(newrow.keys())&set(['lon','lat']))==2 else None
          # lon/lat may be empty
          if not geowkt and (not lonlat or None in lonlat or lonlat[0]==''):
            # get first db geometry & add to newrow dict
            g=geoms[0]
            #newheader.extend(['geowkt'])
            newrow['geowkt']=g.geom.wkt if g.geom else ''
            # there is always jsonb
            # xy = g.geom.coords[0] if g.jsonb['type'] == 'MultiPoint' else g.geom.coords
            xy = g.geom.coords[0] if g.jsonb['type'] == 'MultiPoint' else g.jsonb['coordinates']
            newrow['lon'] = xy[0]
            newrow['lat'] = xy[1]
        #print(newrow)

        # match newrow order to newheader already written
        index_map = {v: i for i, v in enumerate(newheader)}
        ordered_row = sorted(newrow.items(), key=lambda pair: index_map[pair[0]])

        #progress_recorder.set_progress(counter + 1, len(features), description="tsv progress")

        # write it
        csvrow = [o[1] for o in ordered_row]
        writer.writerow(csvrow)
      csvfile.close()
    else:
      print('building lpf file')
      # make file name
      fn = 'media/downloads/'+username+'_'+dslabel+'_'+date+'.json'
      outfile = open(fn, 'w', encoding='utf-8')
      features = []
      for p in qs:
        when = p.whens.first().jsonb
        if 'minmax' in when:
          del when['minmax']
        rec = {
          "type": "Feature",
          "@id": ds.uri_base + (str(p.id) if 'whgazetteer' in ds.uri_base else p.src_id),
          "properties": {"pid": p.id, "src_id": p.src_id, "title": p.title, "ccodes": p.ccodes},
          "geometry": {
            "type": "GeometryCollection",
            "geometries": [g.jsonb for g in p.geoms.all()]},
          "names": [n.jsonb for n in p.names.all()],
          "types": [t.jsonb for t in p.types.all()],
          "links": [l.jsonb for l in p.links.all()],
          "when": when
        }
        features.append(rec)

      count = str(len(qs))
      print('download file for ' + count + ' places')

      result={"type":"FeatureCollection",
              "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
              "filename": "/"+fn,
              "decription": ds.description,
              "features":features}

      outfile.write(json.dumps(result, indent=2).replace('null', '""'))

    Log.objects.create(
      # category, logtype, "timestamp", subtype, note, dataset_id, user_id
      category = 'dataset',
      logtype = 'ds_download',
      note = {"format":req_format, "username":username},
      dataset_id = dsid,
      user_id = userid
    )
  
  # for ajax, just report filename
  completed_message = {"msg": req_format+" written", "filename":fn, "rows":count}
  return completed_message


@shared_task(name="task_emailer")
def task_emailer(tid, dslabel, username, email, counthit, totalhits, test):
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
      html_content_fail="<h3>Greetings, "+username+"</h3> <p>Unfortunately, your <b>"+tasklabel+"</b> reconciliation task for the <b>"+dslabel+"</b> dataset has completed with status: "+ task.status+".</p><p>Error: "+fail_msg+". WHG staff have been notified. We will troubleshoot the issue and get back to you soon.</p>"
    elif test == 'off':
      text_content="Greetings "+username+"! Your "+tasklabel+" reconciliation task has completed with status: "+ \
        task.status+". \n"+str(counthit)+" records got a total of "+str(totalhits)+" hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
      html_content_success="<h3>Greetings, "+username+"</h3> <p>Your <b>"+tasklabel+"</b> reconciliation task for the <b>"+dslabel+"</b> dataset has completed with status: "+ task.status+". "+str(counthit)+" records got a total of "+str(totalhits)+" hits.</p>" + \
        "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"
    else:
      text_content="Greetings "+username+"! Your "+tasklabel+" TEST task has completed with status: "+ \
        task.status+". \n"+str(counthit)+" records got a total of "+str(totalhits)+".\nRefresh the dataset page and view results on the 'Reconciliation' tab."
      html_content_success="<h3>Greetings, "+username+"</h3> <p>Your <b>TEST "+tasklabel+"</b> reconciliation task for the <b>"+dslabel+"</b> dataset has completed with status: "+ task.status+". "+str(counthit)+" records got a total of "+str(totalhits)+" hits.</p>" + \
        "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"
  except:
    print('task lookup in task_emailer() failed on tid', tid, 'how come?')
    text_content="Greetings "+username+"! Your reconciliation task for the <b>"+dslabel+"</b> dataset has completed.\n"+ \
      str(counthit)+" records got a total of "+str(totalhits)+" hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
    html_content_success="<h3>Greetings, "+username+"</h3> <p>Your reconciliation task for the <b>"+dslabel+"</b> dataset has completed. "+str(counthit)+" records got a total of "+str(totalhits)+" hits.</p>" + \
      "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"

  subject, from_email = 'WHG reconciliation result', 'whg@kgeographer.org'
  conn = mail.get_connection(
    host=settings.EMAIL_HOST,
    user=settings.EMAIL_HOST_USER,
    use_ssl=settings.EMAIL_USE_SSL,
    password=settings.EMAIL_HOST_PASSWORD,
    port=settings.EMAIL_PORT
  )
  # msg=EmailMessage(
  msg = EmailMultiAlternatives(
    subject,
    text_content,
    from_email,
    [email],
    connection=conn
  )
  msg.bcc = ['karl@kgeographer.org']
  msg.attach_alternative(html_content_success if task and task.status == 'SUCCESS' else html_content_fail, "text/html")
  msg.send(fail_silently=False)

# test task for uptimerobot
@shared_task(name="testAdd")
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

def maxID(es,idx):
  q={"query": {"bool": {"must" : {"match_all" : {}} }},
       "sort": [{"whg_id": {"order": "desc"}}],
       "size": 1  
       }
  try:
    res = es.search(index=idx, body=q)
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
    #print('"rec" HitRecord',rec)
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
      "id":h['place_id'],
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
      #print('"rec" HitRecord',rec)      
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
    # hit['_source'] keys(): ['id', 'type', 'modified', 'descriptions', 'claims',
    # 'sitelinks', 'variants', 'minmax', 'types', 'location']
    try:
      print('h in normalize',h)
      # TODO: do it in index?
      variants=h['variants']
      title = wdTitle(variants, language)

      #  place_id, dataset, src_id, title
      rec = HitRecord(-1, 'wd', h['id'], title)
      #print('"rec" HitRecord',rec)
      
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

      rec.links = h['authids']

      # dont' know what happened here; h has key 'authids'

      # turn these identifier claims into links
      # qlinks = {'P1566':'gn', 'P1584':'pl', 'P244':'loc', 'P1667':'tgn', 'P214':'viaf', 'P268':'bnf', 'P1667':'tgn', 'P2503':'gov', 'P1871':'cerl', 'P227':'gnd'}
      # links=[]
      # hlinks = list(
      #   set(h['claims'].keys()) & set(qlinks.keys()))
      # if len(hlinks) > 0:
      #   for l in hlinks:
      #     links.append(qlinks[l]+':'+str(h['claims'][l][0]))
      # non-English wp pages do not resolve well, ignore them
      # add en and FIRST {language} wikipedia sitelink OR first sitelink
      # wplinks = []
      # wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == 'en']
      # if language != 'en':
      #   wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == language]
      # links += ['wp:'+l for l in set(wplinks)]
      #
      # rec.links = links
      #print('rec.links',rec.links)

      # look up Q class labels
      htypes = set(h['claims']['P31'])
      qtypekeys = set([t[0] for t in qtypes.items()])
      rec.types = [qtypes[t] for t in list(set(htypes & qtypekeys))]

      # countries
      rec.ccodes = [
        cchash[0][c]['gnlabel'] for c in cchash[0] \
          if cchash[0][c]['wdid'] in h['claims']['P17']
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
      #print('h in normalize', h)

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
        "id":h['tgnid'],
          "ds":"tgn"}]
    else: 
      rec.geoms=[]
    rec.minmax = []
    rec.links = []
    #print(rec)
  #print('normalized hit record',rec.toJSON())
  # TODO: raise any errors
  return rec.toJSON()

# ***
# elasticsearch filter from Area (types: predefined, ccodes, drawn)
# e.g. {'type': ['drawn'], 'id': ['128']}
# called from: es_lookup_tgn(), es_lookup_idx(), es_lookup_wdlocal(), search.SearchView(), 
# FUTURE: parse multiple areas
# ***
def get_bounds_filter(bounds, idx):
  #print('bounds in get_bounds_filter()',bounds)
  id = bounds['id'][0]
  #areatype = bounds['type'][0]
  area = Area.objects.get(id = id)
  # 
  # geofield = "geoms.location" if idx == 'whg' else "location"
  geofield = "geoms.location" if idx == 'whg' else "location"
  filter = { "geo_shape": {
    geofield: {
        "shape": {
          "type": area.geojson['type'],
          "coordinates": area.geojson['coordinates']
        },
        "relation": "intersects" if idx=='whg' else 'within' # within | intersects | contains
      }
  }} 
  return filter


#
# for tests
#bounds={'type': ['userarea'], 'id': ['0']}
#from datasets.static.hashes import aat, parents, aat_q
#from datasets.utils import getQ
#from areas.models import Area
#from places.models import Place
#from datasets.tasks import get_bounds_filter

"""
performs elasticsearch > wdlocal queries
from align_wdlocal()

"""
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
    area_filter = get_bounds_filter(bounds, 'wd')
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
    #print('q0 (no hits)', qobj)
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
      #print('q1: no hits',q1)
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
        #print('q2: no hits',q2)
  result_obj['hit_count'] = hit_count
  return result_obj


"""
manage align/reconcile to local wikidata index
get result_obj per Place via es_lookup_wdlocal()
parse, write Hit records for review

"""
@shared_task(name="align_wdlocal")
def align_wdlocal(pk, **kwargs):
  task_id = align_wdlocal.request.id
  ds = get_object_or_404(Dataset, id=pk)
  user = get_object_or_404(User, pk=kwargs['user'])
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  print('kwargs from align_wdlocal() task', kwargs)
  #bounds = {'type': ['userarea'], 'id': ['0']}
  language = kwargs['lang']
  hit_parade = {"summary": {}, "hits": []}
  [nohits,wdlocal_es_errors,features] = [[],[],[]]
  [count_hit, count_nohit, total_hits, count_p0, count_p1, count_p2] = [0,0,0,0,0,0]
  start = datetime.datetime.now()
  # no test option for wikidata, but needs default
  test = 'off'

  # queryset depends on 'scope'
  qs = ds.places.all() if scope == 'all' else \
    ds.places.filter(~Q(review_wd = 1))
  
  print('wtf? scope, count',scope,qs.count())
  for place in qs:
    print('review_wd',place.review_wd)
    #place = get_object_or_404(Place, pk=6596036)
    # build query object
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
      
    # TODO: ??? skip records that already have a Wikidata record in l_list
    # they are returned as Pass 0 hits right now
    # run pass0-pass2 ES queries
    #print('qobj in align_wd_local()',qobj)
    result_obj = es_lookup_wdlocal(qobj, bounds=bounds)
      
    if result_obj['hit_count'] == 0:
      count_nohit +=1
      nohits.append(result_obj['missed'])
    else:
      # place/task status 0 (unreviewed hits)
      place.review_wd = 0
      place.save()

      count_hit +=1
      total_hits += len(result_obj['hits'])
      for hit in result_obj['hits']:
        #print('pre-write hit', hit)
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
          place = place,
          task_id = task_id,
          query_pass = hit['pass'],
          # prepare for consistent display in review screen
          json = normalize(hit['_source'], 'wdlocal', language),
          src_id = qobj['src_id'],
          score = hit['_score'],
          #geom = loc,
          reviewed = False,
          matched = False
        )
        new.save()
        # print('new hit in align_wdlocal', hit['_source'])
  end = datetime.datetime.now()

  print('wdlocal ES errors:',wdlocal_es_errors)
  hit_parade['summary'] = {
      'count':qs.count(),
      'got_hits':count_hit,
      'total_hits': total_hits, 
      'pass0': count_p0, 
      'pass1': count_p1, 
      'pass2': count_p2, 
      'no_hits': {'count': count_nohit },
      'elapsed': elapsed(end-start)
    }
  print("summary returned",hit_parade['summary'])

  # create log entry and update ds status
  post_recon_update(ds, user, 'wdlocal', test)
    
  # email owner when complete
  task_emailer.delay(
    task_id,
    ds.label,
    user.username,
    user.email,
    count_hit,
    total_hits,
    test
  )
  
  return hit_parade['summary']


"""
# performs elasticsearch > whg index queries
# from align_idx(), returns result_obj

"""
def es_lookup_idx(qobj, *args, **kwargs):
  #print('kwargs from es_lookup_idx',kwargs)
  global whg_id
  idx = 'whg'
  #bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']
  [hitobjlist, _ids] = [[],[]]
  #ds_hits = {}
  #hit_count, err_count = [0,0]
  
  # empty result object
  result_obj = {
    'place_id': qobj['place_id'], 
    'title': qobj['title'], 
    'hits':[], 'missed':-1, 'total_hits':0,
    'hit_count': 0
  }  
  # de-dupe
  variants = list(set(qobj["variants"]))
  links = list(set(qobj["links"]))
  # copy for appends
  linklist = deepcopy(links)
  has_fclasses = len(qobj["fclasses"]) > 0

  # prep spatial constraints
  has_bounds = bounds["id"] != ["0"]
  has_geom = "geom" in qobj.keys()
  has_countries = len(qobj["countries"]) > 0
    
  if has_bounds:
    area_filter = get_bounds_filter(bounds, "whg")
    #print("area_filter", area_filter)
  if has_geom:
    # qobj["geom"] is always a polygon hull
    shape_filter = { "geo_shape": {
      "geoms.location": {
        "shape": {
          "type": qobj["geom"]["type"],
          "coordinates" : qobj["geom"]["coordinates"]},
        "relation": "intersects" }
    }}
    #print("shape_filter", shape_filter)
  if has_countries:
    countries_match = {"terms": {"ccodes":qobj["countries"]}}
    #print("countries_match", countries_match)
  
  """
  prepare queries from qobj
  """
  # q0 is matching concordance identifiers
  q0 = {
    "query": {"bool": { "must": [
      {"terms": {"links.identifier": linklist }}
    ]
  }}}


  # build q1 from qbase + spatial context, fclasses if any
  qbase = {"size": 100,"query": { 
    "bool": {
      "must": [
        {"exists": {"field": "whg_id"}},
        # must match one of these (exact)
        {"bool": {
            "should": [
              {"terms": {"names.toponym": variants}},
              {"terms": {"title": variants}},
              {"terms": {"searchy": variants}}
            ]
          }
        }
      ],
      "should": [
        # bool::"should" outside of "must" boosts score
        # {"terms": {"links.identifier": qobj["links"] }},
        {"terms": {"types.identifier": qobj["placetypes"]}}
      ],
      # spatial filters added according to what"s available
      "filter": []
    }
  }}

  # ADD SPATIAL
  if has_geom:
    qbase["query"]["bool"]["filter"].append(shape_filter)
    
  # no geom, use country codes if there
  if not has_geom and has_countries:
    qbase["query"]["bool"]["must"].append(countries_match)
    
  # has no geom but has bounds (region or user study area)
  if not has_geom and has_bounds:
    # area_filter (predefined region or study area)
    qbase["query"]["bool"]["filter"].append(area_filter)
    if has_countries:
      # add weight for country match
      qbase["query"]["bool"]["should"].append(countries_match)

  # ADD fclasses IF ANY
  # if has_fclasses:
  #   qbase["query"]["bool"]["must"].append(
  #   {"terms": {"fclasses": qobj["fclasses"]}})

  # grab a copy
  q1 = qbase
  print('q1', q1)

  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  # pass0a, pass0b (identifiers)
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  try:
    result0a = es.search(index=idx, body=q0)
    hits0a = result0a["hits"]["hits"]
    #print('len(hits0)',len(hits0a))
  except:
    print("q0a, ES error:", q0, sys.exc_info())
  if len(hits0a) > 0:
    # >=1 matching identifier
    result_obj['hit_count'] += len(hits0a)
    for h in hits0a:
      # add full hit to result
      result_obj["hits"].append(h)
      # pull some fields for analysis
      h["pass"] = "pass0a"
      relation = h["_source"]["relation"]
      hitobj = {
        "_id":h['_id'],
        "pid":h["_source"]['place_id'], 
        "title":h["_source"]['title'],
        "dataset":h["_source"]['dataset'],
        "pass":"pass0",
        "links":[l["identifier"] \
            for l in h["_source"]["links"]],
        "role":relation["name"],
        "children":h["_source"]["children"]
      }        
      if "parent" in relation.keys():
        hitobj["parent"] = relation["parent"]
      # add profile to hitlist
      hitobjlist.append(hitobj)
    print(str(len(hitobjlist))+" hits @ q0a")
    _ids = [h['_id'] for h in hitobjlist]
    for hobj in hitobjlist:
      for l in hobj['links']:
        linklist.append(l) if l not in linklist else linklist

    # if new links, crawl again
    if len(set(linklist)-set(links)) > 0:
      try:
        print('q0 at 0b search, new link identifiers?', q0)
        result0b = es.search(index=idx, body=q0)
        hits0b = result0b["hits"]["hits"]
        print('len(hits0b)',len(hits0b))
      except:
        print("q0b, ES error:", sys.exc_info())
      # add new results if any to hitobjlist and result_obj["hits"]
      result_obj['hit_count'] += len(hits0b)
      for h in hits0b:
        if h['_id'] not in _ids:
          _ids.append(h['_id'])
          relation = h["_source"]["relation"]
          h["pass"] = "pass0b"        
          hitobj = {
            "_id":h['_id'],
            "pid":h["_source"]['place_id'],
            "title":h["_source"]['title'],
            "dataset":h["_source"]['dataset'],
            "pass":"pass0b",
            "links":[l["identifier"] \
                for l in h["_source"]["links"]],
            "role":relation["name"],
            "children":h["_source"]["children"]
          }        
          if "parent" in relation.keys():
            hitobj["parent"] = relation["parent"]
          if hitobj['_id'] not in [h['_id'] for h in hitobjlist]:
            result_obj["hits"].append(h)
            hitobjlist.append(hitobj)
          result_obj['total_hits'] = len((result_obj["hits"]))
      
  #   
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  # run pass1 whether pass0 had hits or not
  # q0 only found identifier matches
  # now get other potential hits in normal manner
  # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
  try:
    result1 = es.search(index=idx, body=q1)
    hits1 = result1["hits"]["hits"]
  except:
    print("q1, ES error:", q1, sys.exc_info())  
    h["pass"] = "pass1"

  result_obj['hit_count'] += len(hits1)

  for h in hits1:
    # filter out _ids found in pass0
    # any hit on identifiers will also turn up here based on context
    if h['_id'] not in _ids:
      _ids.append(h['_id'])
      relation = h["_source"]["relation"]
      h["pass"] = "pass1"        
      hitobj = {
        "_id":h['_id'],
        "pid":h["_source"]['place_id'],
        "title":h["_source"]['title'],
        "dataset":h["_source"]['dataset'],
        "pass":"pass1",
        "links":[l["identifier"] \
            for l in h["_source"]["links"]],
        "role":relation["name"],
        "children":h["_source"]["children"]
      }        
      if "parent" in relation.keys():
        hitobj["parent"] = relation["parent"]
      if hitobj['_id'] not in [h['_id'] for h in hitobjlist]:
        # result_obj["hits"].append(hitobj)
        result_obj["hits"].append(h)
        hitobjlist.append(hitobj)
      result_obj['total_hits'] = len(result_obj["hits"])
  #ds_hits[p.id] = hitobjlist
  # no more need for hitobjlist
  
  # return index docs to align_idx() for Hit writing
  return result_obj

"""
# align/accession to whg index
# gets result_obj per Place
# writes 'union' Hit records to db for review
# OR writes seed parent to whg index 
"""
# TODO (1): "passive analysis option" reports unmatched and matched only
# TODO (2): "passive analysis option" that reports matches within datasets in a collection
# TODO (3): option with collection constraint; writes place_link records for partner records
@shared_task(name="align_idx")
def align_idx(pk, *args, **kwargs):
  print('kwargs in align_idx()',kwargs)
  test = kwargs['test']
  task_id = align_idx.request.id
  ds = get_object_or_404(Dataset, id=pk)
  idx = 'whg'
  user = get_object_or_404(User, id=kwargs['user'])
  # get last index identifier (used for _id)
  whg_id = maxID(es, idx)
  """
  kwargs: {'ds': 1231, 'dslabel': 'owt10b', 'owner': 14, 'user': 1, 
    'bounds': {'type': ['userarea'], 'id': ['0']}, 'aug_geom': 'on', 
    'scope': 'all', 'lang': 'en', 'test': 'false'}
  """
  """
    {'csrfmiddlewaretoken': ['Z3vg1TOlJRNTYSErmyNYuuaoTYMmk8235pMea2nXHtxvpfmvmdqPsqRHeefFqt2u'], 
    'ds': ['1231'], 
    'wd_lang': [''], 
    'recon': ['idx'], 
    'lang': ['']}>
  """
  # open file for writing new seed/parents for inspection
  # wd = "/Users/karlg/Documents/repos/_whgazetteer/_scratch/accessioning/"
  # fn1 = "new-parents_"+str(ds.id)+".txt"
  # fout1 = codecs.open(wd+fn1, mode="w", encoding="utf8")
  
  # bounds = {'type': ['userarea'], 'id': ['0']}
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  
  hit_parade = {"summary": {}, "hits": []}
  [count_hit,count_nohit,total_hits,count_p0,count_p1] = [0,0,0,0,0]
  [count_errors,count_seeds,count_kids,count_fail] = [0,0,0,0]
  new_seeds = []  
  start = datetime.datetime.now()
    
  # limit scope if some are already indexed
  qs = ds.places.filter(indexed=False)
  # TODO: scope = 'all' should be not possible for align_idx
  # qs = ds.places.all() if scope == 'all' else ds.places.filter(indexed=False) \
  #   if scope == 'unindexed' else ds.places.filter(review_wd != 1)
  
  """
  for each place, create qobj and run es_lookup_idx(qobj)
  if hits: write Hit instances for review
  if no hits: write new parent doc in index
  """
  for p in qs:
    qobj = build_qobj(p)
    
    result_obj = es_lookup_idx(qobj, bounds=bounds)
    
    # PARSE RESULTS
    # no hits on any pass, it's a new seed/parent
    if len(result_obj['hits']) == 0:
      # create new parent (write to file for inspection)
      whg_id +=1
      doc = makeDoc(p)
      doc['relation']['name'] = 'parent'
      doc['whg_id'] = whg_id
      # get names for search fields
      names = [p.toponym for p in p.names.all()]
      doc['searchy'] = names
      print("seed", whg_id, doc)
      new_seeds.append(doc)
      if test == 'off':
        res = es.index(index=idx, id=str(whg_id), document=json.dumps(doc))
        p.indexed = True
        p.save()
        # es.index(idx, doc, id=whg_id)
      
    # got some hits, format json & write to db as for align_wdlocal, etc.
    elif len(result_obj['hits']) > 0:
      count_hit +=1  # this record got >=1 hits
      # set place/task status to 0 (has unreviewed hits)
      p.review_whg = 0
      p.save()
      
      hits = result_obj['hits']
      [count_kids,count_errors] = [0,0]
      total_hits += result_obj['total_hits']

      # identify parents and children
      parents = [profileHit(h) for h in hits \
                if h['_source']['relation']['name']=='parent']
      children = [profileHit(h) for h in hits \
                if h['_source']['relation']['name']=='child']

      """ *** """
      p0 = len(set(['pass0a','pass0b']) & set([p['pass'] for p in parents])) > 0
      p1 = 'pass1' in [p['pass'] for p in parents]
      if p0:
        count_p0 += 1
      elif p1:
        count_p1 +=1

      def uniq_geom(lst):
        for _, grp in itertools.groupby(lst, lambda d: (d['coordinates'])):
          yield list(grp)[0]

      # if there are any
      for par in parents:
        # 20220828 test
        print('parent minmax', par['minmax'])
        # any children of *this* parent in this result?
        kids = [c for c in children if c['_id'] in par['children']] or None
        # merge values into hit.json object
        # profile keys ['_id', 'pid', 'title', 'role', 'dataset', 'parent', 'children', 'links', 'countries', 'variants', 'geoms']
        # boost parent score if kids
        score = par['score']+sum([k['score'] for k in kids]) if kids else par['score']
        #
        hitobj = {
          'whg_id': par['_id'],
          'pid': par['pid'],
          'score': score,
          'titles': [par['title']],
          'countries': par['countries'],
          'geoms': list(uniq_geom(par['geoms'])),
          'links': par['links'],
          'sources': [
            {'dslabel': par['dataset'], 
             'pid': par['pid'],
             'variants': par['variants'],
             'types': par['types'],
             'related': par['related'],
             'children': par['children'],
             'minmax': par['minmax'],
             'pass': par['pass'][:5]
             }]
        }
        if kids:
          hitobj['titles'].extend([k['title'] for k in kids])
          hitobj['countries'].extend([','.join(k['countries']) for k in kids])
          
          # unnest
          hitobj['geoms'].extend(list(chain.from_iterable([k['geoms'] for k in kids])))
          hitobj['links'].extend(list(chain.from_iterable([k['links'] for k in kids])))
          
          # add kids to parent in sources
          hitobj['sources'].extend(
            [{'dslabel':k['dataset'],
              'pid':k['pid'],
              'variants':k['variants'],
              'types':k['types'],
              'related': par['related'],
              'minmax':k['minmax'],
              'pass':k['pass'][:5]} for k in kids])

        passes = list(set([s['pass'] for s in hitobj['sources']]))
        hitobj['passes'] = passes

        hitobj['titles'] = ', '.join(list(dict.fromkeys(hitobj['titles'])))
        
        if hitobj['links']:
          hitobj['links'] = list(dict.fromkeys(hitobj['links']))
  
        hitobj['countries'] = ', '.join(list(dict.fromkeys(hitobj['countries'])))

        new = Hit(
          task_id = task_id,
          authority = 'whg',
          
          # incoming place
          dataset = ds,
          place = p, 
          src_id = p.src_id,
          
          # candidate parent, might have children
          authrecord_id = par['_id'],
          query_pass = ', '.join(passes), #
          score = hitobj['score'],
          geom = hitobj['geoms'],
          reviewed = False,
          matched = False,
          json = hitobj
        )
        new.save()
        #print(json.dumps(jsonic,indent=2))
  
  # testing: write new index seed/parent docs for inspection
  # fout1.write(json.dumps(new_seeds, indent=2))
  # fout1.close()
  # print(str(len(new_seeds)) + ' new index seeds written to '+ fn1)
  
  end = datetime.datetime.now()
  
  hit_parade['summary'] = {
    'count':qs.count(), # records in dataset
    'got_hits':count_hit, # count of parents
    'total_hits': total_hits, # overall total
    'seeds': len(new_seeds), # new index seeds
    'pass0': count_p0, 
    'pass1': count_p1, 
    'elapsed_min': elapsed(end-start),
    'skipped': count_fail
  }
  print("hit_parade['summary']",hit_parade['summary'])
  
  # create log entry and update ds status
  post_recon_update(ds, user, 'idx', test)

  # email owner when complete
  task_emailer.delay(
    task_id,
    ds.label,
    user.username,
    user.email,
    count_hit,
    total_hits,
    test,
  )    
  print('elapsed time:', elapsed(end-start))


  return hit_parade['summary']


"""
perform elasticsearch > tgn queries
from align_tgn()

"""
def es_lookup_tgn(qobj, *args, **kwargs):
  print('es_lookup_tgn qobj', qobj)
  bounds = kwargs['bounds']
  hit_count = 0

  # empty result object
  result_obj = {
    'place_id': qobj['place_id'], 'hits': [],
    'missed': -1, 'total_hits': -1
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
        {"terms": {"names.name": variants}},
        {"terms": {"types.id": placetypes}}
      ],
      "should": [
        {"terms": {"parents": parent}}
        # ,{"terms": {"types.id":placetypes}}
      ],
      "filter": [get_bounds_filter(bounds, 'tgn')] if bounds['id'] != ['0'] else []
    }
  }}

  qbare = {"query": {
    "bool": {
      "must": [
        {"terms": {"names.name": variants}}
      ],
      "should": [
        {"terms": {"parents": parent}}
      ],
      "filter": [get_bounds_filter(bounds, 'tgn')] if bounds['id'] != ['0'] else []
    }
  }}

  # grab deep copy of qbase, add w/geo filter if 'geom'
  q1 = deepcopy(qbase)

  # create 'within polygon' filter and add to q1
  if 'geom' in qobj.keys():
    location = qobj['geom']
    # always polygon returned from hully(g_list)
    filter_within = {"geo_polygon": {
      "repr_point": {
        "points": location['coordinates']
      }
    }}
    q1['query']['bool']['filter'].append(filter_within)

  # /\/\/\/\/\/
  # pass1: must[name]; should[type,parent]; filter[bounds,geom]
  # /\/\/\/\/\/
  # print('q1',q1)
  try:
    res1 = es.search(index="tgn", body=q1)
    hits1 = res1['hits']['hits']
  except:
    print('pass1 error:', sys.exc_info())
  if len(hits1) > 0:
    for hit in hits1:
      hit_count += 1
      hit['pass'] = 'pass1'
      result_obj['hits'].append(hit)
  elif len(hits1) == 0:
    # print('q1 no result:)',q1)
    # /\/\/\/\/\/
    # pass2: revert to qbase{} (drops geom)
    # /\/\/\/\/\/
    q2 = qbase
    try:
      res2 = es.search(index="tgn", body=q2)
      hits2 = res2['hits']['hits']
    except:
      print('pass2 error:', sys.exc_info())
    if len(hits2) > 0:
      for hit in hits2:
        hit_count += 1
        hit['pass'] = 'pass2'
        result_obj['hits'].append(hit)
    elif len(hits2) == 0:
      # print('q2 no result:)',q2)
      # /\/\/\/\/\/
      # pass3: revert to qbare{} (drops placetype)
      # /\/\/\/\/\/
      q3 = qbare
      # print('q3 (bare)',q3)
      try:
        res3 = es.search(index="tgn", body=q3)
        hits3 = res3['hits']['hits']
      except:
        print('pass3 error:', sys.exc_info())
      if len(hits3) > 0:
        for hit in hits3:
          hit_count += 1
          hit['pass'] = 'pass3'
          result_obj['hits'].append(hit)
      else:
        # no hit at all, name & bounds only
        # print('q3 no result:)',q3)
        result_obj['missed'] = qobj['place_id']
  result_obj['hit_count'] = hit_count
  return result_obj


"""
manage align/reconcile to tgn
get result_obj per Place via es_lookup_tgn()
parse, write Hit records for review

"""
@shared_task(name="align_tgn")
def align_tgn(pk, *args, **kwargs):
  task_id = align_tgn.request.id
  ds = get_object_or_404(Dataset, id=pk)
  user = get_object_or_404(User, pk=kwargs['user'])
  bounds = kwargs['bounds']
  scope = kwargs['scope']
  print('args, kwargs from align_tgn() task', args, kwargs)
  hit_parade = {"summary": {}, "hits": []}
  [nohits, tgn_es_errors, features] = [[], [], []]
  [count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3] = [0, 0, 0, 0, 0, 0]
  start = datetime.datetime.now()

  # queryset depends 'scope'
  qs = ds.places.all() if scope == 'all' else \
    ds.places.filter(~Q(review_tgn=1))

  for place in qs:
    # place=get_object_or_404(Place,id=131735)
    # build query object
    qobj = {"place_id": place.id,
            "src_id": place.src_id,
            "title": place.title}
    [variants, geoms, types, ccodes, parents] = [[], [], [], [], []]

    # ccodes (2-letter iso codes)
    for c in place.ccodes:
      ccodes.append(c.upper())
    qobj['countries'] = place.ccodes

    # types (Getty AAT identifiers)
    # all have 'aat:' prefixes
    for t in place.types.all():
      # types.append('aat:'+t.jsonb['identifier'])
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
      g_list = [g.jsonb for g in place.geoms.all()]
      print('g_list', g_list)
      # make everything a simple polygon hull for spatial filter
      qobj['geom'] = hully(g_list)

    ## run pass1-pass3 ES queries
    # print('qobj in align_tgn()',qobj)
    result_obj = es_lookup_tgn(qobj, bounds=bounds)

    if result_obj['hit_count'] == 0:
      count_nohit += 1
      nohits.append(result_obj['missed'])
      print('no hits in align_tgn() for', place.id, place.title)
    else:
      # place/task status 0 (unreviewed hits)
      place.review_tgn = 0
      place.save()
      count_hit += 1
      total_hits += len(result_obj['hits'])
      # print("hit[0]: ",result_obj['hits'][0]['_source'])
      # print('hits from align_tgn',result_obj['hits'])
      for hit in result_obj['hits']:
        if hit['pass'] == 'pass1':
          count_p1 += 1
        elif hit['pass'] == 'pass2':
          count_p2 += 1
        elif hit['pass'] == 'pass3':
          count_p3 += 1
        hit_parade["hits"].append(hit)
        # correct lower case 'point' in tgn index
        # TODO: fix in index
        if 'location' in hit['_source'].keys():
          loc = hit['_source']['location']
          loc['type'] = "Point"
        else:
          loc = {}
        new = Hit(
          authority='tgn',
          authrecord_id=hit['_id'],
          dataset=ds,
          place=place,
          task_id=align_tgn.request.id,
          query_pass=hit['pass'],
          # prepare for consistent display in review screen
          json=normalize(hit['_source'], 'tgn'),
          src_id=qobj['src_id'],
          score=hit['_score'],
          geom=loc,
          reviewed=False,
          matched=False,
        )
        new.save()
  end = datetime.datetime.now()

  print('tgn ES errors:', tgn_es_errors)
  hit_parade['summary'] = {
    'count': qs.count(),
    'got_hits': count_hit,
    'total_hits': total_hits,
    'pass1': count_p1,
    'pass2': count_p2,
    'pass3': count_p3,
    'no_hits': {'count': count_nohit},
    'elapsed': elapsed(end - start)
  }
  print("summary returned", hit_parade['summary'])

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
