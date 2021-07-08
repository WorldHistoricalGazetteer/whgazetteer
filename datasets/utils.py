#from django.core import serializers
from django.contrib.gis.geos import GEOSGeometry
from django.http import FileResponse, JsonResponse, HttpResponse
from django.shortcuts import get_object_or_404, render #, redirect
from django.views.generic import View

import codecs, csv, datetime, sys, openpyxl, os, pprint, re, time
import simplejson as json
#from celery import task, shared_task
from chardet import detect
from django_celery_results.models import TaskResult
from frictionless import validate as fvalidate
from goodtables import validate as gvalidate
from jsonschema import draft7_format_checker, validate 
from shapely import wkt

from areas.models import Country
from datasets.models import Dataset, DatasetUser, Hit
from datasets.static.hashes import aat, parents, aat_q
from datasets.static.hashes import aliases as al
#from datasets.tasks import make_download
from main.models import Log
from places.models import PlaceGeom, Type
pp = pprint.PrettyPrinter(indent=1)

# ***
# DOWNLOAD FILES
# ***
# TODO: use DRF serializer? download_{format} methods on api.PlaceList() view?
# https://stackoverflow.com/questions/38697529/how-to-return-generated-file-download-with-django-rest-framework

# initiate celery tasks
def downloader(request, *args, **kwargs):
  user = request.user
  print('request.user', request.user)
  print('kwargs', kwargs)
  from datasets.tasks import make_download
  # POST *should* be the only case...
  if request.method == 'POST' and request.is_ajax:
    print('ajax == True')
    print('request.POST (ajax)', request.POST)
    dsid=request.POST['dsid']
    format=request.POST['format']
    download_task = make_download.delay(
      {"username":user.username, "userid":user.id},
      dsid=dsid,
      format=format,
    )
    print('handed off to Celery', download_task.task_id)
    # return task_id
    obj={'task_id':download_task.task_id}
    
    #return render(request, 'datasets/ds_meta.html', context=context)
    return HttpResponse(json.dumps(obj), content_type='application/json')    
    
  elif request.method == 'POST' and not request.is_ajax:
    print('request.POST (not ajax)', request.POST)


  elif request.method == 'GET':
    print('request.GET', request.GET)


""" deprecatING (still used from collection download modal ) """
def download_augmented(request, *args, **kwargs):
  from django.db import connection
  print('download_augmented kwargs',kwargs)
  print('download_augmented request',request)
  user = request.user.username
  ds=get_object_or_404(Dataset,pk=kwargs['id'])
  dslabel = ds.label
  url_prefix='http://whgazetteer.org/api/place/'
  fileobj = ds.files.all().order_by('-rev')[0]
  date=maketime()

  req_format = kwargs['format']
  if req_format is not None:
    print('download format',req_format)
  
  features=ds.places.all().order_by('id')

  print('download_augmented() file format', fileobj.format)
  print('download_augmented() req. format', req_format)
  
  if fileobj.format == 'delimited' and req_format in ['tsv', 'delimited']:
    # get header
    header = ds.files.all().order_by('id')[0].header
    print('making a tsv file')
    # make file name
    fn = 'media/user_'+user+'/'+ds.label+'_aug_'+date+'.tsv'

    def augLinks(linklist):
      aug_links = []
      for l in linklist:
        aug_links.append(l.jsonb['identifier'])
      return ';'.join(aug_links)

    def augGeom(qs_geoms):
      gobj = {'new':[]}
      for g in qs_geoms:
        if not g.task_id:
          # it's an original
          gobj['lonlat'] = g.jsonb['coordinates']
        else:
          # it's an aug/add
          gobj['new'].append({"id":g.jsonb['citation']['id'],"coordinates":g.jsonb['coordinates']})
      return gobj

    # TODO: return valid LP-TSV, incl. geowkt where applic.
    with open(fn, 'w', newline='', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter='\t', quotechar='', quoting=csv.QUOTE_NONE)
      writer.writerow(['id','whg_pid','title','ccodes','lon','lat','added','matches'])
      #writer.writerow(header)
      for f in features:
        geoms = f.geoms.all()
        gobj = augGeom(geoms)
        #print('gobj',f.id, gobj)
        row = [str(f.src_id),
               str(f.id),
               f.title,
               ';'.join(f.ccodes),
               gobj['lonlat'][0] if 'lonlat' in gobj else None,
               gobj['lonlat'][1] if 'lonlat' in gobj else None,
               gobj['new'] if 'new' in gobj else None,
               str(augLinks(f.links.all()))
               ]
        writer.writerow(row)
        #progress_recorder.set_progress(i + 1, len(features), description="tsv progress")
    response = FileResponse(open(fn, 'rb'), content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="'+os.path.basename(fn)+'"'

    return response
  else:
    print('building lpf file')
    # make file name
    fn = 'media/user_'+user+'/'+ds.label+'_aug_'+date+'.json'
    result={"type":"FeatureCollection","features":[],
            "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
            "filename": "/"+fn}
    print('augmented lpf template', result)
    with open(fn, 'w', encoding='utf-8') as outfile:
      with connection.cursor() as cursor:
        cursor.execute("""with namings as 
          (select place_id, jsonb_agg(jsonb) as names from place_name pn 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          placetypes as 
          (select place_id, jsonb_agg(jsonb) as "types" from place_type pt 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          placelinks as 
          (select place_id, jsonb_agg(jsonb) as links from place_link pl 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          geometry as 
          (select place_id, jsonb_agg(jsonb) as geoms from place_geom pg 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          placewhens as
          (select place_id, jsonb as whenobj from place_when pw 
          where place_id in (select id from places where dataset = '{ds}')),
          placerelated as
          (select place_id, jsonb_agg(jsonb) as rels from place_related pr 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          descriptions as
          (select place_id, jsonb_agg(jsonb) as descrips from place_description pdes 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id ),
          depictions as
          (select place_id, jsonb_agg(jsonb) as depicts from place_depiction pdep 
          where place_id in (select id from places where dataset = '{ds}')
          group by place_id )	
          select jsonb_build_object(
            'type','Feature',
            '@id', p.src_id,
            'properties', jsonb_build_object(
                'pid', '{urlpre}'||p.id,
                'title', p.title),
            'names', n.names,
            'types', coalesce(pt.types, '[]'),
            'links', coalesce(pl.links, '[]'),
            'geometry', case when g.geoms is not null 
                then jsonb_build_object(
                'type','GeometryCollection',
                'geometries', g.geoms)
                else jsonb_build_object(
                'type','Point','coordinates','{a}'::char[])
                end,
            'when', pw.whenobj,
            'relations',coalesce(pr.rels, '[]'),
            'descriptions',coalesce(pdes.descrips, '[]'),
            'depictions',coalesce(pdep.depicts, '[]')
          ) from places p 
          left join namings n on p.id = n.place_id
          left join placetypes pt on p.id = pt.place_id
          left join placelinks pl on p.id = pl.place_id
          left join geometry g on p.id = g.place_id
          left join placewhens pw on p.id = pw.place_id
          left join placerelated pr on p.id = pr.place_id
          left join descriptions pdes on p.id = pdes.place_id
          left join depictions pdep on p.id = pdep.place_id
          where dataset = '{ds}'        
        """.format(urlpre=url_prefix, ds=dslabel, a='{}'))
        for row in cursor:
          g = row[0]['geometry']
          # get rid of empty/unknown geometry
          if g['type'] != 'GeometryCollection' and g['coordinates'] == []:
            row[0].pop('geometry')
          result['features'].append(row[0])
          #progress_recorder.set_progress(i + 1, len(features), description="lpf progress")
        outfile.write(json.dumps(result,indent=2))
        #outfile.write(json.dumps(result))
        
    # response is reopened file
    response = FileResponse(open(fn, 'rb'), content_type='text/json')
    #response = HttpResponse(open(fn, 'rb'), content_type='text/json')
    response['Content-Disposition'] = 'attachment; filename="'+os.path.basename(fn)+'"'

    return response

""" just gets a file and downloads it to File/Save window """
def download_file(request, *args, **kwargs):
  ds=get_object_or_404(Dataset,pk=kwargs['id'])
  fileobj = ds.files.all().order_by('-rev')[0]
  fn = 'media/'+fileobj.file.name
  file_handle = fileobj.file.open()
  print('download_file: kwargs,fn,fileobj.format',kwargs,fn,fileobj.format)
  # set content type
  response = FileResponse(file_handle, content_type='text/csv' if fileobj.format=='delimited' else 'text/json')
  response['Content-Disposition'] = 'attachment; filename="'+fileobj.file.name+'"'

  return response

#
# experiment (deprecated?)
def download_augmented_slow(request, *args, **kwargs):
  print('download_augmented kwargs',kwargs)
  user = request.user.username
  ds=get_object_or_404(Dataset,pk=kwargs['id'])
  fileobj = ds.files.all().order_by('-rev')[0]
  date=maketime()

  req_format = kwargs['format']
  if req_format is not None:
    print('got format',req_format)
    #qs = qs.filter(title__icontains=query)

  features=ds.places.all() 

  if fileobj.format == 'delimited' and req_format == 'tsv':
    print('augmented for delimited')
    # make file name
    fn = 'media/user_'+user+'/'+ds.label+'_aug_'+date+'.tsv'
    def augLinks(linklist):
      aug_links = []
      for l in linklist:
        aug_links.append(l.jsonb['identifier'])
      return ';'.join(aug_links)

    def augGeom(qs_geoms):
      gobj = {'new':[]}
      for g in qs_geoms:
        if not g.task_id:
          # it's an original
          gobj['lonlat'] = g.jsonb['coordinates']
        else:
          # it's an aug/add
          gobj['new'].append({"id":g.jsonb['citation']['id'],"coordinates":g.jsonb['coordinates']})
      return gobj

    with open(fn, 'w', newline='', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter='\t', quotechar='', quoting=csv.QUOTE_NONE)
      writer.writerow(['id','whg_pid','title','ccodes','lon','lat','added','matches'])
      for f in features:
        geoms = f.geoms.all()
        gobj = augGeom(geoms)
        row = [
          str(f.src_id),
          str(f.id),f.title,
          ';'.join(f.ccodes),
          gobj['lonlat'][0] if 'lonlat' in gobj else None,
          gobj['lonlat'][1] if 'lonlat' in gobj else None,
          gobj['new'] if 'new' in gobj else None,
          str(augLinks(f.links.all())) ]
        writer.writerow(row)
        #print(row)
    response = FileResponse(open(fn, 'rb'),content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="'+os.path.basename(fn)+'"'

    return response
  else:
    # make file name
    fn = 'media/user_'+user+'/'+ds.label+'_aug_'+date+'.json'

    with open(fn, 'w', encoding='utf-8') as outfile:
      #fcoll = {"type":"FeatureCollection","features":[]}
      for f in features:
        print('dl_aug, lpf adding feature:',f)
        feat={"type":"Feature",
              "properties":{
                "@id":f.dataset.uri_base+f.src_id,
                "src_id":f.src_id,
                "title":f.title,
                "whg_pid":f.id}}
        if len(f.geoms.all()) >1:
          feat['geometry'] = {'type':'GeometryCollection'}
          feat['geometry']['geometries'] = [g.jsonb for g in f.geoms.all()]
        elif len(f.geoms.all()) == 1:
          feat['geometry'] = f.geoms.first().jsonb
        else: # no geoms
          feat['geometry'] = feat['geometry'] = {'type':'GeometryCollection','geometries':[]}
        feat['names'] = [n.jsonb for n in f.names.all()]
        feat['types'] = [t.jsonb for t in f.types.all()]
        feat['when'] = [w.jsonb for w in f.whens.all()]
        feat['relations'] = [r.jsonb for r in f.related.all()]
        feat['links'] = [l.jsonb for l in f.links.all()]
        feat['descriptions'] = [des.jsonb for des in f.descriptions.all()]
        feat['depictions'] = [dep.jsonb for dep in f.depictions.all()]
        #fcoll['features'].append(feat)  
        outfile.write(json.dumps(feat,indent=2))
      #outfile.write(json.dumps(fcoll,indent=2))

    # response is reopened file
    response = FileResponse(open(fn, 'rb'), content_type='text/json')
    #response['Content-Disposition'] = 'attachment; filename="'+os.path.basename(fn)+'"'
    response['Content-Disposition'] = 'filename="'+os.path.basename(fn)+'"'

    return response

# TODO: crude, only properties are ids
def download_gis(request, *args, **kwargs):
  print('download_gis kwargs',kwargs)
  user = request.user.username
  ds=get_object_or_404(Dataset,pk=kwargs['id'])
  date=maketime()
  # make file name
  fn = 'media/user_'+user+'/'+ds.label+'_aug_'+date+'.json'
  # open it for write
  fout = codecs.open(fn,'w','utf8')

  # build a flat FeatureCollection 
  features=PlaceGeom.objects.filter(place_id__in=ds.placeids).values_list('jsonb','place_id','src_id')
  fcoll = {"type":"FeatureCollection","features":[]}
  for f in features:
    feat={"type":"Feature","properties":{"pid":f[1],"src_id":f[2]},"geometry":f[0]}
    fcoll['features'].append(feat)
  fout.write(json.dumps(fcoll))
  fout.close()
  # response is reopened file and content type
  response = FileResponse(open(fn, 'rb'),content_type='text/json')
  response['Content-Disposition'] = 'attachment; filename="mydata.geojson"'

  #return HttpResponse(content='download_gis: '+fn)
  return response

# *** /end DOWNLOAD FILES

def get_encoding_excel(fn):
  fin = codecs.open(fn, 'r')
  fin.close()
  return fin.encoding

def get_encoding_delim(fn):
  with open(fn, 'rb') as f:
    rawdata = f.read()
  return detect(rawdata)['encoding']

def groom_files(user):
  return 'groomed files for ' + user

  # ***
# format validation errors for display
# ***
def parse_errors_tsv(errors):
  new_errors = []
  for e in errors:
    newe = re.sub('a constraint: constraint', 'the constraint:', e)
    newe = re.sub('at position "(\\d+)" does', 'does', newe)
    newe = re.sub('row at position "(\\d+)"', 'row \\1', newe)
    newe = re.sub('value', 'cell', newe)
    new_errors.append(newe)
  return new_errors
# tsv error: ['The cell "" in row at position "2" and field "id" at position "1" does not conform to a constraint: constraint "required" is "True"', 'The row at position "2" does not conform to the primary key constraint: cells composing the primary keys are all "None"']

# lpf error: [{'feat': 2, 'error': "'null' is not of type 'object'"}, {'feat': 3, 'error': "'null' is not of type 'object'"}]

# *** NOT YET CALLED
# format lpf validation errors for modal display
# *** 
#def parse_errors_lpf(errors):
  #new_errors = []
  #for e in error
  #print('parse_errors_lpf()',errors)

# ***
# parse start & end for insert to db
# TODO: is year-only minmax useful in GUI?
# parsedates for ds_insert_lpf will be different
# ***
#def intmap(arr):
  #return [int(a) for a in arr]

def parse_errors_lpf(errors):
  print('relative_path 0',errors[0]['error'].relative_path)
  "deque(['geometry', 'geometries', 0])"
  msg = [{"row":e['feat'], "msg":e['error'].message, "path":
         re.search('deque\(\[(.*)\]\)', 
          str(e['error'].relative_path)).group(1) } for e in errors]
  return msg

#
# called by ds_insert_tsv()
# returns object for PlaceWhen.jsonb in db
# and minmax int years for PlacePortalView()
#
def parsedates_tsv(s,e):
  s_yr=s[:5] if s[0] == '-' else s[:4]
  e_yr=e[:5] if e[0] == '-' else e[:4]
  #union = intmap([*set(e.split('/')), *set(s.split('/'))])
  return {"timespans":[
    {"start": {"earliest":s}, "end": {"latest":e}}],
          "minmax":[int(s_yr),int(e_yr)]}

# extract integers for new Place from lpf
def timespansReduce(tsl):
  result = []
  for ts in tsl:
    s = ts['start'][list(ts['start'].keys())[0]]
    s_yr=s[:5] if s[0] == '-' else s[:4]
    #e = ts['end'][list(ts['end'].keys())[0]] \
            #if 'end' in ts else s
    # lpf imports from tsv exports can have '' in end
    end = ts['end'][list(ts['end'].keys())[0]] if 'end' in ts else None # no end
    e = end if end and end != '' else s
    e_yr=e[:5] if e[0] == '-' else e[:4]
    result.append([int(s_yr), int(e_yr)])
    #s = int(ts['start'][list(ts['start'].keys())[0]])
    #e = int(ts['end'][list(ts['end'].keys())[0]]) \
      #if 'end' in ts else s
    #result.append([s,e])
  return result

#
# called by ds_insert_lpf()
# TODO: replicate outcome of parsedates_tsv()
#
def parsedates_lpf(feat):
  print('feat.when in parsedates_lpf()',feat['when'])
  intervals=[]
  # gather all when elements
  # global when?
  if 'when' in feat and 'timespans' in feat['when']:
    try:
      intervals += timespansReduce(feat['when']['timespans']) 
    except:
      print('parsedates_lpf hung on', feat['@id'])
    
  # which feat keys might have a when?
  possible_keys = list(set(feat.keys() & \
                    set(['names','types','relations','geometry'])))
  print('possible_keys in parsedates_lpf()',possible_keys)
  
  # first, geometry
  # collections...
  geom = feat['geometry'] if 'geometry' in feat else None
  if geom and geom['type'] == 'GeometryCollection':
    for g in geom['geometries']:
      if 'when' in g and 'timespans' in g['when']:
        intervals += timespansReduce(g['when']['timespans'])
  # or singleton
  else:
    if geom and 'when' in geom:
      if 'timespans' in geom['when']:
        intervals += timespansReduce(g['when']['timespans'])
        
  # then the rest
  for k in possible_keys:
    if k != 'geometry':    
      obj = feat[k]
      for o in obj:
        if 'when' in o and 'timespans' in o['when']:
          intervals += timespansReduce(o['when']['timespans']) 
  # features must have >=1 when, with >=1 timespan
  # absent end replaced by start by timespansReduce()
  starts = [ts[0] for ts in intervals]
  ends = [ts[1] for ts in intervals]
  # some lpf records have no time at all b/c not required as with lp-tsv
  minmax = [
    int(min(starts)) if len(starts)>0 else None, 
    int(max(ends))  if len(ends)>0 else None
  ]
  # de-duplicate
  unique=list(set(tuple(sorted(sub)) for sub in intervals))
  return {"intervals": unique, "minmax": minmax}
# 
# validate Linked Places json-ld (w/jsonschema)
# format ['coll' (FeatureCollection) | 'lines' (json-lines)]
def validate_lpf(tempfn,format):
  #wd = '/Users/karlg/Documents/Repos/_whgazetteer/'
  #schema = json.loads(codecs.open('datasets/static/validate/schema_lpf_v1.1.json','r','utf8').read())
  schema = json.loads(codecs.open('datasets/static/validate/schema_lpf_v1.2.json','r','utf8').read())
  # rename tempfn
  newfn = tempfn+'.jsonld'
  os.rename(tempfn,newfn)
  infile = codecs.open(newfn, 'r', 'utf8')
  result = {"format":"lpf","errors":[]}
  [countrows,count_ok] = [0,0]

  # TODO: handle json-lines
  jdata = json.loads(infile.read())
  if len(set(['type', '@context', 'features'])-set(jdata.keys())) > 0 \
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
        print('res: some kinda error',err[1].args)
        #result["errors"].append({"feat":countrows,'error':err[1].args[0]})
        result["errors"].append({"feat":countrows,'error':err[1]})
    result['count'] = countrows
  return result

#
# validate LP-TSV file (w/frictionless.py)
# 
#wd='/Users/karlg/documents/repos/_whgazetteer/_testdata/'
#fn=wd+'priest_1line.tsv'
def validate_tsv(fn, ext):
  # incoming csv or tsv
  print('validate_tsv() fn', fn)
  result = {"format":"delimited", "errors":[]}
  schema_lptsv = json.loads(codecs.open('datasets/static/validate/schema_tsv.json', 'r', 'utf8').read())
  report = fvalidate(fn, schema=schema_lptsv, sync_schema=True)
  #print(report)
  rpt = report['tables'][0]
  req = ['id','title','title_source','start']
  
  result['count'] = rpt['stats']['rows'] # count
  result['columns'] = rpt['header']

  # filter harmless errors 
  result['errors'] = [x['message'] for x in rpt['errors'] \
            #if x['code'] not in ["blank-header"]]
            if x['code'] not in ["blank-header", "missing-header"]]
  if len(list(set(req) - set(rpt['header']))) >0:
    result['errors'].insert(0,'Required field(s) missing: '+', '.join(list(set(req)-set(rpt['header']))))

  # TODO: filter cascade errors, e.g. caused by missing-cell
    
  return result


# nextgen goodtables, allows xlsx, ods but has issues
def frictionless_tsv(tempfn):
  result = {"errors":[],"format":"delimited"}
  # TODO: detect encoding
  newfn = tempfn+'.tsv'
  os.rename(tempfn,newfn)
  print('tempfn,newfn',tempfn,type(tempfn),newfn,type(newfn))
  schema_lptsv = json.loads(codecs.open('datasets/static/validate/schema_tsv.json', 'r', 'utf8').read())
  report = gvalidate(newfn,
                     schema=schema_lptsv,
                     order_fields=True,
                     #row_limit=20000,
                     row_limit=30000,
                     skip_errors=['missing-header','missing-cell','non-matching-header'])
  pp.pprint(report)  
  #print('error count',report['error-count'])
  result['count'] = report['tables'][0]['row-count']-1 # counts header apparently
  result['columns'] = report['tables'][0]['headers']
  result['file'] = report['tables'][0]['source']
  # make sense of errors for users
  # filter harmless errors (a goodtable library bug IMO)
  errors = [x for x in report['tables'][0]['errors'] if x['code'] not in ["blank-header","missing-header"]]
  error_types = list(set([x['code'] for x in errors]))
  if 'non-matching-header' in error_types:
    # have user fix that issue and try again
    result['errors'].append('One or more column heading is invalid: '+str(result['columns']))
  else:
    result['errors'] = [x['message'].replace('and format "default"','') for x in errors]
  return result

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

def is_aat(string):
  return True if string.startswith('aat') or 'aat/' in string else False

# null fclass: 300239103, 300056006, 300155846
# refactored to use Type model in db
def aat_lookup(aid):
  try:
    typeobj = get_object_or_404(Type, aat_id=aid)
    return {"label": typeobj.term, "fclass":typeobj.fclass or None}
  except:
    print(str(aid)+' broke aat_lookup()', sys.exc_info())
    return {"label": None, "fclass":None}

u='https://catalogue.bnf.fr/ark:/12148/cb193409'
def aliasIt(url):
  r1=re.compile(r"\/(?:.(?!\/))+$")
  id=re.search(r1,url)
  if id: 
    id = id.group(0)[1:].replace('cb','')
  r2 = re.compile(r"bnf|cerl|dbpedia|geonames|d-nb|loc|pleiades|tgn|viaf|wikidata|whg|wikipedia")
  tag=re.search(r2,url)
  if tag and id:
    return al.tags[tag.group(0)]['alias']+':'+id
  else:
    return url

# flattens nested tuple list
def flatten(l):
  for el in l:
    if isinstance(el, tuple) and any(isinstance(sub, tuple) for sub in el):
      for sub in flatten(el):
        yield sub
    else:
      yield el

def format_size(num):
  return round(num/1000000, 2)

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

def parse_wkt(g):
  #print('wkt',g)
  from shapely.geometry import mapping
  gw = wkt.loads(g)
  feature = json.loads(json.dumps(mapping(gw)))
  #print('wkt, feature',g, feature)
  return feature

#
def maketime():
  ts = time.time()
  sttime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
  return sttime
#
def myprojects(me):
  return DatasetUser.objects.filter(user_id_id=me.id).values_list('dataset_id_id')
#
def parsejson(value,key):
  """returns value for given key"""
  print('parsejson() value',value)
  obj = json.loads(value.replace("'",'"'))
  return obj[key]
#
def makeCoords(lonstr,latstr):
  #print(type(lonstr),latstr)
  lon = float(lonstr) if lonstr not in ['','nan'] else ''
  lat = float(latstr) if latstr not in ['','nan'] else ''
  #lon = float(lonstr) if lonstr != '' else ''
  #lat = float(latstr) if latstr != '' else ''
  coords = [] if (lonstr == ''  or latstr == '') else [lon,lat]
  return coords

# might be GeometryCollection or singleton
def ccodesFromGeom(geom):
  #print('ccodesFromGeom() geom',geom)
  if geom['type'] == 'Point' and geom['coordinates'] ==[]:
    ccodes = []
    return ccodes
    #print(ccodes)
  else:    
    g = GEOSGeometry(str(geom))
    if g.geom_type == 'GeometryCollection':
      # just hull them all
      qs = Country.objects.filter(mpoly__intersects=g.convex_hull)
    else:
      qs = Country.objects.filter(mpoly__intersects=g)       
    ccodes = [c.iso for c in qs]
    return ccodes
    #print(ccodes)
#
def elapsed(delta):
  minutes, seconds = divmod(delta.seconds, 60)
  return '{:02}:{:02}'.format(int(minutes), int(seconds))

# called from es_lookup_tgn()
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


# TODO: faster?
class UpdateCountsView(View):
  """ Returns counts of unreviewed hits, per pass and total; also deferred per task 
  TODO: counts of unreviewed *records* not hits
  """
  @staticmethod
  def get(request):
    print('UpdateCountsView GET:',request.GET)
    """
    args in request.GET:
        [integer] ds_id: dataset id
    """
    ds = get_object_or_404(Dataset, id=request.GET.get('ds_id'))

    def defcountfunc(taskname, pids):
      if taskname[6:] in ['whg', 'idx']:
        return ds.places.filter(id__in=pids, review_whg = 2).count()
      elif taskname[6:].startswith('wd'):
        return ds.places.filter(id__in=pids, review_wd = 2).count()
      else:
        return ds.places.filter(id__in=pids, review_tgn = 2).count()
    
    def placecounter(th):
      pcounts={}      
      #for th in taskhits.all():
      pcounts['p0'] = th.filter(query_pass='pass0').values('place_id').distinct().count()
      pcounts['p1'] = th.filter(query_pass='pass1').values('place_id').distinct().count()
      pcounts['p2'] = th.filter(query_pass='pass2').values('place_id').distinct().count()
      pcounts['p3'] = th.filter(query_pass='pass3').values('place_id').distinct().count()
      return pcounts
    
    updates={}
    # counts of distinct place ids w/unreviewed hits per task/pass
    for t in ds.tasks.all():
      taskhits = Hit.objects.filter(task_id=t.task_id, reviewed=False)
      pcounts = placecounter(taskhits)
      # ids of all unreviewed places
      pids = list(set(taskhits.all().values_list("place_id",flat=True)))
      defcount = defcountfunc(t.task_name, pids)
      
      updates[t.task_id] = {
        "task":t.task_name,
        "total":len(pids),
        #"pass0":taskhits.filter(query_pass='pass0', place_id__in=pids).count(),
        #"pass1":taskhits.filter(query_pass='pass1', place_id__in=pids).count(),
        #"pass2":taskhits.filter(query_pass='pass2', place_id__in=pids).count(),
        #"pass3":taskhits.filter(query_pass='pass3', place_id__in=pids).count(),
        "pass0":pcounts['p0'],
        "pass1":pcounts['p1'],
        "pass2":pcounts['p2'],
        "pass3":pcounts['p3'],
        "deferred": defcount
      }
    print(json.dumps(updates, indent=2))        
    return JsonResponse(updates, safe=False)

class UpdateCountsViewBak(View):
  """ Returns counts of unreviewed hits, per pass and total """
  @staticmethod
  def get(request):
    print('UpdateCountsView GET:',request.GET)
    """
    args in request.GET:
        [integer] ds_id: dataset id
    """
    ds = get_object_or_404(Dataset, id=request.GET.get('ds_id'))
    deferred_wd = ds.places.filter(review_wd = 2).values_list('id', flat=True)
    deferred_tgn = ds.places.filter(review_tgn = 2).values_list('id', flat=True)
    deferred_whg = ds.places.filter(review_whg = 2).values_list('id', flat=True)

    updates={}
    # counts of distinct place ids w/unreviewed hits per task/pass
    for t in ds.tasks.all():
      hits0 = Hit.objects.filter(task_id=t.task_id,query_pass='pass0', reviewed=False).values_list("place_id",flat=True).distinct()
      hits1 = Hit.objects.filter(task_id=t.task_id,query_pass='pass1', reviewed=False).values_list("place_id",flat=True).distinct()
      hits2 = Hit.objects.filter(task_id=t.task_id,query_pass='pass2', reviewed=False).values_list("place_id",flat=True).distinct()
      hits3 = Hit.objects.filter(task_id=t.task_id,query_pass='pass3', reviewed=False).values_list("place_id",flat=True).distinct()

      sum = hits0.count()+hits1.count()+hits2.count()+hits3.count()
      updates[t.task_id] = {
        "task":t.task_name,
        "total":sum, 
        "pass0":hits0.count(), 
        "pass1":hits1.count(), 
        "pass2":hits2.count(), 
        "pass3":hits3.count() }
    return JsonResponse(updates, safe=False)


# ***
# UPLOAD UTILS
# ***
def xl_tester(): 
  fn = '/Users/karlg/repos/_whgdata/data/_source/CentralEurasia/bregel_in progress.xlsx'
  from openpyxl import load_workbook
  wb = load_workbook(filename = fn)
  sheet_ranges = wb['range names']

def xl_upload(request):
  if "GET" == request.method:
    return render(request, 'datasets/xl.html', {})
  else:
    excel_file = request.FILES["excel_file"]

    # you may put validations here to check extension or file size

    wb = openpyxl.load_workbook(excel_file)

    # getting all sheets
    sheets = wb.sheetnames
    print(sheets)

    # getting a particular sheet by name out of many sheets
    ws = wb["Sheet1"]
    print(ws)

    excel_data = list()
    # iterating over the rows and
    # getting value from each cell in row
    for row in ws.iter_rows():
      row_data = list()
      for cell in row:
        #row_data.append(str(cell.value))
        row_data.append(cell.value)
      excel_data.append(row_data)

    return render(request, 'datasets/xl.html', {"excel_data":excel_data}) 


