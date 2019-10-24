# datasets.views
from django.contrib import messages
from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator
from django.db.models import Q
from django.forms import formset_factory, modelformset_factory
from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import render, get_object_or_404, redirect, render_to_response
from django.urls import reverse
from django.views.generic import (
  CreateView, ListView, UpdateView, DeleteView, DetailView )
from django_celery_results.models import TaskResult

import codecs, tempfile, os, re, ipdb, sys
import simplejson as json
from itertools import islice
from pprint import pprint
from areas.models import Area
from main.choices import AUTHORITY_BASEURI
from places.models import *
from datasets.forms import DatasetModelForm, HitModelForm, DatasetDetailModelForm
from datasets.models import Dataset, Hit
from datasets.static.hashes.parents import ccodes
from datasets.tasks import align_tgn, align_whg, align_wd, maxID
from datasets.utils import *
from es.es_utils import makeDoc

"""
functions to batch large database inserts, e.g. TGN
"""

#   PlaceName.objects.bulk_create(objs['PlaceName'])
#def create_data(model, data):
  #bulk_create(model, generator())
  
#def bulk_create(model, generator, batch_size=10000):
  #"""
  #Uses islice to call bulk_create on batches of
  #Model objects from a generator.
  #"""
  #while True:
    #items = list(islice(generator, batch_size))
    #if not items:
      #break
    #model.objects.bulk_create(items)

#def generator(data):
  #for row in data:
    #yield MyModel(field1=data['field1'])
""" end """

def link_uri(auth,id):
  baseuri = AUTHORITY_BASEURI[auth]
  uri = baseuri + str(id)
  return uri
# present reconciliation (and accessioning!) hits for review
# for reconciliation: write place_link & place_geom (if aug_geom == 'on') records if matched
# for accessioning: if close or exact -> if match is parent -> make child else if match is child -> make sibling
def indexMatch(pid, hit_pid=None):
  print('indexMatch, wtf?',pid)
  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  idx='whg02'
  
  if hit_pid == None:
    print('making '+str(pid)+' a parent')
    # TODO:
    whg_id=maxID(es,idx) +1
    place=get_object_or_404(Place,id=pid)
    print('new whg_id',whg_id)
    parent_obj = makeDoc(place,'none')
    parent_obj['relation']={"name":"parent"}
    parent_obj['whg_id']=whg_id
    # add its own names to the suggest field
    for n in parent_obj['names']:
      parent_obj['suggest']['input'].append(n['toponym'])
    # add its title
    if place.title not in parent_obj['suggest']['input']:
      parent_obj['suggest']['input'].append(place.title)
    #index it
    try:
      res = es.index(index=idx, doc_type='place', id=str(whg_id), body=json.dumps(parent_obj))
      #count_seeds +=1
    except:
      #print('failed indexing '+str(place.id), parent_obj)
      print('failed indexing (as parent)'+str(pid))
      pass
    print('created parent:',pid,place.title)    
  else:
    # get _id of hit
    q_hit_pid={"query": {"bool": {"must": [{"match":{"place_id": hit_pid}}]}}}
    res = es.search(index=idx, body=q_hit_pid)
    
    # if hit is a child, get _id of its parent; this will be a sibling 
    # if hit is a parent, get its _id, this will be a child
    if res['hits']['hits'][0]['_source']['relation']['name'] == 'child':
      parent_whgid = res['hits']['hits'][0]['_source']['relation']['parent']
    else:
      parent_whgid = res['hits']['hits'][0]['_id'] #; print(parent_whgid)
    
    # get db record of place, mine its names, make an index doc
    place=get_object_or_404(Place,id=pid)
    match_names = [p.toponym for p in place.names.all()]
    child_obj = makeDoc(place,'none')
    child_obj['relation']={"name":"child","parent":parent_whgid}
    
    # all or nothing; pass if error
    try:
      res = es.index(index=idx,doc_type='place',id=place.id,
                     routing=1,body=json.dumps(child_obj))
      #count_kids +=1                
      print('added '+str(place.id) + ' as child of '+ str(hit_pid))
      # add variants from this record to the parent's suggest.input[] field
      q_update = { "script": {
          "source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id)",
          "lang": "painless",
          "params":{"names": match_names, "id": str(place.id)}
        },
        "query": {"match":{"_id": parent_whgid}}}
      es.update_by_query(index=idx, doc_type='place', body=q_update, conflicts='proceed')
      print('indexed '+str(pid)+' as child of '+str(parent_whgid), child_obj)
    except:
      print('failed indexing '+str(pid)+' as child of '+str(parent_whgid), child_obj)
      #count_fail += 1
      pass
      #sys.exit(sys.exc_info())
  
def review(request, pk, tid, passnum): # dataset pk, celery recon task_id
  #print('review() request', request)
  ds = get_object_or_404(Dataset, id=pk)
  task = get_object_or_404(TaskResult, task_id=tid)
  kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  #print('task_kwargs as json',kwargs)
  # filter place records by passnum for those with unreviewed hits on this task
  cnt_pass = Hit.objects.values('place_id').filter(task_id=tid, reviewed=False, query_pass=passnum).count()
  pass_int = int(passnum[4])
  passnum = passnum if cnt_pass > 0 else 'pass'+str(pass_int+1)
  # [place_id] for places with >0 hits
  
  hitplaces = Hit.objects.values('place_id').filter(
    task_id=tid,
      reviewed=False,
        query_pass=passnum)
  
  if len(hitplaces) > 0:
    record_list = Place.objects.order_by('title').filter(pk__in=hitplaces)
  else:
    context = {"nohits":True,'ds_id':pk,'task_id': tid, 'passnum': passnum}
    return render(request, 'datasets/review.html', context=context)
    # no unreviewed hits

  paginator = Paginator(record_list, 1)
  page = 1 if not request.GET.get('page') else request.GET.get('page')
  records = paginator.get_page(page)
  count = len(record_list)

  placeid = records[0].id
  place = get_object_or_404(Place, id=placeid)
  print('placeid',placeid)

  # recon task hits
  raw_hits = Hit.objects.all().filter(place_id=placeid, task_id=tid).order_by('query_pass','-score')

  # convert ccodes to names
  countries = []
  for r in records[0].ccodes:
  #for r in place.ccodes:
    try:
      countries.append(ccodes[0][r]['gnlabel']+' ('+ccodes[0][r]['tgnlabel']+')')
    except:
      pass

  context = {
    'ds_id':pk, 'ds_label': ds.label, 'task_id': tid,
      'hit_list':raw_hits, 'authority': task.task_name[6:],
        'records': records, 'countries': countries, 'passnum': passnum,
        'page': page if request.method == 'GET' else str(int(page)-1)
  }

  # Hit model fields = ['task_id','authority','dataset','place_id',
  #     'query_pass','src_id','authrecord_id','json','geom' ]
  HitFormset = modelformset_factory(
    Hit,
    fields = ('id','authority','authrecord_id','query_pass','score','json'),
    form=HitModelForm, extra=0)
  formset = HitFormset(request.POST or None, queryset=raw_hits)
  context['formset'] = formset
  #print('context:',context)
  #print('formset data:',formset.data)
  method = request.method
  if method == 'GET':
    print('a GET, just rendering next')
  else:
    try:
      if formset.is_valid():
        # get the task
        hits = formset.cleaned_data
        matches = 0
        for x in range(len(hits)):
          hit = hits[x]['id']
          #if hits[x]['match'] not in ['none','related']:
          if hits[x]['match'] not in ['none']:
            matches += 1
            if task.task_name != 'align_whg':
              # not accessioning; augmenting with links (& geom if requested)
              print('posting links from this hit:',hits[x])
              # if 'accept geometries' was checked in 'Initiate Reconciliation'
              if kwargs['aug_geom'] == 'on' and 'geoms' in hits[x]['json']:
                geom = PlaceGeom.objects.create(
                  place_id = place,
                  task_id = tid,
                  src_id = place.src_id,
                  # {"type": "Point", "geowkt": "POINT(20.58 -19.83)", "citation": {"id": "dplace:SCCS", "label": "Standard cross-cultural sample"}, "coordinates": [20.58, -19.83]}
                  jsonb = {
                    "type":hits[x]['json']['geoms'][0]['type'],
                    "citation":{"id":"wd:"+hits[x]['authrecord_id'],"label":"Wikidata"},
                    "coordinates":hits[x]['json']['geoms'][0]['coordinates']
                  }
                )
              ds.save()

              # create place_link for matched authority record
              print('place_link for', task.task_name,hits[x]['authrecord_id'])
              link = PlaceLink.objects.create(
                place_id = place,
                task_id = tid,
                src_id = place.src_id,
                jsonb = {
                  "type":hits[x]['match'],
                  "identifier":link_uri(task.task_name,hits[x]['authrecord_id'] if hits[x]['authority'] != 'whg' \
                      else hits[x]['json']['place_id'])
                }
              )
              print('place_link', link)
              
              # update <ds>.numlinked, <ds>.total_links
              ds.numlinked = ds.numlinked +1
              ds.total_links = ds.total_links +1
              ds.save()
  
              # grab links in the case of wikidata
              # TODO: check not duplicate
              if 'links' in hits[x]['json']:
                for l in hits[x]['json']['links']:
                  link = PlaceLink.objects.create(
                    place_id = place,
                    task_id = tid,
                    src_id = place.src_id,
                    jsonb = {
                      #"type": re.search("^(.*?):", l).group(1),
                      "type": hits[x]['match'],
                      "identifier": re.search("\: (.*?)$", l).group(1)
                    }
                  )
                  print('posted',link.jsonb)
                  # update totals
                  ds.numlinked = ds.numlinked +1
                  ds.total_links = ds.total_links +1
                  ds.save()
              
            elif task.task_name == 'align_whg':
              # 
              print('see if match for '+str(placeid)+' ('+str(hits[x]['json']['place_id'])+
                    ') is parent or child in index')
              indexMatch(placeid, hits[x]['json']['place_id'])
          elif hits[x]['match'] == 'none' and ds.label in ['gn500','gnmore','tgn_filtered_01']:
            indexMatch(placeid)
            print('made record for:',placeid)
          # flag as reviewed
          matchee = get_object_or_404(Hit, id=hit.id)
          matchee.reviewed = True
          matchee.save()
          #
        if matches == 0:
          print('no matches for',placeid)
          # none are matches, make this place a parent
          #indexMatch(placeid)
          
        return redirect('/datasets/'+str(pk)+'/review/'+tid+'/'+passnum+'?page='+str(int(page)))
      else:
        print('formset is NOT valid')
        #print('formset data:',formset.data)
        print('errors:',formset.errors)
    except:
      sys.exit(sys.exc_info())

  return render(request, 'datasets/review.html', context=context)


# *
# initiate, monitor align_tgn Celery task
def dataset_recon(request, pk):
  ds = get_object_or_404(Dataset, id=pk)
  # TODO: handle multipolygons from "#area_load" and "#area_draw"
  me = request.user
  #print('me',me,me.id)
  context = {"dataset": ds.name}

  types_ok=['ccodes','copied','drawn']
  userareas = Area.objects.all().filter(type__in=types_ok).order_by('-created')
  # TODO: this line throws an error but executes !?
  context['area_list'] = userareas if me.username == 'whgadmin' else userareas.filter(owner=me)

  predefined = Area.objects.all().filter(type='predefined').order_by('-created')
  context['region_list'] = predefined

  if request.method == 'GET':
    print('request:',request)
  elif request.method == 'POST' and request.POST:
    print('request.POST:',request.POST)
    # TODO: has this dataset/authority been done before?
    auth = request.POST['recon']
    # what task?
    func = eval('align_'+auth)
    # TODO: let this vary per authority?
    region = request.POST['region'] # pre-defined UN regions
    userarea = request.POST['userarea'] # from ccodes, loaded, or drawn
    aug_geom = request.POST['geom'] if 'geom' in request.POST else '' # on == write geom if matched
    bounds={
      "type":["region" if region !="0" else "userarea"],
          "id": [region if region !="0" else userarea]
    }
    #print('bounds',bounds)
    # run celery/redis tasks e.g. align_tgn, align_whg, align_wd
    result = func.delay(
      ds.id,
      ds=ds.id,
        dslabel=ds.label,
        owner=ds.owner.id,
        bounds=bounds,
        aug_geom=aug_geom
    )

    context['task_id'] = result.id
    context['response'] = result.state
    context['dataset id'] = ds.label
    context['authority'] = request.POST['recon']
    context['region'] = request.POST['region']
    context['userarea'] = request.POST['userarea']
    context['geom'] = aug_geom
    context['result'] = result.get()
    pprint(locals())
    ds.status = 'reconciling'
    ds.save()
    return render(request, 'datasets/dataset_recon.html', {'ds':ds, 'context': context})

  print('context recon GET',context)
  return render(request, 'datasets/dataset_recon.html', {'ds':ds, 'context': context})

def task_delete(request,tid,scope="foo"):
  hits = Hit.objects.all().filter(task_id=tid)
  tr = get_object_or_404(TaskResult, task_id=tid)
  ds = tr.task_args[1:-1]

  # in any case, reviewed=false for hits; clear match records
  for h in hits:
    h.reviewed = False
    h.save()
  placelinks = PlaceLink.objects.all().filter(task_id=tid)
  placegeoms = PlaceGeom.objects.all().filter(task_id=tid)
  #placenames = PlaceName.objects.all().filter(task_id=tid)
  #placedescriptions = PlaceDescription.objects.all().filter(task_id=tid)

  placelinks.delete()
  placegeoms.delete()
  #placenames.delete()
  #placedescriptions.delete()

  # zap task record & its hits
  if scope == 'task':
    tr.delete()
    hits.delete()

  return redirect('/datasets/'+ds+'/detail')


# better table for viewing datasets
#def drf_table(request, label, f):
  ## need only for title; calls API w/javascript for data
  #ds = get_object_or_404(Dataset, label=label)
  #filt = f
  #return render(request, 'datasets/drf_table.html', {'ds':ds,'filter':filt})

# replacing drf_table
def dataset_browse(request, label, f):
  # need only for title; calls API w/javascript for data
  ds = get_object_or_404(Dataset, label=label)
  filt = f
  return render(request, 'datasets/dataset_browse.html', {'ds':ds,'filter':filt})

def ds_list(request, label):
  # fetch places in specified dataset
  print('in ds_list() for',label)
  qs = Place.objects.all().filter(dataset=label)
  geoms=[]
  for p in qs.all():
    feat={"type":"Feature",
          "properties":{"src_id":p.src_id,"name":p.title},
              "geometry":p.geoms.first().jsonb}
    geoms.append(feat)
  return JsonResponse(geoms,safe=False)

# insert lpf into database
def ds_insert_lpf(request, pk):
  import os,codecs,json
  ds = get_object_or_404(Dataset, id=pk)
  [countrows,countlinked]= [0,0]
  infile = ds.file.open(mode="r")
  #objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
          #"PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
            #"PlaceDepictions":[]}
  with ds.file:
    jdata = json.loads(ds.file.read())
    #ds.file.open('rU')
    #print('jdata from insert',jdata)
    for feat in jdata['features']:
      print('feat properties:',feat['properties'])
      objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
              "PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
                "PlaceDepictions":[]}
      countrows += 1
      #
      print(feat['@id'],feat['properties']['title'],feat.keys())
      # TODO: get src_id into LP format

      # start Place record & save to get id
      # Place: src_id, title, ccodes, dataset
      newpl = Place(
        # TODO: add src_id to properties in LP format?
        src_id=feat['@id'] if 'http' not in feat['@id'] and len(feat['@id']) < 25 \
          else re.search("(\/|=)(?:.(?!\/|=))+$",feat['@id']).group(0)[1:],
        dataset=ds,
        title=feat['properties']['title'],
        ccodes=feat['properties']['ccodes'] if 'ccodes' in feat['properties'].keys() else []
      )
      newpl.save()

      # PlaceName: place_id,src_id,toponym,task_id,jsonb:{toponym, lang,citation,when{}}
      # TODO: adjust for 'ethnic', 'demonym'
      for n in feat['names']:
        print('from feat[names]:',n)
        if 'toponym' in n.keys():
          objs['PlaceNames'].append(PlaceName(
            place_id=newpl,
            src_id=newpl.src_id,
            toponym=n['toponym'],
            jsonb=n,
            task_id='initial'
          ))

      # PlaceType: place_id,src_id,task_id,jsonb:{identifier,label,src_label}
      if 'types' in feat.keys():
        for t in feat['types']:
          #print('from feat[types]:',t)
          objs['PlaceTypes'].append(PlaceType(
            place_id=newpl,
            src_id=newpl.src_id,
            jsonb=t
          ))

      # PlaceWhen: place_id,src_id,task_id,minmax,jsonb:{timespans[],periods[],label,duration}
      if 'whens' in feat.keys():
        for w in feat['whens']:
          objs['PlaceWhens'].append(PlaceWhen(
            place_id=newpl,src_id=newpl.src_id,jsonb=w))

      # PlaceGeom: place_id,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
      if 'geometry' in feat.keys():
        for g in feat['geometry']['geometries']:
          #print('from feat[geometry]:',g)
          objs['PlaceGeoms'].append(PlaceGeom(
            place_id=newpl,src_id=newpl.src_id,jsonb=g))

      # PlaceLink: place_id,src_id,task_id,jsonb:{type,identifier}
      if 'links' in feat.keys():
        for l in feat['links']:
          if len(feat['links'])>0: countlinked +=1
          objs['PlaceLinks'].append(PlaceLink(
            place_id=newpl,src_id=newpl.src_id,jsonb=l,task_id='initial'))

      # PlaceRelated: place_id,src_id,task_id,jsonb{relationType,relationTo,label,when{}}
      if 'relations' in feat.keys():
        for r in feat['relations']:
          objs['PlaceRelated'].append(PlaceRelated(
            place_id=newpl,src_id=newpl.src_id,jsonb=r))

      # PlaceDescription: place_id,src_id,task_id,jsonb{@id,value,lang}
      if 'descriptions' in feat.keys():
        for des in feat['descriptions']:
          objs['PlaceDescriptions'].append(PlaceDescription(
            place_id=newpl,src_id=newpl.src_id,jsonb=des))

      # PlaceDepiction: place_id,src_id,task_id,jsonb{@id,title,license}
      if 'depictions' in feat.keys():
        for dep in feat['depictions']:
          objs['PlaceDepictions'].append(PlaceDepiction(
            place_id=newpl,src_id=newpl.src_id,jsonb=dep))

      #print("objs['PlaceNames']",objs['PlaceNames'])
      PlaceName.objects.bulk_create(objs['PlaceNames'])
      PlaceType.objects.bulk_create(objs['PlaceTypes'])
      PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
      PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
      PlaceLink.objects.bulk_create(objs['PlaceLinks'])
      PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
      PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
      PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])

      # write some summary attributes
      ds.numrows = countrows
      ds.numlinked = countlinked
      ds.total_links = len(objs['PlaceLinks'])
      ds.status = 'uploaded'
      ds.save()

    #print('record:', ds.__dict__)
    ds.file.close()

  print(str(countrows)+' processed')
  messages.add_message(request, messages.INFO, 'inserted lpf for '+str(countrows)+' places')
  return redirect('/dashboard')

#
# insert LP-TSV file to database
def ds_insert_tsv(request, pk):
  # retrieve just-added file, insert to db
  import os, csv, codecs,json
  dataset = get_object_or_404(Dataset, id=pk)
  context = {'status': 'inserting'} #??

  #infile=codecs.open('example_data/tgn2000.tsv','r','utf-8')
  #infile=codecs.open('example_data/tgn2000.tsv','r','utf-8')
  infile = dataset.file.open(mode="r")
  print('ds_insert_tsv(); request.GET; infile',request.GET,infile)
  # should already know delimiter
  try:
    dialect = csv.Sniffer().sniff(infile.read(16000),['\t',';','|'])
    reader = csv.reader(infile, dialect)
  except:
    reader = csv.reader(infile, delimiter='\t')
  infile.seek(0)
  header = next(reader, None)
  print('header', header)

  objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
          "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[],
            "PlaceDepiction":[]}

  # TSV * = required; ^ = encouraged
  # lists within fields are ';' delimited, no brackets
  # id*, title*, title_source*, title_uri^, ccodes[]^, matches[]^, variants[]^, types[]^, aat_types[]^,
  # parent_name, parent_id, lon, lat, geowkt, geo_source, geo_id, start, end

  def makeCoords(lonstr,latstr):
    lon = float(lonstr) if lonstr != '' else ''
    lat = float(latstr) if latstr != '' else ''
    coords = [] if (lonstr == ''  or latstr == '') else [lon,lat]
    return coords

  #
  # TODO: what if simultaneous inserts?
  countrows=0
  countlinked = 0
  countlinks = 0
  for r in reader:
  #for i, r in zip(range(10000), reader):
    src_id = r[header.index('id')]
    #print('src_id from tsv_insert',src_id)
    title = r[header.index('title')].replace("' ","'")
    # for PlaceName insertion, strip anything in parens
    title = re.sub('\(.*?\)', '', title)
    title_source = r[header.index('title_source')]
    print('src_id, title, title_source from tsv_insert',src_id,title,title_source)
    title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
    variants = [x.strip() for x in r[header.index('variants')].split(';')] \
      if 'variants' in header else []
    types = [x.strip() for x in r[header.index('types')].split(';')] \
      if 'types' in header else []
    aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
      if 'aat_types' in header else []
    #print('types, aat_types',types, aat_types)
    ccodes = [x.strip() for x in r[header.index('ccodes')].split(';')] \
      if 'ccodes' in header else []
    parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
    parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
    coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
      if 'lon' in header and 'lat' in header else []
    #matches = [x.strip() for x in r[header.index('matches')].split(';')] \
      #if 'matches' in header else []
    matches = [x.strip() for x in r[header.index('matches')].split(';')] \
      if 'matches' in header and r[header.index('matches')] != '' else []
    start = r[header.index('start')] if 'start' in header else ''
    end = r[header.index('end')] if 'end' in header else ''
    # not sure this will get used
    minmax = [start,end]
    description = r[header.index('description')] \
      if 'description' in header else ''

    # build and save Place object
    # id now available as newpl
    newpl = Place(
      src_id = src_id,
      dataset = dataset,
      title = title,
      ccodes = ccodes
    )
    newpl.save()
    countrows += 1

    # build associated objects and add to arrays
    # PlaceName()
    objs['PlaceName'].append(
      PlaceName(
        place_id=newpl,
        src_id = src_id,
        toponym = title,
        jsonb={"toponym": title, "citation": {"id":title_uri,"label":title_source}}
    ))

    # variants if any; same source as title toponym?
    if len(variants) > 0:
      for v in variants:
        objs['PlaceName'].append(
          PlaceName(
            place_id=newpl,
            src_id = src_id,
            toponym = v,
            jsonb={"toponym": v, "citation": {"id":"","label":title_source}}
        ))

    # PlaceType()
    if len(types) > 0:
      for i,t in enumerate(types):
        aatnum=aat_types[i] if len(aat_types) >= len(types) else ''
        objs['PlaceType'].append(
          PlaceType(
            place_id=newpl,
            src_id = src_id,
            jsonb={ "identifier":aatnum,
                    "sourceLabel":t,
                    "label":aat_lookup(int(aatnum)) if aatnum !='' else ''
                  }
        ))

    # PlaceGeom()
    # TODO: test geometry type or force geojson
    if len(coords) > 0:
      objs['PlaceGeom'].append(
        PlaceGeom(
          place_id=newpl,
          src_id = src_id,
          jsonb={"type": "Point", "coordinates": coords,
                      "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
      ))
    elif 'geowkt' in header and r[header.index('geowkt')] not in ['',None]: # some rows no geom
      objs['PlaceGeom'].append(
        PlaceGeom(
          place_id=newpl,
          src_id = src_id,
          # make GeoJSON using shapely
          jsonb=parse_wkt(r[header.index('geowkt')])
      ))

    # PlaceLink() - all are closeMatch
    if len(matches) > 0:
      countlinked += 1
      for m in matches:
        countlinks += 1
        objs['PlaceLink'].append(
          PlaceLink(
            place_id=newpl,
            src_id = src_id,
            jsonb={"type":"closeMatch", "identifier":m}
        ))

    # PlaceRelated()
    if parent_name != '':
      objs['PlaceRelated'].append(
        PlaceRelated(
          place_id=newpl,
          src_id=src_id,
          jsonb={
            "relationType": "gvp:broaderPartitive",
            "relationTo": parent_id,
            "label": parent_name}
      ))

    # PlaceWhen()
    # timespans[{start{}, end{}}], periods[{name,id}], label, duration
    if start != '':
      objs['PlaceWhen'].append(
        PlaceWhen(
          place_id=newpl,
          src_id = src_id,
          jsonb={
                "timespans": [{"start":{"earliest":minmax[0]}, "end":{"latest":minmax[1]}}]
              }
      ))

    #
    # PlaceDescription()
    if description != '':
      objs['PlaceDescription'].append(
        PlaceDescription(
          place_id=newpl,
          src_id = src_id,
          jsonb={
          }
        ))

  print('COUNTS:')
  print('PlaceName:',len(objs['PlaceName']))
  print('PlaceType:',len(objs['PlaceType']))
  print('PlaceGeom:',len(objs['PlaceGeom']))
  print('PlaceLink:',len(objs['PlaceLink']))
  print('PlaceRelated:',len(objs['PlaceRelated']))
  print('PlaceWhen:',len(objs['PlaceWhen']))
  print('PlaceDescription:',len(objs['PlaceDescription']))
  print('max places.id', )

  # bulk_create(Class, batch_size=n) for each
  PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10000)
  print('names done')
  PlaceType.objects.bulk_create(objs['PlaceType'],batch_size=10000)
  print('types done')
  PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
  print('geoms done')
  PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
  print('links done')
  PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
  print('related done')
  PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
  print('whens done')
  PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)
  print('descriptions done')

  context['status'] = 'uploaded'
  print('rows,linked,links:',countrows,countlinked,countlinks)
  dataset.numrows = countrows
  dataset.numlinked = countlinked
  dataset.total_links = countlinks
  dataset.header = header
  dataset.status = 'uploaded'
  dataset.save()
  print('record:', dataset.__dict__)
  print('context from ds_insert_tsv():',context)
  infile.close()
  # dataset.file.close()

  #return render(request, '/datasets/dashboard.html', {'context': context})
  return redirect('/dashboard', context=context)


# list user datasets, area, place collections
class DashboardView(ListView):
  context_object_name = 'dataset_list'
  template_name = 'datasets/dashboard.html'

  def get_queryset(self):
    # TODO: make .team() a method on User
    me = self.request.user
    if me.username in ['whgadmin','karlg']:
      print('in get_queryset() if',me)
      return Dataset.objects.all().order_by('status','-spine','-id')
      #return Dataset.objects.all().filter(id__gt=7).order_by('id')
    else:
      print('in get_queryset() else')
      print('myteam(me)',myteam(me))
      #return Dataset.objects.filter(owner__in=myteam(me)).order_by('id')
      #return Dataset.objects.filter( Q(owner__in=myteam(me)) | Q(spine="True")).order_by('-id')
      return Dataset.objects.filter( Q(owner__in=myteam(me)) | Q(id__lt=3)).order_by('-id')


  def get_context_data(self, *args, **kwargs):
    teamtasks=[]
    me = self.request.user
    context = super(DashboardView, self).get_context_data(*args, **kwargs)
    print('in get_context',me)

    types_ok=['ccodes','copied','drawn']
    # list areas
    userareas = Area.objects.all().filter(type__in=types_ok).order_by('created')
    context['area_list'] = userareas if me.username == 'whgadmin' else userareas.filter(owner=self.request.user)

    # list team tasks WHY?????
    #if me.username == 'whgadmin':
      #context['review_list'] = TaskResult.objects.filter(status='SUCCESS').order_by('-date_done')
    #else:
      #for t in TaskResult.objects.filter(status='SUCCESS'):
        #tj=json.loads(t.task_kwargs.replace("\'", "\""))
        #u=get_object_or_404(User,id=tj['owner'])
        #print('get_context else...args,task owner',tj,u)
        #if u in myteam(me):
          #teamtasks.append(t.task_id)
      #context['review_list'] = TaskResult.objects.filter(task_id__in=teamtasks).order_by('-date_done')

    # status >= 'uploaded'
    context['viewable'] = ['uploaded','reconciling','review_hits','reviewed','review_whg','indexed']
    # TODO: user place collections
    #print('DashboardView context:', context)
    return context


# upload file, verify format
class DatasetCreateView(CreateView):
  form_class = DatasetModelForm
  template_name = 'datasets/dataset_create.html'
  queryset = Dataset.objects.all()
  #success_url = reverse('datasets:dataset-create')
  def form_valid(self, form):
    context={}
    #if form.is_valid():
    u=self.request.user
    print('form is valid',u)
    format = form.cleaned_data['format']
    label = form.cleaned_data['name'][:16]+'_'+u.first_name[:1]+u.last_name[:1]
    # open & write tempf to a temp location;
    # call it tempfn for reference
    tempf, tempfn = tempfile.mkstemp()
    try:
      for chunk in form.cleaned_data['file'].chunks():
        os.write(tempf, chunk)
    except:
      raise Exception("Problem with the input file %s" % request.FILES['file'])
    finally:
      os.close(tempf)
    # open the temp file
    fin = codecs.open(tempfn, 'r', 'utf8')
    # send for format validation
    if format == 'delimited':
      result = validate_tsv(fin)
    elif format == 'lpf':
      # coll = FeatureCollection
      # TODO: alternate json-lines
      result = validate_lpf(fin,'coll')
    # print('cleaned_data',form.cleaned_data)
    fin.close()

    print('got past validation, still in DatasetCreateView')
    # insert to db & advance to dataset_detail if validated
    # otherwise present form again with errors
    if len(result['errors']) == 0:
      context['status'] = 'format_ok'
      print('cleaned_data:after ->',form.cleaned_data)
      #print('columns, type', result['columns'], type(result['columns']))
      obj = form.save(commit=False)
      obj.status = 'format_ok'
      obj.format = result['format']
      obj.delimiter = result['delimiter'] if "delimiter" in result.keys() else "n/a"
      obj.numrows = result['count']
      obj.header = result['columns'] if "columns" in result.keys() else []
      obj.save()

      # inserts data, goes to detail page
      # return self.render_to_response(self.get_context_data(form=form,context=context))
      return super().form_valid(form)

    else:
      context['status'] = 'format_error'
      context['errors'] = result
      context['action'] = 'review'
      result['columns'] if "columns" in result.keys() else []
      print('result:', result)
      return self.render_to_response(self.get_context_data(form=form,context=context))
    #else:
      #print('form not valid', form.errors)
      #context['errors'] = form.errors
    #context['status'] = 'format_ok'
    #context['action'] = 'review'
    #print('context from Create',context)
    #return super().form_valid(form)
    # dataset is valid and record created; not imported


  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCreateView, self).get_context_data(*args, **kwargs)
    #context['action'] = 'create'
    return context


# detail
class DatasetDetailView(UpdateView):
  form_class = DatasetDetailModelForm
  template_name = 'datasets/dataset_detail.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/datasets/'+str(id_)+'/detail'

  def form_valid(self, form):
    context={}
    if form.is_valid():
      print('form is valid')
      print('cleaned_data: before ->', form.cleaned_data)
    else:
      print('form not valid', form.errors)
      context['errors'] = form.errors
    return super().form_valid(form)

  def get_object(self):
    #print('kwargs:',self.kwargs)
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetDetailView, self).get_context_data(*args, **kwargs)
    print('DatasetDetailView get_context_data() kwargs:',self.kwargs)
    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)
    context['updates'] = {}
    # fumbling to include to-be-reviewed count updates here
    #task_ids=[t.task_id for t in ds.tasks.all()]
    #for tid in task_ids:
      #context['updates'][tid] = Hit.objects.all().filter(task_id=tid,reviewed=False).count()
    bounds = self.kwargs.get("bounds")
    # print('ds',ds.label)
    context['status'] = ds.status
    context['format'] = ds.format
    placeset = Place.objects.filter(dataset=ds.label)
    context['tasks'] = TaskResult.objects.all().filter(task_args = [id_],status='SUCCESS')
    # initial (non-task)
    context['num_links'] = PlaceLink.objects.filter(
      place_id_id__in = placeset, task_id = None).count()
    context['num_names'] = PlaceName.objects.filter(place_id_id__in = placeset, task_id = None).count()
    context['num_geoms'] = PlaceGeom.objects.filter(
      place_id_id__in = placeset, task_id = None).count()
    context['num_descriptions'] = PlaceDescription.objects.filter(
      place_id_id__in = placeset, task_id = None).count()
    # others
    context['num_types'] = PlaceType.objects.filter(
      place_id_id__in = placeset).count()
    context['num_when'] = PlaceWhen.objects.filter(
      place_id_id__in = placeset).count()
    context['num_related'] = PlaceRelated.objects.filter(
      place_id_id__in = placeset).count()
    context['num_depictions'] = PlaceDepiction.objects.filter(
      place_id_id__in = placeset).count()

    # augmentations (has task_id)
    context['links_added'] = PlaceLink.objects.filter(
      place_id_id__in = placeset, task_id__contains = '-').count()
    context['names_added'] = PlaceName.objects.filter(
      place_id_id__in = placeset, task_id__contains = '-').count()
    context['geoms_added'] = PlaceGeom.objects.filter(
      place_id_id__in = placeset, task_id__contains = '-').count()
    context['descriptions_added'] = PlaceDescription.objects.filter(
      place_id_id__in = placeset, task_id__contains = '-').count()

    # insert to db immediately if format okay
    if context['status'] == 'format_ok':
      print('format_ok, inserting dataset '+str(id_))
      if context['format'] == 'delimited':
        ds_insert_tsv(self.request, id_)
      else:
        ds_insert_lpf(self.request,id_)

    print('context from DatasetDetailView',context)
    return context


# confirm ok on delete
class DatasetDeleteView(DeleteView):
  template_name = 'datasets/dataset_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_success_url(self):
    return reverse('dashboard')

