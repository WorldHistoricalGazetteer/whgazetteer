# datasets.views
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User
from django.contrib.gis.geos import GEOSGeometry
from django.core.files import File
from django.core.mail import send_mail
from django.core.paginator import Paginator
from django.db.models import Q
from django.forms import modelformset_factory
from django.http import HttpResponseServerError, JsonResponse, HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, redirect
from django.urls import reverse
from django.views.generic import (CreateView, ListView, UpdateView, DeleteView, DetailView)
from django_celery_results.models import TaskResult

# external
from celery import current_app as celapp
from chardet import detect
import codecs, math, mimetypes, os, re, shutil, sys, tempfile
import django_tables2 as tables
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
import pandas as pd
import simplejson as json
from pathlib import Path
from shutil import copyfile
#
from areas.models import Area
from collection.models import Collection
from datasets.forms import HitModelForm, DatasetDetailModelForm, DatasetCreateModelForm
from datasets.models import Dataset, Hit, DatasetFile
from datasets.static.hashes import mimetypes_plus as mthash_plus
from datasets.static.hashes.parents import ccodes as cchash
# NB these task names ARE in use; they are generated dynamically
from datasets.tasks import align_wdlocal, align_idx, align_tgn, maxID
from datasets.utils import *
from elastic.es_utils import makeDoc,deleteFromIndex, replaceInIndex
from main.choices import AUTHORITY_BASEURI
from main.models import Log, Comment
from places.models import *
from resources.models import Resource


"""used for Celery down notice"""
def emailer(subj, msg, from_addr, to_addr):
  print('subj, msg, from_addr, to_addr',subj, msg, from_addr, to_addr)
  send_mail(
      subj, msg, from_addr, to_addr,
      fail_silently=False,
  )
  #'whgazetteer@gmail.com',
  #['karl@kgeographer.org'],

def celeryUp():
  response = celapp.control.ping(timeout=1.0)
  return len(response)>0
""" append src_id to base_uri"""
def link_uri(auth,id):
  baseuri = AUTHORITY_BASEURI[auth]
  uri = baseuri + str(id)
  return uri

"""
# from datasets.views.review()
# indexes a db record upon match reviewing align_idx hits
# if close or exact -> if match is parent -> make child else if match is child -> make sibling
"""
def indexMatch(pid, hit_pid=None):
  print('indexMatch(): pid '+str(pid)+' w/hit_pid '+str(hit_pid))
  from elasticsearch import Elasticsearch
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  idx='whg'

  if hit_pid == None:
    print('making '+str(pid)+' a parent')
    # TODO:
    whg_id=maxID(es,idx) +1
    place=get_object_or_404(Place,id=pid)
    print('new whg_id',whg_id)
    #parent_obj = makeDoc(place,'none')
    parent_obj = makeDoc(place)
    parent_obj['relation']={"name":"parent"}
    # parents get an incremented _id & whg_id
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
      place.indexed = True
      place.save()
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
    #child_obj = makeDoc(place,'none')
    child_obj = makeDoc(place)
    child_obj['relation']={"name":"child","parent":parent_whgid}

    # all or nothing; pass if error
    try:
      # index child
      es.index(index=idx,doc_type='place',id=place.id,
                routing=1,body=json.dumps(child_obj))
      #count_kids +=1
      print('added '+str(place.id) + ' as child of '+ str(hit_pid))

      # add child's names to parent's searchy & suggest.input[] fields
      q_update = { "script": {
          "source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id); ctx._source.searchy.addAll(params.names)",
          "lang": "painless",
          "params":{"names": match_names, "id": str(place.id)}
        },
        "query": {"match":{"_id": parent_whgid}}}
      es.update_by_query(index=idx, doc_type='place', body=q_update, conflicts='proceed')
      place.indexed = True
      place.save()
      print('indexed '+str(pid)+' as child of '+str(parent_whgid), child_obj)
    except:
      print('failed indexing '+str(pid)+' as child of '+str(parent_whgid), child_obj)
      #count_fail += 1
      pass
      #sys.exit(sys.exc_info())



#def isOwner(user):
  #task = get_object_or_404(TaskResult, task_id=tid)
  #kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  #return kwargs['owner'] == user.id


"""
# review reconciliation results
# called from detail#reconciliation passnum links on
# dataset pk, celery task_id
# responds to GET for display, POST if 'save' button submits
"""
# .../datasets/835/review/b4cad8c9-0bcd-492f-83c6-be68bc6bdca4/pass2
def review(request, pk, tid, passnum):
  ds = get_object_or_404(Dataset, id=pk)
  task = get_object_or_404(TaskResult, task_id=tid)
  auth = task.task_name[6:].replace('local','')
  authname = 'Wikidata' if auth == 'wd' else 'Getty TGN' \
    if auth == 'tgn' else 'WHG'
  kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  #review_request = request.GET.__dict__ if request.method == 'GET' \
    #else request.POST.__dict__
  #print('review() request', review_request)
  beta = 'beta' in list(request.user.groups.all().values_list('name',flat=True))

  # try addin place list table in left column
  #table = PlaceTable(ds.places.all())

  # filter place records by passnum for those with unreviewed hits on this task
  # if request passnum is complete, increment
  cnt_pass = Hit.objects.values('place_id').filter(task_id=tid, reviewed=False, query_pass=passnum).count()

  # TODO: refactor this awful mess; controls whether PASS appears in review dropdown
  cnt_pass0 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass0').count()
  cnt_pass1 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass1').count()
  cnt_pass2 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass2').count()
  cnt_pass3 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass3').count()

  # TODO: get reviewed/deferred status from ds.places.filter(review_wd__in) e.g.
  if passnum.startswith('pass'):
    pass_int = int(passnum[4])
    # if no unreviewed left, go to next pass
    passnum = passnum if cnt_pass > 0 else 'pass'+str(pass_int+1)
    hitplaces = Hit.objects.values('place_id').filter(
      task_id=tid,
      reviewed=False,
      query_pass=passnum)
    # remove any deferred
  else:
    # queue deferred from any pass
    hitplaces = Hit.objects.values('place_id').filter(task_id=tid, reviewed=False)

  print('review() hitplaces', hitplaces)
  # separate review pages
  if auth in ['whg','idx']:
    review_page = 'accession.html'
  else:
    review_page = 'review.html'

  # record_list is all unreviewed or only deferred
  review_field = 'review_whg' if auth in ['whg','idx'] else \
    'review_wd' if auth.startswith('wd') else 'review_tgn'
  #lookup = '__'.join([review_field, 'exact'])
  lookup = '__'.join([review_field, 'in'])
  # 2 is deferred; 0 is unreviewed
  status = [2] if passnum == 'def' else [0,2]
  #status = [2] if passnum == 'def' else [0]
  #record_list = ds.places.order_by('id').filter(pk__in=hitplaces, **{lookup: status})
  record_list = ds.places.order_by('id').filter(**{lookup: status}, pk__in=hitplaces)

  #if passnum != 'def' and hitplaces.count() >0:

  # no records left for pass (or in deferred queue)
  if len(record_list) == 0:
    context = {
      "nohits":True,
      'ds_id':pk,
      'task_id': tid,
      'passnum': passnum,
    }
    return render(request, 'datasets/'+review_page, context=context)

  # TODO: if 2 reviewers, save by one flags
  # manage pagination & urls
  # gets next place record as records[0]
  paginator = Paginator(record_list, 1)
  page = 1 if not request.GET.get('page') else \
    request.GET.get('page')
  records = paginator.get_page(page)
  count = len(record_list)
  placeid = records[0].id
  place = get_object_or_404(Place, id=placeid)
  #print('reviewing '+str(count)+' hits for place', records[0])
  if passnum.startswith('pass'):
    raw_hits = Hit.objects.filter(place_id=placeid, task_id=tid, query_pass=passnum).order_by('-score')
  else:
    raw_hits = Hit.objects.filter(place_id=placeid, task_id=tid).order_by('-score')
  #print('raw_hits for '+str(records[0]), raw_hits)
  # convert ccodes to names
  countries = []
  #for r in records[0].ccodes:
  for r in place.ccodes:
    #print('r',r.upper())
    try:
      countries.append(cchash[0][r.upper()]['gnlabel']+
        ' ('+cchash[0][r.upper()]['tgnlabel']+')')
    except:
      pass


  # TODO: if auth in ['whg','idx], group children within parents
  #print('records[0] in review()',records[0].__dict__)
  # prep some context
  context = {
    'ds_id': pk, 'ds_label': ds.label, 'task_id': tid,
    'hit_list': raw_hits,
    'authority': task.task_name[6:8] if auth=='wdlocal' else task.task_name[6:],
    'records': records,
    'countries': countries,
    'passnum': passnum,
    'page': page if request.method == 'GET' else str(int(page)-1),
    'aug_geom': json.loads(task.task_kwargs.replace("'",'"'))['aug_geom'],
    'mbtokenmb': settings.MAPBOX_TOKEN_MB,
    'count_pass0': cnt_pass0,
    'count_pass1': cnt_pass1,
    'count_pass2': cnt_pass2,
    'count_pass3': cnt_pass3,
    'deferred': True if passnum =='def' else False,

  }

  # Hit model fields = ['task_id','authority','dataset','place_id',
  #     'query_pass','src_id','authrecord_id','json','geom' ]
  HitFormset = modelformset_factory(
    Hit,
    fields = ('id','authority','authrecord_id','query_pass','score','json'),
    form=HitModelForm, extra=0)
  formset = HitFormset(request.POST or None, queryset=raw_hits)
  context['formset'] = formset
  method = request.method

  # GET: just display; POST: process match/no match choices
  if method == 'GET':
    print('review() GET, just rendering next')
  else:
    # process review choices
    place_post = get_object_or_404(Place,pk=request.POST['place_id'])
    if formset.is_valid():
      hits = formset.cleaned_data
      #print('hits (formset.cleaned_data)',hits)
      matches = 0
      for x in range(len(hits)):
        hit = hits[x]['id']
        # is this hit a match?
        if hits[x]['match'] not in ['none']:
          matches += 1
          # if wd or tgn, write place_geom, place_link record(s) now
          # IF someone didn't just review it!
          if task.task_name[6:] in ['wdlocal','wd','tgn']:
            #print('task.task_name', task.task_name)
            hasGeom = 'geoms' in hits[x]['json'] and len(hits[x]['json']['geoms']) > 0
            # only if 'accept geometries' was checked
            if kwargs['aug_geom'] == 'on' and hasGeom \
               and tid not in place_post.geoms.all().values_list('task_id',flat=True):
              gtype = hits[x]['json']['geoms'][0]['type']
              coords = hits[x]['json']['geoms'][0]['coordinates']
              gobj = json.dumps({"type":gtype,"coordinates":coords})
              PlaceGeom.objects.create(
                place = place_post,
                task_id = tid,
                src_id = place.src_id,
                geom = gobj,
                jsonb = {
                  "type":gtype,
                  "citation":{"id":auth+':'+hits[x]['authrecord_id'],"label":authname},
                  "coordinates":coords
                }
              )

            # create single PlaceLink for matched authority record
            # IF someone didn't just do it for this record
            if tid not in place_post.links.all().values_list('task_id',flat=True):
              link = PlaceLink.objects.create(
                place = place_post,
                task_id = tid,
                src_id = place.src_id,
                jsonb = {
                  "type":hits[x]['match'],
                  "identifier":link_uri(task.task_name,hits[x]['authrecord_id'] \
                      if hits[x]['authority'] != 'whg' else hits[x]['json']['place_id'])
                }
              )
              print('created place_link instance:', link)

            # create multiple PlaceLink records (e.g. Wikidata)
            # TODO: filter duplicates
            if 'links' in hits[x]['json']:
              #print('json links', hits[x]['json']['links'])
              for l in hits[x]['json']['links']:
                #print('l in links',l)
                authid = re.search("\: ?(.*?)$", l).group(1)
                print('authid',authid)
                if authid not in place.authids:
                  link = PlaceLink.objects.create(
                    place = place,
                    task_id = tid,
                    src_id = place.src_id,
                    jsonb = {
                      "type": hits[x]['match'],
                      #"identifier": authid.strip()
                      "identifier": l.strip()
                    }
                  )
                  #print('PlaceLink record created',link.jsonb)
                  # update totals
                  ds.numlinked = ds.numlinked +1 if ds.numlinked else 1
                  ds.total_links = ds.total_links +1
                  ds.save()
          # else: accessioning to whg index
          elif task.task_name == 'align_idx':
            print('indexing '+place_post.__str__()+' in some relation to hit: '+
                  str(hits[x]['id']))
            # match is to parent doc in the index
            # index as child
            # TODO: write database PlaceLink records for incoming & matched
            #indexMatch(placeid, hits[x]['json']['place_id'])
            place_post.indexed = True
            place_post.save()
          # informational lookup on whg index
          elif task.task_name == 'align_whg':
            print('align_whg (non-accessioning) DOING NOTHING (YET)')

        # in any case, flag hit as reviewed...
        print('hit '+str(hit.id)+' flagged reviewed')
        matchee = get_object_or_404(Hit, id=hit.id)
        matchee.reviewed = True
        matchee.save()

      # no matches for align_idx > index as parent
      if matches == 0 and task.task_name == 'align_idx':
        # index as new parent/seed
        print('indexing '+place_post.__str__()+' as new parent/seed')
        #indexMatch(placeid, None)
        #place_post.indexed = True
        #place_post.save()

      # set review_field status
      setattr(place_post, review_field, 1)
      place_post.save()

      return redirect('/datasets/'+str(pk)+'/review/'+tid+'/'+passnum+'?page='+str(int(page)))
    else:
      print('formset is NOT valid')
      print('formset data:',formset.data)
      print('errors:',formset.errors)
    #except:
      #sys.exit(sys.exc_info())
  print('review_page', review_page)
  return render(request, 'datasets/'+review_page, context=context)

"""
write_wd_pass0(taskid)
called from dataset_detail>reconciliation tab
accepts all pass0 wikidata matches, writes geoms and links

"""
# TEST SETUP
#from django.shortcuts import get_object_or_404
#from datasets.models import Dataset, Hit
#from places.models import Place, PlaceGeom, PlaceLink
#from django_celery_results.models import TaskResult
#import simplejson as json
#tid = '4770d8a4-cd43-4e08-8542-fa57418d5f2e' # wdlocal, ds=985, croniken_og_json
def write_wd_pass0(request, tid):
  task = get_object_or_404(TaskResult,task_id=tid)
  kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  referer = request.META.get('HTTP_REFERER') + '#reconciliation'
  auth = task.task_name[6:].replace('local','')
  ds = get_object_or_404(Dataset, pk=kwargs['ds'])
  authname = 'Wikidata'
  # get unreviewed pass0 hits
  hits = Hit.objects.filter(
    task_id=tid,
    query_pass='pass0',
    reviewed=False
  )
  for h in hits:
    hasGeom = 'geoms' in h.json and len(h.json['geoms']) > 0
    hasLinks = 'links' in h.json and len(h.json['links']) > 0
    #place = h.place_id # object
    place = h.place # object
    # existing for the place
    authids=place.links.all().values_list('jsonb__identifier',flat=True)
    # GEOMS
    # confirm another user hasn't just done this...
    if hasGeom and kwargs['aug_geom'] == 'on' \
       and tid not in place.geoms.all().values_list('task_id',flat=True):
      for g in h.json['geoms']:
        #place.geoms.all().values_list('task_id',flat=True)
        geom = PlaceGeom.objects.create(
          place = place,
          task_id = tid,
          src_id = place.src_id,
          jsonb = {
            "type":g['type'],
            "citation":{"id":auth+':'+h.authrecord_id,"label":authname},
            "coordinates":g['coordinates']
          }
        )
      #print('created place_geom instance:', geom)
    # LINKS
    link_counter = 0
    # add PlaceLink record for wikidata hit if not already there
    if 'wd:'+h.authrecord_id not in authids:
      link_counter += 1
      link = PlaceLink.objects.create(
        place = place,
        task_id = tid,
        src_id = place.src_id,
        jsonb = {
          "type": "closeMatch",
          "identifier":link_uri(task.task_name, h.authrecord_id)
        }
      )
      print('created wd place_link instance:', link)

    # create link for each wikidata concordance, if any
    if hasLinks:
      #authids=place.links.all().values_list(
        #'jsonb__identifier',flat=True)
      for l in h.json['links']:
        link_counter += 1
        authid = re.search("\:?(.*?)$", l).group(1)
        print(authid)
        # TODO: same no-dupe logic in review()
        # don't write duplicates
        if authid not in authids:
          link = PlaceLink.objects.create(
            place = place,
            task_id = tid,
            src_id = place.src_id,
            jsonb = {
              "type": "closeMatch",
              "identifier": authid
            }
          )
      print('created '+str(len(h.json['links']))+' place_link instances')

    # update dataset totals for metadata page
    #ds.numlinked = ds.numlinked +1 if ds.numlinked else 1
    # count distinct(place_id) in
    ds.numlinked = len(set(PlaceLink.objects.filter(place_id__in=ds.placeids).values_list('place_id',flat=True)))
    ds.total_links += link_counter
    ds.save()

    # flag hit as reviewed
    h.reviewed = True
    h.save()

    # flag place as reviewed
    place.review_wd = 1
    place.save()

  #return redirect('/datasets/'+str(ds.id)+'/detail#reconciliation')
  return HttpResponseRedirect(referer)

"""
write_idx_pass0(taskid)
called from dataset_detail>reconciliation tab
accepts all pass0 whg matches, indexes new child doc for each
if >1 match, compute parent winner and merge others as children
"""
#from django.shortcuts import get_object_or_404
#from datasets.models import Hit
#from places.models import Place
#from elastic.es_utils import makeDoc, topParent, demoteParents
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#tid = 'afe74607-da91-4317-801d-09243bdea61b'
def write_idx_pass0(request, tid):
  task = get_object_or_404(TaskResult,task_id=tid)
  kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  ds = get_object_or_404(Dataset, pk=kwargs['ds'])
  referer = request.META.get('HTTP_REFERER')
  #print('referer',referer)
  # get unreviewed pass0 hits
  hits = Hit.objects.filter(
    task_id=tid,
    query_pass='pass0',
    reviewed=False
  )
  # some have more than one hit
  pids = set([h.place_id for h in hits])
  chosen = [] # gather parent _ids
  for pid in pids:
    hset = [h for h in hits if h.place_id == pid]
    doc = makeDoc(get_object_or_404(Place, pk=pid))
    if len(hset) == 1:
      # index as child
      parent_id = hset[0].json['whg_id']
      doc['relation'] = {"name":"child", "parent": parent_id}
      names = list(set([n['toponym'] for n in doc['names']]))
      #print('index '+str(pid)+' as child of '+hset[0].json['whg_id'])
      # addChildren(pids[],parent)
      # es.index(index='whg', doc_type='place', id=d ,body=newsrcd, routing=1)
      chosen.append(parent_id)
      pass
    else:
      #
      parent_ids = [h.json['whg_id'] for h in hset]
      # calc weight as len(sources) + len(links)
      # create (whg_id, weight) sets
      parents = [(h.json['whg_id'], len(h.json['sources']) + \
                  len(h.json['links'])) for h in hset]
      already = len(set(chosen) & set([p[0] for p in parent_ids])) > 0
      names = list(set([n['toponym'] for n in doc['names']]))
      #print('index '+str(pid)+' as child of winner between '+', '.join(parent_ids))
      winner_id = topParent(parents,'set')
      doc['relation'] = {"name":"child", "parent": winner_id}

      # index this as child; add names to winner searchy and suggest.input
      #es.index('whg', doc, id=pid)

      # demote others & transfer kids, names
      demoted = parent_ids.remove(winner_id)
      demoteParents(demoted, winner_id, pid)

      # log this winner, may be needed
      chosen.append(winner_id)

    print(len(hset), hset)
  print('write_idx_pass0(); process '+str(hits.count())+' hits')
  return HttpResponseRedirect(referer)

"""
# ds_recon(pk)
# initiates & monitors Celery tasks against Elasticsearch indexes
# i.e. align_[wdlocal | idx | tgn ] in tasks.py
# url: datasets/{ds.id}/reconcile ('ds_reconcile'; from ds_addtask.html)
# params: pk (dataset id), auth, region, userarea, geom, scope
# each align_{auth} task runs matching es_lookup_{auth}() and writes Hit instances
"""
def ds_recon(request, pk):
  ds = get_object_or_404(Dataset, id=pk)
  # TODO: handle multipolygons from "#area_load" and "#area_draw"
  user = request.user
  context = {"dataset": ds.title}

  if request.method == 'GET':
    #print('recon request.GET:',request.GET)
    print('ds_recon() GET')
  elif request.method == 'POST' and request.POST:
    print('ds_recon() request.POST:',request.POST)
    auth = request.POST['recon']
    language = request.LANGUAGE_CODE
    # a90d2c4f-a4b6-49bc-acf9-84b295305c63
    # previous task of this type? bool
    previous = ds.tasks.filter(task_name='align_'+auth,status='SUCCESS')
    prior = request.POST['prior'] if 'prior' in request.POST else 'na'
    if previous.count() > 0:
      # get its id
      tid = previous.first().task_id
      #hadhits = Hit.objects.filter(task_id=tid,reviewed=True).count() > 0
      # delete it, keep/zap links + geoms per value of prior
      #if hadhits:
      task_archive(tid, prior)
      # submit only unreviewed if previous
      scope = 'unreviewed'
      print('recon(): archived previous task')
      #else:
        #task_delete(tid)
        #scope = 'all'
        #print('recon(): deleted previous task')
      print('recon(): links+geoms were '+ ('kept' if prior=='keep' else 'zapped'))
    else:
      # no existing task, submit all rows
      print('ds_recon(): no previous, submitting all')
      scope = 'all'

    print('ds_recon() scope', scope)
    # which task? wdlocal, tgn, idx, whg (future)
    func = eval('align_'+auth)

    # TODO: let this vary per task?
    region = request.POST['region'] # pre-defined UN regions
    userarea = request.POST['userarea'] # from ccodes, or drawn
    aug_geom = request.POST['geom'] if 'geom' in request.POST else '' # on == write geom if matched
    #bounds= {'type': ['userarea'], 'id': ['0']}
    bounds={
      "type":["region" if region !="0" else "userarea"],
      "id": [region if region !="0" else userarea]}

    # check Celery service
    if not celeryUp():
      print('Celery is down :^(')
      emailer('Celery is down :^(','if not celeryUp() -- look into it, bub!','whgazetteer@gmail.com', ['karl@kgeographer.org'])
      messages.add_message(request, messages.INFO, "Sorry! WHG reconciliation services appears to be down. The system administrator has been notified.")
      return redirect('/datasets/'+str(ds.id)+'/reconcile')

    # initiate celery/redis task
    # 'func' == align_[wdlocal | tgn | idx | whg ]
    try:
      result = func.delay(
        ds.id,
        ds=ds.id,
        dslabel=ds.label,
        owner=ds.owner.id,
        user=user.id,
        bounds=bounds,
        aug_geom=aug_geom,
        scope=scope,
        lang=language,
      )
      messages.add_message(request, messages.INFO, "<span class='text-danger'>Your reconciliation task is under way.</span><br/>When complete, you will receive an email and if successful, results will appear below (you may have to refresh screen). <br/>In the meantime, you can navigate elsewhere.")
      return redirect('/datasets/'+str(ds.id)+'/reconcile')
    except:
      print('failed: align_'+auth )
      print(sys.exc_info())
      messages.add_message(request, messages.INFO, "Sorry! Reconciliation services appear to be down. The system administrator has been notified.<br/>"+ str(sys.exc_info()))
      emailer('WHG recon task failed',
              'a reconciliation task has failed for dataset #'+ds.id+', w/error: \n' +str(sys.exc_info())+'\n\n',
              'whgazetteer@gmail.com',
              'karl@kgeographer.org')

      return redirect('/datasets/'+str(ds.id)+'/reconcile')



"""
# task_delete(tid, scope)
# delete results of a reconciliation task:
# hits + any geoms and links added by review
# reset Place.review_{auth} to null
#
"""
def task_delete(request, tid, scope="foo"):
  hits = Hit.objects.all().filter(task_id=tid)
  tr = get_object_or_404(TaskResult, task_id=tid)
  dsid = tr.task_args[1:-1]
  ds=get_object_or_404(Dataset,pk=dsid)
  auth = tr.task_name[6:]
  places = Place.objects.filter(id__in=[h.place_id for h in hits])
  #places = Place.objects.filter(dataset = ds.label)
  placelinks = PlaceLink.objects.all().filter(task_id=tid)
  placegeoms = PlaceGeom.objects.all().filter(task_id=tid)
  print('task_delete()',{'tid':tr,'dsid':dsid,'auth':auth})

  # reset Place.review_{auth} to null
  for p in places:
    if auth in ['whg','idx']:
      p.review_whg = None
    elif auth.startswith('wd'):
      p.review_wd = None
    else:
      p.review_tgn = None
    p.save()

  # zap task record & its hits
  if scope == 'task':
    tr.delete()
    hits.delete()
    placelinks.delete()
    placegeoms.delete()
  elif scope == 'geoms':
    placegeoms.delete()

  return redirect('/datasets/'+dsid+'/reconcile')


"""
# task_archive(tid, scope, prior)
# delete hits
# if prior = 'zap: delete geoms and links added by review
# reset Place.review_{auth} to null
# set task status to 'ARCHIVED'
"""
def task_archive(tid, prior):
  hits = Hit.objects.all().filter(task_id=tid)
  tr = get_object_or_404(TaskResult, task_id=tid)
  dsid = tr.task_args[1:-1]
  auth = tr.task_name[6:]
  places = Place.objects.filter(id__in=[h.place_id for h in hits])
  print('task_archive()',{'tid':tr,'dsid':dsid,'auth':auth})

  # reset Place.review_{auth} to null
  for p in places:
    if auth in ['whg','idx'] and p.review_whg != 1:
      p.review_whg = None
    elif auth.startswith('wd') and p.review_wd !=1:
      p.review_wd = None
    elif auth == 'tgn' and p.review_tgn !=1:
      p.review_tgn = None
    p.save()

  # zap hits
  hits.delete()
  if prior == 'na':
    tr.delete()
  else:
    # flag task as ARCHIVED
    tr.status = 'ARCHIVED'
    tr.save()
    # zap prior links/geoms if requested
    if prior == 'zap':
      PlaceLink.objects.all().filter(task_id=tid).delete()
      PlaceGeom.objects.all().filter(task_id=tid).delete()

"""
# remove collaborator from dataset (all roles)
# TODO: limit to role?
#
"""

"""
add collaborator to dataset in role
"""
def collab_add(request, dsid, v):
  print('collab_add() request, dsid', request, dsid)
  try:
    uid=get_object_or_404(User,username=request.POST['username']).id
    role=request.POST['role']
  except:
    # TODO: raise error to screen
    messages.add_message(
      request, messages.INFO, "Please check username, we don't have '" + request.POST['username']+"'")
    if not v:
      return redirect('/datasets/'+str(dsid)+'/collab')
    else:
      return HttpResponseRedirect(request.META.get('HTTP_REFERER'))
  print('collab_add():',request.POST['username'],role, dsid, uid)
  DatasetUser.objects.create(user_id_id=uid, dataset_id_id=dsid, role=role)
  if v == '1':
    return redirect('/datasets/'+str(dsid)+'/collab')
  else:
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

"""
collab_delete(uid, dsid)
remove collaborator from dataset
"""
def collab_delete(request, uid, dsid, v):
  print('collab_delete() request, uid, dsid', request, uid, dsid)
  get_object_or_404(DatasetUser,user_id_id=uid, dataset_id_id=dsid).delete()
  if v == '1':
    return redirect('/datasets/'+str(dsid)+'/collab')
  else:
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

"""
dataset_file_delete(ds)
delete all uploaded files for a dataset
"""
def dataset_file_delete(ds):
  dsf_list = ds.files.all()
  for f in dsf_list:
    ffn = 'media/'+f.file.name
    if os.path.exists(ffn):
      os.remove(ffn)
      print('zapped file '+ffn)
    else:
      print('did not find file '+ffn)


"""
update_rels_tsv(pobj, row)
updates objects related to a Place (pobj)
make new child objects of pobj: names, types, whens, related, descriptions
for geoms and links, add from row if not there
row is a pandas dict
"""
def update_rels_tsv(pobj, row):
  header = list(row.keys())
  print('update_rels_tsv(): pobj, row, header',pobj,row,header)
  src_id = row['id']
  title = row['title']
  # for PlaceName insertion, strip anything in parens
  title = re.sub('\(.*?\)', '', title)
  title_source = row['title_source']
  title_uri = row['title_uri'] if 'title_uri' in header else ''
  variants = [x.strip() for x in row['variants'].split(';')] \
    if 'variants' in header else []
  types = [x.strip() for x in row['types'].split(';')] \
    if 'types' in header and str(row['types']) not in ('nan','') else []
  #aat_types = [x.strip() for x in row['aat_types'].split(';')] \
  aat_types = [x.strip() for x in row['aat_types'].split(';')] \
    if 'aat_types' in header and str(row['aat_types']) not in ('nan','') else []
  parent_name = row['parent_name'] if 'parent_name' in header else ''
  parent_id = row['parent_id'] if 'parent_id' in header else ''
  coords = makeCoords(row['lon'], row['lat']) \
    if 'lon' in header and 'lat' in header and not math.isnan(row['lon']) else []
  matches = [x.strip() for x in row['matches'].split(';')] \
    if 'matches' in header and row['matches'] != '' else []
  description = row['description'] \
    if 'description' in header else ''

  # build associated objects and add to arrays
  objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
          "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[],
            "PlaceDepiction":[]}

  # title as a PlaceName
  objs['PlaceName'].append(
    PlaceName(
      place=pobj,
      src_id = src_id,
      toponym = title,
      jsonb={"toponym": title, "citation": {"id":title_uri,"label":title_source}}
  ))

  # add variants as PlaceNames, if any
  if len(variants) > 0:
    for v in variants:
      haslang = re.search("@(.*)$", v.strip())
      new_name = PlaceName(
        place=pobj,
        src_id = src_id,
        toponym = v.strip(),
        jsonb={"toponym": v.strip(), "citation": {"id":"","label":title_source}}
      )
      if haslang:
        new_name.jsonb['lang'] = haslang.group(1)
      objs['PlaceName'].append(new_name)

  #
  # PlaceType()
  # TODO: parse t
  if len(types) > 0:
    for i,t in enumerate(types):
      # i always 0 in tsv
      aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) else ''
      objs['PlaceType'].append(
        PlaceType(
          place=pobj,
          src_id = src_id,
          jsonb={ "identifier":aatnum,
                  "sourceLabel":t,
                  "label":aat_lookup(int(aatnum[4:])) if aatnum !='aat:' else ''
                }
      ))


  #
  # PlaceGeom()
  # TODO: test geometry type or force geojson
  if len(coords) > 0:
    geom = {"type": "Point",
            "coordinates": coords,
            "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
  elif 'geowkt' in header and row['geowkt'] not in ['',None]: # some rows no geom
    geom = parse_wkt(row['geowkt'])

  print('new geom',geom)
  def trunc4(val):
    print('val in trunc4()',val)
    return round(val,4)
  new_coords = list(map(trunc4,list(geom['coordinates'])))
  # only add new geometry
  if len(pobj.geoms.all()) >0:
    for g in pobj.geoms.all():
      if list(map(trunc4,g.jsonb['coordinates'])) != new_coords:
        objs['PlaceGeom'].append(
            PlaceGeom(
              place=pobj,
              src_id = src_id,
              jsonb=geom
          ))

  #
  # PlaceLink() - all are closeMatch
  if len(matches) > 0:
    # any existing? only add new
    exist_links = list(pobj.links.all().values_list('jsonb__identifier',flat=True))
    if set(matches)-set(exist_links) > 0:
      # one or more new matches; add 'em
      for m in matches:
        objs['PlaceLink'].append(
          PlaceLink(
            place=pobj,
            src_id = src_id,
            jsonb={"type":"closeMatch", "identifier":m}
        ))

  #
  # PlaceRelated()
  if parent_name != '':
    objs['PlaceRelated'].append(
      PlaceRelated(
        place=pobj,
        src_id=src_id,
        jsonb={
          "relationType": "gvp:broaderPartitive",
          "relationTo": parent_id,
          "label": parent_name}
    ))

  #
  # PlaceWhen()
  # timespans[{start{}, end{}}], periods[{name,id}], label, duration
  objs['PlaceWhen'].append(
    PlaceWhen(
      place=pobj,
      src_id = src_id,
      jsonb={
            "timespans": [{
              "start":{"earliest":pobj.minmax[0]},
              "end":{"latest":pobj.minmax[1]}}]
          }
  ))
  #
  # PlaceDescription()
  # @id, value, lang
  if description != '':
    objs['PlaceDescription'].append(
      PlaceDescription(
        place=pobj,
        src_id = src_id,
        jsonb={
          "@id": "", "value":description, "lang":""
        }
      ))

  # what came from this row
  print('COUNTS:')
  print('PlaceName:',len(objs['PlaceName']))
  print('PlaceType:',len(objs['PlaceType']))
  print('PlaceGeom:',len(objs['PlaceGeom']))
  print('PlaceLink:',len(objs['PlaceLink']))
  print('PlaceRelated:',len(objs['PlaceRelated']))
  print('PlaceWhen:',len(objs['PlaceWhen']))
  print('PlaceDescription:',len(objs['PlaceDescription']))
  print('max places.id', )

  # TODO: update place.fclasses, place.minmax, place.timespans

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


"""
ds_update()
perform updates to database and index
given new datafile
"""
def ds_update(request):
  if request.method == 'POST':
    dsid=request.POST['dsid']
    ds = get_object_or_404(Dataset, id=dsid)
    file_format=request.POST['format']
    # keep previous recon/review results?
    keepg = request.POST['keepg']
    keepl = request.POST['keepl']

    #print('keepg,type in ds_update() request',keepg,type(keepg))

    # compare_data {'compare_result':{}}
    compare_data = json.loads(request.POST['compare_data'])
    compare_result = compare_data['compare_result']
    #print('compare_data from ds_compare', compare_data)

    # tempfn has .tsv or .jsonld extension from validation step
    tempfn = compare_data['tempfn']
    filename_new = compare_data['filename_new']
    dsfobj_cur = ds.files.all().order_by('-rev')[0]
    rev_cur = dsfobj_cur.rev

    # rename file if already exists in user area
    if Path('media/'+filename_new).exists():
      fn=os.path.splitext(filename_new)
      #filename_new=filename_new[:-4]+'_'+tempfn[-11:-4]+filename_new[-4:]
      filename_new=fn[0]+'_'+tempfn[-11:-4]+fn[1]

    # user said go...copy tempfn to media/{user} folder
    filepath = 'media/'+filename_new
    copyfile(tempfn,filepath)

    # and create new DatasetFile instance
    DatasetFile.objects.create(
      dataset_id = ds,
      file = filename_new,
      rev = rev_cur + 1,
      format = file_format,
      # TODO: accept csv, track delimiter
      #delimiter = result['delimiter'] if "delimiter" in result.keys() else "n/a",
      #df_status = 'updating',
      upload_date = datetime.date.today(),
      header = compare_result['header_new'],
      numrows = compare_result['count_new']
    )

    # (re-)open files as panda dataframes; a = current, b = new
    # test files
    # cur: user_whgadmin/diamonds135.tsv
    # new: user_whgadmin/diamonds135_rev2.tsv
    if file_format == 'delimited':
      #adf = pd.read_csv('media/user_whgadmin/diamonds135.tsv', delimiter='\t',dtype={'id':'str','ccodes':'str'})
      #bdf = pd.read_csv('/var/folders/f4/x09rdl7n3lg7r7gwt1n3wjsr0000gn/T/tmpcfees9hd.tsv', delimiter='\t',dtype={'id':'str','ccodes':'str'})
      adf = pd.read_csv('media/'+compare_data['filename_cur'], delimiter='\t',dtype={'id':'str','ccodes':'str'})
      bdf = pd.read_csv(filepath, delimiter='\t')
      bdf = bdf.astype({"id":str,"ccodes":str})
      print('reopened old file, # lines:',len(adf))
      print('reopened new file, # lines:',len(bdf))
      ids_a = adf['id'].tolist()
      ids_b = bdf['id'].tolist()
      delete_srcids = [str(x) for x in (set(ids_a)-set(ids_b))]
      replace_srcids = set.intersection(set(ids_b),set(ids_a))

      # CURRENT
      places = Place.objects.filter(dataset=ds.label)
      # Place.id lists
      rows_delete = list(places.filter(src_id__in=delete_srcids).values_list('id',flat=True))
      rows_replace = list(places.filter(src_id__in=replace_srcids).values_list('id',flat=True))
      #rows_add = list(places.filter(src_id__in=compare_result['rows_add']).values_list('id',flat=True))

      # delete places with ids missing in new data (CASCADE includes links & geoms)
      places.filter(id__in=rows_delete).delete()

      # delete related instances for the rest (except links and geoms)
      PlaceName.objects.filter(place_id__in=places).delete()
      PlaceType.objects.filter(place_id__in=places).delete()
      PlaceWhen.objects.filter(place_id__in=places).delete()
      PlaceRelated.objects.filter(place_id__in=places).delete()
      PlaceDescription.objects.filter(place_id__in=places).delete()
      PlaceDepiction.objects.filter(place_id__in=places).delete()

      count_updated, count_new = [0,0]
      # update remaining place instances w/data from new file
      # AND add new
      place_fields = {'id', 'title', 'ccodes','start','end'}
      for index, row in bdf.iterrows():
        # make 3 dicts: all; for Places; for PlaceXxxxs
        rd = row.to_dict()
        print('rd in ds_update',rd)
        #rdp = {key:rd[key][0] for key in place_fields}
        rdp = {key:rd[key] for key in place_fields}
        # look for corresponding current place
        #p = places.filter(src_id='1.0').first()
        p = places.filter(src_id=rdp['id']).first()
        print('rdp (new row)',rdp)
        start = int(rdp['start']) if 'start' in rdp else None
        end = int(rdp['end']) if 'end' in rdp and str(rdp['end']) != 'nan' else start
        minmax_new = [start,end] if start else [None]
        if p != None:
          # place exists, update it
          count_updated +=1
          p.title = rdp['title']
          p.ccodes = [] if str(rdp['ccodes']) == 'nan' else rdp['ccodes'].replace(' ','').split(';')
          p.minmax = minmax_new
          p.timespans = [minmax_new]
          p.save()
          pobj = p
        else:
          # entirely new place + related records
          count_new +=1
          newpl = Place.objects.create(
            src_id = rdp['id'],
            title = re.sub('\(.*?\)', '', rdp['title']),
            ccodes = [] if str(rdp['ccodes']) == 'nan' else rdp['ccodes'].replace(' ','').split(';'),
            dataset = ds,
            minmax = minmax_new,
            timespans = [minmax_new]
          )
          newpl.save()
          pobj = newpl
          #print('new place, related:', newpl, rdrels)

        # TODO: needs to update, not add
        # create related records (place_name, etc)
        # pobj is either a current (now updated) place or entirely new
        # rd is row dict
        print('pobj,rd for add_rels_tsv()',pobj,rd)
        update_rels_tsv(pobj, rd)


      # update numrows
      ds.numrows = ds.places.count()
      ds.save()

      # initiate a result object
      result = {"status": "updated", "update_count":count_updated ,
                "new_count":count_new, "del_count": len(rows_delete), "newfile": filepath,
                "format":file_format}
      #
      # if dataset is indexed, update it there too
      # TODO: if new records, new recon task & accessioning tasks needed
      if compare_data['count_indexed'] > 0:
        from elasticsearch import Elasticsearch
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        idx='whg'

        result["indexed"] = True

        # surgically remove as req.
        if len(rows_delete)> 0:
          deleteFromIndex(es, idx, rows_delete)

        # update others
        if len(rows_replace) > 0:
          replaceInIndex(es, idx, rows_replace)

        # process new
        #if len(rows_add) > 0:
          # notify need to reconcile & accession them

      else:
        print('not indexed, that is all')

      # write log entry
      Log.objects.create(
        # category, logtype, "timestamp", subtype, note, dataset_id, user_id
        category = 'dataset',
        logtype = 'ds_update',
        note = json.dumps(compare_result),
        dataset_id = dsid,
        user_id = request.user.id
      )

      return JsonResponse(result,safe=False)
    elif file_format == 'lpf':
      print("ds_update for lpf; doesn't get here yet")

"""
ds_compare()
validates dataset update file & compares w/existing
called by ajax function from modal button
returns json result object
"""
def ds_compare(request):
  if request.method == 'POST':
    print('request.POST',request.POST)
    print('request.FILES',request.FILES)
    dsid=request.POST['dsid'] # 586 for diamonds
    user=request.user.username
    format=request.POST['format']
    ds = get_object_or_404(Dataset, id=dsid)

    # {idxcount, submissions[{task_id,date}]}
    ds_status = ds.status_idx

    # how many exist, whether from recon or original?
    #count_geoms = PlaceGeom.objects.filter(place_id__in=ds.placeids,task_id__isnull=False).count()
    #count_links = PlaceLink.objects.filter(place_id__in=ds.placeids,task_id__isnull=False).count()
    count_geoms = PlaceGeom.objects.filter(place_id__in=ds.placeids).count()
    count_links = PlaceLink.objects.filter(place_id__in=ds.placeids).count()

    # wrangling names
    # current (previous) file
    file_cur = ds.files.all().order_by('-rev')[0].file
    filename_cur = file_cur.name

    # new file
    file_new=request.FILES['file']
    tempf, tempfn = tempfile.mkstemp()

    # write new file as temporary to /var/folders/../...
    try:
      for chunk in file_new.chunks():
        os.write(tempf, chunk)
    except:
      raise Exception("Problem with the input file %s" % request.FILES['file'])
    finally:
      os.close(tempf)

    print('tempfn,filename_cur,file_new.name',tempfn,filename_cur,file_new.name)

    # format validation
    if format == 'delimited':
      # goodtable wants filename only
      # returns [x['message'] for x in errors]
      vresult = validate_tsv(tempfn)
    elif format == 'lpf':
      # TODO: feed tempfn only?
      # TODO: accept json-lines; only FeatureCollections ('coll') now
      vresult = validate_lpf(tempfn,'coll')
    print('format, vresult:',format,vresult)

    # if errors, parse & return to modal
    # which expects {validation_result{errors['','']}}
    if len(vresult['errors']) > 0:
      errormsg = {"failed":{
        "errors":vresult['errors']
      }}
      return JsonResponse(errormsg,safe=False)

    # give new file a path
    filename_new = 'user_'+user+'/'+file_new.name
    # temp files were given extensions in validation functions
    tempfn_new = tempfn+'.tsv' if format == 'delimited' else tempfn+'.jsonld'

    # begin report
    comparison={
      "id": dsid,
      "filename_cur": filename_cur,
      "filename_new": filename_new,
      "format": format,
      "validation_result": vresult,
      "tempfn": tempfn_new,
      "count_links": count_links,
      "count_geoms": count_geoms,
      "count_indexed": ds_status['idxcount'],
    }
    print('count_geoms in ds_compare:894',count_geoms)
    # perform comparison
    fn_a = 'media/'+filename_cur
    fn_b = tempfn_new
    if format == 'delimited':
      adf = pd.read_csv(fn_a, delimiter='\t')
      bdf = pd.read_csv(fn_b, delimiter='\t')
      ids_a = adf['id'].tolist()
      ids_b = bdf['id'].tolist()
      # new or removed columns?
      cols_del = list(set(adf.columns)-set(bdf.columns))
      cols_add = list(set(bdf.columns)-set(adf.columns))

      comparison['compare_result'] = {
        "count_new":len(ids_b),
        'count_diff':len(ids_b)-len(ids_a),
        'count_replace': len(set.intersection(set(ids_b),set(ids_a))),
        'cols_del': cols_del,
        'cols_add': cols_add,
        'header_new': vresult['columns'],
        'rows_add': [str(x) for x in (set(ids_b)-set(ids_a))],
        'rows_del': [str(x) for x in (set(ids_a)-set(ids_b))]
      }
    # TODO: process LP format, collections + json-lines
    elif format == 'lpf':
      print('need to compare lpf files:',fn_a,fn_b)
      comparison['compare_result'] = "it's lpf...tougher row to hoe"

    print('comparison',comparison)
    # back to calling modal
    return JsonResponse(comparison,safe=False)


""" recovered from server 29 July """
def ds_insert_lpf(request, pk):
  import json
  [countrows,countlinked,total_links]= [0,0,0]
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  # latest file
  dsf = ds.files.all().order_by('-rev')[0]
  uribase = ds.uri_base
  print('new dataset, uri_base', ds.label, uribase)

  # TODO: lpf can get big; json-lines
  
  # insert only if empty   
  dbcount = Place.objects.filter(dataset = ds.label).count()
  print('dbcount',dbcount)

  if dbcount == 0:
    try:
      infile = dsf.file.open(mode="r")
      print('ds_insert_lpf() for dataset',ds) 
      print('ds_insert_lpf() request.GET, infile',request.GET,infile) 
      with infile:
        jdata = json.loads(infile.read())
        
        print('count of features',len(jdata['features']))
        #print('0th feature',jdata['features'][0])
        
        for feat in jdata['features']:
          # create Place, save to get id, then build associated records for each
          objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
                  "PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
                  "PlaceDepictions":[]}
          countrows += 1
    
          # build attributes for new Place instance
          title=re.sub('\(.*?\)', '', feat['properties']['title'])
          
          # geometry
          geojson = feat['geometry'] if 'geometry' in feat.keys() else None
          
          # ccodes  
          if 'ccodes' not in feat['properties'].keys():
            if geojson:
              # a GeometryCollection
              ccodes = ccodesFromGeom(geojson)
            else:
              ccodes = []
          else:
            ccodes = feat['properties']['ccodes']
          
          # temporal
          # send entire feat for time summary
          # (minmax and intervals[])
          datesobj=parsedates_lpf(feat)
          
          # TODO: compute fclasses
          newpl = Place(
            # strip uribase from @id
            src_id=feat['@id'] if uribase in ['', None] else feat['@id'].replace(uribase,''),
            dataset=ds,
            title=title,
            ccodes=ccodes,
            minmax = datesobj['minmax'],
            timespans = datesobj['intervals']
          )
          print('new place: ',newpl.title)
          newpl.save()
    
          # PlaceName: place,src_id,toponym,task_id,
          # jsonb:{toponym, lang, citation[{label, year, @id}], when{timespans, ...}}
          # TODO: adjust for 'ethnic', 'demonym'
          for n in feat['names']:
            if 'toponym' in n.keys():
              # if comma-separated listed, get first
              objs['PlaceNames'].append(PlaceName(
                place=newpl,
                src_id=newpl.src_id,
                toponym=n['toponym'].split(', ')[0],
                jsonb=n
              ))
    
          # PlaceType: place,src_id,task_id,jsonb:{identifier,label,src_label}
          #try:        
          if 'types' in feat.keys():
            fclass_list = []
            for t in feat['types']:
              if 'identifier' in t.keys() and t['identifier'][:4] == 'aat:' \
                 and int(t['identifier'][4:]) in Type.objects.values_list('aat_id',flat=True):
                fc = get_object_or_404(Type, aat_id=int(t['identifier'][4:])).fclass \
                  if t['identifier'][:4] == 'aat:' else None
                fclass_list.append(fc)
              else:
                fc = None
              print('from feat[types]:',t)
              objs['PlaceTypes'].append(PlaceType(
                place=newpl,
                src_id=newpl.src_id,
                jsonb=t,
                fclass=fc
              ))
            newpl.fclasses = fclass_list
            newpl.save()
          
          # PlaceWhen: place,src_id,task_id,minmax,jsonb:{timespans[],periods[],label,duration}
          if 'when' in feat.keys() and feat['when'] != {}:
            objs['PlaceWhens'].append(PlaceWhen(
              place=newpl,
              src_id=newpl.src_id,
              jsonb=feat['when']))
    
          # PlaceGeom: place,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
          #if 'geometry' in feat.keys() and feat['geometry']['type']=='GeometryCollection':
          if geojson and geojson['type']=='GeometryCollection':
            #for g in feat['geometry']['geometries']:
            for g in geojson['geometries']:
              #print('from feat[geometry]:',g)
              objs['PlaceGeoms'].append(PlaceGeom(
                place=newpl,
                src_id=newpl.src_id,
                jsonb=g
                ,geom=GEOSGeometry(json.dumps(g))
              ))
          elif geojson:
            objs['PlaceGeoms'].append(PlaceGeom(
              place=newpl,
              src_id=newpl.src_id,
              jsonb=geojson
              ,geom=GEOSGeometry(json.dumps(geojson))
            ))
            
          # PlaceLink: place,src_id,task_id,jsonb:{type,identifier}
          if 'links' in feat.keys() and len(feat['links'])>0:
            countlinked +=1 # record has *any* links
            #print('countlinked',countlinked)
            for l in feat['links']:
              total_links += 1 # record has n links
              objs['PlaceLinks'].append(PlaceLink(
                place=newpl,
                src_id=newpl.src_id,
                # alias uri base for known authorities
                jsonb={"type":l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))}
              ))
    
          # PlaceRelated: place,src_id,task_id,jsonb{relationType,relationTo,label,when{}}
          if 'relations' in feat.keys():
            for r in feat['relations']:
              objs['PlaceRelated'].append(PlaceRelated(
                place=newpl,src_id=newpl.src_id,jsonb=r))
    
          # PlaceDescription: place,src_id,task_id,jsonb{@id,value,lang}
          if 'descriptions' in feat.keys():
            for des in feat['descriptions']:
              objs['PlaceDescriptions'].append(PlaceDescription(
                place=newpl,src_id=newpl.src_id,jsonb=des))
    
          # PlaceDepiction: place,src_id,task_id,jsonb{@id,title,license}
          if 'depictions' in feat.keys():
            for dep in feat['depictions']:
              objs['PlaceDepictions'].append(PlaceDepiction(
                place=newpl,src_id=newpl.src_id,jsonb=dep))
    
          #
          # create related objects 
          PlaceName.objects.bulk_create(objs['PlaceNames'])
          PlaceType.objects.bulk_create(objs['PlaceTypes'])
          PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
          PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
          PlaceLink.objects.bulk_create(objs['PlaceLinks'])
          PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
          PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
          PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])
          #print('new place record: ',newpl.src_id)
          
          # TODO: compute newpl.ccodes (if geom), newpl.fclasses, newpl.minmax
          # something failed in *any* Place creation; delete dataset
          
        print('new dataset:', ds.__dict__)
        infile.close()
        
      return({"numrows":countrows,
              "numlinked":countlinked,
              "total_links":total_links})
    except:
      # drop the (empty) database
      ds.delete()
      # email to user, admin
      subj = 'World Historical Gazetteer error followup'
      msg = 'Hello '+ user.username+', \n\nWe see your recent upload for the '+ds.label+' dataset failed, very sorry about that! We will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team'
      emailer(subj,msg,'admin@whgazetteer@gmail.com',[user.email, 'whgadmin@kgeographer.com'])
      
      # return message to 500.html
      messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      return HttpResponseServerError()
      
  else:
    print('insert_ skipped, already in')    
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/dashboard')    


"""
ds_insert_tsv(pk)
insert tsv into database
file is validated, dataset exists
if insert fails anywhere, delete dataset + any related objects
"""
""" recovered from server 29 July """
def ds_insert_tsv(request, pk):
  import csv, re
  csv.field_size_limit(300000)
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  print('ds_insert_tsv()',ds)
  # retrieve just-added file
  dsf = ds.files.all().order_by('-rev')[0]

  # insert only if empty 
  dbcount = Place.objects.filter(dataset = ds.label).count()
  print('dbcount',dbcount)
  
  if dbcount == 0:
    try:
      infile = dsf.file.open(mode="r")
      reader = csv.reader(infile, delimiter=dsf.delimiter)
      
      infile.seek(0)
      header = next(reader, None)
      header = [col.lower() for col in header]
      print('header.lower()',[col.lower() for col in header])

      # strip BOM character if exists
      header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
      #header = header if type(header) = list else 
      print('header', header)
    
      objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
              "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}
        
      #
      # TODO: what if simultaneous inserts?
      countrows=0
      countlinked = 0
      total_links = 0
      for r in reader:
        # build attributes for new Place instance
        src_id = r[header.index('id')]
        title = r[header.index('title')].replace("' ","'") # why?
        # strip anything in parens for title only
        title = re.sub('\(.*?\)', '', title)
        title_source = r[header.index('title_source')]
        title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
        ccodes = r[header.index('ccodes')] if 'ccodes' in header else []
        variants = [x.strip() for x in r[header.index('variants')].split(';')] \
          if 'variants' in header and r[header.index('variants')] !='' else []
        types = [x.strip() for x in r[header.index('types')].split(';')] \
          if 'types' in header else []
        aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
          if 'aat_types' in header else []
        parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
        parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
        coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
          if 'lon' in header and 'lat' in header else None
        geowkt = r[header.index('geowkt')] if 'geowkt' in header else None
        geojson = None # zero it out
  
        # make Point geometry from lon/lat if there
        if coords and len(coords) == 2:
          geojson = {"type": "Point", "coordinates": coords,
                      "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
        # else make geometry (any) w/Shapely if geowkt
        if geowkt and geowkt not in ['',None]:
          geojson = parse_wkt(r[header.index('geowkt')])
          
        # ccodes; compute if missing and there is geometry
        if len(ccodes) == 0:
          if geojson:
            ccodes = ccodesFromGeom(geojson)
          else:
            ccodes = []
        else:
          ccodes = [x.strip().upper() for x in r[header.index('ccodes')].split(';')]
        # TODO: assign aliases if wd, tgn, pl, bnf, gn, viaf
        matches = [aliasIt(x.strip()) for x in r[header.index('matches')].split(';')] \
          if 'matches' in header and r[header.index('matches')] != '' else []
        
        start = r[header.index('start')] if 'start' in header else None
        # validate_tsv() ensures there is always a start
        has_end = 'end' in header and r[header.index('end')] !=''
        end = r[header.index('end')] if has_end else start
        
        datesobj = parsedates_tsv(start,end) 
        # returns {timespans:[{}],minmax[]}
  
        
        description = r[header.index('description')] \
          if 'description' in header else ''
        # print('description (src_id)', description)

        # create new Place object
        # TODO: generate fclasses
        newpl = Place(
          src_id = src_id,
          dataset = ds,
          title = title,
          ccodes = ccodes,
          minmax = datesobj['minmax'],
          timespans = [datesobj['minmax']] # list of lists
        )
        newpl.save()
        countrows += 1
    
        #** build associated objects and add to arrays **#
        #
        # PlaceName(); title, then variants
        #
        objs['PlaceName'].append(
          PlaceName(
            place=newpl,
            src_id = src_id,
            toponym = title,
            jsonb={"toponym": title, "citations": [{"id":title_uri,"label":title_source}]}
        ))
        # variants if any; assume same source as title toponym
        if len(variants) > 0:
          for v in variants:
            try:              
              haslang = re.search("@(.*)$", v.strip())
              if len(v.strip()) > 200:
                print(v.strip())
                pass
              else:
                print('variant for', newpl.id, v)
                new_name = PlaceName(
                  place=newpl,
                  src_id = src_id,
                  toponym = v.strip(),
                  jsonb={"toponym": v.strip(), "citations": [{"id":"","label":title_source}]}
                )
                if haslang:
                  new_name.jsonb['lang'] = haslang.group(1)
                
                objs['PlaceName'].append(new_name)
            except:
              print('error on variant', sys.exc_info())
              print('error on variant for newpl.id', newpl.id, v)
  
        #
        # PlaceType()
        #
        if len(types) > 0:
          fclass_list=[]
          for i,t in enumerate(types):
            aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) and aat_types[i] !='' else None
            if aatnum:
              fclass_list.append(get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass)
            objs['PlaceType'].append(
              PlaceType(
                place=newpl,
                src_id = src_id,
                jsonb={ "identifier":aatnum if aatnum else '',
                        "sourceLabel":t,
                        "label":aat_lookup(int(aatnum[4:])) if aatnum else ''
                      }
            ))
        # add fclasses to new Place
          newpl.fclasses = fclass_list
          newpl.save()
        
        #
        # PlaceGeom()
        # 
        if geojson:
          objs['PlaceGeom'].append(
            PlaceGeom(
              place=newpl,
              src_id = src_id,
              jsonb=geojson
              ,geom=GEOSGeometry(json.dumps(geojson))
          ))
            
        #
        # PlaceWhen()
        # via parsedates_tsv(): {"timespans":[{start{}, end{}}]}
        if start != '':
          objs['PlaceWhen'].append(
            PlaceWhen(
              place=newpl,
              src_id = src_id,
              #jsonb=datesobj['timespans']          
              jsonb=datesobj         
          ))
      
          
        #
        # PlaceLink() - all are closeMatch
        #
        if len(matches) > 0:
          countlinked += 1
          for m in matches:
            total_links += 1
            objs['PlaceLink'].append(
              PlaceLink(
                place=newpl,
                src_id = src_id,
                jsonb={"type":"closeMatch", "identifier":m}
            ))
        #
        # PlaceRelated()
        #
        if parent_name != '':
          objs['PlaceRelated'].append(
            PlaceRelated(
              place=newpl,
              src_id=src_id,
              jsonb={
                "relationType": "gvp:broaderPartitive",
                "relationTo": parent_id,
                "label": parent_name}
          ))
  
        #
        # PlaceDescription()
        # @id, value, lang
        if description != '':
          objs['PlaceDescription'].append(
            PlaceDescription(
              place=newpl,
              src_id = src_id,
              jsonb={
                #"@id": "", "value":description, "lang":""
                "value":description
              }
            ))
        
        
      # bulk_create(Class, batch_size=n) for each
      PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10000)
      PlaceType.objects.bulk_create(objs['PlaceType'],batch_size=10000)
      PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
      PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
      PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
      PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
      PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)
    
      infile.close()
    
      #print('ds record pre-update:', ds.__dict__)
      print('rows,linked,links:', countrows, countlinked, total_links)
    except:
      print('tsv insert failed', sys.exc_info())
      # drop the (empty) dataset if insert wasn't complete
      # DON'T DROP for test
      ds.delete()
      # email to user, admin
      subj = 'World Historical Gazetteer error followup'
      msg = 'Hello '+ user.username+', \n\nWe see your recent upload for the '+ds.label+' dataset failed, very sorry about that! We will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team'
      emailer(subj,msg,'whgazetteer@gmail.com',[user.email, 'karl@kgeographer.org'])
      
      # return message to 500.html
      messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      return HttpResponseServerError()
  else:
    print('insert_tsv skipped, already in')    
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/dashboard')    
  
  return({"numrows":countrows,
          "numlinked":countlinked,
          "total_links":total_links})  

"""
DashboardView()
list user datasets, study areas, collections
"""
class DashboardView(LoginRequiredMixin, ListView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  context_object_name = 'dataset_list'
  template_name = 'datasets/dashboard.html'

  def get_queryset(self):
    # groups.filter(name__in=['beta', 'admins', 'whg_team']).exists()
    me = self.request.user
    if me.is_superuser or 'whg_team' in [g.name for g in me.groups.all()]:
      print('in get_queryset() if',me)
      #return Dataset.objects.all().order_by('ds_status','-core','-id')
      return Dataset.objects.all().order_by('-create_date')
    else:
      #dsids = [g.dataset_id_id for g in me.ds_collab.all()]
      #return Dataset.objects.filter(id__in=dsids)
      #return Dataset.objects.filter( Q(id__in=myprojects(me)) | Q(owner=me) | Q(id__lt=3)).order_by('-id')
      return Dataset.objects.filter( Q(owner=me) ).order_by('-id')


  def get_context_data(self, *args, **kwargs):
    me = self.request.user
    context = super(DashboardView, self).get_context_data(*args, **kwargs)
    print('in get_context',me)

    types_ok=['ccodes','copied','drawn']
    # returns owned and shared datasets (rw)
    context['shared_list'] = Dataset.objects.filter(id__in=myprojects(me)).order_by('-create_date')

    context['public_list'] = Dataset.objects.filter(public=True).order_by('-numrows')

    # list areas
    userareas = Area.objects.all().filter(type__in=types_ok).order_by('created')
    context['area_list'] = userareas if me.is_superuser else userareas.filter(owner=me)

    # list collections
    collection_list = Collection.objects.all().order_by('create_date')
    context['collections'] = collection_list if me.is_superuser else \
      collection_list.filter(owner=me)

    # list teaching resources
    resource_list = Resource.objects.all().order_by('create_date')
    context['resources'] = resource_list if me.is_superuser else \
      resource_list.filter(owner=me)

    context['viewable'] = ['uploaded','inserted','reconciling','review_hits','reviewed','review_whg','indexed']

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins', 'whg_team']).exists() else False
    # TODO: assigning users to 'teacher' group
    context['teacher'] = True if self.request.user.groups.filter(name__in=['teacher']).exists() else False
    # TODO: user place collections
    #print('DashboardView context:', context)
    return context


"""
PublicListView()
list public datasets and collections
"""
class PublicListsView(ListView):
  #login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  context_object_name = 'dataset_list'
  template_name = 'datasets/public_list.html'
  model = Dataset

  def get_queryset(self):
    # original qs
    qs = super().get_queryset()
    return qs.filter(public = True).order_by('core','title')

  def get_context_data(self, *args, **kwargs):
    context = super(PublicListsView, self).get_context_data(*args, **kwargs)

    # public datasets available as dataset_list
    # public collections
    context['coll_list'] = Collection.objects.filter(public=True).order_by('create_date')
    context['viewable'] = ['uploaded','inserted','reconciling','review_hits','reviewed','review_whg','indexed']

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    #print('DashboardView context:', context)
    return context

def failed_upload_notification(user, tempfn):
    subj = 'World Historical Gazetteer error followup'
    msg = 'Hello ' + user.username + \
        ', \n\nWe see your recent upload failed -- very sorry about that! We will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team\n\n\n['+tempfn+']'
    emailer(subj, msg, 'whgazetteer@gmail.com',
            [user.email, 'karl@kgeographer.org'])



"""
DatasetCreateView()
initial create
upload file, validate format, create DatasetFile instance,
redirect to dataset.html for db insert if context['format_ok']
"""
""" *** from server 29 July ; trying to restore ***"""
class DatasetCreateView(LoginRequiredMixin, CreateView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'
  
  form_class = DatasetCreateModelForm
  template_name = 'datasets/dataset_create.html'
  success_message = 'dataset created'
  
  def form_invalid(self, form):
    print('form invalid...',form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)
      
  def form_valid(self, form):
    data=form.cleaned_data
    print('data from create form', data)
    context={"format":data['format']}
    user=self.request.user
    file=self.request.FILES['file']
    filename = file.name
    mimetype = file.content_type
       
    newfn, newtempfn = ['', '']
    print('form_valid() mimetype',mimetype)
    

    # open & write tempf to a temp location;
    # call it tempfn for reference
    tempf, tempfn = tempfile.mkstemp()
    try:
      for chunk in data['file'].chunks():
        os.write(tempf, chunk)
    except:
      raise Exception("Problem with the input file %s" % self.request.FILES['file'])
    finally:
      os.close(tempf)

    print('tempfn in DatasetCreate()',tempfn)

    # open, sniff, validate
    # pass to ds_insert_{tsv|lpf} if valid

    fin = codecs.open(tempfn, 'r')
    valid_mime = mimetype in mthash_plus.mimetypes

    if valid_mime:
      if mimetype.startswith('text/'):
        encoding = get_encoding_delim(tempfn)
      elif 'spreadsheet' in mimetype:
        encoding = get_encoding_excel(tempfn) 
      elif mimetype.startswith('application/'):
        encoding = fin.encoding
      print('encoding in DatasetCreate()', encoding)
    else:
      context['errors'] = "Not a valid file type; must be one of [.csv, .tsv, .xlsx, .ods, .json]"
      return self.render_to_response(self.get_context_data(form=form, context=context))

    # it's csv, tsv, spreadsheet, or json...
    # if utf8, get extension and validate
    # TODO: disabled utf-8 check here 9 March
    #if encoding and encoding.lower().startswith('utf-8'):
    ext = mthash_plus.mimetypes[mimetype]
    print('DatasetCreateView() extension', ext)
    if ext == 'json':
      try:
        result = validate_lpf(tempfn, 'coll')
      except:
        # subj = 'World Historical Gazetteer error followup'
        # msg = 'Hello '+ user.username+', \n\nWe see your recent upload failed -- very sorry about that! We will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team\n\n\n['+tempfn+']'
        # emailer(subj,msg,'whgazetteer@gmail.com',[user.email, 'karl@kgeographer.org'])
        
        # email to user, admin
        failed_upload_notification(user, tempfn)
        # return message to 500.html
        messages.error(self.request, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
        # messages.error(None, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
        return HttpResponseServerError()

    elif ext in ['csv', 'tsv']:
      try:
        # fvalidate() wants an extension
        newfn = tempfn+'.'+ext
        os.rename(tempfn, newfn)
        result = validate_tsv(newfn, ext)
      except:
        # email to user, admin
        failed_upload_notification(user, tempfn)
        messages.error(self.request, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>" +
        # messages.error(None, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>" +
                       user.username+'</b> ('+user.email+')')
        return HttpResponseServerError()

    elif ext in ['xlsx', 'ods']:
      try:
        print('spreadsheet, use pandas')
        import pandas as pd
        
        # open new file for tsv write
        newfn = tempfn + '.tsv'
        fout=codecs.open(newfn, 'w', encoding='utf8')
        
        # add ext to tempfn (pandas need this)
        newtempfn = tempfn+'.'+ext
        os.rename(tempfn, newtempfn)
        print('renamed tempfn for pandas:', tempfn)
        
        # dataframe from spreadsheet
        df = pd.read_excel(newtempfn, converters={
          'id': str, 'start':str, 'end':str, 
          'aat_types': str, 'lon': float, 'lat': float})
        
        # write it as tsv
        table=df.to_csv(sep='\t', index=False).replace('\nan','')
        fout.write(table)
        fout.close()
        
        print('to validate_tsv(newfn):', newfn)
        # validate it...
        result = validate_tsv(newfn, 'tsv')
      except:
        # email to user, admin
        failed_upload_notification(user, newfn)
        messages.error(self.request, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>" +
        # messages.error(None, "Database insert failed and we aren't sure why. The WHG team has been notified and will follow up by email to <b>" +
                       user.username+'</b> ('+user.email+')')
        return HttpResponseServerError()

    #else:
      ## return form with error
      #context['action'] = "errors"
      #context['errors'] = ["Dataset file encoding must be UTF-8; this file is <b>"+encoding+'</b>.']
      #return self.render_to_response(self.get_context_data(form=form, context=context))
    
    print('validation complete, still in DatasetCreateView')
    
    # validated -> create Dataset, DatasetFile, Log instances, 
    # advance to dataset_detail 
    # else present form again with errors
    if len(result['errors']) == 0:
      context['status'] = 'format_ok'
      
      print('validated, no errors; result:', result)      
      print('cleaned_data',form.cleaned_data)
      
      # new Dataset record ('owner','id','label','title','description')
      dsobj = form.save(commit=False)
      dsobj.ds_status = 'format_ok'
      dsobj.numrows = result['count']
      if not form.cleaned_data['uri_base']:
        dsobj.uri_base = 'http://whgazetteer.org/places/'+form.cleaned_data['label']+'/'

      # links will be counted later on insert
      dsobj.numlinked = 0
      dsobj.total_links = 0
      try:
        dsobj.save()
      except:
        args['form'] = form
        return render(request,'datasets/dataset_create.html', args)

      # 
      # create user directory if necessary
      userdir = r'media/user_'+user.username+'/'
      if not Path(userdir).exists():
        os.makedirs(userdir)
        
      # build path, and rename file if already exists in user area
      file_exists = Path(userdir+filename).exists()
      if not file_exists:
        filepath = userdir+filename
      else:
        splitty = filename.split('.')
        filename=splitty[0]+'_'+tempfn[-7:]+'.'+splitty[1]
        filepath = userdir+filename

      # write log entry
      Log.objects.create(
        # category, logtype, "timestamp", subtype, dataset_id, user_id
        category = 'dataset',
        logtype = 'ds_create',
        subtype = data['datatype'],
        dataset_id = dsobj.id,
        user_id = user.id
      )
      
      print('pre-write')
      print('ext='+ext+'; newfn='+newfn+'; filepath='+filepath+
            '; tempfn='+tempfn+'; newtempfn='+newtempfn)
      
      # write request obj file to user directory
      if ext in ['csv', 'tsv', 'json']:
        fout = codecs.open(filepath,'w','utf8')
        try:
          for chunk in file.chunks():
            fout.write(chunk.decode("utf-8"))
        except:
          print('error writing file; chunk'+str(chunk))
          sys.exit(sys.exc_info())
          
      # if spreadsheet, copy newfn (tsv conversion)
      if ext in ['xlsx', 'ods']:
        print('copying newfn -> filepath', newfn, filepath)
        shutil.copy(newfn, filepath+'.tsv')
      
      
      # create initial DatasetFile record
      DatasetFile.objects.create(
        dataset_id = dsobj,
        # uploaded valid file as is
        file = filepath[6:]+'.tsv' if ext in ['xlsx','ods'] else filepath[6:], 
        rev = 1,
        format = result['format'],
        delimiter = '\t' if ext in ['tsv','xlsx','ods'] else ',' if ext == 'csv' else 'n/a',
        df_status = 'format_ok',
        upload_date = None,
        header = result['columns'] if "columns" in result.keys() else [],
        numrows = result['count']
      )
      
      # data will be written on load of dataset.html w/dsobj.status = 'format_ok'
      #return redirect('/datasets/'+str(dsobj.id)+'/detail')
      return redirect('/datasets/'+str(dsobj.id)+'/summary')

    else:
      context['action'] = 'errors'
      context['format'] = result['format']
      context['errors'] = parse_errors_lpf(result['errors']) \
        if ext == 'json' else parse_errors_tsv(result['errors'])
      context['columns'] = result['columns'] \
        if ext != 'json' else []

      #os.remove(tempfn)

      return self.render_to_response(
        self.get_context_data(
          form=form, context=context
      ))

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCreateView, self).get_context_data(*args, **kwargs)
    #context['action'] = 'create'
    return context



class DatasetPublicView(DetailView):
  template_name = 'datasets/ds_meta.html'

  model = Dataset

  def get_context_data(self, **kwargs):
    context = super(DatasetPublicView, self).get_context_data(**kwargs)
    #id_ = self.kwargs.get("pk")
    print('self, kwargs',self, self.kwargs)

    ds = get_object_or_404(Dataset, id = self.kwargs['pk'])
    file = ds.file
    ##coll_set = [cd.dataset for cd in qs]

    placeset = ds.places.all()

    #context['ds_list'] = [cd.dataset for cd in qs]
    context['foo'] = 'bar'
    if file.file:
      context['current_file'] = file
      context['format'] = file.format
      context['numrows'] = file.numrows
      context['filesize'] = round(file.file.size/1000000, 1)

      context['links_added'] = PlaceLink.objects.filter(
        place_id__in = placeset, task_id__contains = '-').count()
      context['geoms_added'] = PlaceGeom.objects.filter(
        place_id__in = placeset, task_id__contains = '-').count()
    return context

"""
# load page for confirm ok on delete
# delete dataset, with CASCADE to DatasetFile, places, place_name, etc
# also deletes from index if indexed (fails silently if not)
# also removes dataset_file records
# TODO: delete other stuff: disk files; archive??
"""
class DatasetDeleteView(DeleteView):
  template_name = 'datasets/dataset_delete.html'

  def delete_complete(self):
    ds=get_object_or_404(Dataset,pk=self.kwargs.get("id"))
    dataset_file_delete(ds)
    if ds.ds_status == 'indexed':
      pids=list(ds.placeids)
      deleteFromIndex(es,'whg',pids)

  def get_object(self):
    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)
    return(ds)

  def get_context_data(self, **kwargs):
    context = super(DatasetDeleteView, self).get_context_data(**kwargs)
    ds = get_object_or_404(Dataset, id=self.kwargs.get("id"))
    context['owners'] = ds.owners
    return context


  def get_success_url(self):
    self.delete_complete()
    return reverse('dashboard')


#
# fetch places in specified dataset 
# 
def ds_list(request, label):
  print('in ds_list() for',label)
  qs = Place.objects.all().filter(dataset=label)
  geoms=[]
  for p in qs.all():
    feat={"type":"Feature",
          "properties":{"src_id":p.src_id,"name":p.title},
              "geometry":p.geoms.first().jsonb}
    geoms.append(feat)
  return JsonResponse(geoms,safe=False)


""" 
undo last review match action
delete any geoms or links created
reset flags for hit.reviewed and place.review_xxx
"""
def match_undo(request, ds, tid, pid):
  print('in match_undo() ds, task, pid:',ds,tid,pid)
  # 81474, 81445 (2), 81417, 81420, 81436, 81442, 81469
  from django_celery_results.models import TaskResult
  
  geom_matches = PlaceGeom.objects.all().filter(task_id=tid, place_id=pid)
  link_matches = PlaceLink.objects.all().filter(task_id=tid, place_id=pid)
  geom_matches.delete()
  link_matches.delete()
  
  # reset place.review_xxx to 0
  tasktype = TaskResult.objects.get(task_id=tid).task_name[6:]
  place = Place.objects.filter(pk=pid)
  # TODO: variable field name?
  if tasktype.startswith('wd'):
    place.update(review_wd = 0)
  elif tasktype == 'tgn':
    place.update(review_tgn = 0)
  else:
    place.update(review_whg = 0)
    
  # match task_id, place_id in hits; set reviewed = false
  Hit.objects.filter(task_id=tid, place_id=pid).update(reviewed=False)
  
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))
  #return redirect('/datasets/'+str(ds)+'/review/'+tid+'/pass1')

"""
modified, broke things?
"""
# ds_summary
class DatasetSummaryView(LoginRequiredMixin, UpdateView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  form_class = DatasetDetailModelForm

  template_name = 'datasets/ds_summary.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    #print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/summary'

  # Dataset has been edited, form submitted
  def form_valid(self, form):
    data=form.cleaned_data
    ds = get_object_or_404(Dataset,pk=self.kwargs.get("id"))
    dsid = ds.id
    user = self.request.user
    file=data['file']
    filerev = ds.files.all().order_by('-rev')[0].rev
    print('DatasetDetailViewDev kwargs',self.kwargs)
    print('DatasetDetailViewDev form_valid() data->', data)
    if data["file"] == None:
      print('data["file"] == None')
      # no file, updating dataset only
      ds.title = data['title']
      ds.description = data['description']
      ds.uri_base = data['uri_base']
      ds.save()
    return super().form_valid(form)

  def form_invalid(self, form):
    print('kwargs',self.kwargs)
    context = {}
    print('form not valid', form.errors)
    print('cleaned_data', form.cleaned_data)
    context['errors'] = form.errors
    return super().form_invalid(form)

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetSummaryView, self).get_context_data(*args, **kwargs)
    #context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    #context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    print('DatasetSummaryView get_context_data() kwargs:',self.kwargs)
    print('DatasetSummaryView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")
    #bounds = self.kwargs.get("bounds")
    ds = get_object_or_404(Dataset, id=id_)
    # print('ds',ds.label)

    """ TRIED THIS INSERT ACTION IN DatasetCreateView()"""
    # coming from DatasetCreateView(),
    # insert to db immediately (file.df_status == format_ok)
    # most recent data file
    file = ds.file
    if file.df_status == 'format_ok':
      print('format_ok , inserting dataset '+str(id_))
      if file.format == 'delimited':
        result = ds_insert_tsv(self.request, id_)
        print('tsv result',result)
      else:
        result = ds_insert_lpf(self.request, id_)
        print('lpf result',result)
      print('ds_insert_xxx() result',result)
      ds.numrows = result['numrows']
      #ds.numrows = result['count']
      ds.numlinked = result['numlinked']
      ds.total_links = result['total_links']
      ds.ds_status = 'uploaded'
      file.df_status = 'uploaded'
      file.numrows = result['numrows']
      ds.save()
      file.save()


    # build context for rendering dataset.html
    me = self.request.user
    #placeset = Place.objects.filter(dataset=ds.label)
    placeset = ds.places.all()

    context['updates'] = {}
    context['ds'] = ds
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners

    # excludes datasets uploaded directly (1 & 2)
    if file.file:
      context['current_file'] = file
      context['format'] = file.format
      context['numrows'] = file.numrows
      context['filesize'] = round(file.file.size/1000000, 1)

    # initial (non-task)
    context['num_names'] = PlaceName.objects.filter(place_id__in = placeset).count()
    context['num_links'] = PlaceLink.objects.filter(
      place_id__in = placeset, task_id = None).count()
    context['num_geoms'] = PlaceGeom.objects.filter(
      place_id__in = placeset, task_id = None).count()

    # augmentations (has task_id)
    context['links_added'] = PlaceLink.objects.filter(
      place_id__in = placeset, task_id__contains = '-').count()
    context['geoms_added'] = PlaceGeom.objects.filter(
      place_id__in = placeset, task_id__contains = '-').count()

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    print('context from DatasetSummaryView', context)
    return context


""" public dataset browse table """
class DatasetPlacesView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_places.html'


  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/places'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetPlacesView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    print('DatasetPlacesView get_context_data() kwargs:',self.kwargs)
    print('DatasetPlacesView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")

    ds = get_object_or_404(Dataset, id=id_)
    #me = self.request.user
    #ds_tasks = [t.task_name[6:] for t in ds.tasks.all()]
    #placeset = Place.objects.filter(dataset=ds.label)
    context['updates'] = {}
    context['ds'] = ds
    #context['tgntask'] = 'tgn' in ds_tasks
    #context['whgtask'] = len(set(['whg','idx']) & set(ds_tasks)) > 0
    #context['wdtask'] = len(set(['wd','wdlocal']) & set(ds_tasks)) > 0
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

""" data owner browse table """
class DatasetBrowseView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_browse.html'

  #form_class = DatasetDetailModelForm

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/browse'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetBrowseView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    print('DatasetBrowseView get_context_data() kwargs:',self.kwargs)
    print('DatasetBrowseView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")

    ds = get_object_or_404(Dataset, id=id_)
    me = self.request.user
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners
    #ds_tasks = [t.task_name[6:] for t in ds.tasks.all()]
    ds_tasks = [t.task_name[6:] for t in ds.tasks.filter(status='SUCCESS')]
    #placeset = Place.objects.filter(dataset=ds.label)
    context['updates'] = {}
    context['ds'] = ds
    context['tgntask'] = 'tgn' in ds_tasks
    context['whgtask'] = len(set(['whg','idx']) & set(ds_tasks)) > 0
    context['wdtask'] = len(set(['wd','wdlocal']) & set(ds_tasks)) > 0
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context


#"""public view"""
#class DatasetDetailView(DetailView):
  #template_name = 'datasets/ds_detail.html'

  #model = Dataset

  #def get_context_data(self, **kwargs):
    #context = super(DatasetDetailView, self).get_context_data(**kwargs)
    #id_ = self.kwargs.get("pk")
    #print('self, kwargs',self, self.kwargs)

    ##qs = Dataset.objects.filter(collection_id = id_)
    ###coll_set = [cd.dataset for cd in qs]


    ##context['ds_list'] = [cd.dataset for cd in qs]
    #context['links_added'] = 1
    #context['geoms_added'] = 1
    #return context
#
class DatasetReconcileView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_reconcile.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/reconcile'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetReconcileView, self).get_context_data(*args, **kwargs)

    #print('DatasetReconcileView get_context_data() kwargs:',self.kwargs)
    #print('DatasetReconcileView get_context_data() request.user:',self.request.user)
    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)

    # build context for rendering dataset.html
    me = self.request.user

    # omits FAILURE and ARCHIVED
    ds_tasks = TaskResult.objects.all().filter(task_args = [id_], status='SUCCESS')
    #archived_tasks = TaskResult.objects.all().filter(task_args = [id_], status='ARCHIVED')

    #context['region_list'] = predefined
    #context['updates'] = {}
    context['ds'] = ds
    context['log'] = ds.log.filter(category='dataset').order_by('-timestamp')
    context['comments'] = Comment.objects.filter(place_id__dataset=ds).order_by('-created')
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners
    context['tasks'] = ds_tasks

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    #print('context["tasks"] from DatasetDetailView', context['tasks'])

    return context
#
class DatasetCollabView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = DatasetUser
  template_name = 'datasets/ds_collab.html'

  #form_class = DatasetDetailModelForm
  #queryset = Dataset.objects.filter(id=self.kwargs.get("id"))

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/collab'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCollabView, self).get_context_data(*args, **kwargs)

    print('DatasetCollabView get_context_data() kwargs:',self.kwargs)
    print('DatasetCollabView get_context_data() request.user:',self.request.user)
    id_ = self.kwargs.get("id")
    #bounds = self.kwargs.get("bounds")
    ds = get_object_or_404(Dataset, id=id_)

    # build context for rendering dataset.html
    me = self.request.user

    context['ds'] = ds

    context['collabs'] = ds.collabs.all()
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

#
class DatasetAddTaskView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_addtask.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/log'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetAddTaskView, self).get_context_data(*args, **kwargs)
    """ maps need these """
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    #print('DatasetAddTaskView get_context_data() kwargs:',self.kwargs)
    #print('DatasetAddTaskView get_context_data() request.user:',self.request.user)
    id_ = self.kwargs.get("id")
    #bounds = self.kwargs.get("bounds")
    ds = get_object_or_404(Dataset, id=id_)
    # print('ds',ds.label)

    # build context for rendering ds_addtask.html
    me = self.request.user
    area_types=['ccodes','copied','drawn']

    # user study areas
    userareas = Area.objects.all().filter(type__in=area_types).values('id','title').order_by('-created')
    context['area_list'] = userareas if me.username == 'whgadmin' else userareas.filter(owner=me)

    # pre-defined UN regions
    predefined = Area.objects.all().filter(type='predefined').values('id','title')

    gothits={}
    for t in ds.tasks.filter(status='SUCCESS'):
      gothits[t.task_id] = int(json.loads(t.result)['got_hits'])

    # deliver status messae(s) to template
    msg_unreviewed = """There is a <span class='strong'>%s</span> task in progress, and all %s records that got hits remain unreviewed. <span class='text-danger strong'>Starting this new task will delete the existing one</span>, with no impact on your dataset."""
    msg_inprogress = """<p class='mb-1'>There is a <span class='strong'>%s</span> task in progress, and %s of the %s records that had hits have been reviewed. <span class='text-danger strong'>Starting this new task will archive the existing task and submit only unreviewed records.</span>. If you proceed, you can keep or delete prior match results (links and/or geometry):</p>"""
    msg_done = """All records have been submitted for reconciliation to %s and reviewed. To begin the step of accessioning to the WHG index, please <a href="%s">contact our editorial team</a>"""
    for i in ds.taskstats.items():
      auth = i[0][6:]
      if len(i[1]) > 0:
        #auth = i[0][6:]
        tid = i[1][0]['tid']
        remaining = i[1][0]['total']
        hadhits = gothits[tid]
        reviewed = hadhits-remaining
        print('auth, tid, remaining, hadhits', auth, tid, remaining, hadhits)
        if remaining == 0:
          context['msg_'+auth] = {
            'msg': msg_done%(auth,"/contact"),
            'type': 'done'}
        elif remaining < hadhits:
          context['msg_'+auth] = {
            'msg': msg_inprogress%(auth, reviewed, hadhits),
            'type': 'inprogress'}
          #print(context)
        else:
          context['msg_'+auth] = {
            'msg': msg_unreviewed%(auth, hadhits),
            'type': 'unreviewed'
          }
      else:
        context['msg_'+auth] = {
          'msg': "no tasks of this type",
          'type': 'none'
        }

    active_tasks = dict(filter(lambda elem: len(elem[1]) > 0, ds.taskstats.items()))
    remaining = {}
    for t in active_tasks.items():
      remaining[t[0][6:]] = t[1][0]['total']
    context['region_list'] = predefined
    context['ds'] = ds
    context['collaborators'] = ds.collabs.all()
    context['owners'] = ds.owners
    context['remain_to_review'] = remaining
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    #print('context["tasks"] from DatasetAddTaskView', context['tasks'])

    return context

#
class DatasetLogView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_log.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    user = self.request.user
    print('messages:', messages.get_messages(self.kwargs))
    return '/datasets/'+str(id_)+'/log'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetLogView, self).get_context_data(*args, **kwargs)

    print('DatasetLogView get_context_data() kwargs:',self.kwargs)
    print('DatasetLogView get_context_data() request.user:',self.request.user)
    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)

    me = self.request.user

    context['ds'] = ds
    context['log'] = ds.log.filter(category='dataset').order_by('-timestamp')
    context['comments'] = Comment.objects.filter(place_id__dataset=ds).order_by('-created')
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context
