# datasets.views
# NB!!! some imports greyed out but ARE USED!
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.models import User
from django.contrib.gis.geos import GEOSGeometry
from django.core.mail import send_mail
from django.core.paginator import Paginator
from django.db.models import Q
from django.db.utils import DataError
from django.forms import modelformset_factory
from django.http import HttpResponseServerError, HttpResponseRedirect, JsonResponse, HttpResponse
from django.shortcuts import redirect, get_object_or_404, render
from django.test import Client
from django.urls import reverse
from django.views.generic import (CreateView, ListView, UpdateView, DeleteView, DetailView)
from django_celery_results.models import TaskResult


# external
from celery import current_app as celapp
from copy import deepcopy
import codecs, math, mimetypes, os, re, shutil, sys, tempfile
from deepdiff import DeepDiff as diff
import numpy as np
from elasticsearch7 import Elasticsearch
es = settings.ES_CONN
import pandas as pd
from pathlib import Path
from shapely import wkt
from shutil import copyfile

from areas.models import Area
from collection.models import Collection, CollectionUser
from datasets.forms import HitModelForm, DatasetDetailModelForm, DatasetCreateModelForm, DatasetCreateEmptyModelForm
from datasets.models import Dataset, Hit, DatasetFile
from datasets.static.hashes import mimetypes_plus as mthash_plus
from datasets.static.hashes.parents import ccodes as cchash

# NB these task names ARE in use; they are generated dynamically
from datasets.tasks import align_wdlocal, align_idx, align_tgn, maxID

# from datasets.update import deleteFromIndex
from datasets.utils import *
from elastic.es_utils import makeDoc, removePlacesFromIndex, replaceInIndex, removeDatasetFromIndex
from main.choices import AUTHORITY_BASEURI
from main.models import Log, Comment
from places.models import *
from resources.models import Resource

"""
  email on failures: ds_insert_lpf, Celery down notice
  TODO: add email on success: register
  to ['whgazetteer@gmail.com','karl@kgeographer.org'],
"""
def emailer(subj, msg, from_addr, to_addr):
  print('subj, msg, from_addr, to_addr',subj, msg, from_addr, to_addr)
  send_mail(
      subj, msg, from_addr, to_addr,
      fail_silently=False,
  )

""" check Celery process is running before initiating reconciliation task """
def celeryUp():
  response = celapp.control.ping(timeout=1.0)
  return len(response)>0

""" append src_id to base_uri"""
def link_uri(auth,id):
  baseuri = AUTHORITY_BASEURI[auth]
  uri = baseuri + str(id)
  return uri

"""
  from datasets.views.review()
  indexes a db record upon a single hit match in align_idx review
  new record becomes child in the matched hit group 
"""
def indexMatch(pid, hit_pid=None):
  print('indexMatch(): pid '+str(pid)+' w/hit_pid '+str(hit_pid))
  from elasticsearch7 import Elasticsearch
  es = settings.ES_CONN
  idx='whg'
  place = get_object_or_404(Place, id=pid)

  # is this place already indexed (e.g. by pass0 automatch)?
  q_place = {"query": {"bool": {"must": [{"match": {"place_id": pid}}]}}}
  res = es.search(index=idx, body=q_place)
  if res['hits']['total']['value'] == 0:
    # not indexed, make a new doc
    new_obj = makeDoc(place)
    p_hits = None
  else:
    # it's indexed, get parent
    p_hits = res['hits']['hits']
    place_parent = p_hits[0]['_source']['relation']['parent']

  if hit_pid == None and not p_hits:
    # there was no match and place is not already indexed
    print('making '+str(pid)+' a parent')
    new_obj['relation']={"name":"parent"}

    # increment whg_id
    print('maxID at :109', maxID(es, idx))
    whg_id = maxID(es, idx) + 1
    print('whg_id at :111', whg_id)
    # parents get an incremented _id & whg_id
    new_obj['whg_id']=whg_id
    print('new_obj', new_obj)
    # sys.exit()

    # add its own names to the suggest field
    for n in new_obj['names']:
      new_obj['suggest']['input'].append(n['toponym'])
    # add its title
    if place.title not in new_obj['suggest']['input']:
      new_obj['suggest']['input'].append(place.title)
    #index it
    try:
      res = es.index(index=idx, id=str(whg_id), body=json.dumps(new_obj))
      place.indexed = True
      place.save()
    except:
      print('failed indexing (as parent)'+str(pid))
      pass
    print('created parent:',pid,place.title)
  else:
    # get hit record in index
    q_hit = {"query": {"bool": {"must": [{"match": {"place_id": hit_pid}}]}}}
    res = es.search(index=idx, body=q_hit)
    hit = res['hits']['hits'][0]

    # see if new place (pid) is already indexed (i.e. due to prior automatch)
    q_place = {"query": {"bool": {"must": [{"match": {"place_id": pid}}]}}}
    res = es.search(index=idx, body=q_place)
    if len(res['hits']['hits']) >0:
      # it's already in, (almost) certainly a child...of what?
      place_hit = res['hits']['hits'][0]

    # if hit is a child, get _id of its parent; this will be a sibling
    # if hit is a parent, get its _id, this will be a child
    if hit['_source']['relation']['name'] == 'child':
      parent_whgid = hit['_source']['relation']['parent']
    else:
      parent_whgid = hit['_id']

    # mine new place for its names, make an index doc
    match_names = [p.toponym for p in place.names.all()]
    new_obj['relation']={"name":"child","parent":parent_whgid}

    # all or nothing; pass if error
    try:
      # index child
      es.index(index=idx, id=place.id, routing=1, body=json.dumps(new_obj))
      #count_kids +=1
      print('added '+str(place.id) + ' as child of '+ str(hit_pid))

      # add child's names to parent's searchy fields
      q_update = { "script": {
          "source": "ctx._source.children.add(params.id); ctx._source.searchy.addAll(params.names)",
          "lang": "painless",
          "params":{"names": match_names, "id": str(place.id)}
        },
        "query": {"match":{"_id": parent_whgid}}}
      es.update_by_query(index=idx, body=q_update, conflicts='proceed')
      print('indexed? ', place.indexed)
    except:
      print('failed indexing '+str(pid)+' as child of '+str(parent_whgid), new_obj)
      pass

"""
  from datasets.views.review()
  indexes a db record given multiple hit matches in align_idx review
  a LOT has to happen (see _notes/accession-psudocode.txt): 
    - pick a single 'winner' among the matched hits (max score)
    - make new record its child
    - demote all non-winners in index from parent to child
      - whg_id and children[] ids (if any) added to winner
      - name variants added to winner's searchy[] and suggest.item[] lists
"""
def indexMultiMatch(pid, matchlist):
  # matchlist is [{'whg_id': , 'pid': , 'score': ,'links': }]
  print('indexMultiMatch(): pid '+str(pid)+' matches '+str(matchlist))
  from elasticsearch7 import RequestError
  es = settings.ES_CONN
  idx='whg'

  # create index doc for new record
  place = Place.objects.get(id=pid)
  from elastic.es_utils import makeDoc
  new_obj = makeDoc(place)

  # bins for new values going to winner
  addnames = []
  addkids = [str(pid)] # pid will also be the new record's _id

  """ 
  - all matchlist items are index 'parent' records,
    possibly with multiple children already
  - winner is existing index hit with max(score)
  - any other matches are to become children of winner
  - this is indexMultuMatch() so >=1 demoted
  """
  winner = max(matchlist, key=lambda x: x['score']) # 14158663
  demoted = [str(i['whg_id']) for i in matchlist
             if not (i['whg_id'] == winner['whg_id'])] # ['14090523']

  # add winner as parent to 'new_obj{}'
  new_obj['relation'] = {"name": "child", "parent": winner['whg_id']}
  # gather toponyms for adding to winner's 'searchy' field later
  for n in new_obj['names']:
    addnames.append(n['toponym'])
  if place.title not in addnames:
    addnames.append(place.title)

  # script to update winner w/kids and names
  # from new record and any kids of 'other' matched parents
  def q_updatewinner(addkids, addnames):
    return {"script":{
      "source": """ctx._source.children.addAll(params.newkids);
      ctx._source.searchy.addAll(params.names);
      """,
      "lang": "painless",
      "params":{
        "newkids": addkids,
        "names": addnames }
    }}

  # index the new record as child of winner
  try:
    es.index(index=idx, id=str(pid), routing=1, body=json.dumps(new_obj))
  except RequestError as rq:
    print('Error: ', rq.error, rq.info)

  # demote others
  for _id in demoted:
    print('find & demote whg_id', _id)
    # get index record stuff, to be altered then re-indexed
    # ES won't allow altering parent/child relations directly
    q_demote = {"query": {"bool": {"must": [{"match": {"whg_id": _id}}]}}}
    res = es.search(body=q_demote, index=idx)
    srcd = res['hits']['hits'][0]['_source']
    # add names in suggest to names[]
    sugs = srcd['suggest']['input']
    for sug in sugs:
      addnames.append(sug)
    addnames = list(set(addnames))
    # _id of demoted (a whg_id) belongs in winner's children[]
    addkids.append(str(srcd['whg_id']))

    haskids = len(srcd['children']) > 0
    # if demoted record has kids, add to addkids[] list
    # for 'adoption' by topdog later
    if haskids:
      morekids = srcd['children']
      for kid in morekids:
        addkids.append(str(kid))

    # update the 'winner' parent
    q=q_updatewinner(list(set(addkids)), list(set(addnames))) # ensure only unique
    try:
      # es8 on server needs explicit parameters
      # es.update(idx, winner['whg_id'], body=q)
      es.update(index=idx, id=winner['whg_id'], body=q)
    except RequestError as rq:
      print('q_updatewinner failed (whg_id)', winner['whg_id'])
      print('Error: ', rq.error, rq.info)

    from copy import deepcopy
    newsrcd = deepcopy(srcd)
    # update it to reflect demotion
    newsrcd['relation'] = {"name":"child", "parent":winner['whg_id']}
    newsrcd['children'] = []
    if 'whg_id' in newsrcd:
      newsrcd.pop('whg_id')

    # zap the demoted, reindex with same _id and modified doc (newsrcd)
    try:
      es.delete(index='whg', id=_id)
      es.index(index='whg', id=_id, body=newsrcd, routing=1)
    except RequestError as rq:
      print('reindex failed (demoted)',d)
      print('Error: ', rq.error, rq.info)

    # re-assign parent for kids of all/any demoted parents
    if len(addkids) > 0:
      for kid in addkids:
        q_adopt = {"script": {
          "source": "ctx._source.relation.parent = params.new_parent; ",
          "lang": "painless",
          "params": {"new_parent": winner['whg_id']}
          },
          "query": {"match": {"place_id": kid}}}
        es.update_by_query(index=idx, body=q_adopt, conflicts='proceed')

""" 
  GET   returns review.html for Wikidata, or accession.html for accessioning
  POST  for each record that got hits, process user matching decisions 
"""
def review(request, pk, tid, passnum):
  pid = None
  if 'pid' in request.GET:
    pid = request.GET['pid']
  ds = get_object_or_404(Dataset, id=pk)
  task = get_object_or_404(TaskResult, task_id=tid)
  auth = task.task_name[6:].replace('local','')
  authname = 'Wikidata' if auth == 'wd' else 'Getty TGN' \
    if auth == 'tgn' else 'WHG'
  kwargs=json.loads(task.task_kwargs.replace("'",'"'))
  # print('review() kwargs', kwargs)
  test = kwargs['test'] if 'test' in kwargs else "off"
  beta = 'beta' in list(request.user.groups.all().values_list('name',flat=True))
  # filter place records by passnum for those with unreviewed hits on this task
  # if request passnum is complete, increment
  cnt_pass = Hit.objects.values('place_id').filter(task_id=tid, reviewed=False, query_pass=passnum).count()
  # print('in review()', {'auth':auth, 'ds':ds,'task': task})
  # TODO: refactor this awful mess; controls whether PASS appears in review dropdown
  cnt_pass0 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass0').count()
  cnt_pass1 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass1').count()
  cnt_pass2 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass2').count()
  cnt_pass3 = Hit.objects.values('place_id').filter(
    task_id=tid, reviewed=False, query_pass='pass3').count()

  # calling link passnum may be 'pass*', 'def', or '0and1' (for idx)
  # if 'pass*', just get place_ids for that pass
  if passnum.startswith('pass'):
    pass_int = int(passnum[4])
    # if no unreviewed left, go to next pass
    passnum = passnum if cnt_pass > 0 else 'pass'+str(pass_int+1)
    hitplaces = Hit.objects.values('place_id').filter(
      task_id=tid,
      reviewed=False,
      query_pass=passnum
    )
  else:
    # all unreviewed
    hitplaces = Hit.objects.values('place_id').filter(task_id=tid, reviewed=False)
  # print('review() hitplaces', [p['place_id'] for p in hitplaces])

  # set review page returned
  if auth in ['whg','idx']:
    review_page = 'accession.html'
  else:
    review_page = 'review.html'

  #
  review_field = 'review_whg' if auth in ['whg','idx'] else \
    'review_wd' if auth.startswith('wd') else 'review_tgn'
  lookup = '__'.join([review_field, 'in'])
  """
    2 = deferred; 1 = reviewed, 0 = unreviewed; NULL = no hits
    status = [2] if passnum == 'def' else [0,2]
    by default, don't return deferred
  """
  status = [2] if passnum == 'def' else [0]

  # unreviewed place objects from place_ids (a single pass or all)
  record_list = ds.places.order_by('id').filter(**{lookup: status}, pk__in=hitplaces)

  # no records left for pass (or in deferred queue)
  if len(record_list) == 0:
    context = {
      "nohits":True,
      'ds_id':pk,
      'task_id': tid,
      'passnum': passnum,
    }
    return render(request, 'datasets/'+review_page, context=context)

  # manage pagination & urls
  # gets next place record as records[0]
  # TODO: manage concurrent reviewers; i.e. 2 people have same page 1
  paginator = Paginator(record_list, 1)

  # handle request for singleton (e.g. deferred from browse table)
  # if 'pid' in request.GET, bypass per-pass sequential loading
  if pid:
    print('pid in URI, just show that', pid)
    # get its index and add 1 to get page
    page = (*record_list,).index(Place.objects.get(id=pid)) +1
    print('pagenum', page)
  else:
    # default action, sequence of all pages for the pass
    page = 1 if not request.GET.get('page') else \
      request.GET.get('page')
  records = paginator.get_page(page)
  count = len(record_list)
  # get hits for this record
  placeid = records[0].id
  place = get_object_or_404(Place, id=placeid)
  if passnum.startswith('pass') and auth not in ['whg','idx']:
    # this is wikidata review, list only for this pass
    raw_hits = Hit.objects.filter(place_id=placeid, task_id=tid, query_pass=passnum).order_by('-score')
  else:
    # accessioning -> get all regardless of pass
    # raw_hits = Hit.objects.filter(place_id=placeid, task_id=tid).order_by('-score')
    raw_hits = Hit.objects.filter(place_id=placeid, task_id=tid).order_by('-score')

  # print('raw_hits', [h.json['titles'] for h in raw_hits])
  # ??why? get pass contents for all of a place's hits
  passes = list(set([item for sublist in [[s['pass'] for s in h.json['sources']] for h in raw_hits]
                     for item in sublist])) if auth in ['whg','idx'] else None

  # convert ccodes to names
  countries = []
  for r in place.ccodes:
    try:
      countries.append(cchash[0][r.upper()]['gnlabel']+' ('+cchash[0][r.upper()]['tgnlabel']+')')
    except:
      pass

  # prep some context
  context = {
    'ds_id': pk, 'ds_label': ds.label, 'task_id': tid,
    'hit_list': raw_hits,
    'passes':passes,
    'authority': task.task_name[6:8] if auth=='wdlocal' else task.task_name[6:],
    'records': records,
    'countries': countries,
    'passnum': passnum,
    'page': page if request.method == 'GET' else str(int(page)-1),
    'aug_geom': json.loads(task.task_kwargs.replace("'",'"'))['aug_geom'],
    'mbtoken': settings.MAPBOX_TOKEN_WHG,
    'count_pass0': cnt_pass0,
    'count_pass1': cnt_pass1,
    'count_pass2': cnt_pass2,
    'count_pass3': cnt_pass3,
    'deferred': True if passnum =='def' else False,
    'test':test,
  }

  # print('raw_hits at formset', [h.json['titles'] for h in raw_hits])
  # build formset from hits, add to context
  HitFormset = modelformset_factory(
    Hit,
    fields = ('id','authority','authrecord_id','query_pass','score','json'),
    form=HitModelForm, extra=0)
  formset = HitFormset(request.POST or None, queryset=raw_hits)
  context['formset'] = formset
  method = request.method

  # GET -> just display
  if method == 'GET':
    print('review() GET, just displaying next')
  elif method == 'POST':
    # process match/no match choices made by save in review or accession page
    # NB very different cases.
    #   For wikidata review, act on each hit considered (new place_geom and place_link records if matched)
    #   For accession, act on index 'clusters'
    place_post = get_object_or_404(Place,pk=request.POST['place_id'])
    review_status = getattr(place_post, review_field)
    # proceed with POST only if place is unreviewed or deferred; else return to a GET (and next place)
    # NB. other reviewer(s) *not* notified
    if review_status == 1:
      context["already"] = True
      messages.success(request, ('Last record ('+place_post.title+') reviewed by another'))
      return redirect('/datasets/'+str(pk)+'/review/'+task.task_id+'/'+passnum)
    elif formset.is_valid():
      hits = formset.cleaned_data
      # print('formset valid', hits)
      matches = 0
      matched_for_idx = [] # for accession
      # are any of the listed hits matches?
      for x in range(len(hits)):
        hit = hits[x]['id']
        # is this hit a match?
        if hits[x]['match'] not in ['none']:
          # print('json of matched hit/cluster (in review())', hits[x]['json'])
          matches += 1
          # if wd or tgn, write place_geom, place_link record(s) now
          # IF someone didn't just review it!
          if task.task_name[6:] in ['wdlocal','wd','tgn']:
            #print('task.task_name', task.task_name)
            hasGeom = 'geoms' in hits[x]['json'] and len(hits[x]['json']['geoms']) > 0
            # create place_geom records if 'accept geometries' was checked
            if kwargs['aug_geom'] == 'on' and hasGeom \
               and tid not in place_post.geoms.all().values_list('task_id',flat=True):
              gtype = hits[x]['json']['geoms'][0]['type']
              coords = hits[x]['json']['geoms'][0]['coordinates']
              # TODO: build real postgis geom values
              gobj = GEOSGeometry(json.dumps({"type":gtype,"coordinates":coords}))
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

            # create single PlaceLink for matched wikidata record
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
              for l in hits[x]['json']['links']:
                authid = re.search("\: ?(.*?)$", l).group(1)
                # print('authid, authids',authid, place.authids)
                if l not in place.authids:
                # if authid not in place.authids:
                  link = PlaceLink.objects.create(
                    place = place_post,
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
          # this is accessioning to whg index, add to matched[]
          elif task.task_name == 'align_idx':
            if 'links' in hits[x]['json']:
              links_count = len(hits[x]['json'])
            matched_for_idx.append({'whg_id':hits[x]['json']['whg_id'],
                            'pid':hits[x]['json']['pid'],
                            'score':hits[x]['json']['score'],
                            'links': links_count})
        # in any case, flag hit as reviewed...
        hitobj = get_object_or_404(Hit, id=hit.id)
        hitobj.reviewed = True
        hitobj.save()
        print('hit # '+str(hitobj.id)+' flagged reviewed')

      # handle accessioning match results
      if len(matched_for_idx) == 0 and task.task_name == 'align_idx':
        # no matches during accession, index as seed (parent
        print('no accession matches, index '+str(place_post.id)+' as seed (parent)')
        print('maxID() in review()', maxID(es,'whg'))
        indexMatch(str(place_post.id))
        place_post.indexed = True
        place_post.save()
      elif len(matched_for_idx) == 1:
        print('one accession match, make record '+str(place_post.id)+' child of hit ' + str(matched_for_idx[0]))
        indexMatch(str(place_post.id), matched_for_idx[0]['pid'])
        place_post.indexed = True
        place_post.save()
      elif len(matched_for_idx) > 1:
        indexMultiMatch(place_post.id, matched_for_idx)
        place_post.indexed = True
        place_post.save()

      if ds.unindexed == 0:
        setattr(ds, 'ds_status', 'indexed')
        ds.save()

      # if none are left for this task, change status, email user & staff
      if auth in ['wd'] and ds.recon_status['wdlocal'] == 0:
        ds.ds_status = 'wd-complete'
        ds.save()
        status_emailer(ds, 'wd')
        print('sent status email')
      elif auth == 'idx' and ds.recon_status['idx'] == 0:
        ds.ds_status = 'indexed'
        ds.save()
        status_emailer(ds, 'idx')
        print('sent status email')

      print('review_field', review_field)
      setattr(place_post, review_field, 1)
      place_post.save()

      return redirect('/datasets/'+str(pk)+'/review/'+tid+'/'+passnum+'?page='+str(int(page)))
    else:
      print('formset is NOT valid. errors:',formset.errors)
      print('formset data:',formset.data)
  # print('context', context)
  return render(request, 'datasets/'+review_page, context=context)

def write_idx_pass0(request, tid):
  print('in write_idx_pass0(), doing nothing')
  return

"""
  write_wd_pass0(taskid)
  called from dataset_detail>linking tab
  accepts all pass0 wikidata matches, writes geoms and links
"""
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
  # print('writing '+str(len(hits))+' pass0 matched records for', ds.label)
  for h in hits:
    hasGeom = 'geoms' in h.json and len(h.json['geoms']) > 0
    hasLinks = 'links' in h.json and len(h.json['links']) > 0
    place = h.place # object
    # existing for the place
    authids=place.links.all().values_list('jsonb__identifier',flat=True)
    # GEOMS
    # confirm another user hasn't just done this...
    if hasGeom and kwargs['aug_geom'] == 'on' \
       and tid not in place.geoms.all().values_list('task_id',flat=True):
      for g in h.json['geoms']:
        pg = PlaceGeom.objects.create(
          place = place,
          task_id = tid,
          src_id = place.src_id,
          geom=GEOSGeometry(json.dumps({"type": g['type'], "coordinates": g['coordinates']})),
          jsonb = {
            "type":g['type'],
            "citation":{"id":auth+':'+h.authrecord_id,"label":authname},
            "coordinates":g['coordinates']
          }
        )
      print('created place_geom instance in write_wd_pass0', pg)

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
    ds.numlinked = len(set(PlaceLink.objects.filter(place_id__in=ds.placeids).values_list('place_id',flat=True)))
    ds.total_links += link_counter
    ds.save()

    # flag hit as reviewed
    # h.reviewed = True
    # h.save()

    # flag place as reviewed
    place.review_wd = 1
    place.save()

  # previously unreviewed pass0 hits for this task
  hits.update(reviewed = True)
  return redirect('/datasets/' + str(ds.id) + '/reconcile')
  # return HttpResponseRedirect(referer)

"""
  ds_recon()
  initiates & monitors Celery tasks against Elasticsearch indexes
  i.e. align_[wdlocal | idx | tgn ] in tasks.py
  url: datasets/{ds.id}/reconcile ('ds_reconcile'; from ds_addtask.html)
  params: pk (dataset id), auth, region, userarea, geom, scope
  each align_{auth} task runs matching es_lookup_{auth}() and writes Hit instances
"""
def ds_recon(request, pk):
  ds = get_object_or_404(Dataset, id=pk)
  # TODO: handle multipolygons from "#area_load" and "#area_draw"
  user = request.user
  context = {"dataset": ds.title}
  if request.method == 'GET':
    print('ds_recon() GET')
  elif request.method == 'POST' and request.POST:
    print('ds_recon() request.POST:',request.POST)
    test = 'on' if 'test' in request.POST else 'off'
    auth = request.POST['recon']
    language = request.LANGUAGE_CODE
    if auth == 'idx' and ds.public == False and test == 'off':
      messages.add_message(request, messages.ERROR, """Dataset must be public before indexing!""")
      return redirect('/datasets/' + str(ds.id) + '/addtask')
    # previous successful task of this type?
    #   wdlocal? archive previous, scope = unreviewed
    #   idx? scope = unindexed
    previous = ds.tasks.filter(task_name='align_'+auth, status='SUCCESS')
    prior = request.POST['prior'] if 'prior' in request.POST else 'na'
    rerun = 'rerun' in request.POST
    if previous.count() > 0:
      if auth == 'idx':
        scope = "unindexed"
      else:
        # get its id and archive it
        tid = previous.first().task_id
        task_archive(tid, prior)
        scope = 'rerun' if rerun else 'unreviewed'
        print('recon(): archived previous task')
        print('ds_recon(): links & geoms were '+ ('kept' if prior=='keep' else 'zapped'))
    else:
      # no existing task, submit all rows
      print('ds_recon(): no previous, submitting all')
      scope = 'all'

    print('ds_recon() scope', scope)
    print('ds_recon() auth', auth)
    # return
    # which task? wdlocal, tgn, idx, whg (future)
    func = eval('align_'+auth)

    # TODO: let this vary per task?
    region = request.POST['region'] # pre-defined UN regions
    userarea = request.POST['userarea'] # from ccodes, or drawn
    aug_geom = request.POST['geom'] if 'geom' in request.POST else '' # on == write geom if matched
    bounds={
      "type":["region" if region !="0" else "userarea"],
      "id": [region if region !="0" else userarea]}

    # check Celery service
    if not celeryUp():
      print('Celery is down :^(')
      emailer('Celery is down :^(',
              'if not celeryUp() -- look into it, bub!',
              'whg@pitt.edu',
              ['karl@kgeographer.org'])
      messages.add_message(request, messages.INFO, """Sorry! WHG reconciliation services appears to be down. 
        The system administrator has been notified.""")
      return redirect('/datasets/'+str(ds.id)+'/reconcile')

    # sys.exit()
    # initiate celery/redis task
    # NB 'func' resolves to align_wdlocal() or align_idx() or align_tgn()
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
        test=test,
      )
      messages.add_message(request, messages.INFO, "<span class='text-danger'>Your reconciliation task is under way.</span><br/>When complete, you will receive an email and if successful, results will appear below (you may have to refresh screen). <br/>In the meantime, you can navigate elsewhere.")
      return redirect('/datasets/'+str(ds.id)+'/reconcile')
    except:
      print('failed: align_'+auth )
      print(sys.exc_info())
      messages.add_message(request, messages.INFO, "Sorry! Reconciliation services appear to be down. The system administrator has been notified.<br/>"+ str(sys.exc_info()))
      emailer('WHG recon task failed',
              'a reconciliation task has failed for dataset #'+ds.id+', w/error: \n' +str(sys.exc_info())+'\n\n',
              'whg@pitt.edu',
              'whgadmin@kgeographer.org')

      return redirect('/datasets/'+str(ds.id)+'/reconcile')

"""
  task_delete(tid, scope)
  delete results of a reconciliation task:
  hits + any geoms and links added by review
  reset Place.review_{auth} to null
"""
# TODO: needs overhaul to account for ds.ds_status
def task_delete(request, tid, scope="foo"):
  hits = Hit.objects.all().filter(task_id=tid)
  tr = get_object_or_404(TaskResult, task_id=tid)
  auth = tr.task_name[6:] # wdlocal, idx
  dsid = tr.task_args[1:-1]
  test = json.loads(tr.task_kwargs.replace("'",'"'))['test'] \
    if 'test' in tr.task_kwargs else 'off'
  ds=get_object_or_404(Dataset, pk=dsid)
  ds_status = ds.ds_status
  print('task_delete() dsid', dsid)
  # return HttpResponse(
  #   content='<h3>stopped task_delete()</h3>')

  # only the places that had hit(s) in this task
  places = Place.objects.filter(id__in=[h.place_id for h in hits])
  # links and geometry added by a task have the task_id
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
    p.defer_comments.delete()
    p.save()

  # zap task record & its hits
  # or only geoms if that was the choice
  if scope == 'task':
    tr.delete()
    hits.delete()
    placelinks.delete()
    placegeoms.delete()
  elif scope == 'geoms':
    placegeoms.delete()

  # delete dataset from index
  # undoes any acceessioning work
  if auth in ['whg', 'idx'] and test == 'off':
    removeDatasetFromIndex('whg', dsid)
  # set status
  print('ds.tasks.all()', ds.tasks.all())
  print('ds.file.file.name', ds.file.file.name)
  if ds.tasks.count() == 0:
    if ds.file.file.name.startswith('dummy'):
      ds.ds_status = 'remote'
    else:
      ds.ds_status = 'uploaded'
  ds.save()

  return redirect('/datasets/'+dsid+'/reconcile')

"""
  task_archive(tid, scope, prior)
  delete hits
  if prior = 'zap: delete geoms and links added by review
  reset Place.review_{auth} to null
  set task status to 'ARCHIVED'
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
    p.defer_comments.delete()
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
    if os.path.exists(ffn) and f.file.name != 'dummy_file.txt':
      os.remove(ffn)
      print('zapped file '+ffn)
    else:
      print('did not find or ignored file '+ffn)

"""
  update_rels_tsv(pobj, row) refactored 26 Nov 2022 (backup below)
  updates objects related to a Place (pobj)
  make new child objects of pobj: names, types, whens, related, descriptions
  for geoms and links, add from row if not there
  row is a pandas dict  
"""
def update_rels_tsv(pobj, row):
  header = list(row.keys())
  # print('update_rels_tsv(): pobj, row, header', pobj, row, header)
  src_id = row['id']
  title = row['title']
  # TODO: leading parens problematic for search on title
  title = re.sub('^\(.*?\)', '', title).strip()
  title_source = row['title_source']
  title_uri = row['title_uri'] if 'title_uri' in header else ''
  variants = [x.strip() for x in row['variants'].split(';')] \
    if 'variants' in header and row['variants'] not in ['','None',None] else []

  types = [x.strip() for x in row['types'].split(';')] \
    if 'types' in header and str(row['types']) not in ['','None',None] else []

  aat_types = [x.strip() for x in row['aat_types'].split(';')] \
    if 'aat_types' in header and str(row['aat_types']) not in ['','None',None] else []

  parent_name = row['parent_name'] if 'parent_name' in header else ''

  parent_id = row['parent_id'] if 'parent_id' in header else ''

  # empty lon and lat are None
  coords = makeCoords(row['lon'], row['lat']) \
    if 'lon' in header and 'lat' in header and row['lon'] else []
  print('coords', coords)
  try:
    matches = [x.strip() for x in row['matches'].split(';')] \
      if 'matches' in header and row['matches'] else []
  except:
    print('matches, error', row['matches'], sys.exc_info())

  description = row['description'] \
    if row['description'] else ''

  # lists for associated objects
  objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
          "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}

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
  print('objs after names', objs)
  #
  # PlaceType()
  print('types', types)
  if len(types) > 0:
    for i,t in enumerate(types):
      fclass_list = []
      # i always 0 in tsv
      aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) else None
      # get fclass(es) to add to Place (pobj)
      if aatnum and int(aatnum[4:]) in Type.objects.values_list('aat_id', flat=True):
        fc = get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass
        fclass_list.append(fc)
      objs['PlaceType'].append(
        PlaceType(
          place=pobj,
          src_id = src_id,
          jsonb={ "identifier":aatnum,
                  "sourceLabel":t,
                  "label":aat_lookup(int(aatnum[4:])) if aatnum !='aat:' else ''
                }
      ))
    pobj.fclasses = fclass_list
    pobj.save()
  print('objs after types', objs)

  #
  # PlaceGeom()
  # TODO: test no existing identical geometry
  print('coords', coords)
  if len(coords) > 0:
    geom = {"type": "Point",
            "coordinates": coords,
            "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
  elif 'geowkt' in header and row['geowkt'] not in ['',None]: # some rows no geom
    geom = parse_wkt(row['geowkt'])
    print('from geowkt', geom)
  else:
    geom = None
  print('geom', geom)
  # TODO:
  # if pobj is existing place, add geom only if it's new
  # if pobj is new place and row has geom, always add it
  if geom:
    def trunc4(val):
      return round(val,4)

    new_coords = list(map(trunc4,list(geom['coordinates'])))

    # if no geoms, add this one
    if pobj.geoms.count() == 0:
      objs['PlaceGeom'].append(
        PlaceGeom(
          place=pobj,
          src_id=src_id,
          jsonb=geom,
          geom=GEOSGeometry(json.dumps(geom))
        ))
    # otherwise only add if coords don't match
    elif pobj.geoms.count() > 0:
      try:
        for g in pobj.geoms.all():
          print('exist. coords', list(map(trunc4, g.jsonb['coordinates'])))
          print('new_coords', new_coords)
          if list(map(trunc4, g.jsonb['coordinates'])) != new_coords:
            objs['PlaceGeom'].append(
                PlaceGeom(
                  place=pobj,
                  src_id = src_id,
                  jsonb=geom,
                  geom=GEOSGeometry(json.dumps(geom))
              ))
      except:
        print('failed on ', pobj, sys.exc_info())
  print('objs after geom', objs)

  # PlaceLink() - all are closeMatch
  # Pandas turns nulls into NaN strings, 'nan'
  print('matches', matches)
  if len(matches) > 0:
    # any existing? only add new
    exist_links = list(pobj.links.all().values_list('jsonb__identifier',flat=True))
    print('matches, exist_links at create', matches, exist_links)
    if len(set(matches)-set(exist_links)) > 0:
      # one or more new matches; add 'em
      for m in matches:
        objs['PlaceLink'].append(
          PlaceLink(
            place=pobj,
            src_id = src_id,
            jsonb={"type":"closeMatch", "identifier":m}
        ))
  # print('objs after matches', objs)

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
  # print('objs after related', objs)

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
  # print('objs after when', objs)

  #
  # PlaceDescription()
  # @id, value, lang
  print('description', description)
  if description != '':
    objs['PlaceDescription'].append(
      PlaceDescription(
        place=pobj,
        src_id = src_id,
        jsonb={
          "@id": "", "value":description, "lang":""
        }
      ))

  print('objs after all', objs)

  # what came from this row
  print('COUNTS:')
  print('PlaceName:', len(objs['PlaceName']))
  print('PlaceType:', len(objs['PlaceType']))
  print('PlaceGeom:', len(objs['PlaceGeom']))
  print('PlaceLink:', len(objs['PlaceLink']))
  print('PlaceRelated:', len(objs['PlaceRelated']))
  print('PlaceWhen:', len(objs['PlaceWhen']))
  print('PlaceDescription:', len(objs['PlaceDescription']))
  # no depictions in LP-TSV

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
  ds_update() refactored 26 Nov 2022 (backup below)
  perform updates to database and index, given ds_compare() results
  params: dsid, format, keepg, keepl, compare_data (json string)
"""
""" TEST VALUES 
    from datasets.models import Dataset, DatasetFile
    from django.shortcuts import get_object_or_404
    ds=Dataset.objects.get(id=1460)
    dsid=1460
    ds_places = ds.places.all()
    from django.test import Client
    import pandas as pd
    import numpy as np
    import datetime, re
    file_format = 'delimited'
    keepg, keepl = [True, True]
    compare_data={'id': '1460', 'filename_cur': 'user_whgadmin/sample7h.txt', 'filename_new': 'user_whgadmin/sample7h_new.txt', 'format': 'delimited', 'validation_result': {'format': 'delimited', 'errors': [], 'columns': ['id', 'title', 'title_source', 'start', 'end', 'title_uri', 'ccodes', 'variants', 'types', 'aat_types', 'matches', 'lon', 'lat', 'geowkt', 'geo_source', 'geo_id', 'description'], 'count': 0}, 'tempfn': '/var/folders/w1/ms_2x6rj0ls88v79q33lvds80000gp/T/tmpo40rw66r', 'count_indexed': 7, 'count_links_added': 5, 'count_geoms_added': 5, 'compare_result': {'count_new': 7, 'count_diff': 0, 'count_replace': 6, 'cols_del': [], 'cols_add': ['title_uri', 'description'], 'header_new': ['id', 'title', 'title_source', 'start', 'end', 'title_uri', 'ccodes', 'variants', 'types', 'aat_types', 'matches', 'lon', 'lat', 'geowkt', 'geo_source', 'geo_id', 'description'], 'rows_add': ['717_4'], 'rows_del': ['717_2']}}

    compare_result = compare_data['compare_result']
    tempfn = compare_data['tempfn']
    filename_new = compare_data['filename_new']
    dsfobj_cur = ds.files.all().order_by('-rev')[0]
    rev_num = dsfobj_cur.rev
    from pathlib import Path
    from shutil import copyfile
    if Path('media/'+filename_new).exists():
      fn=os.path.splitext(filename_new)
      #filename_new=filename_new[:-4]+'_'+tempfn[-11:-4]+filename_new[-4:]
      filename_new=fn[0]+'_'+tempfn[-11:-4]+fn[1]
    filepath = 'media/'+filename_new
    copyfile(tempfn,filepath)
    
"""

def ds_update(request):
  if request.method == 'POST':
    print('request.POST ds_update()', request.POST)
    dsid=request.POST['dsid']
    ds = get_object_or_404(Dataset, id=dsid)
    file_format=request.POST['format']

    # keep previous recon/review results?
    keepg = request.POST['keepg']
    keepl = request.POST['keepl']
    print('keepg, keepl', keepg, keepl)

    # comparison returned by ds_compare
    compare_data = json.loads(request.POST['compare_data'])
    compare_result = compare_data['compare_result']
    print('compare_data from ds_compare', compare_data)

    # tempfn has .tsv or .jsonld extension from validation step
    tempfn = compare_data['tempfn']
    filename_new = compare_data['filename_new']
    dsfobj_cur = ds.files.all().order_by('-rev')[0]
    rev_num = dsfobj_cur.rev

    # rename file if already exists in user area
    if Path('media/'+filename_new).exists():
      fn=os.path.splitext(filename_new)
      #filename_new=filename_new[:-4]+'_'+tempfn[-11:-4]+filename_new[-4:]
      filename_new=fn[0]+'_'+tempfn[-11:-4]+fn[1]

    # user said go...copy tempfn to media/{user} folder
    filepath = 'media/'+filename_new
    copyfile(tempfn,filepath)

    # and create new DatasetFile; increment rev
    DatasetFile.objects.create(
      dataset_id = ds,
      file = filename_new,
      rev = rev_num + 1,
      format = file_format,
      upload_date = datetime.date.today(),
      header = compare_result['header_new'],
      numrows = compare_result['count_new']
    )

    # reopen new file as panda dataframe bdf
    if file_format == 'delimited':
      try:
        bdf = pd.read_csv(filepath, delimiter='\t')

        # replace pandas NaN with None
        bdf = bdf.replace({np.nan: ''})
        # bdf = bdf.replace({np.nan: None})
        # force data types
        bdf = bdf.astype({"id":str, "ccodes":str, "types":str, "aat_types":str})
        print('reopened new file, # lines:',len(bdf))
      except:
        raise

      # CURRENT PLACES
      ds_places = ds.places.all()
      print('ds_places', ds_places)
      # pids of missing src_ids
      rows_delete = list(ds_places.filter(src_id__in=compare_result['rows_del']).values_list('id',flat=True))
      print('rows_delete', rows_delete) # 6880702

      # CASCADE includes links & geoms
      try:
        ds_places.filter(id__in=rows_delete).delete()
      except:
        raise

      # for use below
      def delete_related(pid):
        # option to keep prior links and geoms matches; remove the rest
        if not keepg:
          # keep no geoms
          PlaceGeom.objects.filter(place_id=pid).delete()
        else:
          # leave results of prior matches
          PlaceGeom.objects.filter(place_id=pid, task_id__isnull=True).delete()
        if not keepl:
          # keep no links
          PlaceLink.objects.filter(place_id=pid).delete()
        else:
          # leave results of prior matches
          PlaceLink.objects.filter(place_id=pid, task_id__isnull=True).delete()
        PlaceName.objects.filter(place_id=pid).delete()
        PlaceType.objects.filter(place_id=pid).delete()
        PlaceWhen.objects.filter(place_id=pid).delete()
        PlaceRelated.objects.filter(place_id=pid).delete()
        PlaceDescription.objects.filter(place_id=pid).delete()

      # counts for report
      count_new, count_replaced, count_redo = [0,0,0]
      # pids for index operations
      rows_add = []
      idx_delete = []

      place_fields = {'id', 'title', 'ccodes','start','end','attestation_year'}
      alldiffs=[]
      # bdfx=bdf.iloc[1:]
      # for index, row in bdfx.iterrows():
      for index, row in bdf.iterrows():
        # row=bdf.iloc[1]
        # new row as dict
        row = row.to_dict()
        print('row as dict', row)

        start = int(row['start']) if 'start' in row else int(row['attestation_year']) \
          if ('attestation_year' in row) else None
        end = int(row['end']) if 'end' in row and str(row['end']) != 'nan' else start
        minmax_new = [start, end] if start else [None]

        # extract coords from upload file
        row_coords = makeCoords(row['lon'], row['lat']) \
          if row['lon'] and row['lat'] else None
        if row['geowkt']:
          gtype = wkt.loads(row['geowkt']).type
          if 'Multi' not in gtype:
            row_coords = [list(u) for u in wkt.loads(row['geowkt']).coords]
          else:
            row_coords = [list(u) for u in wkt.loads(row['geowkt']).xy]
        # all columns in mew file
        header = list(bdf.keys())
        # row_mapper = [{k: row[k]} for k in header]
        row_mapper = {
          'src_id': row['id'],
          'title': row['title'],
          'minmax': minmax_new,
          'title_source': row['title_source'] if 'title_source' in header else '',
          'title_uri': row['title_uri'] if 'title_uri' in header else '',
          'ccodes': row['ccodes'].split(';') if 'ccodes' in header and row['ccodes'] else [],
          'matches': row['matches'].split(';') if 'matches' in header and row['matches']else [],
          'variants': row['variants'].split(';') if 'variants' in header and row['variants']else [],
          'types': row['types'].split(';') if 'types' in header and row['types'] else [],
          'aat_types': row['aat_types'].split(';') if 'aat_types' in header and row['aat_types'] else [],
          'parent_name': row['parent_name'] if 'parent_name' in header else '',
          'parent_id': row['parent_id'] if 'parent_id' in header else '',
          'geo_source': row['geo_source'] if 'geo_source' in header else '',
          'geo_id': row['geo_id'] if 'geo_id' in header else '',
          'description': row['description'] if 'description' in header else '',
          'coords': row_coords or [],
        }

        try:
          # is there corresponding current Place?
          p = ds_places.get(src_id=row['id'])
          # fetch existing API record
          c = Client()
          from datasets.utils import PlaceMapper
          try:
            # result = c.get('/api/place_compare/' + str(6873911) + '/')
            result = c.get('/api/place_compare/' + str(p.id) + '/')
            pobj = result.json()
            pobj = {key: val for key, val in sorted(pobj.items(), key=lambda ele: ele[0])}
          except:
            print('pobj failed', p.id, sys.exc_info())

          # build object for comparison
          # TODO: build separate serializer(s) for this? performance?
          p_mapper = PlaceMapper(
            pobj['id'],
            pobj['src_id'],
            pobj['title'],
          )

          # id,title,title_source,title_uri,ccodes,matches,variants,types,aat_types,
          # parent_name,parent_id,geo_source,geo_id,description
          # add key:value pairs to consider
          p_mapper['minmax'] = pobj['minmax']
          title_name = next(n for n in pobj['names'] if n['toponym'] == pobj['title']) or None
          p_mapper['title_source'] = title_name['citation']['label'] if \
            'citation' in title_name and 'label' in title_name['citation'] else ''
          p_mapper['title_id'] = title_name['citation']['id'] if \
            'citation' in title_name and 'id' in title_name['citation'] else ''
          p_mapper['ccodes'] = pobj['ccodes'] or []
          p_mapper['types'] = [t['sourceLabel'] for t in pobj['types']] or []
          p_mapper['aat_types'] = [t['identifier'][4:] for t in pobj['types']] or []
          p_mapper['variants'] = [n['toponym'] for n in pobj['names'] if n['toponym'] != pobj['title']] or []
          p_mapper['coords'] = [g['coordinates'] for g in pobj['geoms']] or []

          p_mapper['geo_sources'] = [g['citation']['label'] for g in pobj['geoms'] \
              if 'citation' in g and 'label' in g['citation']] or []
          p_mapper['geo_ids'] = [g['citation']['id'] for g in pobj['geoms'] \
              if 'citation' in g and 'id' in g['citation']]  or []

          p_mapper['links'] = [l['identifier'] for l in pobj['links']] or []
          p_mapper['related'] = [r['label'] for r in pobj['related']]
          p_mapper['related_id'] = [r['identifier'] for r in pobj['related']]
          p_mapper['descriptions'] = [d['value'] for d in pobj['related']]

          # diff incoming (row_mapper) & database (p_mapper)
          # meaningful = title, variants, aat_types, links/matches, coords
          diffs = []

          # [:8] not meaningful (don't affect reconciliation)
          diffs.append(row_mapper['title_source'] == p_mapper['title_source'] if row_mapper['title_source'] else True)
          diffs.append(row_mapper['title_uri'] == p_mapper['title_id'] if row_mapper['title_uri'] else True)
          diffs.append(row_mapper['parent_name'] in p_mapper['related'] if row_mapper['parent_name'] else True)
          diffs.append(row_mapper['parent_id'] in p_mapper['related_id'] if row_mapper['parent_id'] else True)
          diffs.append(row_mapper['geo_source'] in p_mapper['geo_sources'] if row_mapper['geo_source'] !='' else True)
          diffs.append(row_mapper['geo_id'] in p_mapper['geo_ids'] if row_mapper['geo_id'] !='' else True)
          diffs.append(row_mapper['description'] in p_mapper['descriptions'] if row_mapper['description'] else True)
          diffs.append(row_mapper['minmax'] == p_mapper['minmax'])
          diffs.append(sorted(row_mapper['types']) == sorted(p_mapper['types']))

          # [9:] meaningful
          diffs.append(row_mapper['title'] == p_mapper['title'])
          diffs.append(sorted(row_mapper['variants']) == sorted(p_mapper['variants']))
          diffs.append(sorted(row_mapper['aat_types']) == sorted(p_mapper['aat_types']))
          diffs.append(sorted(row_mapper['matches']) == sorted(p_mapper['links']))
          diffs.append(sorted(row_mapper['ccodes']) == sorted(p_mapper['ccodes']))
          if row_mapper['coords'] != []:
            diffs.append(row_mapper['coords'] == p_mapper['coords'])

          print('diffs', diffs)
          alldiffs.append({'title':row_mapper['title'], 'diffs':diffs})

          # update Place record in all cases
          count_replaced += 1
          p.title = row_mapper['title']
          p.ccodes = row_mapper['ccodes']
          p.minmax = minmax_new
          p.timespans = [minmax_new]

          if False in diffs:
            # there was SOME change(s) -> add to delete-from-index list
            # (will be reindexed after re-reconciling)
            idx_delete.append(p.id)
          if False not in diffs[9:]:
            # no meaningful changes
            # replace related, preserving geoms & links if keepg, keepl
            # leave review_wd and flag status intact
            delete_related(p)
            update_rels_tsv(p, row)
          else:
            # meaningful change(s) exist
            count_redo +=1
            # replace related, including geoms and links
            keepg, keepl = [False, False]
            delete_related(p)
            update_rels_tsv(p, row)

            # (re)set Place.review_wd & Place.flag (needs reconciliation)
            p.review_wd = None
            p.flag = True

            # meaningful change, so
            # add to list for index deletion
            if p.id not in idx_delete:
              idx_delete.append(p.id)

          p.save()
        except:
          # no corresponding Place, create new one
          print('new place record needed from rdp', row)
          count_new +=1
          newpl = Place.objects.create(
            src_id = row['id'],
            title = re.sub('\(.*?\)', '', row['title']),
            ccodes = [] if str(row['ccodes']) == 'nan' else row['ccodes'].replace(' ','').split(';'),
            dataset = ds,
            minmax = minmax_new,
            timespans = [minmax_new],
            # flax for reconciling
            flag = True
          )
          newpl.save()
          pobj = newpl
          rows_add.append(pobj.id)
          print('new place, related:', newpl)
          # add related rcords (PlaceName, PlaceType, etc.)
          update_rels_tsv(pobj, row)
        # except:
        #   print('update failed on ', row)
        #   print('error', sys.exc_info())

      # update numrows
      ds.numrows = ds.places.count()
      ds.save()

      # initiate a result object
      result = {"status": "updated", "format":file_format,
                "update_count":count_replaced, "redo_count": count_redo,
                "new_count":count_new, "deleted_count": len(rows_delete),
                "newfile": filepath}

      print('update result', result)
      print("compare_data['count_indexed']", compare_data['count_indexed'])

      #
      if compare_data['count_indexed'] > 0:
        result["indexed"] = True

        # surgically remove as req.
        # rows_delete(gone from db) + idx_delete(rows with meaningful change)
        idx_delete = rows_delete + idx_delete
        print('idx_delete', idx_delete)
        if len(idx_delete) > 0:
          es = settings.ES_CONN
          idx = 'whg'
          print('pids to delete from index:', idx_delete)
          removePlacesFromIndex(es, idx, idx_delete)
      else:
        print('not indexed, that is all')

      # write log entry
      Log.objects.create(
        # category, logtype, "timestamp", subtype, note, dataset_id, user_id
        category = 'dataset',
        logtype = 'ds_update',
        note = json.dumps(compare_result),
        dataset_id = dsid,
        # user_id = 1
        user_id = request.user.id
      )
      ds.ds_status = 'updated'
      ds.save()
      # return to update modal
      return JsonResponse(result, safe=False)
    elif file_format == 'lpf':
      print("ds_update for lpf; doesn't get here yet")

"""
  ds_compare() refactored 26 Nov 2022 (backup below)
  validates updated dataset file & compares w/existing
  called by ajax function from modal in ds_summary.html
  returns json result object 'comparison' for use by ds_update()
"""
def ds_compare(request):
  if request.method == 'POST':
    print('ds_compare() request.POST', request.POST)
    print('ds_compare() request.FILES', request.FILES)
    dsid = request.POST['dsid']
    user = request.user.username
    format = request.POST['format']
    ds = get_object_or_404(Dataset, id=dsid)
    ds_status = ds.ds_status

    # get most recent file
    file_cur = ds.files.all().order_by('-rev')[0].file
    file_cur_delimiter = ds.files.all().order_by('-rev')[0].delimiter
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

    print('tempfn,filename_cur,file_new.name',tempfn, filename_cur, file_new.name)

    # validate new file (tempfn is file path)
    # if errors, stop and return them to modal
    if format == 'delimited':
      print('format:', format)
      try:
        vresult = validate_tsv(tempfn, 'delimited')
      except:
        print('validate_tsv() failed:', sys.exc_info())

    elif format == 'lpf':
      # TODO: feed tempfn only?
      # TODO: accept json-lines; only FeatureCollections ('coll') now
      vresult = validate_lpf(tempfn,'coll')
      # print('format, vresult:',format,vresult)

    # which expects {validation_result{errors['','']}}
    print('vresult', vresult)
    if len(vresult['errors']) > 0:
      errormsg = {"failed":{
        "errors":vresult['errors']
      }}
      return JsonResponse(errormsg,safe=False)

    # give new file a path
    filename_new = 'user_'+user+'/'+file_new.name
    # temp files were given extensions in validation functions
    tempfn_new = tempfn+'.tsv' if format == 'delimited' else tempfn+'.jsonld'
    print('tempfn_new', tempfn_new)

    # begin comparison report
    comparison={
      "id": dsid,
      "filename_cur": filename_cur,
      "filename_new": filename_new,
      "format": format,
      "validation_result": vresult,
      "tempfn": tempfn,
      "count_indexed": ds.status_idx['idxcount'],
      'count_links_added': ds.links.filter(task_id__isnull=False).count(),
      'count_geoms_added': ds.geometries.filter(task_id__isnull=False).count()
    }

    # create pandas (pd) objects, then perform comparison
    # a = existing, b = new
    fn_a = 'media/'+filename_cur
    fn_b = tempfn
    if format == 'delimited':
      adf = pd.read_csv(fn_a,
                        # delimiter='\t',
                        delimiter=file_cur_delimiter,
                        dtype={'id': 'str', 'aat_types': 'str'})
      try:
        # must have same delimiter as original
        bdf = pd.read_csv(fn_b, delimiter=file_cur_delimiter)
        # bdf = pd.read_csv(fn_b, delimiter='\t')
      except:
        print('bdf read failed', sys.exc_info())

      ids_a = adf['id'].tolist()
      ids_b = bdf['id'].tolist()
      print('ids_a, ids_b', ids_a[:10], ids_b[:10])
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
      # print('need to compare lpf files:',fn_a,fn_b)
      comparison['compare_result'] = "it's lpf...tougher row to hoe"

    print('comparison (compare_data)',comparison)
    # back to calling modal
    return JsonResponse(comparison,safe=False)

""" 
  ds_insert_lpf
  insert LPF into database
"""
def ds_insert_lpf(request, pk):
  import json
  [countrows,countlinked,total_links]= [0,0,0]
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  # latest file
  dsf = ds.files.all().order_by('-rev')[0]
  uribase = ds.uri_base
  print('new dataset, uri_base', ds.label, uribase)

  # TODO: lpf can get big; support json-lines

  # insert only if empty
  dbcount = Place.objects.filter(dataset = ds.label).count()
  print('dbcount',dbcount)

  if dbcount == 0:
    errors=[]
    try:
      infile = dsf.file.open(mode="r")
      print('ds_insert_lpf() for dataset',ds)
      print('ds_insert_lpf() request.GET, infile',request.GET,infile)
      with infile:
        jdata = json.loads(infile.read())

        print('count of features',len(jdata['features']))

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
              print('ccodes', ccodes)
            else:
              ccodes = []
          else:
            ccodes = feat['properties']['ccodes']

          # temporal
          # send entire feat for time summary
          # (minmax and intervals[])
          datesobj=parsedates_lpf(feat)

          # TODO: compute fclasses
          try:
            newpl = Place(
              # strip uribase from @id
              src_id=feat['@id'] if uribase in ['', None] else feat['@id'].replace(uribase,''),
              dataset=ds,
              title=title,
              ccodes=ccodes,
              minmax = datesobj['minmax'],
              timespans = datesobj['intervals']
            )
            newpl.save()
            print('new place: ',newpl.title)
          except:
            print('failed id' + title + 'datesobj: '+str(datesobj))
            print(sys.exc_info())

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
              print('PlaceType record newpl,newpl.src_id,t,fc',newpl,newpl.src_id,t,fc)
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
              jsonb=feat['when'],
              minmax=newpl.minmax
            ))

          # PlaceGeom: place,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
          #if 'geometry' in feat.keys() and feat['geometry']['type']=='GeometryCollection':
          if geojson and geojson['type']=='GeometryCollection':
            #for g in feat['geometry']['geometries']:
            for g in geojson['geometries']:
              # print('from feat[geometry]:',g)
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

          # throw errors into user message
          def raiser(model, e):
            print('Bulk load for '+ model + ' failed on', newpl)
            errors.append({"field":model, "error":e})
            print('error', e)
            raise DataError

          # create related objects
          try:
            PlaceName.objects.bulk_create(objs['PlaceNames'])
          except DataError as e:
            raiser('Name', e)

          try:
            PlaceType.objects.bulk_create(objs['PlaceTypes'])
          except DataError as de:
            raiser('Type', e)

          try:
            PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
          except DataError as de:
            raiser('When', e)

          try:
            PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
          except DataError as de:
            raiser('Geom', e)

          try:
            PlaceLink.objects.bulk_create(objs['PlaceLinks'])
          except DataError as de:
            raiser('Link', e)

          try:
            PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
          except DataError as de:
            raiser('Related', e)

          try:
            PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
          except DataError as de:
            raiser('Description', e)

          try:
            PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])
          except DataError as de:
            raiser('Depiction', e)

          # TODO: compute newpl.ccodes (if geom), newpl.fclasses, newpl.minmax
          # something failed in *any* Place creation; delete dataset

        print('new dataset:', ds.__dict__)
        infile.close()

      return({"numrows":countrows,
              "numlinked":countlinked,
              "total_links":total_links})
    except:
      # drop the (empty) database
      # ds.delete()
      # email to user, admin
      subj = 'World Historical Gazetteer error followup'
      msg = 'Hello '+ user.username+', \n\nWe see your recent upload for the '+ds.label+\
            ' dataset failed, very sorry about that!'+\
            '\nThe likely cause was: '+str(errors)+'\n\n'+\
            "If you can, fix the cause. If not, please respond to this email and we will get back to you soon.\n\nRegards,\nThe WHG Team"
      emailer(subj,msg,'whg@pitt.edu',[user.email, 'whgadmin@kgeographer.com'])

      # return message to 500.html
      # messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      # return redirect(request.GET.get('from'))
      return HttpResponseServerError()

  else:
    print('insert_ skipped, already in')
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/mydata')

"""
  ds_insert_tsv(pk)
  insert tsv into database
  file is validated, dataset exists
  if insert fails anywhere, delete dataset + any related objects
"""
def ds_insert_tsv(request, pk):
  import csv, re
  csv.field_size_limit(300000)
  ds = get_object_or_404(Dataset, id=pk)
  user = request.user
  # retrieve just-added file
  dsf = ds.files.all().order_by('-rev')[0]
  print('ds_insert_tsv(): ds, file', ds, dsf)
  # insert only if empty
  dbcount = Place.objects.filter(dataset = ds.label).count()
  # print('dbcount',dbcount)
  insert_errors = []
  if dbcount == 0:
    try:
      infile = dsf.file.open(mode="r")
      reader = csv.reader(infile, delimiter=dsf.delimiter)
      # reader = csv.reader(infile, delimiter='\t')

      infile.seek(0)
      header = next(reader, None)
      header = [col.lower().strip() for col in header]
      # print('header.lower()',[col.lower() for col in header])

      # strip BOM character if exists
      header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
      # print('header', header)

      objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
              "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}

      # TODO: what if simultaneous inserts?
      countrows=0
      countlinked = 0
      total_links = 0
      for r in reader:
        # build attributes for new Place instance
        src_id = r[header.index('id')]
        title = r[header.index('title')].strip() # don't try to correct incoming except strip()
        # title = r[header.index('title')].replace("' ","'") # why?
        # strip anything in parens for title only
        # title = re.sub('\(.*?\)', '', title)
        title_source = r[header.index('title_source')]
        title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
        ccodes = r[header.index('ccodes')] if 'ccodes' in header else []
        variants = [x.strip() for x in r[header.index('variants')].split(';')] \
          if 'variants' in header and r[header.index('variants')] !='' else []
        types = [x.strip() for x in r[header.index('types')].split(';')] \
          if 'types' in header else []
        aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
          if 'aat_types' in header else []
        print('aat_types', aat_types)
        parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
        parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
        coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
          if 'lon' in header and 'lat' in header else None
        geowkt = r[header.index('geowkt')] if 'geowkt' in header else None
        # replaced None with '' 25 May 2023
        geosource = r[header.index('geo_source')] if 'geo_source' in header else ''
        geoid = r[header.index('geo_id')] if 'geo_id' in header else None
        geojson = None # zero it out
        # print('geosource:', geosource)
        # print('geoid:', geoid)
        # make Point geometry from lon/lat if there
        if coords and len(coords) == 2:
          geojson = {"type": "Point", "coordinates": coords,
                      "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
        # else make geometry (any) w/Shapely if geowkt
        if geowkt and geowkt not in ['']:
          geojson = parse_wkt(geowkt)
        if geojson and (geosource or geoid):
          geojson['citation']={'label':geosource,'id':geoid}

        # ccodes; compute if missing and there is geometry
        if len(ccodes) == 0:
          if geojson:
            try:
              ccodes = ccodesFromGeom(geojson)
            except:
              pass
          else:
            ccodes = []
        else:
          ccodes = [x.strip().upper() for x in r[header.index('ccodes')].split(';')]

        # TODO: assign aliases if wd, tgn, pl, bnf, gn, viaf
        matches = [aliasIt(x.strip()) for x in r[header.index('matches')].split(';')] \
          if 'matches' in header and r[header.index('matches')] != '' else []

        # TODO: patched Apr 2023; needs refactor
        # there _should_ always be a start or attestation_year
        # not forced by validation yet
        # start = r[header.index('start')] if 'start' in header else None
        start = r[header.index('start')] if 'start' in header else None
        # source_year = r[header.index('attestation_year')] if 'attestation_year' in header else None
        has_end = 'end' in header and r[header.index('end')] !=''
        has_source_yr = 'attestation_year' in header and r[header.index('attestation_year')] !=''
        end = r[header.index('end')] if has_end else None
        source_year = r[header.index('attestation_year')] if has_source_yr else None
        # end = r[header.index('end')] if has_end else start
        # print('row r' , r)
        # print('start:'+start,'; end:'+end, ';year'+source_year)
        dates = (start,end,source_year)
        # print('dates tuple', dates)
        # must be start and/or source_year
        datesobj = parsedates_tsv(dates)
        # returns, e.g. {'timespans': [{'start': {'earliest': 1015}, 'end': None}],
        #  'minmax': [1015, None],
        #  'source_year': 1962}
        description = r[header.index('description')] \
          if 'description' in header else ''

        # print('title, src_id (pre-newpl):', title, src_id)
        # print('datesobj', datesobj)
        # create new Place object
        newpl = Place(
          src_id = src_id,
          dataset = ds,
          title = title,
          ccodes = ccodes,
          minmax = datesobj['minmax'],
          timespans = [datesobj['minmax']], # list of lists
          attestation_year = datesobj['source_year'] # integer or None
        )
        newpl.save()
        countrows += 1

        # build associated objects and add to arrays #
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
                # print(v.strip())
                pass
              else:
                # print('variant for', newpl.id, v)
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
            print('aatnum in insert_tsv() PlaceTypes', aatnum)
            if aatnum:
              try:
                fclass_list.append(get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass)
              except:
                # report type lookup issue
                insert_errors.append(
                  {'src_id':src_id,
                  'title':newpl.title,
                  'field':'aat_type',
                  'msg':aatnum + ' not in WHG-supported list;'}
                )
                raise
                # messages.add_message(request, messages.INFO, 'choked on an invalid AAT place type id')
                # return redirect('/mydata')
                # continue
            objs['PlaceType'].append(
              PlaceType(
                place=newpl,
                src_id = src_id,
                jsonb={ "identifier":aatnum if aatnum else '',
                        "sourceLabel":t,
                        "label":aat_lookup(int(aatnum[4:])) if aatnum else ''
                      }
            ))
          newpl.fclasses = fclass_list
          newpl.save()

        #
        # PlaceGeom()
        #
        print('geojson', geojson)
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
        # if not start in ('',None):
        # if start != '':
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
      try:
        PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
      except:
        print('geom insert failed', newpl, sys.exc_info())
        pass
      PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
      PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
      PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
      PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)

      infile.close()
      print('insert_errors', insert_errors)
      # print('rows,linked,links:', countrows, countlinked, total_links)
    except:
      print('tsv insert failed', sys.exc_info())
      # drop the (empty) dataset if insert wasn't complete
      ds.delete()
      # email to user, admin
      failed_upload_notification(user, dsf.file.name, ds.label)

      # return message to 500.html
      messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
      return HttpResponseServerError()
  else:
    print('insert_tsv skipped, already in')
    messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
    return redirect('/mydata')

  return({"numrows":countrows,
          "numlinked":countlinked,
          "total_links":total_links})

"""
  DataListsView()
  Returns lists for various data types
"""
class DataListsView(LoginRequiredMixin, ListView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  # templates per list type
  template_d = 'datasets/data_datasets.html'
  template_c = 'datasets/data_collections.html'
  template_a = 'datasets/data_areas.html'
  template_r = 'datasets/data_resources.html'

  # which template to use?
  def get_template_names(self, *args, **kwargs):
    print('self.request.path', self.request.path)
    if self.request.path == reverse('data-datasets'):
      return [self.template_d]
    elif self.request.path == reverse('data-collections'):
      return [self.template_c]
    elif self.request.path == reverse('data-areas'):
      return [self.template_a]
    else:
      return [self.template_r]

# refactored these 3 2 Oct 2023
  def get_queryset(self, **kwargs):
    me = self.request.user

    if self.request.path == reverse('data-datasets') or self.request.path == reverse('data-collections'):
      return self._get_data_or_collections_queryset(me)
    elif self.request.path in [reverse('data-areas'), reverse('data-resources')]:
      return self._get_areas_or_resources_queryset(me)

  def _get_data_or_collections_queryset(self, user):
    is_whg_team_member = user.groups.filter(name='whg_team').exists()

    if is_whg_team_member:
      if self.request.path == reverse('data-datasets'):
        return Dataset.objects.all().order_by('-create_date')
      else:
        return Collection.objects.all().order_by('-created')
    else:
      filter_conditions = Q(owner=user) | Q(collabs__user_id=user.id)
      if self.request.path == reverse('data-datasets'):
        return Dataset.objects.filter(filter_conditions).distinct().order_by('-create_date')
      else:
        return Collection.objects.filter(filter_conditions).distinct().order_by('-created')

  def _get_areas_or_resources_queryset(self, user):
    is_whg_team_member = user.groups.filter(name='whg_team').exists()

    if self.request.path == reverse('data-areas'):
      queryset = Area.objects
    else:
      queryset = Resource.objects

    if is_whg_team_member:
      if self.request.path == reverse('data-areas'):
        return queryset.all().distinct().order_by('-created')
      else:
        return queryset.all().distinct().order_by('-create_date')
    else:
      return queryset.filter(owner=user).distinct().order_by('-created')

  def get_context_data(self, *args, **kwargs):
    me = self.request.user
    context = super(DataListsView, self).get_context_data(*args, **kwargs)
    # print('in get_context', me)
    context['viewable'] = ['uploaded', 'inserted', 'reconciling', 'review_hits', 'reviewed', 'review_whg', 'indexed']
    context['beta_or_better'] = True if me.groups.filter(name__in=['beta', 'admins', 'whg_team']).exists() \
      else False
    context['whgteam'] = True if me.groups.filter(name__in=['admins', 'whg_team']).exists() else False
    # TODO: assign users to 'teacher' group
    context['teacher'] = True if self.request.user.groups.filter(name__in=['teacher']).exists() else False
    return context

"""
  PublicListView()
  list public datasets and collections
"""
class PublicListsView(ListView):
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
    context['coll_list'] = Collection.objects.filter(status='published').order_by('created')
    context['viewable'] = ['uploaded','inserted','reconciling','review_hits','reviewed','review_whg','indexed']

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    return context

def failed_upload_notification(user, fn, ds=None):
    subj = 'World Historical Gazetteer error followup '
    subj += 'on dataset ('+ds+')' if ds else ''
    msg = 'Hello ' + user.username + \
      ', \n\nWe see your recent upload was not successful '
    if ds:
      msg += 'on insert to the database '
    else:
      msg += 'on initial validation '
    msg +='-- very sorry about that! ' + \
      '\nWe will look into why and get back to you within a day.\n\nRegards,\nThe WHG Team\n\n\n['+fn+']'
    emailer(subj, msg, settings.DEFAULT_FROM_EMAIL, [user.email, settings.EMAIL_HOST_USER])

"""
  DatasetCreateView()
  initial create
  upload file, validate format, create DatasetFile instance,
  redirect to dataset.html for db insert if context['format_ok']
"""
class DatasetCreateEmptyView (LoginRequiredMixin, CreateView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  form_class = DatasetCreateEmptyModelForm
  template_name = 'datasets/dataset_create_empty.html'
  success_message = 'empty dataset created'

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    data=form.cleaned_data
    print('cleaned_data',data)
    context={"format": "empty"}
    # context={"format":data['format']}
    user=self.request.user
    # validated -> create Dataset, DatasetFile, Log instances,
    # advance to dataset_detail
    # else present form again with errors
    # if len(result['errors']) == 0:
    context['status'] = 'format_ok'

    # print('validated, no errors')
    # print('validated, no errors; result:', result)
    nolabel = form.cleaned_data["label"] == ''
    # new Dataset record ('owner','id','label','title','description')
    dsobj = form.save(commit=False)
    dsobj.ds_status = 'format_ok'
    dsobj.numrows = 0
    clean_label=form.cleaned_data['label'].replace(' ','_')
    if not form.cleaned_data['uri_base']:
      dsobj.uri_base = 'https://whgazetteer.org/api/db/?id='

    # links will be counted later on insert
    dsobj.numlinked = 0
    dsobj.total_links = 0
    try:
      dsobj.save()
      ds = Dataset.objects.get(id=dsobj.id)
      label='ds_' + str(ds.id)
      print('new dataset label', 'ds_' + label)
      # generate a unique label if none was entered
      if dsobj.label == '':
        ds.label = 'ds_' + str(dsobj.id)
        ds.save()
    except:
      # self.args['form'] = form
      return render(self.request,'datasets/dataset_create.html', self.args)

    #
    # create user directory if necessary
    userdir = r'media/user_'+user.username+'/'
    if not Path(userdir).exists():
      os.makedirs(userdir)

    # build path, and rename file if already exists in user area
    # file_exists = Path(userdir+filename).exists()
    # if not file_exists:
    #   filepath = userdir+filename
    # else:
    #   splitty = filename.split('.')
    #   filename=splitty[0]+'_'+tempfn[-7:]+'.'+splitty[1]
    #   filepath = userdir+filename

    # write log entry
    Log.objects.create(
      # category, logtype, "timestamp", subtype, dataset_id, user_id
      category = 'dataset',
      logtype = 'ds_create_empty',
      subtype = data['datatype'],
      dataset_id = dsobj.id,
      user_id = user.id
    )

    # create initial DatasetFile record
    DatasetFile.objects.create(
      dataset_id = dsobj,
      file = 'dummy_file.txt',
      rev = 1,
      format = 'delimited',
      delimiter = 'n/a',
      df_status = 'dummy',
      upload_date = None,
      header = [],
      numrows = 0
    )


    # data will be written on load of dataset.html w/dsobj.status = 'format_ok'
    #return redirect('/datasets/'+str(dsobj.id)+'/detail')
    return redirect('/datasets/'+str(dsobj.id)+'/summary')

  # else:
    context['action'] = 'errors'
    # context['format'] = result['format']
    # context['errors'] = parse_errors_lpf(result['errors']) \
    #   if ext == 'json' else parse_errors_tsv(result['errors'])
    # context['columns'] = result['columns'] \
    #   if ext != 'json' else []

    #os.remove(tempfn)

    return self.render_to_response(
      self.get_context_data(
        form=form, context=context
    ))

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCreateEmptyView, self).get_context_data(*args, **kwargs)
    #context['action'] = 'create'
    return context

"""
  DatasetCreateView()
  initial create
  upload file, validate format, create DatasetFile instance,
  redirect to dataset.html for db insert if context['format_ok']
"""
class DatasetCreateView(LoginRequiredMixin, CreateView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  form_class = DatasetCreateModelForm
  template_name = 'datasets/dataset_create.html'
  success_message = 'dataset created'

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    data=form.cleaned_data
    context={"format":data['format']}
    user=self.request.user
    file=self.request.FILES['file']
    filename = file.name
    mimetype = file.content_type

    newfn, newtempfn = ['', '']
    print('form_valid() mimetype', mimetype)


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

    # print('tempfn in DatasetCreate()', tempfn)

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
      if encoding.lower() not in ['utf-8', 'ascii']:
        context['errors'] = ["The encoding of uploaded files must be unicode (utf-8). This file seems to be "+encoding]
        context['action'] = 'errors'
        return self.render_to_response(self.get_context_data(form=form, context=context))
    else:
      context['errors'] = "Not a valid file type; must be one of [.csv, .tsv, .xlsx, .ods, .json]"
      return self.render_to_response(self.get_context_data(form=form, context=context))

    # it's csv, tsv, spreadsheet, or json...
    # if utf8, get extension and validate
    # if encoding and encoding.lower().startswith('utf-8'):
    ext = mthash_plus.mimetypes[mimetype]
    print('DatasetCreateView() extension:', ext)
    fail_msg = "A database insert failed and we aren't sure why. The WHG team has been notified "+\
               "and will follow up by email to <b>"+user.username+"</b> ("+user.email+")"

    # this validates per row and always gets a result, even if errors
    if ext == 'json':
      try:
        result = validate_lpf(tempfn, 'coll')
      except:
        # email to user, admin
        failed_upload_notification(user, tempfn)
        # return message to 500.html
        messages.error(self.request, fail_msg)
        return HttpResponseServerError()

    # for delimited, fvalidate() is performed on the entire file
    # on fail, raises server error
    elif ext in ['csv', 'tsv']:
      try:
        # fvalidate() wants an extension
        newfn = tempfn+'.'+ext
        os.rename(tempfn, newfn)
        result = validate_tsv(newfn, ext)
        print('newfn in create()', newfn)
      except:
        # email to user, admin
        failed_upload_notification(user, tempfn)
        messages.error(self.request, fail_msg)
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
        # print('renamed tempfn for pandas:', tempfn)

        # dataframe from spreadsheet
        df = pd.read_excel(newtempfn, converters={
          'id': str, 'start':str, 'end':str,
          'aat_types': str, 'lon': float, 'lat': float})

        # write it as tsv
        table=df.to_csv(sep='\t', index=False).replace('\nan','')
        fout.write(table)
        fout.close()

        # print('to validate_tsv(newfn):', newfn)
        # validate it...
        result = validate_tsv(newfn, 'tsv')
      except:
        # email to user, admin
        failed_upload_notification(user, newfn)
        messages.error(self.request, "Database insert failed and we aren't sure why. "+
                       "The WHG team has been notified and will follow up by email to <b>" +
                       user.username+'</b> ('+user.email+')')
        return HttpResponseServerError()

    print('validation complete, still in DatasetCreateView')

    # validated -> create Dataset, DatasetFile, Log instances,
    # advance to dataset_detail
    # else present form again with errors
    if len(result['errors']) == 0:
      context['status'] = 'format_ok'

      print('validated, no errors')
      # print('validated, no errors; result:', result)
      print('cleaned_data',form.cleaned_data)
      nolabel = form.cleaned_data["label"] == ''
      # new Dataset record ('owner','id','label','title','description')
      dsobj = form.save(commit=False)
      dsobj.ds_status = 'format_ok'
      dsobj.numrows = result['count']
      clean_label=form.cleaned_data['label'].replace(' ','_')
      if not form.cleaned_data['uri_base']:
        dsobj.uri_base = 'https://whgazetteer.org/api/db/?id='

      # links will be counted later on insert
      dsobj.numlinked = 0
      dsobj.total_links = 0
      try:
        dsobj.save()
        ds = Dataset.objects.get(id=dsobj.id)
        label='ds_' + str(ds.id)
        print('new dataset label', 'ds_' + label)
        # generate a unique label if none was entered
        if dsobj.label == '':
          ds.label = 'ds_' + str(dsobj.id)
          ds.save()
      except:
        # self.args['form'] = form
        return render(self.request,'datasets/dataset_create.html', self.args)

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

      # print('pre-write')
      # print('ext='+ext+'; newfn='+newfn+'; filepath='+filepath+
      #       '; tempfn='+tempfn+'; newtempfn='+newtempfn)

      # write request obj file to user directory
      if ext in ['csv', 'tsv', 'json']:
        fout = codecs.open(filepath,'w','utf8')
        try:
          for chunk in file.chunks():
            fout.write(chunk.decode("utf-8", errors="ignore"))
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
      # email to user, admin
      # emailer('WHG: dataset upload successful', 'Hello '+user.username+',\n\nYour dataset upload was successful. ')
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

"""
  returns public dataset 'mets' (summary) page
"""
class DatasetPublicView(DetailView):
  template_name = 'datasets/ds_meta.html'

  model = Dataset

  def get_context_data(self, **kwargs):
    context = super(DatasetPublicView, self).get_context_data(**kwargs)
    print('self, kwargs',self, self.kwargs)

    ds = get_object_or_404(Dataset, id = self.kwargs['pk'])
    file = ds.file

    placeset = ds.places.all()

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
  loads page for confirm ok on delete
    - delete dataset, with CASCADE to DatasetFile, places, place_name, etc
    - also deletes from index if indexed (fails silently if not)
    - also removes dataset_file records
"""
# TODO: delete other stuff: disk files; archive??
class DatasetDeleteView(DeleteView):
  template_name = 'datasets/dataset_delete.html'

  def delete_complete(self):
    ds=get_object_or_404(Dataset,pk=self.kwargs.get("id"))
    dataset_file_delete(ds)
    if ds.ds_status == 'indexed':
      pids=list(ds.placeids)
      removePlacesFromIndex(es, 'whg', pids)

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
    return reverse('data-datasets')

"""
  fetch places in specified dataset
  utility used for place collections
"""
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
  - delete any geoms or links created
  - reset flags for hit.reviewed and place.review_xxx
"""
def match_undo(request, ds, tid, pid):
  print('in match_undo() ds, task, pid:', ds, tid, pid)
  from django_celery_results.models import TaskResult
  geom_matches = PlaceGeom.objects.filter(task_id=tid, place_id=pid)
  link_matches = PlaceLink.objects.filter(task_id=tid, place_id=pid)
  geom_matches.delete()
  link_matches.delete()

  # reset place.review_xxx to 0
  tasktype = TaskResult.objects.get(task_id=tid).task_name[6:]
  print('tasktype', tasktype)
  place = Place.objects.get(pk=pid)
  # remove any defer comments
  place.defer_comments.delete()
  # TODO: variable field name?
  if tasktype.startswith('wd'):
    place.review_wd = 0
  elif tasktype == 'tgn':
    place.review_tgn = 0
  else:
    place.review_whg = 0
  place.save()

  # match task_id, place_id in hits; set reviewed = false
  Hit.objects.filter(task_id=tid, place_id=pid).update(reviewed=False)

  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

"""
  returns dataset owner metadata page
"""
class DatasetSummaryView(LoginRequiredMixin, UpdateView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  form_class = DatasetDetailModelForm

  template_name = 'datasets/ds_summary.html'

  # Dataset has been edited, form submitted
  def form_valid(self, form):
    data=form.cleaned_data
    ds = get_object_or_404(Dataset,pk=self.kwargs.get("id"))
    dsid = ds.id
    user = self.request.user
    file=data['file']
    filerev = ds.files.all().order_by('-rev')[0].rev
    # print('DatasetSummaryView kwargs',self.kwargs)
    print('DatasetSummaryView form_valid() data->', data)
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
    print('form not valid; errors:', form.errors)
    print('cleaned_data', form.cleaned_data)
    return super().form_invalid(form)

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetSummaryView, self).get_context_data(*args, **kwargs)

    # print('DatasetSummaryView get_context_data() kwargs:',self.kwargs)
    # print('DatasetSummaryView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)

    """
      when coming from DatasetCreateView() (file.df_status == format_ok)
      runs ds_insert_tsv() or ds_insert_lpf()
      using most recent dataset file
    """
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
      ds.numlinked = result['numlinked']
      ds.total_links = result['total_links']
      ds.ds_status = 'uploaded'
      file.df_status = 'uploaded'
      file.numrows = result['numrows']
      ds.save()
      file.save()

    # build context for rendering ds_summary.html
    me = self.request.user
    placeset = ds.places.all()

    context['updates'] = {}
    context['ds'] = ds
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

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

    # print('context from DatasetSummaryView', context)
    return context

""" 
  returns dataset owner browse table 
"""
class DatasetBrowseView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_browse.html'

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
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    print('DatasetBrowseView get_context_data() kwargs:',self.kwargs)
    print('DatasetBrowseView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")

    ds = get_object_or_404(Dataset, id=id_)
    me = self.request.user
    ds_tasks = [t.task_name[6:] for t in ds.tasks.filter(status='SUCCESS')]

    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners
    context['updates'] = {}
    context['ds'] = ds
    context['tgntask'] = 'tgn' in ds_tasks
    context['whgtask'] = len(set(['whg','idx']) & set(ds_tasks)) > 0
    context['wdtask'] = len(set(['wd','wdlocal']) & set(ds_tasks)) > 0
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

""" 
  returns public dataset browse table 
"""
class DatasetPlacesView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Dataset
  template_name = 'datasets/ds_places.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Dataset, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetPlacesView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    print('DatasetPlacesView get_context_data() kwargs:',self.kwargs)
    print('DatasetPlacesView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")

    ds = get_object_or_404(Dataset, id=id_)
    me = self.request.user

    if not me.is_anonymous:
      context['collections'] = Collection.objects.filter(owner=me, collection_class='place')

    context['loggedin'] = 'true' if not me.is_anonymous else 'false'

    context['updates'] = {}
    context['ds'] = ds
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

"""
  returns dataset owner "Linking" tab listing reconciliation tasks
"""
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

    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)

    # build context for rendering dataset.html
    me = self.request.user

    # omits FAILURE and ARCHIVED
    ds_tasks = ds.tasks.filter(status='SUCCESS')

    context['ds'] = ds
    context['tasks'] = ds_tasks
    context['owners'] = ds.owners

    context['editorial'] = True if me.groups.filter(name__in=['editorial']).exists() else False
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'whg_team']).exists() else False

    return context

""" 
  returns add (reconciliation) task page 
"""
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
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    id_ = self.kwargs.get("id")
    ds = get_object_or_404(Dataset, id=id_)

    # build context for rendering ds_addtask.html
    me = self.request.user
    area_types=['ccodes','copied','drawn']

    # user study areas
    userareas = Area.objects.filter(type__in=area_types).values('id','title').order_by('-created')
    context['area_list'] = userareas if me.username == 'whgadmin' else userareas.filter(owner=me)

    # user datasets
    # userdatasets = Dataset.objects.filter(owner=me).values('id','title').order_by('-created')
    context['ds_list'] = Dataset.objects.filter(owner=me).values('id','title').order_by('-create_date')
    # context['ds_list'] = Dataset.objects.filter(owner=me, ds_status='indexed').values('id','title').order_by('-create_date')
    # context['ds_list'] = userdatasets if me.username == 'whgadmin' else userdatasets.filter(owner=me)

    # user dataset collections
    # usercollections = Collection.objects.filter(type__in=area_types).values('id','title').order_by('-created')
    idlist = [obj.id for obj in Collection.objects.all() if me in obj.owners or
              me in obj.collaborators]
    context['coll_list'] = Collection.objects.filter(id__in=idlist).order_by("-id")
    # context['coll_list'] = Collection.objects.filter(owner=me, collection_class='dataset').values('id','title').order_by('-created')
    # context['coll_list'] = usercollections if me.username == 'whgadmin' else usercollections.filter(owner=me)

    # pre-defined UN regions
    predefined = Area.objects.all().filter(type='predefined').values('id','title')

    gothits={}
    for t in ds.tasks.filter(status='SUCCESS'):
      gothits[t.task_id] = int(json.loads(t.result)['got_hits'])

    # deliver status message(s) to template
    msg_unreviewed = """There is a <span class='strong'>%s</span> task in progress, 
      and all %s records that got hits remain unreviewed. <span class='text-danger strong'>Starting this new task 
      will delete the existing one</span>, with no impact on your dataset."""
    msg_inprogress = """<p class='mb-1'>There is a <span class='strong'>%s</span> task in progress, 
      and %s of the %s records that had hits have been reviewed. <span class='text-danger strong'>Starting this new task 
      will archive the existing task and submit only unreviewed records.</span>. 
      If you proceed, you can keep or delete prior match results (links and/or geometry):</p>"""
    msg_updating = """This dataset has been updated, <span class='strong'>Starting this new task 
      will archive the previous task and re-submit all new and altered records. If you proceed, you can keep or delete prior 
      matching results (links and geometry)</span>. <a href="%s">Questions? Contact our editorial team.</a>"""
    msg_done = """All records have been submitted for reconciliation to %s and reviewed. 
      To begin the step of accessioning to the WHG index, please <a href="%s">contact our editorial team.</a>"""
    for i in ds.taskstats.items():
      auth = i[0][6:]
      if len(i[1]) > 0: # there's a SUCCESS task
        tid = i[1][0]['tid']
        remaining = i[1][0]['total']
        hadhits = gothits[tid]
        reviewed = hadhits-remaining
        print('auth, tid, remaining, hadhits', auth, tid, remaining, hadhits)
        if remaining == 0 and ds.ds_status != 'updated':
        # if remaining == 0:
          context['msg_'+auth] = {
            'msg': msg_done%(auth,"/contact"),
            'type': 'done'}
        elif remaining < hadhits and ds.ds_status != 'updated':
          context['msg_'+auth] = {
            'msg': msg_inprogress%(auth, reviewed, hadhits),
            'type': 'inprogress'}
        elif ds.ds_status == 'updated':
          context['msg_'+auth] = {
            'msg': msg_updating%("/contact"),
            # 'msg': msg_updating%(auth, reviewed, hadhits),
            'type': 'inprogress'}
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
    context['remain_to_review'] = remaining

    context['owners'] = ds.owners
    context['collaborators'] = ds.collabs.all()
    context['whgteam'] = True if self.request.user.groups.filter(name__in=['whg_team']).exists() else False
    context['editorial'] = True if self.request.user.groups.filter(name__in=['editorial']).exists() else False
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'whg_team']).exists() else False

    return context

"""
  returns dataset owner "Collaborators" tab
"""
class DatasetCollabView(LoginRequiredMixin, DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = DatasetUser
  template_name = 'datasets/ds_collab.html'

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
    ds = get_object_or_404(Dataset, id=id_)

    # build context for rendering dataset.html
    me = self.request.user

    context['ds'] = ds

    context['collabs'] = ds.collabs.all()
    context['collaborators'] = ds.collaborators.all()
    context['owners'] = ds.owners

    context['editorial'] = True if me.groups.filter(name__in=['editorial']).exists() else False
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

"""
  returns dataset owner "Log & Comments" tab
"""
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

    # commented_places=
    me = self.request.user
    # context['notes']=[{"pid": p.id, "title": p.title, "note": c.note, "tag": c.tag, "date": c.created} for c in p.comment_set.all()]
    context['ds'] = ds
    context['log'] = ds.log.filter(category='dataset').order_by('-timestamp')
    # context['comments'] =
    context['comments'] = Comment.objects.filter(place_id__dataset=ds).order_by('-created')
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context


"""
  ds_compare_bak() backup copy 5 Dec 2022; pre-refactor
"""
def ds_compare_bak(request):
  if request.method == 'POST':
    print('ds_compare() request.POST', request.POST)
    print('ds_compare() request.FILES', request.FILES)
    dsid = request.POST['dsid']
    user = request.user.username
    format = request.POST['format']
    ds = get_object_or_404(Dataset, id=dsid)
    ds_status = ds.ds_status

    # wrangling names
    # current (previous) file
    file_cur = ds.files.all().order_by('-rev')[0].file
    filename_cur = file_cur.name

    # new file
    file_new = request.FILES['file']
    tempf, tempfn = tempfile.mkstemp()
    # write new file as temporary to /var/folders/../...
    try:
      for chunk in file_new.chunks():
        os.write(tempf, chunk)
    except:
      raise Exception("Problem with the input file %s" % request.FILES['file'])
    finally:
      os.close(tempf)

    print('tempfn,filename_cur,file_new.name', tempfn, filename_cur, file_new.name)

    # validation (tempfn is file path)
    if format == 'delimited':
      print('format:', format)
      try:
        vresult = validate_tsv(tempfn, 'delimited')
      except:
        print('validate_tsv() failed:', sys.exc_info())

    elif format == 'lpf':
      # TODO: feed tempfn only?
      # TODO: accept json-lines; only FeatureCollections ('coll') now
      vresult = validate_lpf(tempfn, 'coll')
      # print('format, vresult:',format,vresult)

    # if validation errors, stop and return them to modal
    # which expects {validation_result{errors['','']}}
    print('vresult', vresult)
    if len(vresult['errors']) > 0:
      errormsg = {"failed": {
        "errors": vresult['errors']
      }}
      return JsonResponse(errormsg, safe=False)

    # give new file a path
    # filename_new = 'user_'+user+'/'+'sample7cr_new.tsv'
    filename_new = 'user_' + user + '/' + file_new.name
    # temp files were given extensions in validation functions
    tempfn_new = tempfn + '.tsv' if format == 'delimited' else tempfn + '.jsonld'
    print('tempfn_new', tempfn_new)

    # begin report
    comparison = {
      "id": dsid,
      "filename_cur": filename_cur,
      "filename_new": filename_new,
      "format": format,
      "validation_result": vresult,
      "tempfn": tempfn,
      "count_indexed": ds.status_idx['idxcount'],
    }
    # create pandas (pd) objects, then perform comparison
    # a = existing, b = new
    fn_a = 'media/' + filename_cur
    fn_b = tempfn
    if format == 'delimited':
      adf = pd.read_csv(fn_a, file_cur.delimiter)
      try:
        bdf = pd.read_csv(fn_b, delimiter='\t')
      except:
        print('bdf read failed', sys.exc_info())

      ids_a = adf['id'].tolist()
      ids_b = bdf['id'].tolist()
      print('ids_a, ids_b', ids_a[:10], ids_b[:10])
      # new or removed columns?
      cols_del = list(set(adf.columns) - set(bdf.columns))
      cols_add = list(set(bdf.columns) - set(adf.columns))

      # count of *added* links
      comparison['count_links_added'] = ds.links.filter(task_id__isnull=False).count()
      # count of *added* geometries
      comparison['count_geoms_added'] = ds.geometries.filter(task_id__isnull=False).count()

      comparison['compare_result'] = {
        "count_new": len(ids_b),
        'count_diff': len(ids_b) - len(ids_a),
        'count_replace': len(set.intersection(set(ids_b), set(ids_a))),
        'cols_del': cols_del,
        'cols_add': cols_add,
        'header_new': vresult['columns'],
        'rows_add': [str(x) for x in (set(ids_b) - set(ids_a))],
        'rows_del': [str(x) for x in (set(ids_a) - set(ids_b))]
      }
    # TODO: process LP format, collections + json-lines
    elif format == 'lpf':
      # print('need to compare lpf files:',fn_a,fn_b)
      comparison['compare_result'] = "it's lpf...tougher row to hoe"

    print('comparison (compare_data)', comparison)
    # back to calling modal
    return JsonResponse(comparison, safe=False)

