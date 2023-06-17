# collection.views (collections)
from dateutil.parser import isoparse
from datetime import date
import json

from django import forms
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.forms.models import inlineformset_factory
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView, ListView )

from .forms import CollectionModelForm, CollectionGroupModelForm
from .models import *
from main.models import Log, Link
from traces.forms import TraceAnnotationModelForm
from traces.models import TraceAnnotation

""" sets collection to inactive, removing from lists """
def inactive(request, *args, **kwargs):
  print('inactive() request.POST', request.POST)
  coll = Collection.objects.get(id=request.POST['id'])
  coll.active = False
  coll.save()
  result = {"msg": "collection " + coll.title + '('+str(coll.id)+') flagged inactive'}
  return JsonResponse(result, safe=False)

""" removes dataset from collection, refreshes page"""
def remove_link(request, *args, **kwargs):
  #print('kwargs', kwargs)
  link = Link.objects.get(id=kwargs['id'])
  # link = CollectionLink.objects.get(id=kwargs['id'])
  print('remove_link()', link)
  link.delete()
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

"""
  set collection status by group leader: reviewed, nominated
"""
def status_update(request, *args, **kwargs):
  print('in status_update()', request.POST)
  status = request.POST['status']
  coll = Collection.objects.get(id=request.POST['coll'])

  coll.status = status
  coll.save()

  return JsonResponse({'status': status, 'coll': coll.title}, safe=False,
                      json_dumps_params={'ensure_ascii': False, 'indent': 2})

"""
  set/unset nominated flag by group leader: boolean
"""
def nominator(request, *args, **kwargs):
  print('in nominator()', request.POST)
  nominated = True if request.POST['nominated'] == 'true' else False
  coll = Collection.objects.get(id=request.POST['coll'])
  if nominated:
    coll.nominated = True
    status = 'nominated'
  else:
    coll.nominated = False
    status = 'withdrawn'
  coll.save()

  return JsonResponse({'status': status, 'coll': coll.title}, safe=False,
                      json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  add (submit) or remove collection to/from collection group
"""
def group_connect(request, *args, **kwargs):
  action = request.POST['action']
  coll = Collection.objects.get(id=request.POST['coll'])
  cg = CollectionGroup.objects.get(id=request.POST['group'])
  if action == 'submit':
    cg.collections.add(coll)
    # coll.submitted = True
    coll.save()
    status = 'added to'
  else:
    # cg.collections.remove(coll)
    coll.group = None
    coll.submit_date = None
    coll.save()
    status = 'removed from'

  return JsonResponse({'status': status, 'coll': coll.title, 'group': cg.title}, safe=False,
                      json_dumps_params={'ensure_ascii': False, 'indent': 2})

"""
  add collaborator to collection in role
"""
def collab_add(request, cid):
  print('collab_add() request, cid', request, cid)
  try:
    uid=get_object_or_404(User, email=request.POST['email']).id
    role=request.POST['role']
  except:
    #
    messages.add_message(
      request, messages.INFO, "Please check email, we don't have '<b>" + request.POST['email']+"</b>'")
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

  # TODO: send collaborator an email
  print('collection collab_add():',request.POST['email'],role, cid, uid)
  CollectionUser.objects.create(user_id=uid, collection_id=cid, role=role)

  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

"""
  collab_delete(uid, cid)
  remove collaborator from collection
"""
def collab_delete(request, uid, cid):
  print('collab_delete() request, uid, cid', request, uid, cid)
  get_object_or_404(CollectionUser,user=uid, collection=cid).delete()
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

""" utility: get next sequence for a collection """
def seq(coll):
  cps = CollPlace.objects.filter(collection=coll).values_list("sequence",flat=True)
  if cps:
    next=max(cps)+1
  else:
    next=0
  print(next)
  return next

""" 
  add list of >=1 places to collection 
  i.e. new CollPlace and TraceAnnotation rows
  ajax call from ds_places.html and place_portal.html
"""
# TODO: essentially same as add_dataset(); needs refactor
def add_places(request, *args, **kwargs):
  print('args', args)
  print('kwargs', kwargs)
  if request.method == 'POST':
    user = request.user
    status, msg = ['','']
    dupes = []
    added = []
    # print('add_places request', request.POST)
    coll = Collection.objects.get(id=request.POST['collection'])
    place_list = [int(i) for i in request.POST['place_list'].split(',')]
    for p in place_list:
      place = Place.objects.get(id=p)
      gotplace = TraceAnnotation.objects.filter(collection=coll, place=place)
      if not gotplace:
        t = TraceAnnotation.objects.create(
          place = place,
          src_id = place.src_id,
          collection = coll,
          motivation = 'locating',
          owner = user,
          anno_type = 'place',
          saved = 0
        )
        # coll.places.add(p)
        CollPlace.objects.create(
          collection=coll,
          place=place,
          sequence=seq(coll)
        )
        added.append(p)
      else:
        dupes.append(place.title)
      print('add_places() result',{"added": added, "dupes": dupes})
    return JsonResponse({'status': status, 'msg': msg}, safe=False)

""" 
  deletes CollPlace record(s) and 
  archives TraceAnnotation(s) for list of pids 
"""
def archive_traces(request, *args, **kwargs):
  if request.method == 'POST':
    print('archive_traces request', request.POST)
    coll = Collection.objects.get(id=request.POST['collection'])
    place_list = [int(i) for i in request.POST['place_list'].split(',')]
    print('place_list to remove', place_list)
    # remove CollPlace, archive TraceAnnotation
    for pid in place_list:
      place = Place.objects.get(id=pid)
      if place in coll.places.all():
        # print('collection place', place)
        coll.places.remove(place)
      if place.traces:
        # can be only one but .update only works on filter
        TraceAnnotation.objects.filter(collection=coll,place=place).update(archived=True)
    return JsonResponse({'result': str(len(place_list))+' places removed, we think'}, safe=False)

""" update sequence of annotated places """
def update_sequence(request, *args, **kwargs):
  print('request.POST', request.POST)
  new_sequence = json.loads(request.POST['seq'])
  # print('new_sequence', new_sequence)
  cid = request.POST['coll_id']
  for cp in CollPlace.objects.filter(collection=cid):
    cp.sequence = new_sequence[str(cp.place_id)]
    cp.save()
  return JsonResponse({"msg": "updated?", "POST": new_sequence})

""" 
create place collection on the fly; return id for adding place(s) to it 
"""
def flash_collection_create(request, *args, **kwargs):
  print('flash_collection_create request.POST', request.POST)
  print('flash_collection_create kwargs', kwargs)
  if request.method == 'POST':
    collobj = Collection.objects.create(
      owner = request.user,
      title = request.POST['title'],
      collection_class = 'place',
      description = 'new collection',
      # keywords = '{replace, these, please}'
    )
    collobj.save()
    result = {"id": collobj.id, 'title': collobj.title}
  return JsonResponse(result, safe=False)

def stringer(str):
  if str:
    return isoparse(str).strftime('%Y' if len(str)<=5 else '%b %Y' if len(str)<=8 else '%d %b %Y')
  else:
    return None
def when_format(ts):
  return [stringer(ts[0]), stringer(ts[1])]; print(result)

""" gl map needs this """
# TODO:
def fetch_geojson_coll(request, *args, **kwargs):
  # print('fetch_geojson_coll kwargs',kwargs)
  id_=kwargs['id']
  coll=get_object_or_404(Collection, id=id_)
  pids = [p.id for p in coll.places_all]
  rel_keywords = coll.rel_keywords
  # build FeatureCollection
  features_t = [
    {"type": "Feature", "geometry": t.place.geoms.all()[0].jsonb,
     "properties":{
       "pid":t.place.id,
       "title": t.place.title,
       # "relation": t.relation,
       "relation": t.relation[0],
       "when": when_format([t.start, t.end]),
       "note": t.note
     }}
    for t in coll.traces.filter(archived=False)
  ]
  fcoll = {"type":"FeatureCollection","features":features_t,"relations":rel_keywords}

  return JsonResponse(fcoll, safe=False, json_dumps_params={'ensure_ascii':False,'indent':2})

""" returns json for display """
class ListDatasetView(View):
  @staticmethod
  def get(request):
    print('ListDatasetView() GET', request.GET)
    #coll = Collection.objects.get(id=request.GET['coll_id'])
    ds = Dataset.objects.get(id=request.GET['ds_id'])
    #coll.datasets.add(ds)
    result = {
      "id": ds.id,
      "label": ds.label,
      "title": ds.title,
      "create_date": ds.create_date,
      "description": ds.description[:100]+'...',
      "numrows": ds.places.count()
    }
    return JsonResponse(result, safe=False)

"""
  adds all places in a dataset as CollPlace records
  i.e. new CollPlace and TraceAnnotation rows
  url call from place_collection_build.html
  adds dataset to db:collections_datasets
"""
# TODO: essentially same as add_places(); needs refactor
def add_dataset(request, *args, **kwargs):
  print('method', request.method)
  print('add_dataset() kwargs', kwargs)
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  user = request.user
  print('add_dataset(): coll, ds', coll, ds)
  status, msg = ['', '']
  dupes = []
  added = []
  coll.datasets.add(ds)
  for place in ds.places.all():
    # has non-archived trace annotation?
    gottrace = TraceAnnotation.objects.filter(collection=coll, place=place, archived=False)
    if not gottrace:
      t = TraceAnnotation.objects.create(
        place=place,
        src_id=place.src_id,
        collection=coll,
        motivation='locating',
        owner=user,
        anno_type='place',
        saved=0
      )
      # coll.places.add(p)
      CollPlace.objects.create(
        collection=coll,
        place=place,
        sequence=seq(coll)
      )
      added.append(place.id)
    else:
      dupes.append(place.title)
    msg = {"added": added, "dupes": dupes}
  # return JsonResponse({'status': status, 'msg': msg}, safe=False)
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

""" 
  removes dataset from collection & clean up "omitted"; refreshes page 
  remove   
"""
def remove_dataset(request, *args, **kwargs):
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  print('remove_dataset(): coll, ds', coll, ds)

  # remove CollPlace records
  CollPlace.objects.filter(place_id__in=ds.placeids).delete()
  # remove dataset from collections_dataset
  coll.datasets.remove(ds)
  # archive any non-blank trace annotations
  # someone will want to recover them, count on it
  current_traces = coll.traces.filter(collection=coll, place__in=ds.placeids)
  non_blank = [t.id for t in current_traces.all() if t.blank == False]
  blanks = current_traces.exclude(id__in=non_blank)
  if non_blank:
    current_traces.filter(id__in=non_blank).update(archived=True)
    current_traces.filter(archived=False).delete()
  blanks.delete()
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

def create_collection_group(request, *args, **kwargs):
  # must be member of group_leaders
  result = {"status": "", "id": "", 'title': ""}
  if request.method == 'POST':
    print('request.POST', request.POST)
    owner = get_user_model().objects.get(id=request.POST['ownerid'])
    group_title = request.POST['title']
    description = request.POST['description']
    if group_title in CollectionGroup.objects.all().values_list('title', flat=True):
      result['status'] = "dupe"
    else:
      newgroup = CollectionGroup.objects.create(
        owner = owner,
        title = group_title,
        description = description,
      )
      # newgroup.user_set.add(request.user)
      result = {"status": "ok", "id": newgroup.id, 'title': newgroup.title}

  return JsonResponse(result, safe=False)

CollectionLinkFormset = inlineformset_factory(
    Collection, CollectionLink, fields=('uri','label','link_type'), extra=2,
    widgets={
      'link_type': forms.Select(choices=('webpage'))}
)

""" 
  PLACE COLLECTIONS
  collections from places and/or datasets; uses place_collection_build.html
"""
# TODO: refactor to fewer views
class PlaceCollectionCreateView(LoginRequiredMixin, CreateView):
  form_class = CollectionModelForm
  template_name = 'collection/place_collection_build.html'
  queryset = Collection.objects.all()

  def get_form_kwargs(self, **kwargs):
    kwargs = super(PlaceCollectionCreateView, self).get_form_kwargs()
    return kwargs

  def get_context_data(self, *args, **kwargs):
    user = self.request.user
    print('PlaceCollectionCreateView() user', user)
    context = super(PlaceCollectionCreateView, self).get_context_data(**kwargs)
    context['mbtoken'] = settings.MAPBOX_TOKEN_MB

    datasets = []
    # add 1 or more links, images (?)
    if self.request.POST:
      context["links_form"] = CollectionLinkFormset(self.request.POST)
      # context["images_form"] = CollectionImageFormset(self.request.POST)
    else:
      context["links_form"] = CollectionLinkFormset()
      # context["images_form"] = CollectionImageFormset()

    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]
    if not user.is_superuser:
      ds_select.insert(len(ds_select)-1, Dataset.objects.get(label='owt10'))

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context

  def form_valid(self, form):
    context = self.get_context_data()
    self.object = form.save()

    # TODO: write log entry
    # Log.objects.create(
    #   # category, logtype, "timestamp", subtype, dataset_id, user_id
    #   category='collection',
    #   logtype='coll_create',
    #   subtype='place',
    #   coll_id=self.object.id,
    #   user_id=self.request.user.id
    # )

    print('form is valid, cleaned_data',form.cleaned_data)
    print('referrer', self.request.META.get('HTTP_REFERER'))
    return super().form_valid(form)

  def form_invalid(self,form):
    context = self.get_context_data()
    context['errors'] = form.errors
    print('form invalid...',form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def get_success_url(self):
    Log.objects.create(
      # category, logtype, "timestamp", subtype, note, dataset_id, user_id
      category = 'collection',
      logtype = 'create',
      note = 'created collection id: '+str(self.object.id),
      user_id = self.request.user.id
    )
    # return to update page after create
    return reverse('collection:place-collection-update', kwargs = {'id':self.object.id})

""" update place collection; uses place_collection_build.html """
class PlaceCollectionUpdateView(LoginRequiredMixin, UpdateView):
  form_class = CollectionModelForm
  template_name = 'collection/place_collection_build.html'
  queryset = Collection.objects.all()

  def get_form_kwargs(self, **kwargs):
    kwargs = super(PlaceCollectionUpdateView, self).get_form_kwargs()
    return kwargs

  def get_object(self):
    # print('PlaceCollectionUpdateView() kwargs', self.kwargs)
    # print('POST', self.request.POST)
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    data = form.cleaned_data
    print('cleaned_data', data)
    print('referrer', self.request.META.get('HTTP_REFERER'))
    id_ = self.kwargs.get("id")
    obj = form.save(commit=False)
    if obj.group:
      obj.status = 'group'
      obj.submit_date = date.today()
    else:
      obj.nominated = False
      obj.submit_date = None
    obj.save()

    Log.objects.create(
      # category, logtype, "timestamp", subtype, note, dataset_id, user_id
      category = 'collection',
      logtype = 'update',
      note = 'collection id: '+ str(obj.id) + ' by '+ self.request.user.name,
      user_id = self.request.user.id
    )
    # return to page, or to browse
    if 'update' in self.request.POST:
      return redirect('/collections/' + str(id_) + '/update_pl')
    else:
      return redirect('/collections/' + str(id_) + '/browse_pl')

  def get_context_data(self, *args, **kwargs):
    context = super(PlaceCollectionUpdateView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    coll = self.object
    datasets = self.object.datasets.all()

    form_anno = TraceAnnotationModelForm(self.request.GET or None, auto_id="anno_%s")
    # anno_form = TraceAnnotationModelForm(self.request.GET or None, prefix="sch")
    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]
    if not user.is_superuser:
      ds_select.insert(len(ds_select)-1, Dataset.objects.get(label='owt10'))
    context['mygroups'] = CollectionGroupUser.objects.filter(user_id=user)
    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets
    context['links'] = Link.objects.filter(collection=coll.id)
    context['owner'] = True if user == coll.owner else False
    context['is_member'] = True if user in coll.owners or user in coll.collaborators else False
    context['is_owner'] = True if user in self.object.owners else False
    context['whgteam'] = True if user.groups.filter(name__in=['whg_team','editorial']).exists() else False
    context['collabs'] = CollectionUser.objects.filter(collection=coll.id)
    # context['links'] = CollectionLink.objects.filter(collection=self.object.id)

    context['form_anno'] = form_anno
    context['seq_places'] = [
      {'id':cp.id,'p':cp.place,'seq':cp.sequence}
        for cp in CollPlace.objects.filter(collection=_id).order_by('sequence')
    ]
    context['created'] = self.object.created.strftime("%Y-%m-%d")
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    # context['whgteam'] = User.objects.filter(groups__name='whg_team')

    return context

""" browse collection *all* places """
class PlaceCollectionBrowseView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Collection
  template_name = 'collection/place_collection_browse.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/places'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_context_data(self, *args, **kwargs):
    id_ = self.kwargs.get("id")
    coll = get_object_or_404(Collection, id=id_)

    context = super(PlaceCollectionBrowseView, self).get_context_data(*args, **kwargs)
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['media_url'] = settings.MEDIA_URL

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    context['ds_list'] = coll.ds_list
    context['ds_counter'] = coll.ds_counter
    context['collabs'] = coll.collaborators.all()
    context['images'] = [ta.image_file.name for ta in coll.traces.all()]
    context['links'] = coll.related_links.all()
    context['places'] = coll.places.all().order_by('title')
    context['updates'] = {}
    context['url_front'] = settings.URL_FRONT

    return context

""" 
COLLECTION GROUPS 
"""
class CollectionGroupCreateView(CreateView):
  form_class = CollectionGroupModelForm
  template_name = 'collection/collection_group_create.html'
  queryset = CollectionGroup.objects.all()

  #
  def get_form_kwargs(self, **kwargs):
    kwargs = super(CollectionGroupCreateView, self).get_form_kwargs()
    # print('kwargs', kwargs)
    print('GET in CollectionGroupCreateView()', self.request.GET)
    return kwargs

  def get_success_url(self):
    cgid = self.kwargs.get("id")
    action = self.kwargs.get("action")
    # def get_success_url(self):
    #         return reverse('doc_aide:prescription_detail', kwargs={'pk': self.object.pk})
    return reverse('collection:collection-group-update', kwargs={'id':self.object.id})
    # return redirect('collections/groups/'+str(cgid)+'/update')
    # return '/accounts/profile/'

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    context = {}
    if form.is_valid():
      print('form is valid, cleaned_data', form.cleaned_data)
      self.object = form.save()
      return HttpResponseRedirect(self.get_success_url())
    # else:
    #   print('form not valid', form.errors)
    #   context['errors'] = form.errors
    # return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionGroupCreateView, self).get_context_data(*args, **kwargs)
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    # print('args',args,kwargs)
    context['action'] = 'create'
    # context['referrer'] = self.request.POST.get('referrer')
    return context

class CollectionGroupDetailView(DetailView):
  model = CollectionGroup
  template_name = 'collection/collection_group_detail.html'

  def get_success_url(self):
    pid = self.kwargs.get("id")
    # print('messages:', messages.get_messages(self.kwargs))
    return '/collection/' + str(pid) + '/detail'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(CollectionGroup, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionGroupDetailView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    print('CollectionGroupDetailView get_context_data() kwargs:', self.kwargs)
    print('CollectionGroupDetailView get_context_data() request.user', self.request.user)
    cg = get_object_or_404(CollectionGroup, pk=self.kwargs.get("id"))
    me = self.request.user
    # if Collection has a group, it is submitted
    context['submitted'] = Collection.objects.filter(group=cg.id).count()
    context['message'] = 'CollectionGroupDetailView() loud and clear'
    context['links'] = Link.objects.filter(collection_group_id=self.get_object())
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

class CollectionGroupDeleteView(DeleteView):
  template_name = 'collection/collection_group_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(CollectionGroup, id=id_)

  def get_success_url(self):
    return reverse('accounts:profile')

"""
  update (edit); uses same template as create; 
  context['action'] governs template display
"""
class CollectionGroupUpdateView(UpdateView):
  form_class = CollectionGroupModelForm
  template_name = 'collection/collection_group_create.html'

  def get_form_kwargs(self, **kwargs):
    kwargs = super(CollectionGroupUpdateView, self).get_form_kwargs()
    return kwargs

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(CollectionGroup, id=id_)

  def form_valid(self, form):
    id_ = self.kwargs.get("id")
    if form.is_valid():
      print('form.cleaned_data', form.cleaned_data)
      obj = form.save(commit=False)
      obj.save()
      return redirect('/collections/group/' + str(id_) + '/update')
    else:
      print('form not valid', form.errors)
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    print('CollectionGroupUpdateView() kwargs', self.kwargs)
    context = super(CollectionGroupUpdateView, self).get_context_data(*args, **kwargs)
    cg= self.get_object()
    members = [m.user for m in cg.members.all()]
    context['action'] = 'update'
    context['members'] = members
    context['collections'] = Collection.objects.filter(group=cg.id)
    context['links'] = Link.objects.filter(collection_group_id = self.get_object())
    return context

class CollectionGroupGalleryView(ListView):
  redirect_field_name = 'redirect_to'

  context_object_name = 'collections'
  template_name = 'collection/collection_group_gallery.html'
  model = Collection

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(CollectionGroup, id=id_)

  def get_queryset(self):
    # original qs
    qs = super().get_queryset()
    return qs
    # return qs.filter(public = True).order_by('core','title')

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionGroupGalleryView, self).get_context_data(*args, **kwargs)
    cg = CollectionGroup.objects.get(id=self.kwargs.get("id"))

    # public datasets available as dataset_list
    # public collections
    context['group'] = self.get_object()
    # context['collections'] = cg.collections.all()
    context['collections'] = Collection.objects.filter(
      group=cg.id,status__in=['reviewed','published']).order_by('submit_date')
    # context['viewable'] = ['uploaded','inserted','reconciling','review_hits','reviewed','review_whg','indexed']

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    return context


""" DATASET COLLECTIONS """
""" datasets only collection 
    uses ds_collection_builder.html
"""
class DatasetCollectionCreateView(LoginRequiredMixin, CreateView):
  # print('hit DatasetCollectionCreateView()')
  form_class = CollectionModelForm
  # template_name = 'collection/ds_collection_builder.html'
  # TODO: new ds collection builder
  template_name = 'collection/ds_collection_build.html'
  queryset = Collection.objects.all()

  def get_success_url(self):
    Log.objects.create(
      # category, logtype, "timestamp", subtype, note, dataset_id, user_id
      category = 'collection',
      logtype = 'create',
      note = 'created collection id: '+str(self.object.id),
      user_id = self.request.user.id
    )
    return reverse('data-collections')
  #
  def get_form_kwargs(self, **kwargs):
    kwargs = super(DatasetCollectionCreateView, self).get_form_kwargs()
    return kwargs

  def form_invalid(self,form):
    print('form invalid...',form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    context={}
    print('form is valid, cleaned_data',form.cleaned_data)
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    user = self.request.user
    context = super(DatasetCollectionCreateView, self).get_context_data(*args, **kwargs)
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    datasets = []
    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or
      user in obj.collaborators or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context

""" update dataset collection 
    uses ds_collection_builder.html
"""
class DatasetCollectionUpdateView(UpdateView):
  form_class = CollectionModelForm
  # template_name = 'collection/ds_collection_builder.html'
  template_name = 'collection/ds_collection_build.html'
  success_url = '/mycollections'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/summary_ds'

  def form_valid(self, form):
    if form.is_valid():
      print(form.cleaned_data)
      obj = form.save(commit=False)
      obj.save()
      Log.objects.create(
        # category, logtype, "timestamp", subtype, note, dataset_id, user_id
        category = 'collection',
        logtype = 'update',
        note = 'collection id: '+ str(obj.id) + ' by '+ self.request.user.name,
        user_id = self.request.user.id
      )
    else:
      print('form not valid', form.errors)
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCollectionUpdateView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('DatasetCollectionUpdateView() kwargs', self.kwargs)

    datasets = self.object.datasets.all()

    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    context['created'] = self.object.created.strftime("%Y-%m-%d")
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    return context

""" public collection view, datasets, bboxes on a map """
class DatasetCollectionSummaryView(DetailView):
  template_name = 'collection/ds_collection_summary.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(DatasetCollectionSummaryView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('CollectionDetailView(), kwargs',self, self.kwargs)

    datasets = self.object.datasets.all()

    # gather bounding boxes
    bboxes = [ds.bounds for ds in datasets]

    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    context['ds_list'] = datasets
    context['links'] = Link.objects.filter(collection=id_)
    context['bboxes'] = bboxes
    return context

""" browse collection dataset places 
    same for owner(s) and public
"""
class DatasetCollectionBrowseView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Collection
  template_name = 'collection/ds_collection_browse.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/places'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(DatasetCollectionBrowseView, self).get_context_data(*args, **kwargs)
    print('DatasetCollectionBrowseView get_context_data() kwargs:',self.kwargs)
    print('DatasetCollectionBrowseView get_context_data() request.user',self.request.user)

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['media_url'] = settings.MEDIA_URL

    id_ = self.kwargs.get("id")
    # compute bounding boxes

    coll = get_object_or_404(Collection, id=id_)
    # "geotypes":ds.geotypes,
    # datasets = [{"id":ds.id,"label":ds.label,"title":ds.title} for ds in coll.ds_list]
                 # "bbox": ds.bounds } for ds in coll.datasets.all()]
    #bboxes = [{"id":ds['id'], "geometry":ds['bounds']} for ds in datasets]

    placeset = coll.places.all()
    context['places'] = placeset
    context['ds_list'] = coll.ds_list
    context['links'] = Link.objects.filter(collection=id_)
    context['updates'] = {}
    context['coll'] = coll
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

""" browse collection collections 
    w/student section?
"""
class CollectionGalleryView(ListView):
  redirect_field_name = 'redirect_to'

  context_object_name = 'collections'
  template_name = 'collection/gallery_main.html'
  model = Collection

  def get_queryset(self):
    qs = super().get_queryset()
    return qs.filter(public = True).order_by('title')

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionGalleryView, self).get_context_data(*args, **kwargs)
    # public collections
    # context['group'] = self.get_object()
    context['place_collections'] = Collection.objects.filter(collection_class='place', public=True)
    context['dataset_collections'] = Collection.objects.filter(collection_class='dataset', public=True)
    context['student_collections'] = Collection.objects.filter(nominated=True)

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    return context

class CollectionDeleteView(DeleteView):
  template_name = 'collection/collection_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    return reverse('data-collections')

""" public collection view, contents, bboxes on a map 
  NOT IN USE, place_collection_browse is single display page """
# class PlaceCollectionSummaryView(DetailView):
#   template_name = 'collection/place_collection_summary.html'
#
#   model = Collection
#
#   def get_context_data(self, **kwargs):
#     context = super(PlaceCollectionSummaryView, self).get_context_data(**kwargs)
#     id_ = self.kwargs.get("pk")
#     print('CollectionDetailView(), kwargs',self, self.kwargs)
#
#     datasets = self.object.datasets.all()
#     places = self.object.places.all().order_by('title')
#     # gather bounding boxes
#     bboxes = [ds.bounds for ds in datasets]
#
#     context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
#     context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
#     context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
#     context['whgteam'] = User.objects.filter(groups__name='whg_team')
#
#     context['place_list'] = places
#     context['ds_list'] = datasets
#     context['bboxes'] = bboxes
#     return context

