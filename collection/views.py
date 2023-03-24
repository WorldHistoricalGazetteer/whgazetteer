# collection.views (collections)

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView )

#from datasets.utils import hully
from .forms import CollectionModelForm, CollectionLinkForm
from .models import *
from collection.models import Collection, CollectionImage
from main.models import Log
from places.models import PlaceGeom
from traces.forms import TraceAnnotationModelForm
from traces.models import TraceAnnotation
from itertools import chain

# sets collection to inactive, removing from lists
def inactive(request, *args, **kwargs):
  print('inactive() request.POST', request.POST)
  coll = Collection.objects.get(id=request.POST['id'])
  coll.active = False
  coll.save()
  result = {"msg": "collection " + coll.title + '('+str(coll.id)+') flagged inactive'}
  return JsonResponse(result, safe=False)

# removes dataset from collection, refreshes page
def remove_link(request, *args, **kwargs):
  #print('kwargs', kwargs)
  link = CollectionLink.objects.get(id=kwargs['id'])
  print('remove_link()', link)
  link.delete()
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

""" create collection_link record """
def create_link(request, *args, **kwargs):
  if request.method == 'POST':
    status, msg = ['','']
    print('create_link() request', request.POST)
    coll = Collection.objects.get(id=request.POST['collection'])
    uri = request.POST['uri']
    label = request.POST['label']
    link_type = request.POST['link_type']
    gotlink = CollectionLink.objects.filter(uri=uri, collection=coll)
    if not gotlink:
      try:
        cl=CollectionLink.objects.create(
          collection = coll,
          uri = uri,
          label = label,
          link_type = link_type
        )
        result = {'uri': cl.uri, 'label': cl.label,
                  'link_type':cl.link_type,
                  'link_icon':cl.get_link_type_display()}
        status="ok"
      except:
        status = "failed"
        result = "CollectionLink *not* created...why?"
    else:
      result = 'dupe'
    return JsonResponse({'status': status, 'result': result}, safe=False)

""" add list of >=1 places to collection """
def add_places(request, *args, **kwargs):
  if request.method == 'POST':
    status, msg = ['','']
    dupes = []
    added = []
    print('add_places request', request.POST)
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
          owner = request.user,
          anno_type = 'place',
          saved = 0
        )
        coll.places.add(p)
        added.append(p)
      else:
        dupes.append(place.title)
      msg = {"added": added, "dupes": dupes}
    return JsonResponse({'status': status, 'msg': msg}, safe=False)

""" remove list of >=1 places from collection """
def remove_places(request, *args, **kwargs):
  if request.method == 'POST':
    print('remove_places request', request.POST)
    coll = Collection.objects.get(id=request.POST['collection'])
    place_list = [int(i) for i in request.POST['place_list'].split(',')]
    print('place_list to remove', place_list)
    # remove from collections_places
    for pid in place_list:
      place = Place.objects.get(id=pid)
      if place in coll.places.all():
        print('collection place', place)
        coll.places.remove(place)
      elif place in coll.places_all:
        print('pid to omitted:',place.id)
        coll.omitted.append(place.id)
        coll.save()
      if place.traces:
        TraceAnnotation.objects.filter(collection=coll, place__in=place_list).delete()
    return JsonResponse({'result': str(len(place_list))+' places removed, we think'}, safe=False)

""" create place collection on the fly
    return id for adding place(s) to it 
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

""" gl map needs this """
def fetch_geojson_coll(request, *args, **kwargs):
  # print('fetch_geojson_coll kwargs',kwargs)
  id_=kwargs['id']
  coll=get_object_or_404(Collection, id=id_)
  pids = [p.id for p in coll.places_all]
  # pids = [p.id for p in coll.places.all()]

  # build FeatureCollection
  features=PlaceGeom.objects.filter(place_id__in=pids).values_list(
    'jsonb','place_id','src_id','place__title','place__minmax',
    'place__fclasses', 'place__dataset__id', 'place__dataset__label')
  fcoll = {"type":"FeatureCollection","features":[]}
  for f in features:
    feat={"type":"Feature",
          "properties":{"pid":f[1],"src_id":f[2],"title":f[3],"minmax":f[4],
                        "fclasses":f[5], "dsid":f[6], "dslabel":f[7]
                        },
          "geometry":f[0]}
    fcoll['features'].append(feat)
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

# adds dataset to collection, refreshes page
def add_dataset(request, *args, **kwargs):
  print('add_dataset() kwargs', kwargs)
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  print('add_dataset(): coll, ds', coll, ds)
  coll.datasets.add(ds)
  # coll.datasets.remove(ds)

  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

# removes dataset from collection & clean up "omitted"; refreshes page
def remove_dataset(request, *args, **kwargs):
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  print('remove_dataset(): coll, ds', coll, ds)
  # remove any "omitted" from ds being removed
  remove_these = list(set(list(ds.placeids)) & set(coll.omitted) )
  coll.omitted = list(set(coll.omitted)-set(remove_these))
  coll.save()
  coll.datasets.remove(ds)

  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


from django.forms.models import inlineformset_factory
CollectionImageFormset = inlineformset_factory(
    Collection, CollectionImage, fields=('image','caption','uri','license'), extra=1
)
from django.forms.models import inlineformset_factory
CollectionLinkFormset = inlineformset_factory(
    Collection, CollectionLink, fields=('uri','label','link_type'), extra=2
)
""" PLACE COLLECTIONS """
""" TODO: refactor to fewer views """
""" collections from places and/or datasets 
    uses place_collection_builder.html
"""
class PlaceCollectionCreateView(LoginRequiredMixin, CreateView):
  form_class = CollectionModelForm
  template_name = 'collection/place_collection_builder.html'
  queryset = Collection.objects.all()

  def get_form_kwargs(self, **kwargs):
    kwargs = super(PlaceCollectionCreateView, self).get_form_kwargs()
    return kwargs

  def get_context_data(self, *args, **kwargs):
    user = self.request.user
    print('PlaceCollectionCreateView() user', user)
    context = super(PlaceCollectionCreateView, self).get_context_data(**kwargs)
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG

    datasets = []
    # add 1 or more links, images (?)
    if self.request.POST:
      context["links_form"] = CollectionLinkFormset(self.request.POST)
      context["images_form"] = CollectionImageFormset(self.request.POST)
    else:
      context["links_form"] = CollectionLinkFormset()
      context["images_form"] = CollectionImageFormset()

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
    # images = context['images']
    self.object = form.save()
    # if images.is_valid():
    #   images.instance = self.object
    #   images.save()

    # write log entry
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

""" update place collection 
    uses place_collection_builder.html
"""
class PlaceCollectionUpdateView(UpdateView):
  form_class = CollectionModelForm
  template_name = 'collection/place_collection_builder.html'
  # success_url = '/mycollections'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  # def get_success_url(self):
  #   id_ = self.kwargs.get("id")
  #   return redirect('/collections/'+str(id_)+'/update_pl')

  def form_valid(self, form):
    print('referrer', self.request.META.get('HTTP_REFERER'))
    print('update kwargs', self.kwargs)
    id_ = self.kwargs.get("id")
    if form.is_valid():
      print('cleaned_data', form.cleaned_data)
      obj = form.save(commit=False)
      obj.save()
      Log.objects.create(
        # category, logtype, "timestamp", subtype, note, dataset_id, user_id
        category = 'collection',
        logtype = 'update',
        note = 'collection id: '+ str(obj.id) + ' by '+ self.request.user.username,
        user_id = self.request.user.id
      )
    else:
      print('form not valid', form.errors)
    if 'update' in self.request.POST:
      return redirect('/collections/' + str(id_) + '/update_pl')
    else:
      return redirect('/collections/' + str(id_) + '/browse_pl')
    # return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    context = super(PlaceCollectionUpdateView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('PlaceCollectionUpdateView() kwargs', self.kwargs)

    datasets = self.object.datasets.all()

    form_anno = TraceAnnotationModelForm(self.request.GET or None)
    # anno_form = TraceAnnotationModelForm(self.request.GET or None, prefix="sch")
    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]
    if not user.is_superuser:
      ds_select.insert(len(ds_select)-1, Dataset.objects.get(label='owt10'))

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets
    context['links'] = CollectionLink.objects.filter(collection=self.object.id)

    # test: send single anno form to template
    context['form_anno'] = form_anno
    context['coll_places'] = self.object.places_all.order_by('title')

    context['created'] = self.object.created.strftime("%Y-%m-%d")
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    return context

""" public collection view, contents, bboxes on a map """
class PlaceCollectionSummaryView(DetailView):
  template_name = 'collection/place_collection_summary.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(PlaceCollectionSummaryView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('CollectionDetailView(), kwargs',self, self.kwargs)

    datasets = self.object.datasets.all()
    places = self.object.places.all().order_by('title')
    # gather bounding boxes
    bboxes = [ds.bounds for ds in datasets]

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    context['place_list'] = places
    context['ds_list'] = datasets
    context['bboxes'] = bboxes
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
    context = super(PlaceCollectionBrowseView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['media_url'] = settings.MEDIA_URL

    id_ = self.kwargs.get("id")

    coll = get_object_or_404(Collection, id=id_)

    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False
    context['coll'] = coll
    context['ds_list'] = coll.ds_list
    context['ds_counter'] = coll.ds_counter
    context['links'] = coll.links.all()
    context['places'] = coll.places.all().order_by('title')
    context['updates'] = {}
    context['url_front'] = settings.URL_FRONT

    return context


""" DATASET COLLECTIONS """
""" datasets only collection 
    uses ds_collection_builder.html
"""
class DatasetCollectionCreateView(LoginRequiredMixin, CreateView):
  form_class = CollectionModelForm
  template_name = 'collection/ds_collection_builder.html'
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
  template_name = 'collection/ds_collection_builder.html'
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
        note = 'collection id: '+ str(obj.id) + ' by '+ self.request.user.username,
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

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

    context['ds_list'] = datasets
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
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
    context['media_url'] = settings.MEDIA_URL

    print('DatasetCollectionBrowseView get_context_data() kwargs:',self.kwargs)
    print('DatasetCollectionBrowseView get_context_data() request.user',self.request.user)
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
    context['updates'] = {}
    context['coll'] = coll
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

class CollectionDeleteView(DeleteView):
  template_name = 'collection/collection_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    return reverse('data-collections')
