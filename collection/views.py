# collection.views (collections)

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView )

#from datasets.utils import hully
from .forms import CollectionModelForm
from .models import *
from main.models import Log
from places.models import PlaceGeom
from traces.forms import TraceAnnotationModelForm
from itertools import chain

""" gl map needs this """
def fetch_geojson_coll(request, *args, **kwargs):
  print('fetch_geojson_coll kwargs',kwargs)
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
  return JsonResponse(fcoll, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

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
      "description": ds.description[:100]+'...',
      "numrows": ds.places.count()
    }
    return JsonResponse(result, safe=False)

# removes dataset from collection, refreshes page
def remove_dataset(request, *args, **kwargs):
  #print('kwargs', kwargs)
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  print('remove_dataset(): coll, ds', coll, ds)
  coll.datasets.remove(ds)

  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

""" BETA: collections from places or datasets """
class CollectionCreateBetaView(LoginRequiredMixin, CreateView):
  form_class = CollectionModelForm
  template_name = 'collection/collection_create_beta.html'
  queryset = Collection.objects.all()

  def get_success_url(self):
    Log.objects.create(
      # category, logtype, "timestamp", subtype, note, dataset_id, user_id
      category = 'collection',
      logtype = 'create',
      note = 'created collection id: '+str(self.object.id),
      user_id = self.request.user.id
    )
    # return to update page after create
    return reverse('collection:collection-update-beta', kwargs = {'id':self.object.id})

  def get_form_kwargs(self, **kwargs):
    kwargs = super(CollectionCreateBetaView, self).get_form_kwargs()
    return kwargs

  def form_invalid(self,form):
    print('form invalid...',form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    context={}
    if form.is_valid():
      print('form is valid, cleaned_data',form.cleaned_data)
    else:
      print('form not valid', form.errors)
      context['errors'] = form.errors
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    user = self.request.user
    print('CollectionCreateBetaView() user', user)
    context = super(CollectionCreateBetaView, self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    datasets = []
    places = []
    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or
      user in obj.collaborators or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context

""" v2 default """
class CollectionCreateView(LoginRequiredMixin, CreateView):
  form_class = CollectionModelForm
  template_name = 'collection/collection_create.html'
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
    kwargs = super(CollectionCreateView, self).get_form_kwargs()
    return kwargs

  def form_invalid(self,form):
    print('form invalid...',form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    context={}
    if form.is_valid():
      print('form is valid, cleaned_data',form.cleaned_data)
    else:
      print('form not valid', form.errors)
      context['errors'] = form.errors
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    user = self.request.user
    context = super(CollectionCreateView, self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    datasets = []
    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or
      user in obj.collaborators or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context

""" BETA: public collection view, contents, bboxes on a map """
class CollectionDetailBetaView(DetailView):
  template_name = 'collection/collection_detail_beta.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(CollectionDetailBetaView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('CollectionDetailView(), kwargs',self, self.kwargs)

    datasets = self.object.datasets.all()

    # compute bounding boxes
    bboxes = [
      {"type":"Feature",
       "properties": {"id":ds.id, "label": ds.label, "title": ds.title},
       "geometry":ds.bounds} for ds in datasets]

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG

    context['ds_list'] = datasets
    context['bboxes'] = bboxes
    return context

""" public collection view, datasets, bboxes on a map """
class CollectionDetailView(DetailView):
  template_name = 'collection/collection_detail.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(CollectionDetailView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('CollectionDetailView(), kwargs',self, self.kwargs)

    datasets = self.object.datasets.all()

    # compute bounding boxes
    # bboxes = [
    #   {"type":"Feature",
    #    "properties": {"id":ds.id, "label": ds.label, "title": ds.title},
    #    "geometry":ds.bounds} for ds in datasets]
    bboxes = [ds.bounds for ds in datasets]


    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG

    context['ds_list'] = datasets
    context['bboxes'] = bboxes
    return context


""" BETA: browse collection *all* places """
class CollectionPlacesBetaView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Collection
  template_name = 'collection/collection_places_beta.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/places'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionPlacesBetaView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['media_url'] = settings.MEDIA_URL

    print('CollectionPlacesView get_context_data() kwargs:',self.kwargs)
    print('CollectionPlacesView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")
    # compute bounding boxes

    coll = get_object_or_404(Collection, id=id_)
    # "geotypes":ds.geotypes,
    # datasets = [{"id":ds.id,"label":ds.label,"title":ds.title} for ds in coll.ds_list]
                 # "bbox": ds.bounds } for ds in coll.datasets.all()]
    #bboxes = [{"id":ds['id'], "geometry":ds['bounds']} for ds in datasets]

    # placeset = coll.places.all()
    # context['places'] = placeset

    context['places'] = coll.trace_places_all
    context['ds_list'] = coll.ds_list
    #context['bboxes'] = bboxes
    context['updates'] = {}
    context['coll'] = coll
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

""" browse collection dataset places """
class CollectionPlacesView(DetailView):
  login_url = '/accounts/login/'
  redirect_field_name = 'redirect_to'

  model = Collection
  template_name = 'collection/collection_places.html'

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/places'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_context_data(self, *args, **kwargs):
    context = super(CollectionPlacesView, self).get_context_data(*args, **kwargs)
    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['media_url'] = settings.MEDIA_URL

    print('CollectionPlacesView get_context_data() kwargs:',self.kwargs)
    print('CollectionPlacesView get_context_data() request.user',self.request.user)
    id_ = self.kwargs.get("id")
    # compute bounding boxes

    coll = get_object_or_404(Collection, id=id_)
    # "geotypes":ds.geotypes,
    # datasets = [{"id":ds.id,"label":ds.label,"title":ds.title} for ds in coll.ds_list]
                 # "bbox": ds.bounds } for ds in coll.datasets.all()]
    #bboxes = [{"id":ds['id'], "geometry":ds['bounds']} for ds in datasets]

    placeset = coll.places.all()
    context['places'] = placeset
    # context['places'] = placeset
    context['ds_list'] = coll.ds_list
    #context['bboxes'] = bboxes
    context['updates'] = {}
    context['coll'] = coll
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

    return context

""" BETA: annotate collection with place """
# def annotate(request, cid, pid):
def annotate(request, *args, **kwargs):
  coll = get_object_or_404(Collection, id=kwargs.get('id'))
  cid = kwargs.get('id')
  for k, v in request.POST.items():
    print(k, v)
  form = TraceAnnotationModelForm(request.POST)
  if form.is_valid():
    pid = request.POST.get('place')
    obj = form.save(commit=False)
    obj.save()
    context = {'id':cid, 'pid':pid}
  else:
    print('trace form not valid', form.errors)

  # collections/38/updatebeta
  return redirect('/collections/'+str(cid)+'/updatebeta')
  # return render(request, 'collections/collection_create_beta.html', context)
  # return render(request, 'collections/'+str(cid)+'/updatebeta', context)
  # return HttpResponseRedirect(request.META.get('HTTP_REFERER'), context)

  #     Log.objects.create(
  #       # category, logtype, "timestamp", subtype, note, dataset_id, user_id
  #       category = 'collection',
  #       logtype = 'annotation',
  #       note = 'trace annotation: '+ str(obj.id) + '. collection ' + cid +\
  #              'w/place: '+str(pid)+ 'by '+ user.username
  #     )
  #   else:
  #     print('trace form not valid', form.errors)

""" BETA: update collection *** function-based view*** """
class CollectionUpdateBetaView(UpdateView):
  form_class = CollectionModelForm
  template_name = 'collection/collection_create_beta.html'
  success_url = '/mycollections'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

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
    context = super(CollectionUpdateBetaView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('CollectionUpdateBetaView() kwargs', self.kwargs)

    datasets = self.object.datasets.all()

    # COLL: get places from datasets, then collection.places
    # qs_list = [d.places.all() for d in datasets]
    # COLL: merge querysets
    # coll_places = list(chain(*qs_list))

    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    # COLL: merged places
    # context['coll_places'] = coll_places
    context['coll_places'] = self.object.trace_places_all

    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    return context

""" update collection """
class CollectionUpdateView(UpdateView):
  form_class = CollectionModelForm
  template_name = 'collection/collection_create.html'
  success_url = '/mycollections'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

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
    context = super(CollectionUpdateView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('CollectionUpdateView() kwargs', self.kwargs)

    datasets = self.object.datasets.all()

    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    return context

class CollectionDeleteView(DeleteView):
  template_name = 'collection/collection_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    return reverse('data-collections')
