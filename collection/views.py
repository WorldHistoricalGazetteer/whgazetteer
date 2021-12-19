# collection.views (collections)

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView )

#from datasets.utils import hully
from .forms import CollectionModelForm
from .models import *
from main.models import Log
from places.models import PlaceGeom

# gl map needs this
def fetch_geojson_coll(request, *args, **kwargs):
  print('download_gis kwargs',kwargs)
  id_=kwargs['id']
  coll=get_object_or_404(Collection, id=id_)
  pids = [p.id for p in coll.places.all()]
  
  # build a fast FeatureCollection 
  features=PlaceGeom.objects.filter(place_id__in=pids).values_list(
    'jsonb','place_id','src_id','place__title','place__minmax', 
    'place__fclasses', 'place__dataset_id', 'place__dataset__label')
  fcoll = {"type":"FeatureCollection","features":[]}
  for f in features:
    feat={"type":"Feature",
          "properties":{"pid":f[1],"src_id":f[2],"title":f[3],"minmax":f[4],
                        "fclasses":f[5], "dsid":f[6], "dslabel":f[7]
                        },
          "geometry":f[0]}
    fcoll['features'].append(feat)
  return JsonResponse(fcoll, safe=False,json_dumps_params={'ensure_ascii':False,'indent':2})

# returns json for display
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

# TODO: merge create and update views (templates are the same)
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
    return reverse('dashboard')
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
    print('CollectionCreateView() user', user)
    context = super(CollectionCreateView, self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    datasets = []
    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all() if user in obj.owners or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context


""" list collection datasets, bboxes on a map """
class CollectionDetailView(DetailView):
  template_name = 'collection/collection_detail.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(CollectionDetailView, self).get_context_data(**kwargs)
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

""" browse all collection places """
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
    datasets = [{"id":ds.id,"label":ds.label,"title":ds.title, \
                 "bbox": ds.bounds } for ds in coll.datasets.all()]
    #bboxes = [{"id":ds['id'], "geometry":ds['bounds']} for ds in datasets]

    placeset = coll.places.all()
    context['places'] = placeset
    context['ds_list'] = datasets
    #context['bboxes'] = bboxes
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
    return reverse('dashboard')

#
# detail & update
#
class CollectionUpdateView(UpdateView):
  form_class = CollectionModelForm
  template_name = 'collection/collection_create.html'
  success_url = '/dashboard'

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
    ds_select = [obj for obj in Dataset.objects.all() if user in obj.owners or user.is_superuser]

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets
    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    return context

