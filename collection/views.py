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
from collection.models import Collection, CollectionImage
from main.models import Log
from places.models import PlaceGeom
from traces.forms import TraceAnnotationModelForm
from itertools import chain

""" add list of >=1 places to collection """
def add_places(request, *args, **kwargs):
  if request.method == 'POST':
    print('add_places request', request.POST)
    coll = Collection.objects.get(id=request.POST['collection'])
    place_list = [int(i) for i in request.POST['place_list'].split(',')]
    for p in place_list:
      coll.places.add(p)
    return JsonResponse({'result': str(len(place_list))+' places added, we think'}, safe=False)

""" create place collection on the fly
    return id for adding place(s) to it 
"""
def flash_collection_create(request, *args, **kwargs):
  print('flash_collection_create request', request)
  print('flash_collection_create kwargs', kwargs)
  if request.method == 'POST':
    collobj = Collection.objects.create(
      owner = request.user,
      title = 'title',
      type = 'place',
      description = 'new collection',
      keywords = '{replace, these, please}'
    )
    collobj.save()
    result = {"id": collobj.id, 'title': collobj.title}
  return JsonResponse(result, safe=False)

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

from django.forms.models import inlineformset_factory
CollectionImageFormset = inlineformset_factory(
    Collection, CollectionImage, fields=('image','caption','uri','license'), extra=1
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
    context = super(PlaceCollectionCreateView, self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB

    datasets = []
    # add images
    if self.request.POST:
      context["images"] = CollectionImageFormset(self.request.POST)
    else:
      context["images"] = CollectionImageFormset()

    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or
                 user in obj.collaborators or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    return context

  def form_valid(self, form):
    context = self.get_context_data()
    images = context['images']
    self.object = form.save()
    if images.is_valid():
      images.instance = self.object
      images.save()

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
  success_url = '/mycollections'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    id_ = self.kwargs.get("id")
    return '/collections/'+str(id_)+'/summary_pl'

  def form_valid(self, form):
    print('referrer', self.request.META.get('HTTP_REFERER'))
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
    context = super(PlaceCollectionUpdateView, self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('PlaceCollectionUpdateView() kwargs', self.kwargs)

    datasets = self.object.datasets.all()

    # COLL: get places from datasets, then collection.places
    # qs_list = [d.places.all() for d in datasets]
    # COLL: merge querysets
    # coll_places = list(chain(*qs_list))

    form_anno = TraceAnnotationModelForm(self.request.GET or None)
    # anno_form = TraceAnnotationModelForm(self.request.GET or None, prefix="sch")
    # populates dropdown
    ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]

    context['action'] = 'update'
    context['ds_select'] = ds_select
    context['coll_dsset'] = datasets

    # test: send single anno form to template
    context['form_anno'] = form_anno

    # COLL: merged places
    # context['coll_places'] = coll_places
    context['coll_places'] = self.object.places_all

    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
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

    # gather bounding boxes
    bboxes = [ds.bounds for ds in datasets]

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
    context['whgteam'] = User.objects.filter(groups__name='whg_team')

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

    context['places'] = coll.places.all()
    context['ds_list'] = coll.ds_list
    #context['bboxes'] = bboxes
    context['updates'] = {}
    context['coll'] = coll
    context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'admins']).exists() else False

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
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
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

    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
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
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
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
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
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
  return redirect('/collections/'+str(cid)+'/update_pl')
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

class CollectionDeleteView(DeleteView):
  template_name = 'collection/collection_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Collection, id=id_)

  def get_success_url(self):
    return reverse('data-collections')
