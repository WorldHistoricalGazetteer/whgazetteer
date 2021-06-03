# collection.views (collections)

from django.conf import settings
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView )

from datasets.utils import hully
from .forms import CollectionModelForm
from .models import *

# adds selected dataset to collection, returns json for display
class AddDatasetView(View):
  @staticmethod
  def get(request):
    print('ListDatasetView() GET', request.GET)
    coll = Collection.objects.get(id=request.GET['coll_id'])
    ds = Dataset.objects.get(id=request.GET['ds_id'])
    coll.datasets.add(ds)
    result = {
      "title": ds.title,
      "label": ds.label,
      "id": ds.id,
      "description": ds.description,
      "numrows": ds.places.count()
    }
    return JsonResponse(result, safe=False)

def remove_dataset(request, *args, **kwargs):
  print('args', args)
  print('kwargs', kwargs)
  coll = Collection.objects.get(id=kwargs['coll_id'])
  ds = Dataset.objects.get(id=kwargs['ds_id'])
  coll.datasets.remove(ds)
  
  return HttpResponseRedirect(request.META.get('HTTP_REFERER'))
  
# TODO: merge create and update views (templates are the same)
class CollectionCreateView(CreateView):
  #print('CollectionCreateView()')
  form_class = CollectionModelForm
  template_name = 'collection/collection_create.html'
  queryset = Collection.objects.all()

  # if called from reconciliation addtask, return there
  def get_form_kwargs(self, **kwargs):
    kwargs = super(CollectionCreateView, self).get_form_kwargs()
    redirect = self.request.GET.get('next')+'#addtask' if 'next' in self.request.GET else ''
    print('GET in CollectionCreate()',self.request.GET)
    #print('redirect',redirect)
    if redirect != '':
      self.success_url = redirect
    else:
      self.success_url = '/dashboard'
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
    context = super(CollectionCreateView, self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    user = self.request.user
    #_id = self.kwargs.get("id")
    print('CollectionCreate() user', user)

    #qs = CollectionDataset.objects.filter(collection_id = _id)
    #coll_set = [cd.dataset for cd in qs]
    #datasets = Collection.objects.get(id=_id).dataset_set.all()
    datasets = []
    # owners create collections from their datasets
    ds_select = [obj for obj in Dataset.objects.all() if user in obj.owners or user.is_superuser]

    context['action'] = 'create'
    context['ds_select'] = ds_select
    context['coll_set'] = datasets

    return context


class CollectionDetailView(DetailView):
  template_name = 'collection/collection_detail.html'

  model = Collection

  def get_context_data(self, **kwargs):
    context = super(CollectionDetailView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('self, kwargs',self, self.kwargs)

    #qs = CollectionDataset.objects.filter(collection_id = id_)

    #g_list =[g.jsonb for g in place.geoms.all()]
    ## make everything a simple polygon hull for spatial filter
    #qobj['geom'] = hully(g_list)
    #GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list])

    #datasets = [cd.dataset for cd in qs]
    datasets = self.dataset_set
    # compute bounding boxes
    bboxes = []
    #from shapely.geometry import shape
    for ds in datasets:
      dsgeom = [g for g in ds.geometries.all()]
      hull = hully(dsgeom)
      bboxes.append(hull)

    context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG

    context['ds_list'] = datasets
    context['bboxes'] = bboxes
    return context

  #def get_object(self):
    #id_ = self.kwargs.get("id")
    #return get_object_or_404(Collection, id=id_)

  #def get_success_url(self):
    #return reverse('dashboard')

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
  #print('CollectionUpdateView()')    
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

