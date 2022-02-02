from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.generic import (
    View, CreateView, FormView, UpdateView, DetailView, DeleteView)
from django.views.generic.list import ListView

from .forms import ResourceModelForm
from .models import *
from main.models import Log

#
# TeachingPortalView()
# displays essay and gallery of resources

class TeachingPortalView(ListView):
  redirect_field_name = 'redirect_to'

  context_object_name = 'resource_list'
  template_name = 'resources/teaching.html'
  model = Resource

  def get_queryset(self):
    # original qs
    qs = super().get_queryset()
    # return qs.filter(public=True).order_by('pub_date', 'title')
    return qs.filter(public=True, featured__isnull=True).order_by('?')

  def get_context_data(self, *args, **kwargs):
    context = super(TeachingPortalView, self).get_context_data(*args, **kwargs)
    context['beta_or_better'] = True if self.request.user.groups.filter(
        name__in=['beta', 'admins']).exists() else False
    regions = list(Resource.objects.all().values_list('regions', flat=True))
    context['regions'] = [x for l in regions for x in l]
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    context['featured'] = Resource.objects.filter(featured__isnull=False).order_by('featured')
    return context


def handle_resource_file(f):
  with open('media/resources/'+f._name, 'wb+') as destination:
    for chunk in f.chunks():
      destination.write(chunk)

#
# create
#
class ResourceCreateView(LoginRequiredMixin, FormView):
  form_class = ResourceModelForm
  template_name = 'resources/resource_create.html'

  def get_success_url(self):
    Log.objects.create(
        category='resource',
        logtype='create',
        user_id=self.request.user.id
    )
    return reverse('dashboard')
  #
  def get_form_kwargs(self, **kwargs):
    kwargs = super(ResourceCreateView, self).get_form_kwargs()
    return kwargs

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    print('form invalid, cleaned_data', form.cleaned_data)
    context = {'form': form}
    return self.render_to_response(context=context)

  def form_valid(self, form):
    context = {}
    if form.is_valid():
      print('form is valid, cleaned_data', form.cleaned_data)
      form.save(commit=True)
    else:
      print('form not valid', form.errors)
      context['errors'] = form.errors
    return super().form_valid(form)

  # def form_valid(self, form):
  #   data = form.cleaned_data
  #   print('data from resource create form', data)
  #   context = {}
  #   user = self.request.user
  #   files = self.request.FILES.getlist('files')
  #   images = self.request.FILES.getlist('images')
  #   print('resources FILES[files]', files)
  #   print('resources FILES[images]', images)

    # save to media/resources
    for f in files:
      # handle_resource_file(f)
      print('file', f, type(f))
      ResourceFile.objects.create(
        file = f
      )
      
    for i in images:
      # handle_resource_file(i)
      print('image', i, type(i))  # Do something with each file.
      ResourceImage.objects.create(
        file=f
      )

    form.save(commit=True)

    return redirect('/dashboard')

    # create 

  # def post(self, request, *args, **kwargs):
  #   print('ResourceCreate() request', request)
  #   form_class = self.get_form_class()
  #   form = self.get_form(form_class)
  #   files = request.FILES.getlist('files')
  #   images = request.FILES.getlist('images')
  #   if form.is_valid():
  #     for f in files:
  #       print('file', f)  # Do something with each file.
  #     for i in images:
  #       print('image', i)  # Do something with each file.
  #     return self.form_valid(form)
  #   else:
  #     print('invalid form', form)
  #     return self.form_invalid(form)



    # return self.form_valid(form)
    # return reverse('dashboard')
    # return self.render_to_response(context=context)

    # saves a Resource object in resources table

    # TODO: handle multiple files
    # https://docs.djangoproject.com/en/2.2/topics/http/file-uploads/
    # files = self.request.FILES.getlist('files')
    # for f in files:
    #   handle_uploaded_file(f)
    # else:
    #   print('form not valid', form.errors)
    #   context['errors'] = form.errors
    # return super().form_valid(form)

  # def get_context_data(self, *args, **kwargs):
  #   context = super(ResourceCreateView,
  #                   self).get_context_data(*args, **kwargs)
  #   context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
  #   user = self.request.user
  #   #_id = self.kwargs.get("id")
  #   print('ResourceCreate() user', user)

  #   context['action'] = 'create'
  #   return context

#
# update (edit)
#
class ResourceUpdateView(UpdateView):
  form_class = ResourceModelForm
  template_name = 'resources/resource_create.html'
  success_url = '/dashboard'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Resource, id=id_)

  def form_valid(self, form):
    if form.is_valid():
      print(form.cleaned_data)
      obj = form.save(commit=False)
      obj.save()
      Log.objects.create(
          # category, logtype, "timestamp", subtype, note, dataset_id, user_id
          category='resource',
          logtype='update',
          note='resource id: ' + str(obj.id) + \
          ' by ' + self.request.user.username,
          user_id=self.request.user.id
      )
    else:
      print('form not valid', form.errors)
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    context = super(ResourceUpdateView,
                    self).get_context_data(*args, **kwargs)
    user = self.request.user
    _id = self.kwargs.get("id")
    print('ResourceUpdateView() kwargs', self.kwargs)

    context['action'] = 'update'
    context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
    return context

#
# detail (public view, no edit)
#
class ResourceDetailView(DetailView):
  template_name = 'resources/resource_detail.html'

  model = Resource

  def get_context_data(self, **kwargs):
    context = super(ResourceDetailView, self).get_context_data(**kwargs)
    id_ = self.kwargs.get("pk")
    print('ResourceDetailView(), kwargs', self, self.kwargs)

    context['primary'] = ResourceFile.objects.filter(resource_id = id_, filetype = 'primary')
    context['supporting'] = ResourceFile.objects.filter(
        resource_id=id_, filetype = 'supporting')
    context['images'] = ResourceImage.objects.filter(resource_id = id_)

    return context

#
# delete (cascade)
#
class ResourceDeleteView(DeleteView):
  template_name = 'resources/resource_delete.html'

  def get_object(self):
    id_ = self.kwargs.get("id")
    return get_object_or_404(Resource, id=id_)

  def get_success_url(self):
    return reverse('dashboard')
