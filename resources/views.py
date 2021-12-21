from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.generic import (
    View, CreateView, UpdateView, DetailView, DeleteView)

from .forms import ResourceModelForm
from .models import *
from main.models import Log

#
# create
#
class ResourceCreateView(LoginRequiredMixin, CreateView):
  form_class = ResourceModelForm
  template_name = 'resources/resource_create.html'
  queryset = Resource.objects.all()

  def get_success_url(self):
    Log.objects.create(
        # category, logtype, "timestamp", subtype, note, dataset_id, user_id
        category='resource',
        logtype='create',
        note='created resource id: '+str(self.object.id),
        user_id=self.request.user.id
    )
    return reverse('dashboard')
  #

  def get_form_kwargs(self, **kwargs):
    kwargs = super(ResourceCreateView, self).get_form_kwargs()
    return kwargs

  def form_invalid(self, form):
    print('form invalid...', form.errors.as_data())
    context = {'form': form}
    return self.render_to_response(context=context)


  def form_valid(self, form):
    context = {}
    if form.is_valid():
      print('form is valid, cleaned_data', form.cleaned_data)
    # TODO: handle multiple files
    # https://docs.djangoproject.com/en/2.2/topics/http/file-uploads/
    # files = self.request.FILES.getlist('files')
    # for f in files:
    #   handle_uploaded_file(f)
    else:
      print('form not valid', form.errors)
      context['errors'] = form.errors
    return super().form_valid(form)

  def get_context_data(self, *args, **kwargs):
    context = super(ResourceCreateView,
                    self).get_context_data(*args, **kwargs)
    context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
    user = self.request.user
    #_id = self.kwargs.get("id")
    print('ResourceCreate() user', user)

    context['action'] = 'create'
    return context

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
