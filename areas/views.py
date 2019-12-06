# areas.views

from django.http import HttpResponseRedirect
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import (
    CreateView,
    ListView,
    DetailView,
    UpdateView,
    DeleteView )

from .forms import AreaModelForm, AreaDetailModelForm
from .models import Area

class AreaCreateView(CreateView):
    form_class = AreaModelForm
    template_name = 'areas/area_create.html'
    queryset = Area.objects.all()
    
    # ** commented for referrer redirect
    #success_url = '/dashboard'

    # ** ADDED for referrer redirect
    def get_form_kwargs(self, **kwargs):
        kwargs = super(AreaCreateView, self).get_form_kwargs()
        redirect = self.request.GET.get('next')
        print('redirect in get_form_kwargs():',redirect)
        if redirect != None:
            self.success_url = redirect
        else:
            self.success_url = '/dashboard'
        #print('cleaned_data in get_form_kwargs()',form.cleaned_data)
        if redirect:
            if 'initial' in kwargs.keys():
                kwargs['initial'].update({'next': redirect})
            else:
                kwargs['initial'] = {'next': redirect}
        print('kwargs in get_form_kwargs():',kwargs)
        return kwargs
    # ** END
    
    def form_valid(self, form):
        context={}
        if form.is_valid():
            print('form is valid, cleaned_data',form.cleaned_data)
        else:
            print('form not valid', form.errors)
            context['errors'] = form.errors
        return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(AreaCreateView, self).get_context_data(*args, **kwargs)
        #print('args',args,kwargs)
        context['action'] = 'create'
        #context['referrer'] = self.request.POST.get('referrer')
        return context

# combines detail and update
class AreaDetailView(UpdateView):
    form_class = AreaDetailModelForm
    template_name = 'areas/area_detail.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        return '/areas/'+str(id_)+'/detail'

    def form_valid(self, form):
        context={}
        if form.is_valid():
            print('form is valid')
            print('cleaned_data: before ->', form.cleaned_data)
        else:
            print('form not valid', form.errors)
            context['errors'] = form.errors
        return super().form_valid(form)

    def get_object(self):
        print('kwargs:',self.kwargs)
        id_ = self.kwargs.get("id")
        return get_object_or_404(Area, id=id_)

    def get_context_data(self, *args, **kwargs):
        context = super(AreaDetailView, self).get_context_data(*args, **kwargs)
        id_ = self.kwargs.get("id")

        return context

class AreaDeleteView(DeleteView):
    template_name = 'areas/area_delete.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Area, id=id_)

    def get_success_url(self):
        return reverse('dashboard')

# TODO: abandon for multipurpose AreaDetailView?
class AreaUpdateView(UpdateView):
    form_class = AreaModelForm
    template_name = 'areas/area_create.html'
    success_url = '/dashboard'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Area, id=id_)

    def form_valid(self, form):
        if form.is_valid():
            print(form.cleaned_data)
            obj = form.save(commit=False)
            obj.save()
        else:
            print('form not valid', form.errors)
        return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(AreaUpdateView, self).get_context_data(*args, **kwargs)
        context['action'] = 'update'
        return context
