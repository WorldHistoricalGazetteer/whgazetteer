# collection.views (collections)

from django.conf import settings
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.views.generic import (CreateView, UpdateView, DeleteView )

from .forms import CollectionModelForm
from .models import *

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
        #print('args',args,kwargs)
        context['action'] = 'create'
        #context['referrer'] = self.request.POST.get('referrer')
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
        context['action'] = 'update'
        context['create_date'] = self.object.create_date.strftime("%Y-%m-%d")
        context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
        #qs = CollectionDataset.objects.filter(collection_id = self.kwargs.get("id"))
        #context['datasets'] = qs
        return context

