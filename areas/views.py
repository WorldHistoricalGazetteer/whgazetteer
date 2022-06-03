# areas.views (study areas)
from django.conf import settings
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.views.generic import (CreateView, UpdateView, DeleteView )

from .forms import AreaModelForm
from .models import Area

class AreaCreateView(CreateView):
    form_class = AreaModelForm
    template_name = 'areas/area_create.html'
    queryset = Area.objects.all()

    # if called from reconciliation addtask, return there
    def get_form_kwargs(self, **kwargs):
        kwargs = super(AreaCreateView, self).get_form_kwargs()
        print('kwargs', kwargs)
        redirect = self.request.GET.get('next')+'#addtask' if 'next' in self.request.GET else ''
        print('GET in AreaCreate()',self.request.GET)
        #print('redirect',redirect)
        if redirect != '':
            self.success_url = redirect
        else:
            self.success_url = '/mystudyareas'
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
        context = super(AreaCreateView, self).get_context_data(*args, **kwargs)
        context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB        
        #print('args',args,kwargs)
        context['action'] = 'create'
        #context['referrer'] = self.request.POST.get('referrer')
        return context


class AreaDeleteView(DeleteView):
    template_name = 'areas/area_delete.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Area, id=id_)

    def get_success_url(self):
        return reverse('data-areas')

#
# detail & update
#
class AreaUpdateView(UpdateView):
    form_class = AreaModelForm
    template_name = 'areas/area_create.html'
    # success_url = '/mystudyareas'

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
        context['mbtokenmb'] = settings.MAPBOX_TOKEN_MB
        return context

