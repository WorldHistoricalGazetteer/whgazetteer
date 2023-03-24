# main.views

from django.conf import settings
from django.core.mail import send_mail, BadHeaderError
from django.http import HttpResponse
from django.shortcuts import render, get_object_or_404, redirect #, render_to_response
from django.urls import reverse_lazy
from django.views.generic.base import TemplateView
from collection.models import Collection
from datasets.models import Dataset
from datasets.tasks import testAdd
from places.models import Place
from bootstrap_modal_forms.generic import BSModalCreateView

from .forms import CommentModalForm, ContactForm
from elasticsearch7 import Elasticsearch
es = settings.ES_CONN
from random import shuffle
# import requests

def custom_error_view(request, exception=None):
    print('error request', request.GET.__dict__)
    return render(request, "main/500.html", {'error':'fubar'})

# experiment with MapLibre
class LibreView(TemplateView):
    template_name = 'datasets/libre.html'

    def get_context_data(self, *args, **kwargs):
        context = super(LibreView, self).get_context_data(*args, **kwargs)
        context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
        context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
        context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
        context['media_url'] = settings.MEDIA_URL
        return context


class Home2b(TemplateView):
    # template_name = 'main/home_v2a.html'
    template_name = 'main/home_v2b.html'

    def get_context_data(self, *args, **kwargs):
        context = super(Home2b, self).get_context_data(*args, **kwargs)
        
        # deliver featured datasets and collections
        f_collections = Collection.objects.exclude(featured__isnull=True)
        f_datasets = list(Dataset.objects.exclude(featured__isnull=True))
        shuffle(f_datasets)
        
        # 2 collections, rotate datasets randomly
        context['featured_coll'] = f_collections.order_by('featured')[:2]
        context['featured_ds'] = f_datasets
        context['mbtokenkg'] = settings.MAPBOX_TOKEN_KG
        context['mbtoken'] = settings.MAPBOX_TOKEN_WHG
        context['mbtokenwhg'] = settings.MAPBOX_TOKEN_WHG
        context['media_url'] = settings.MEDIA_URL
        context['base_dir'] = settings.BASE_DIR
        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False
        context['teacher'] = True if self.request.user.groups.filter(
            name__in=['teacher']).exists() else False

        return context


def statusView(request):
    context = {"status_site": "??",
               "status_database": "??",
               "status_index": "??"}

    # database
    try:
        place = get_object_or_404(Place, id=81011)
        context["status_database"] = "up" if place.title == 'Abydos' else 'error'
    except:
        context["status_database"] = "down"

    # whg index
    # TODO: 20221203 something happened to cause
    # ElasticsearchWarning: The client is unable to verify that the server is Elasticsearch due security privileges on the server side
    # try:
    #     q = {"query": {"bool": {"must": [{"match": {"place_id": "81011"}}]}}}
    #     res1 = es.search(index="whg", body=q)
    #     context["status_index"] = "up" if (res1['hits']['total'] == 1 and res1['hits']['hits'][0]['_source']['title'] == 'Abydos') \
    #         else "error"
    # except:
    #     context["status_index"] = "down"

    # celery recon task
    try:
        result = testAdd.delay(8, 8)
        context["status_tasks"] = "up" if result.get() == 16 else 'error'
    except:
        context["status_tasks"] = "down"

    return render(request, "main/status.html", {"context": context})


def contactView(request):
    print('contact request.GET', request.GET)
    sending_url = request.GET.get('from')
    if request.method == 'GET':
        form = ContactForm()
    else:
        form = ContactForm(request.POST)
        if form.is_valid():
            human = True
            name = form.cleaned_data['name']
            username = form.cleaned_data['username'] # hidden input
            subject = form.cleaned_data['subject']
            from_email = form.cleaned_data['from_email']
            message = name +' ('+username+'; '+from_email+'), on the subject of '+subject+' says: \n\n'+form.cleaned_data['message']
            subject_reply = "WHG message received"
            message_reply = '\nWe received your message concerning "'+subject+'" and will respond soon.\n\n regards,\nThe WHG project team'
            try:
                send_mail(subject, message, from_email, ["karl@kgeographer.org"])
                send_mail(subject_reply, message_reply, 'karl@kgeographer.org', [from_email])
            except BadHeaderError:
                return HttpResponse('Invalid header found.')
            return redirect('/success?return='+sending_url if sending_url else '/')
            # return redirect(sending_url)
        else:
            print('not valid, why?')
                
    return render(request, "main/contact.html", {'form': form, 'user': request.user})


def contactSuccessView(request, *args, **kwargs):
    returnurl = request.GET.get('return')
    print('return, request', returnurl, str(request.GET))
    return HttpResponse(
        '<div style="font-family:sans-serif;margin-top:3rem; width:50%; margin-left:auto; margin-right:auto;"><h4>Thank you for your message! We will reply soon.</h4><p><a href="'+returnurl+'">Return</a><p></div>')


class CommentCreateView(BSModalCreateView):
    template_name = 'main/create_comment.html'
    form_class = CommentModalForm
    success_message = 'Success: Comment was created.'
    success_url = reverse_lazy('')

    def form_valid(self, form, **kwargs):
        form.instance.user = self.request.user
        place = get_object_or_404(Place, id=self.kwargs['rec_id'])
        form.instance.place_id = place
        return super(CommentCreateView, self).form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(CommentCreateView, self).get_context_data(*args, **kwargs)
        context['place_id'] = self.kwargs['rec_id']
        return context

    # ** ADDED for referrer redirect
    def get_form_kwargs(self, **kwargs):
        kwargs = super(CommentCreateView, self).get_form_kwargs()
        redirect = self.request.GET.get('next')
        print('redirect in get_form_kwargs():', redirect)
        if redirect is not None:
            self.success_url = redirect
        else:
            self.success_url = '/mydata'
        # print('cleaned_data in get_form_kwargs()',form.cleaned_data)
        if redirect:
            if 'initial' in kwargs.keys():
                kwargs['initial'].update({'next': redirect})
            else:
                kwargs['initial'] = {'next': redirect}
        print('kwargs in get_form_kwargs():', kwargs)
        return kwargs
    # ** END
