# main.views

from django.core.mail import send_mail, BadHeaderError
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse_lazy

from main.models import Comment
from places.models import Place
from bootstrap_modal_forms.generic import BSModalCreateView

from .forms import CommentModalForm, FeedbackForm
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
import requests

def statusView(request):
    context = {"status_site":"??","status_database":"??","status_index":"??"}
    
    #site
    # TODO: requests timeout on nginx/gunicorn setup
    #site_status = requests.get("http://dev.whgazetteer.org").status_code
    #context["status_site"] = "up" if site_status == 200 else "down"
    
    # database
    try:
        place=get_object_or_404(Place,id=81011)
        context["status_database"] = "up" if place.title == 'Abydos' else 'error'
    except:
        context["status_database"] = "down"
        
    # whg02 index
    try:
        q={"query": {"bool": {"must": [{"match":{"place_id": "81011"}}]}}}
        res1 = es.search(index="whg02", body = q)
        context["status_index"] = "up" if (res1['hits']['total'] == 1 and \
            res1['hits']['hits'][0]['_source']['title'] == 'Abydos') else "error"
    except:
        context["status_index"] = "down"
        
    return render(request, "main/status.html", {"context":context})

def feedbackView(request):
    if request.method == 'GET':
        form = FeedbackForm()
    else:
        form = FeedbackForm(request.POST)
        if form.is_valid():
            subject = form.cleaned_data['subject']
            from_email = form.cleaned_data['from_email']
            message = from_email+' says: \n'+form.cleaned_data['message']
            try:
                #send_mail(subject, message, from_email, ['whgazetteer@gmail.com'])
                send_mail(subject, message, from_email, ['whg@pitt.edu', "karl.geog@gmail.com"])
            except BadHeaderError:
                return HttpResponse('Invalid header found.')
            return redirect('success')
    return render(request, "main/feedback.html", {'form': form})

def feedbackSuccessView(request):
    return render_to_response('home', message='Feedback sent, thanks!')
    #return HttpResponse('Thank you for your feedback!')
    #return redirect('/')

class CommentCreateView(BSModalCreateView):
    template_name = 'main/create_comment.html'
    form_class = CommentModalForm
    success_message = 'Success: Comment was created.'
    success_url = reverse_lazy('')
    
    def form_valid(self, form, **kwargs):
        print('form_valid() kwargs',self.kwargs)
        print('form_valid() form',form.cleaned_data)
        form.instance.user = self.request.user
        place=get_object_or_404(Place,id=self.kwargs['rec_id'])
        #place=get_object_or_404(Place,id=form.place_id)
        form.instance.place_id = place
        return super(CommentCreateView, self).form_valid(form)
        
    def get_context_data(self, *args, **kwargs):
        context = super(CommentCreateView, self).get_context_data(*args, **kwargs)
        context['place_id']=self.kwargs['rec_id']
        return context
        
    # ** ADDED for referrer redirect
    def get_form_kwargs(self, **kwargs):
        kwargs = super(CommentCreateView, self).get_form_kwargs()
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
    
    