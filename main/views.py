# main.views

from django.core.mail import send_mail, BadHeaderError
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse_lazy

from main.models import Comment
from places.models import Place
from bootstrap_modal_forms.generic import BSModalCreateView

from .forms import CommentModalForm, FeedbackForm

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
    #return HttpResponse('Thank you for your feedback!')
    return redirect('/')

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
    
    