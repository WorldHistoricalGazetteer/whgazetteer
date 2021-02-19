from django import forms
from django.db import models
from django.utils.safestring import mark_safe

from main.models import Comment
from main.choices import COMMENT_TAGS
from bootstrap_modal_forms.forms import BSModalForm
from captcha.fields import CaptchaField

class ContactForm(forms.Form):
    from_email = forms.EmailField(
        widget=forms.EmailInput(attrs={'size': 50}),
        required=True,
        label="Your email address ")
    subject = forms.CharField(
        widget=forms.TextInput(attrs={'size': 50}),
        required=True)
    message = forms.CharField(widget=forms.Textarea(attrs={'cols': 50, 'rows': 5}), required=True)
    captcha = CaptchaField()

class CommentModalForm(BSModalForm):
    
    class Meta:
        model = Comment
        # fields: user, place_id, tag, note, created
        fields = ['tag', 'note','place_id']
        hidden_fields = ['created']
        exclude = ['user','place_id']
        #exclude = ['user']
        widgets = {
            'place_id': forms.TextInput(),
            'tag': forms.RadioSelect(choices=COMMENT_TAGS,attrs={'class':'no-bullet'}),
            'note': forms.Textarea(attrs={
                'rows':2,'cols': 40,'class':'textarea'})
        }
        
    def __init__(self, *args, **kwargs):
        #self._user = kwargs.pop('user')
        super(CommentModalForm, self).__init__(*args, **kwargs)  
        self.fields['tag'].label = "Issue"
