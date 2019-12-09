from django import forms
from django.contrib.auth.models import User, Group
from django.db import models


class UserProfileModelForm(forms.ModelForm):
    
    class Meta:
        model = User
        fields = ('username','email','password','first_name','last_name',
                  #'affiliation','user_type','name'
                )
        #widgets = {
        #'description': forms.Textarea(attrs={
          #'rows':1,'cols': 40,'class':'textarea','placeholder':'brief description'}),
        #}
