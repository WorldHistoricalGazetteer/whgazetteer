from django import forms
from django.contrib.postgres.forms import SimpleArrayField
from django.db import models
from .models import Resource

class ResourceModelForm(forms.ModelForm):
    keywords = SimpleArrayField(forms.CharField())
    gradelevels = SimpleArrayField(forms.CharField())

    class Meta:
        model = Resource
        fields = ('id', 'create_date', 'pub_date', 'owner', 'title', 'type', 'description', 
            'subjects', 'gradelevels', 'keywords', 'authors', 'contact', 'webpage', 
            'files', 'images', 'public', 'featured', 'status')
        widgets = {
            'title': forms.TextInput(attrs={'size': 50}),
            'keywords': forms.TextInput(attrs={'size': 50}),
            'contact': forms.TextInput(attrs={'size': 50}),
            'webpage': forms.TextInput(attrs={'size': 50}),
            'description': forms.Textarea(attrs={
                'rows': 3, 'cols': 49, 'class': 'textarea'
            }),
            'files': forms.FileField(
                widget=forms.ClearableFileInput(attrs={'multiple': True})),
            'images': forms.FileField(
                widget=forms.ClearableFileInput(attrs={'multiple': True}))
        }

    def __init__(self, *args, **kwargs):
        super(ResourceModelForm, self).__init__(*args, **kwargs)
