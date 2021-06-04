from django import forms
from django.db import models
from .models import Collection

class CollectionModelForm(forms.ModelForm):
    # ** trying to return to referrer
    next = forms.CharField(required=False)
    # **
    
    class Meta:
        model = Collection
        fields = ('id','owner','title','description', 'tags', 'image_file', 'datasets')
        widgets = {
            'title': forms.TextInput(attrs={'size': 50}),
            'description': forms.Textarea(attrs={
                'rows':2,'cols': 50,'class':'textarea'
            }),
            'tags': forms.TextInput(attrs={'size': 50}),
            'datasets': forms.CheckboxSelectMultiple
        }

    def __init__(self, *args, **kwargs):
        super(CollectionModelForm, self).__init__(*args, **kwargs)

