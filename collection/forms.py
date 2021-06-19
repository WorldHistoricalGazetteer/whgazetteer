from django import forms
from django.db import models
from .models import Collection

class CollectionModelForm(forms.ModelForm):
    # ** trying to return to referrer
    next = forms.CharField(required=False)
    # **
    
    class Meta:
        model = Collection
        fields = ('id','owner','title','description', 'creator', 'contact', \
                  'webpage', 'keywords', 'public', 'featured', 'image_file', 'datasets')
        widgets = {
            'title': forms.TextInput(attrs={'size': 50}),
            'keywords': forms.TextInput(attrs={'size': 50}),
            'creator': forms.TextInput(attrs={'size': 50}),
            'contact': forms.TextInput(attrs={'size': 50}),
            'webpage': forms.TextInput(attrs={'size': 50}),
            'description': forms.Textarea(attrs={
                'rows':3,'cols': 49,'class':'textarea'
            }),
            'image_file':forms.FileInput(),
            'datasets': forms.CheckboxSelectMultiple,
            'featured': forms.TextInput(attrs={'size': 3})
        }

    def __init__(self, *args, **kwargs):
        super(CollectionModelForm, self).__init__(*args, **kwargs)

