from django import forms
from django.db import models
# from leaflet.forms.widgets import LeafletWidget
from .models import Area

class AreaModelForm(forms.ModelForm):
    # ** trying to return to referrer
    next = forms.CharField(required=False)
    # **
    
    class Meta:
        model = Area
        #exclude = tuple()
        fields = ('id','type','owner','title','description','ccodes','geojson','next')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows':2,'cols': 40,'class':'textarea'
            }),
            'ccodes': forms.TextInput(attrs={
                'placeholder':'2-letter codes, e.g. br,ar'
            }),
            'geojson': forms.Textarea(attrs={
                'rows':2,'cols': 40,'class':'textarea',
                'placeholder':''
            }),
        }


    def __init__(self, *args, **kwargs):
        super(AreaModelForm, self).__init__(*args, **kwargs)

class AreaDetailModelForm(forms.ModelForm):
    class Meta:
        model = Area
        fields = ('id','type','owner','title','description','ccodes','geojson')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows':1,'cols': 60,'class':'textarea','placeholder':'brief description'}),
        }

    def __init__(self, *args, **kwargs):
        super(AreaDetailModelForm, self).__init__(*args, **kwargs)
