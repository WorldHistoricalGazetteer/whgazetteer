# datasets.formset

from django import forms
from django.db import models
from .models import Dataset, Hit

MATCHTYPES = [
  ('closeMatch','closeMatch'),
  ('exactMatch','exactMatch'),
  ('related','related'),
  ('none','no match'),]

class HitModelForm(forms.ModelForm):
  match = forms.CharField(
    initial='none',
    widget=forms.RadioSelect(choices=MATCHTYPES))
  #flag = forms.BooleanField(initial=False, required=False)
  
  class Meta:
    model = Hit
    fields = ['id','authority','authrecord_id','query_pass','score','json' ]
    hidden_fields = ['id','authority','authrecord_id','query_pass','score','json' ]
    widgets = {
      'id': forms.HiddenInput(),
      'authority': forms.HiddenInput(),
      'authrecord_id': forms.HiddenInput(),
      'json': forms.HiddenInput()
    }
    
  def __init__(self, *args, **kwargs):
    super(HitModelForm, self).__init__(*args, **kwargs)

    for key in self.fields:
      self.fields[key].required = False     

class DatasetDetailModelForm(forms.ModelForm):
  class Meta:
    model = Dataset
    fields = ('id','name','description','mapbox_id')
    widgets = {
      'description': forms.Textarea(attrs={
            'rows':1,'cols': 40,'class':'textarea','placeholder':'brief description'}),
    }

  def __init__(self, *args, **kwargs):
    super(DatasetDetailModelForm, self).__init__(*args, **kwargs)

class DatasetModelForm(forms.ModelForm):
  # trying to generate a unique label  
  class Meta:
    model = Dataset
    fields = ('id','name','label','description','file','format','datatype',
              'delimiter','status','owner','header','numrows','spine','uri_base')
    widgets = {
      'description': forms.Textarea(attrs={
            'rows':2,'cols': 40,'class':'textarea',
              'placeholder':'brief description'}),
      'format': forms.Select(),
      'datatype': forms.Select(),
    }
    #initial = {'format': 'delimited', 'datatype': 'places', 'uri_base': 'http://whgazetteer.org/api/places/'}
    initial = {'datatype': 'places', 'uri_base': 'fubar'}

  def unique_label(self, *args, **kwargs):
    label = self.cleaned_content['name'][:16]+'_'+user.first_name[:1]+user.last_name[:1]
    return label
    # TODO: test uniqueness somehow

  def __init__(self, *args, **kwargs):
    self.format = 'delimited'
    self.datatype = 'place'
    super(DatasetModelForm, self).__init__(*args, **kwargs)
