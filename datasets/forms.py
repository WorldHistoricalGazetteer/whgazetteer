# datasets.formset

from django import forms
from django.db import models
from django.forms import ClearableFileInput
from datasets.models import Dataset, Hit, DatasetFile
from main.choices import FORMATS, DATATYPES, STATUS

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


class DatasetFileModelForm(forms.ModelForm):
  class Meta:
    model = DatasetFile
    fields = ['file','rev','uri_base','format','dataset_id','delimiter',
              'status','accepted_date','header','numrows']
    widgets = { 'file': ClearableFileInput() }    
  

class DatasetDetailModelForm(forms.ModelForm):
  
  class Meta:
    model = Dataset
    # file fields = ('file','rev','uri_base','format','dataset_id','delimiter',
    #   'status','accepted_date','header','numrows')
    fields = ('id','label','title','description','datatype')
    widgets = {
      'description': forms.Textarea(attrs={
        'rows':2,'cols': 40,'class':'textarea','placeholder':'brief description'}),
    }
  
  # fields for creating new DatasetFile record from form
  file = forms.FileField()
  rev = forms.IntegerField()
  uri_base = forms.URLField()
  format = forms.ChoiceField(choices=FORMATS)
  delimiter = forms.CharField()
  status = forms.ChoiceField(choices=STATUS)
  accepted_date = forms.DateTimeField()
  header = forms.CharField()
  numrows = forms.IntegerField()
  
  def __init__(self, *args, **kwargs):
    super(DatasetDetailModelForm, self).__init__(*args, **kwargs)
    for field in self.fields.values():
      field.error_messages = {'required':'The field {fieldname} is required'.format(
                  fieldname=field.label)}    

class DatasetModelForm(forms.ModelForm):
  # trying to generate a unique label  
  class Meta:
    model = Dataset
    fields = ('id','title','label','description','file','format','datatype',
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

  #def unique_label(self, *args, **kwargs):
    #label = self.cleaned_content['name'][:16]+'_'+user.first_name[:1]+user.last_name[:1]
    #return label
    # TODO: test uniqueness somehow

  def __init__(self, *args, **kwargs):
    self.format = 'delimited'
    self.datatype = 'place'
    super(DatasetModelForm, self).__init__(*args, **kwargs)

  def clean_label(self): 
    label = self.cleaned_data['label']
    print(label)
    labels = Dataset.objects.values_list('label', flat=True)
    if label in labels:
      raise forms.ValidationError("Dataset label must be unique")
    return label