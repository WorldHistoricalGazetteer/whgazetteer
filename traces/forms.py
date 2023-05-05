# traces.forms
from django import forms
from django.db import models
from .models import TraceAnnotation

# date validator
def iso8601_dates(value):
	print('date value', value)
	if value and len(value) < 3:
		raise forms.ValidationError("We're ISO8601 here!")

class TraceAnnotationModelForm(forms.ModelForm):
	# start = forms.CharField(widget=forms.TextInput(attrs={'size': 11}))
	# end = forms.CharField(widget=forms.TextInput(attrs={'size': 11}))
	# start = forms.CharField(validators=[iso8601_dates], widget=forms.TextInput(attrs={'size': 11}))
	# end = forms.CharField(validators=[iso8601_dates], widget=forms.TextInput(attrs={'size': 11}))

	class Meta:
		model = TraceAnnotation
		fields = ('id', 'note', 'relation', 'start', 'end', 'sequence', 'anno_type', 'motivation',
		          'owner', 'collection', 'place', 'image_file')
		widgets = {
			# 'collection': forms.TextInput(attrs={'size': 4}),
			'note': forms.Textarea(attrs={'rows':4,'cols': 38,'class':'textarea'}),
			'collection': forms.TextInput(attrs={'size': 4}),
			'place': forms.TextInput(attrs={'size': 16}),
			'relation': forms.Select(),
			'start': forms.TextInput(attrs={'size': 11, 'placeholder':'yyyy-mm-dd'}),
			'end': forms.TextInput(attrs={'size': 11, 'placeholder':'yyyy-mm-dd'}),
			# 'image_file': forms.ImageField()
			'image_file': forms.FileInput()
		}


	def __init__(self, *args, **kwargs):
		super(TraceAnnotationModelForm, self).__init__(*args, **kwargs)
