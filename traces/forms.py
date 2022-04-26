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
		          'owner', 'collection','place')
		widgets = {
			# 'collection': forms.TextInput(attrs={'size': 4}),
			'note': forms.Textarea(attrs={'rows':2,'cols': 30,'class':'textarea'}),
			'collection': forms.TextInput(attrs={'size': 4}),
			'place': forms.TextInput(attrs={'size': 16}),
			'relation': forms.Select(),
			'start': forms.TextInput(attrs={'size': 11, 'placeholder':'yyyy-mm-dd'}),
			'end': forms.TextInput(attrs={'size': 11, 'placeholder':'yyyy-mm-dd'})
		}

	# class Meta:
	# 	model = TraceAnnotation
	# 	fields = ('id', 'src_id', 'collection', 'place', 'note', 'start', 'end', 'when',
	# 	          'relation', 'sequence', 'trace_type', 'motivation', 'creator')
	# 	widgets = {
	# 		# 'trace_type': forms.Select(),
	# 		'note': forms.Textarea(attrs={
	# 			'rows': 2, 'cols': 25, 'class': 'textarea'
	# 		}),
	# 		'start': forms.TextInput(attrs={'size': 12}),
	# 		'end': forms.TextInput(attrs={'size': 12}),
	# 		'relation': forms.TextInput(attrs={'size': 20}),
	# 		'sequence': forms.TextInput(attrs={'size': 2}),
	# 		# 'relation': forms.Select()
	# 	}
	# 	# 	'title': forms.TextInput(attrs={'size': 50}),
		# 	'keywords': forms.TextInput(attrs={'size': 50}),
		# 	'creator': forms.TextInput(attrs={'size': 50}),
		# 	'contact': forms.TextInput(attrs={'size': 50}),
		# 	'webpage': forms.TextInput(attrs={'size': 50}),

		# 	'image_file': forms.FileInput(),
		# 	'datasets': forms.CheckboxSelectMultiple,
		# 	'featured': forms.TextInput(attrs={'size': 3}),
		# 	'content': TinyMCE(attrs={'cols': 40, 'rows': 6})
		# }

	def __init__(self, *args, **kwargs):
		super(TraceAnnotationModelForm, self).__init__(*args, **kwargs)
