# traces.forms
from django import forms
from django.db import models
from .models import TraceAnnotation

class TraceAnnotationModelForm(forms.ModelForm):

	class Meta:
		model = TraceAnnotation
		fields = ('id', 'src_id', 'collection', 'place', 'note', 'start', 'end', 'when',
		          'relation', 'sequence', 'trace_type', 'motivation', 'creator')
		widgets = {
			# 'trace_type': forms.Select(),
			'note': forms.Textarea(attrs={
				'rows': 2, 'cols': 25, 'class': 'textarea'
			}),
			'start': forms.TextInput(attrs={'size': 12}),
			'end': forms.TextInput(attrs={'size': 12}),
			'relation': forms.TextInput(attrs={'size': 20}),
			'sequence': forms.TextInput(attrs={'size': 2}),
			# 'relation': forms.Select()
		}
		# 	'title': forms.TextInput(attrs={'size': 50}),
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
