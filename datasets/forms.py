# datasets.forms

import codecs, os, tempfile
from chardet import detect
from django import forms
from django.core.exceptions import ValidationError
from datasets.models import Dataset, Hit, DatasetFile
from datasets.static.hashes import mimetypes_plus as mthash_plus
from main.choices import FORMATS, STATUS_FILE

MATCHTYPES = [
    ('closeMatch', 'closeMatch'),
    ('none', 'no match'),
]


class HitModelForm(forms.ModelForm):
    match = forms.CharField(
        initial='none',
        widget=forms.RadioSelect(choices=MATCHTYPES))

    class Meta:
        model = Hit
        fields = ['id', 'authority', 'authrecord_id',
                  'query_pass', 'score', 'json']
        hidden_fields = ['id', 'authority',
                         'authrecord_id', 'query_pass', 'score', 'json']
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
        fields = ('dataset_id', 'file', 'rev', 'format', 'delimiter',
                  'df_status', 'datatype', 'header', 'numrows')

    def __init__(self, *args, **kwargs):
        super(DatasetFileModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}

class DatasetDetailModelForm(forms.ModelForm):
    class Meta:
        model = Dataset
        # file fields = ('file','rev','uri_base','format','dataset_id','delimiter',
        #   'status','accepted_date','header','numrows')
        fields = ('owner', 'creator', 'contributors', 'source', 'id', 'label', 'title', 'uri_base', 'description',
                  'citation', 'datatype', 'numlinked', 'webpage', 'featured', 'image_file', 'public')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 55, 'class': 'textarea', 'placeholder': 'Brief description'}),
            'citation': forms.Textarea(attrs={
                'rows': 2, 'cols': 55, 'class': 'textarea'}),
            'creator': forms.TextInput(attrs={'size': 50}),
            'source': forms.TextInput(attrs={'size': 50}),
            'contributors': forms.TextInput(attrs={'size': 50}),
            'webpage': forms.TextInput(attrs={'size': 50}),
            # 'uri_base': forms.TextInput(attrs={'size': 50}),
            'uri_base': forms.URLInput(attrs={'size': 50}),
            'featured': forms.TextInput(attrs={'size': 4})
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            print("there's a space goddamit")
            raise forms.ValidationError('label cannot contain any space')
        return label

    file = forms.FileField(required=False)
    uri_base = forms.URLField(
        widget=forms.URLInput(
            attrs={'placeholder': 'Leave blank unless changed', 'size': 40}
        ),
        required=False
    )
    format = forms.ChoiceField(choices=FORMATS, initial="delimited")

    def __init__(self, *args, **kwargs):
        super(DatasetDetailModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}

class DatasetCreateEmptyModelForm(forms.ModelForm):
    class Meta:
        model = Dataset
        fields = ('owner', 'id', 'title', 'label', 'datatype', 'description', 'uri_base', 'public',
                  'creator', 'contributors', 'source', 'webpage', 'image_file', 'featured', 'ds_status')

        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 45, 'class': 'textarea', 'placeholder': 'Brief description'}),
            'uri_base': forms.URLInput(attrs={
                    'placeholder': 'Leave blank unless record IDs are URIs', 'size': 45}),
            'title': forms.TextInput(attrs={'size': 45}),
            'label': forms.TextInput(attrs={'placeholder': '20 char max; no spaces','size': 22}),
            'creator': forms.TextInput(attrs={'size': 45}),
            'source': forms.TextInput(attrs={'size': 45}),
            'featured': forms.TextInput(attrs={'size': 4}),
            'webpage': forms.URLInput(attrs={'size': 45, 'placeholder': 'Project home page, if any'})
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            raise forms.ValidationError('Label cannot contain any spaces; replace with underscores (_)')
        return label

    def __init__(self, *args, **kwargs):
        super(DatasetCreateEmptyModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}

"""
  DatasetUpload()
  alternate to DatasetCreateModelForm(); bot-guided
"""
class DatasetUploadForm(forms.ModelForm):

    class Meta:
        model = Dataset
        # file fields = ('file','rev','uri_base','format','dataset_id','delimiter',
        #   'status','accepted_date','header','numrows')
        fields = ('owner', 'id', 'title', 'label', 'datatype', 'description', 'uri_base', 'public',
                  'creator', 'contributors', 'source', 'webpage', 'image_file', 'featured')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 45, 'class': 'textarea', 'placeholder': 'Brief description'}),
            'uri_base': forms.URLInput(attrs={
                    'placeholder': 'Leave blank unless record IDs are URIs', 'size': 45}),
            'title': forms.TextInput(attrs={'size': 45}),
            'label': forms.TextInput(attrs={'placeholder': '20 char max; no spaces','size': 22}),
            'creator': forms.TextInput(attrs={'size': 45}),
            'source': forms.TextInput(attrs={'size': 45}),
            'featured': forms.TextInput(attrs={'size': 4}),
            'webpage': forms.URLInput(attrs={'size': 45, 'placeholder': 'Project home page, if any'})
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            raise forms.ValidationError('Label cannot contain any spaces; replace with underscores (_)')
        return label

    def clean_file(self):
        # print('clean_file in DatasetUploadForm')
        uploaded_file = self.cleaned_data['file']

        # Save the uploaded file to a temporary location
        tempf, tempfn = tempfile.mkstemp()
        try:
            for chunk in uploaded_file.chunks():
                os.write(tempf, chunk)
        except:
            raise forms.ValidationError("Problem with the input file.")
        finally:
            os.close(tempf)

        # You can get the file's content type (MIME type) from the uploaded_file object
        mimetype = uploaded_file.content_type

        if mimetype not in mthash_plus.mimetypes:
            raise forms.ValidationError("Not a valid file type; must be one of [.csv, .tsv, .xlsx, .ods, .json]")

        encoding = self.determine_file_encoding(mimetype, tempfn)
        if encoding and encoding.lower() not in ['utf-8', 'ascii']:
            raise forms.ValidationError(
                f"The encoding of uploaded files must be unicode (utf-8). This file seems to be {encoding}")

        self.cleaned_data['temp_file_path'] = tempfn

        return uploaded_file

    def determine_file_encoding(self, mimetype, filepath):
        """Determine the encoding of a given file based on its MIME type."""
        if mimetype.startswith('text/'):
            return self.get_encoding_delim(filepath)
        elif 'spreadsheet' in mimetype:
            return self.get_encoding_excel(filepath)
        elif mimetype.startswith('application/'):
            with codecs.open(filepath, 'r') as fin:
                return fin.encoding
        else:
            return None

    def get_encoding_excel(self, tempfn):
        with codecs.open(tempfn, 'r') as fin:
            return fin.encoding

    def get_encoding_delim(self, tempfn):
        with open(tempfn, 'rb') as f:
            rawdata = f.read()
        return detect(rawdata)['encoding']

    # fields used to create new DatasetFile record from form
    # uri_base = forms.URLField(widget=forms.URLInput(
        # attrs={'placeholder':'Leave blank unless record IDs are URIs','size':35}))
    file = forms.FileField()
    rev = forms.IntegerField()
    format = forms.ChoiceField(
        choices=FORMATS, widget=forms.RadioSelect, initial='delimited')
    delimiter = forms.CharField()
    header = forms.CharField()
    df_status = forms.ChoiceField(choices=STATUS_FILE)
    numrows = forms.IntegerField()
    upload_date = forms.DateTimeField()

    def __init__(self, *args, **kwargs):
        super(DatasetUploadForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}


class DatasetCreateModelForm(forms.ModelForm):
    class Meta:
        model = Dataset
        # file fields = ('file','rev','uri_base','format','dataset_id','delimiter',
        #   'status','accepted_date','header','numrows')
        fields = ('owner', 'id', 'title', 'label', 'datatype', 'description', 'uri_base', 'public',
                  'creator', 'contributors', 'source', 'webpage', 'image_file', 'featured')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 45, 'class': 'textarea', 'placeholder': 'Brief description'}), 
            'uri_base': forms.URLInput(attrs={
                    'placeholder': 'Leave blank unless record IDs are URIs', 'size': 45}), 
            'title': forms.TextInput(attrs={'size': 45}),
            'label': forms.TextInput(attrs={'placeholder': '20 char max; no spaces','size': 22}),
            'creator': forms.TextInput(attrs={'size': 45}),
            'source': forms.TextInput(attrs={'size': 45}), 
            'featured': forms.TextInput(attrs={'size': 4}), 
            'webpage': forms.URLInput(attrs={'size': 45, 'placeholder': 'Project home page, if any'})
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            raise forms.ValidationError('Label cannot contain any spaces; replace with underscores (_)')
        return label

    # fields used to create new DatasetFile record from form
    # uri_base = forms.URLField(widget=forms.URLInput(
        # attrs={'placeholder':'Leave blank unless record IDs are URIs','size':35}))
    file = forms.FileField()
    rev = forms.IntegerField()
    format = forms.ChoiceField(
        choices=FORMATS, widget=forms.RadioSelect, initial='delimited')
    delimiter = forms.CharField()
    header = forms.CharField()
    df_status = forms.ChoiceField(choices=STATUS_FILE)
    numrows = forms.IntegerField()
    upload_date = forms.DateTimeField()

    def __init__(self, *args, **kwargs):
        super(DatasetCreateModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}
