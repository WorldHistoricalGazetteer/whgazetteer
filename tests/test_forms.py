from django.contrib.auth import get_user_model
User = get_user_model()
from django.test import TestCase
from datasets.forms import DatasetCreateModelForm
from datasets.models import Dataset


class DatasetCreateTestCase(TestCase):
    
    def test_valid_form(self):
        owner=User.objects.create(email='temp@tempy.com',password='tempy')
        print('owner id',owner.id)
        label='ds_testy'
        title='Test Dataset'
        description = "Blah, blah..."
        datatype = 'place'
        obj = Dataset.objects.create(owner=owner, label=label, title=title, description=description, datatype=datatype )

        data = {"owner":owner.id, "label":label, "title":title, "description":description, "datatype":datatype}
        form = DatasetCreateModelForm(data=data)
        print(form.errors.as_data())
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data.get('label') ,obj.label)
        self.assertEqual(form.cleaned_data.get('title'), obj.title)
        self.assertEqual(form.cleaned_data.get('description'), obj.description)
    