#
from django.test import TestCase
from django.contrib.auth.models import User
from datasets.models import Dataset
from places.models import Place
from django.shortcuts import get_object_or_404

class PlaceModelTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        user = User.objects.create(username='tempy',password='tempy')
        print("setUpTestData(): Run once to set up non-modified data for all class methods.")
        

    def test_title(self):
        ds = Place.objects.get(id=1)
        field_title = ds._meta.get_field('title').verbose_name
        self.assertEquals(field_title, 'Timbuktu')
        

class DatasetModelTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        user = User.objects.create(username='tempy',password='tempy')
        print("setUpTestData(): Run once to set up non-modified data for all class methods.")
        Dataset.objects.create(
            owner=user,
            label='test dataset',
            title='longer dataset name',
            description='I could go on and on...',
            uri_base='https://fubar.edu',
            datatype='place',
            public=True
        )
        ds = get_object_or_404(Dataset,pk=1)
        Place.objects.create(
            dataset=ds,
            title='Timbuktu',
            src_id='foo123',
            ccodes=['AR','CA']
        )

    def test_ds_label(self):
        ds = Dataset.objects.get(id=1)
        field_label = ds._meta.get_field('title').verbose_name
        field_value = ds.label
        self.assertEquals(field_label, 'title')
        self.assertEquals(field_value, 'test dataset')
        
    def test_place_title(self):
        p = Place.objects.get(src_id='foo123')
        field_title = p._meta.get_field('title').verbose_name
        field_value = p.src_id
        self.assertEquals(field_title, 'title')
        self.assertEquals(field_value, 'foo123')
        
    #def test_false_is_false(self):
        #print("Method: test_false_is_false.")
        #self.assertFalse(False)

    #def test_false_is_true(self):
        #print("Method: test_false_is_true.")
        #self.assertTrue(False)

    #def test_one_plus_one_equals_two(self):
        #print("Method: test_one_plus_one_equals_two.")
        #self.assertEqual(1 + 1, 3)