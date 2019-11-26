#
from django.test import TestCase
from django.contrib.auth.models import User
from datasets.models import Dataset,Hit

class DatasetModelTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        user = User.objects.create(username='tempy',password='tempy')
        print("setUpTestData(): Run once to set up non-modified data for all class methods.")
        Dataset.objects.create(owner=user,label='test dataset',name='longer dataset name',description='testy',format='delimited',datatype='place')

    def test_label(self):
        ds = Dataset.objects.get(id=1)
        field_label = ds._meta.get_field('label').verbose_name
        self.assertEquals(field_label, 'label')
        
    #def test_false_is_false(self):
        #print("Method: test_false_is_false.")
        #self.assertFalse(False)

    #def test_false_is_true(self):
        #print("Method: test_false_is_true.")
        #self.assertTrue(False)

    #def test_one_plus_one_equals_two(self):
        #print("Method: test_one_plus_one_equals_two.")
        #self.assertEqual(1 + 1, 3)