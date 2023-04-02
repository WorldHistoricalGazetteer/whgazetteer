from django.conf import settings
from django.contrib.auth import get_user_model
User = get_user_model()
from django.http import HttpResponseRedirect
from django.test import TestCase, SimpleTestCase
from django.urls import reverse
from chardet import detect
from datasets.static.hashes import mimetypes as mthash
from datasets.static.hashes import mimetypes_plus as mthash_plus
from datasets.utils import validate_tsv
import os, codecs, json, mimetypes, re, sys
from datasets.models import Dataset
from django.contrib.auth import get_user_model
from django.test import Client, TestCase

def create_user(**params):
	"""Create and return a new user."""
	return get_user_model().objects.create_user(**params)

class CompareAndUpdateTests(TestCase):
  def setUp(self):
    self.client = Client()
    self.user = create_user(email='user@example.com', password='test123', username='user1')
    self.client.force_login(self.user)
    # client.login(username='SomeUser', password='django9999')
    with open('_testdata/_update/sample7.txt') as file_og:
      # print(fp.readlines())
      self.client.post('/datasets/create/', {
        'owner': 1,
        'title': 'Sample 7',
        'label': 'sample7',
        'description': 'Seven-record dataset to be updated',
        'file': file_og,
        'datatype': 'place',
        'rev': -1,
        'df_status': 'uploaded',
        'format': 'delimited',
        'delimiter': 'fubar',
        'numrows': -1,
        'numlinked': 0,
        'header': ['id','title','title_source','start','end','title_uri','ccodes','variants','types','aat_types',
                   'matches','lon','lat','geowkt','geo_source','geo_id','description'],
        'upload_date': '2020-11-10'
      })
      print('last ds', Dataset.objects.last())

  def test_compare(self):
    ds=Dataset.objects.last()
    # dsid=Dataset.objects.last().id
    # dsid=Dataset.objects.get(label='sample7').id
    # upload new file and compare
    with open('_testdata/_update/sample7_new.txt') as file_new:
      # print(fp.readlines())
      comparison = self.client.post('/datasets/compare/', {
        'dsid': ds.id,
        'format': 'delimited',
        'file': file_new,
        'keepg': True,
        'keepl': True,
        'compare_data': {}
      })
      # compare_data = json.loads(request.POST['compare_data'])
      # compare_result = compare_data['compare_result']

      # print('compare type, contents', type(compare), json.loads(compare.content))
      compare_data = json.loads(comparison.content)
      result = compare_data['compare_result']
      print('compare_data (test)', compare_data)
      self.assertEquals(result['count_new'], 7)
      self.assertEquals(result['count_diff'], 0)
      self.assertEquals(result['count_replace'], 6)
      self.assertEquals(result['cols_del'], [])
      self.assertEquals(result['cols_add'], [])
      self.assertEquals(result['header_new'], ['id', 'title', 'title_source', 'start', 'end', 'title_uri', 'ccodes', 'variants', 'types', 'aat_types',
                     'matches', 'lon', 'lat', 'geowkt', 'geo_source', 'geo_id', 'description'])
      self.assertEquals(result['rows_add'], ['717_4'])
      self.assertEquals(result['rows_del'], ['717_2'])

      # self.client = Client()
      # self.user = create_user(email='user@example.com', password='test123', username='user1')
      # self.client.force_login(self.user)
      # print('user', self.user )
      # now update
      update_result = self.client.post('/datasets/update/', {
        'dsid': ds.id,
        'format': 'delimited',
        'compare_data': compare_data,
        'keepg': True,
        'keepl': True
      })
      #
      print('update_result', update_result)
      print('places in ds', ds.places.all())
      print('src_ids', ds.places.all().values_list('src_id', flat=True))
      # self.assertIn(str(ds.places.all().values_list('src_id', flat=True)), '717_4')
      # self.assertFalse('717_2' in str(ds.places.all().values_list('src_id', flat=True)))
    # delete dataset after test
    # ds = Dataset.objects.get(label='sample7')
    # self.ds.delete()

  # print(json.loads(compare.content))

# test assertions
# Replace 6 place records having same IDs
# Add records (717_4)
# Remove records (717_2)
# There are 5 existing place-geometry records...keep them?



# upload a faulty file
# with open('_testdata/_update/sample7_new.txt') as fp:
#   # print(fp.readlines())
#   c.post('/datasets/create/', {
#     'owner': 1,
#     'title': 'Sample 7',
#     'label': 'sample7_og',
#     'description': 'Seven-record dataset to be updated',
#     'file': fp,
#     'datatype': 'place',
#     'rev': -1,
#     'df_status': 'uploaded',
#     'format': 'delimited',
#     'delimiter': 'fubar',
#     'numrows': -1,
#     'numlinked': 0,
#     'header': ['id','title','title_source','start','end','title_uri','ccodes','variants','types','aat_types','matches','lon','lat','geowkt','geo_source','geo_id','description'],
#     'upload_date': '1999-12-31'
#   })
