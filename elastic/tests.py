# tests for Elasticsearch operations
# can't run with manage.py test; must use existing database
from django.test import TestCase

from elasticsearch7 import Elasticsearch
from django.conf import settings
from django.db.models import Q
from datasets.models import Dataset, Hit
idx = 'whg'
from elastic.es_utils import deleteDatasetFromIndex
es = settings.ES_CONN

class RemoveDatasetFromIndex(TestCase):
  """
    requires indexed owt10 test dataset
  """
  def test_remove_dataset_from_index(self):
    ds = Dataset.objects.get(label='owt10')
    placeids = list(ds.placeids)

    # run function
    deleteDatasetFromIndex(idx, ds.id)
    q = { "query": {"bool": {
            "should": [
                {"match": {"dataset": ds.label}},
                {"terms": {"place_id": placeids}},
                {"terms": {"children": placeids}}]
    }}}
    res = es.search(index=idx, body=q)

    """is it gone from index?"""
    zapped = res['hits']['total']['value'] == 0

    """are ds place.review_whg values all unreviewed"""
    unreviewed = ds.places.filter(review_whg = 1).count() == 0
    # == ds.places.count()

    """if current align_idx task.status == 'SUCCESS' are all hit.reviewed values False?"""
    # latest align_idx task
    taskid = ds.tasks.get(task_name='align_idx').task_id
    hits_cleared = Hit.objects.filter(task_id = taskid, reviewed = True).count() == 0

    self.assertEqual(zapped, True)
    self.assertEqual(unreviewed, True)
    self.assertEqual(hits_cleared, True)

  def test_something_that_will_fail(self):
      self.assertTrue(False)
