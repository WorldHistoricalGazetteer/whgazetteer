# test dataset update process, step-by-step
#
from django.test import TestCase
from django.contrib.auth.models import User
from datasets.models import Dataset
from places.models import *
from django.shortcuts import get_object_or_404
from es.es_utils import makeDoc,indexSomeParents

from elasticsearch import Elasticsearch      

# 0.9) one-shot
# create whgtest index and 
# add tgn dataset w/two records from db 
# pids=[5004032,5335754] Pontianak, Sanggau
# 
#idx='whgtest'
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#pids=[5004032,5335754]
#indexSomeParents(es,idx,pids)

# 1)
# create diamonds dataset (diamonds135.tsv) owned by tempy
# create two records w/tgn links and geometry
class DatasetLifecycleTest(TestCase):
  @classmethod
  def setUpTestData(cls):
    user = User.objects.create(username='tempy',password='tempy',email='karl@kgeographer.org')
    print("setUpTestData(): dataset->diamonds; places->Pontianak,Sanggau")
    Dataset.objects.create(
      owner=user, label='diamonds', title='The Indonesian Diamonds test',
      description='I could go on and on...', uri_base='https://fubar.edu',
      datatype='place', public=True )
    ds = get_object_or_404(Dataset,pk=1)
    newpl1 = Place(src_id = '131.0', dataset = ds, title = 'Pontianak', ccodes = ['ID'])
    newpl1.save()
    newpl2 = Place(src_id = '135.0', dataset = ds, title = 'Sanggau', ccodes = ['ID'])
    newpl2.save()
    objs = {
      "PlaceName":[
        PlaceName(place_id_id=newpl1.id,src_id = newpl1.src_id,toponym = "Pontianak",
                  jsonb={"toponym": "Pontianak", "citation": {"id": "", "label": "Broek"}}),        
        PlaceName(place_id_id=newpl2.id,src_id = newpl2.src_id,toponym = "Sanggau",
                  jsonb={"toponym": "Sanggau", "citation": {"id": "", "label": "Broek"}})        
      ], 
      "PlaceGeom":[
        PlaceGeom(place_id_id=newpl1.id,src_id = newpl1.src_id,
          jsonb={"type": "Point", "geowkt": "POINT(109.325 -0.03194)", "coordinates": [109.325, -0.03194]}),
        PlaceGeom(place_id_id=newpl2.id,src_id = newpl2.src_id,
          jsonb={"type": "Point", "geowkt": "POINT(110.6 0.13333)", "coordinates": [110.6, 0.13333]})      
      ],
      "PlaceLink":[
        PlaceLink(place_id_id=newpl1.id,src_id = newpl1.src_id,jsonb={"type": "closeMatch", "identifier": "tgn:7015960"}),
        PlaceLink(place_id_id=newpl2.id,src_id = newpl2.src_id,jsonb={"type": "closeMatch", "identifier": "tgn:1078484"})
      ]
    }
    PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10)
    PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10)
    PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10)    

  # titles match when queried via dataset
  def test_dataset(self):
    ds = Dataset.objects.get(id=1)
    print(ds.places.first())
    # test association of places to ds
    pl1title = ds.places.filter(src_id='131.0').first().title
    pl2title = ds.places.filter(src_id='135.0').first().title
    self.assertEquals(pl1title, 'Pontianak')
    self.assertEquals(pl2title, 'Sanggau')
    
  def test_index_add(self):
    print('index the 2 db records')


# 2)
# simulate acccessioning w/align_whg
# align_whg({dsid},{'bounds':{'type': ['userarea'], 'id': ['0']}})


