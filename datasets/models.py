# datasets.models
from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import GeometryCollection
from django.contrib.auth.models import User
from django.db import models
from django.db.models import Q
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.urls import reverse
#from django.shortcuts import get_object_or_404

from elastic.es_utils import escount_ds
from main.choices import *
from places.models import Place, PlaceGeom
import simplejson as json

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

def ds_image_path(instance, filename):
  # upload to MEDIA_ROOT/datasets/<id>_<filename>
  return 'datasets/{0}_{1}'.format(instance.id, filename)

# owner = models.ForeignKey('auth.User', related_name='snippets', on_delete=models.CASCADE)
class Dataset(models.Model):
  #idx='whg'
  owner = models.ForeignKey(User,related_name='datasets', on_delete=models.CASCADE)
  label = models.CharField(max_length=20, null=False, unique="True",
            error_messages={'unique': 'The dataset label entered is already in use, and must be unique. Try appending a version # or initials.'})
  title = models.CharField(max_length=255, null=False)
  description = models.CharField(max_length=2044, null=False)
  webpage = models.URLField(null=True, blank=True)
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  uri_base = models.URLField(null=True, blank=True)
  image_file = models.FileField(upload_to=ds_image_path, blank=True, null=True)
  featured = models.IntegerField(null=True, blank=True)
  bbox = geomodels.PolygonField(null=True, blank=True, srid=4326)

  core = models.BooleanField(default=False)    
  public = models.BooleanField(default=False)    
  ds_status = models.CharField(max_length=12, null=True, blank=True, choices=STATUS_DS)

  # 4 added 20210619
  creator = models.CharField(max_length=500, null=True, blank=True)
  source = models.CharField(max_length=500, null=True, blank=True)
  contributors = models.CharField(max_length=500, null=True, blank=True)
  # user-added; if absent, generated in browser
  citation = models.CharField(max_length=2044, null=True, blank=True)

  # TODO: these are updated in both Dataset & DatasetFile  (??)
  datatype = models.CharField(max_length=12, null=False,choices=DATATYPES,
                                default='place')
  numrows = models.IntegerField(null=True, blank=True)

  # these are back-filled
  numlinked = models.IntegerField(null=True, blank=True)
  total_links = models.IntegerField(null=True, blank=True)
  
  #collections = models.ManyToManyField("collection.Collection")

  def __str__(self):
    return self.label
    # return '%d: %s' % (self.id, self.label)

  #def get_absolute_url(self):
    #return reverse('datasets:ds_summary', kwargs={'id': self.id})

  # test    
  #from datasets.models import Dataset, DatasetUser, Hit
  #from django.contrib.auth.models import User
  #from places.models import *
  #from collection.models import *
  #from django.shortcuts import get_object_or_404
  #ds = get_object_or_404(Dataset, pk=1034)
  #user = get_object_or_404(User, pk=14)
  #coll = get_object_or_404(Collection,pk=3)

  @property
  def last_modified(self):
    last=self.log.all().order_by('-timestamp')[0].timestamp
    #last=ds.log.all().order_by('-timestamp')[0].timestamp
    return last.strftime("%d %b %Y")
    
  @property
  def minmax(self):
    # temporal is sparse
    timespans = [p.minmax for p in self.places.all() \
                 if p.minmax and len(p.minmax) == 2]
    earliest = min([t[0] for t in timespans]) if len(timespans)>0 else None
    latest = max([t[1] for t in timespans]) if len(timespans)>0 else None
    minmax = [earliest,latest] if earliest and latest else None
    return minmax
  
  @property
  def bounds(self):
    pg_geoms=PlaceGeom.objects.values_list('geom',flat=True).filter(place__dataset=self.label)
    #pg_geoms=PlaceGeom.objects.values_list('geom',flat=True).filter(place__dataset='croniken')
    gc=GeometryCollection(tuple(pg_geoms))
    
    return json.loads(gc.envelope.geojson)

  @property
  def file(self):
    # returns model instance for latest file
    file = self.files.all().order_by('id')[0]
    return file 
  
  @property
  def tasks(self):
    from django_celery_results.models import TaskResult
    return TaskResult.objects.all().filter(task_args = '['+str(self.id)+']',task_name__startswith='align')

  # used in ds_compare()
  @property
  def status_idx(self):
    idx='whg'
    submissions = [
          {"task_id":t.task_id, "date":t.date_done.strftime("%Y-%m-%d %H:%M"),
             "hits_tbr":Hit.objects.filter(task_id=t.task_id, reviewed=False).count() }
            for t in self.tasks.filter(task_name='align_idx').order_by('-date_done')]
    idxcount = escount_ds(idx, self.label)

    result = {"submissions":submissions,"idxcount":idxcount}
    return result
  
  # tasks stats
  @property
  def taskstats(self):
    def distinctPlaces(task):
      p_hits0 = Hit.objects.filter(task_id=t.task_id,query_pass='pass0', reviewed=False).values("place_id").distinct().count()
      p_hits1 = Hit.objects.filter(task_id=t.task_id,query_pass='pass1', reviewed=False).values("place_id").distinct().count()
      p_hits2 = Hit.objects.filter(task_id=t.task_id,query_pass='pass2', reviewed=False).values("place_id").distinct().count()
      p_hits3 = Hit.objects.filter(task_id=t.task_id,query_pass='pass3', reviewed=False).values("place_id").distinct().count()
      p_sum = p_hits0+p_hits1+p_hits2+p_hits3
      
      return {"tid":t.task_id,
       #"task":t.task_name,
       "date":t.date_done.strftime('%Y-%m-%d'),
       "total":p_sum, 
       "pass0":p_hits0, 
       "pass1":p_hits1, 
       "pass2":p_hits2, 
       "pass3":p_hits3 } 
    
    result = {}
    # array for each kind of task
    #task_types = self.tasks.all().values_list("task_name", flat=True)
    task_types = ['align_wdlocal','align_tgn','align_idx','align_whg','align_wd']
    for tt in task_types:
      result[tt] = []
      for t in self.tasks.filter(task_name=tt, status="SUCCESS"):
        result[tt].append(distinctPlaces(t))

    #print(result)
    return result

  # count of unreviewed hits
  @property
  def unreviewed(self):
    unrev=Hit.objects.all().filter(dataset_id=self.id, reviewed=False).count()
    return unrev
  
  # count of unindexed places
  @property
  def unindexed(self):
    unidxed=self.places.filter(indexed=False).count()
    return unidxed

  # list of dataset place_id values
  @property
  def placeids(self):
    return Place.objects.filter(dataset=self.label).values_list('id', flat=True)

  # list of dataset geometries
  @property
  def geometries(self):
    g_list = PlaceGeom.objects.filter(place_id__in=self.placeids).values_list('jsonb', flat=True)
    return g_list

  @property
  def format(self):
    return self.files.first().format

  @property
  def collaborators(self):
    ## includes roles: member, owner
    team = DatasetUser.objects.filter(dataset_id_id = self.id).values_list('user_id_id')
    teamusers = User.objects.filter(id__in=team)
    return teamusers

  @property
  def owners(self):
    du_owner_ids = list(self.collabs.filter(role = 'owner').values_list('user_id_id',flat=True))
    du_owner_ids.append(self.owner.id)
    ds_owners = User.objects.filter(id__in=du_owner_ids)
    return ds_owners

  class Meta:
    managed = True
    db_table = 'datasets'

""" TODO: FK to dataset, not dataset_id"""
class DatasetFile(models.Model):
  dataset_id = models.ForeignKey(Dataset, related_name='files',
    default=-1, on_delete=models.CASCADE)
  rev = models.IntegerField(null=True, blank=True)
  file = models.FileField(upload_to=user_directory_path)
  format = models.CharField(max_length=12, null=False,
                              choices=FORMATS, default='lpf')
  datatype = models.CharField(max_length=12, null=False, choices=DATATYPES,
                                default='place')
  delimiter = models.CharField(max_length=5, null=True, blank=True)
  df_status = models.CharField(max_length=12, null=True, blank=True, choices=STATUS_FILE)
  upload_date = models.DateTimeField(null=True, auto_now_add=True)
  header = ArrayField(models.CharField(max_length=30), null=True, blank=True)
  numrows = models.IntegerField(null=True, blank=True)

  #def __str__(self):
    #return 'file_'+str(self.rev)

  class Meta:
    managed = True
    db_table = 'dataset_file'

class DatasetUser(models.Model):
  dataset_id = models.ForeignKey(Dataset, related_name='collabs',
                                   default=-1, on_delete=models.CASCADE)
  user_id = models.ForeignKey(User, related_name='ds_collab',
                                default=-1, on_delete=models.CASCADE)
  role = models.CharField(max_length=20, null=False, choices=TEAMROLES)

  def __str__(self):
    username = self.user_id.username
    return '<b>' + username + '</b> (' + self.role + ')'

  class Meta:
    managed = True
    db_table = 'dataset_user'


# 
class Hit(models.Model):
  # FK to celery_results_task_result.task_id
  place = models.ForeignKey(Place, on_delete=models.CASCADE)
  # task_id = models.ForeignKey(TaskResult, 
    #related_name='task_id', on_delete=models.CASCADE)
  task_id = models.CharField(max_length=50)
  authority = models.CharField(max_length=12, choices=AUTHORITIES )
  dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
  query_pass = models.CharField(max_length=12, choices=PASSES )
  src_id = models.CharField(max_length=2044)
  score = models.FloatField()
  
  reviewed = models.BooleanField(default=False)
  matched = models.BooleanField(default=False)
  flag = models.BooleanField(default=False)


  # authority record identifier (could be uri)
  authrecord_id = models.CharField(max_length=255)

  # json response; parse later according to authority
  json = JSONField(blank=True, null=True)
  geom = JSONField(blank=True, null=True)

  def __str__(self):
    return str(self.id)

  class Meta:
    managed = True
    db_table = 'hits'

@receiver(pre_delete, sender=Dataset)
def remove_files(**kwargs):
  print('pre_delete remove_files()',kwargs)
  ds_instance = kwargs.get('instance')
  files = DatasetFile.objects.filter(dataset_id_id=ds_instance.id)
  files.delete()
