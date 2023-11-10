# place.models
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.gis.db import models as geomodels
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import models

from django_celery_results.models import TaskResult

# from datasets.models import Dataset
from datasets.static.hashes.parents import ccodes as cc
from main.choices import FEATURE_CLASSES, STATUS_REVIEW
from traces.models import TraceAnnotation
#from edtf import parse_edtf

def yearPadder(y):
  year = str(y).zfill(5) if str(y)[0] == '-' else str(y).zfill(4)
  return year

class Place(models.Model):
  # id is auto-maintained, per Django
  title = models.CharField(max_length=255)
  src_id = models.CharField(max_length=2044, blank=True)
  # note FK is label, not id
  dataset = models.ForeignKey('datasets.Dataset', db_column='dataset',
                                to_field='label', related_name='places', on_delete=models.CASCADE)
  ccodes = ArrayField(models.CharField(max_length=2, null=True), blank=True)
  minmax = ArrayField(models.IntegerField(blank=True, null=True),null=True,blank=True)
  timespans = JSONField(blank=True,null=True) # for list of lists
  fclasses = ArrayField(models.CharField(max_length=1, choices=FEATURE_CLASSES), null=True, blank=True)
  indexed = models.BooleanField(default=False)  
  idx_pub = models.BooleanField(default=False)
  flag = models.BooleanField(default=False) # not in use
  # added Apr 2023, for case of no start/end
  attestation_year = models.IntegerField(null=True,blank=True)

  # 0=hits:unreviewed, 1=hits:reviewed, 2=hits:deferred, null=no hits
  review_wd = models.IntegerField(null=True, choices=STATUS_REVIEW)
  review_tgn = models.IntegerField(null=True, choices=STATUS_REVIEW)
  review_whg = models.IntegerField(null=True, choices=STATUS_REVIEW)

  def __str__(self):
    return '%s:%s' % (self.id, self.title)

  @property
  def authids(self):
    return [i.jsonb['identifier'] for i in self.links.all()]

  @property
  def countries(self):
    return [cc[0][x]['gnlabel'] for x in self.ccodes]

  @property
  def defer_comments(self):
    return self.comment_set.filter(tag='defer')

  @property
  def dsid(self):
    return self.dataset.id

  @property
  def geom_count(self):
    return self.geoms.count()

  @property
  def geomtype(self):
    return self.geoms.all()[0].geom.geom_type

  @property
  def hashits_wd(self):
    return self.hit_set.filter(authority__in=['wd','wdlocal']).count() >0

  @property
  def hashits_whg(self):
    return self.hit_set.filter(authority__in=['whg','idx']).count()>0

  @property
  def hashits_tgn(self):
    return self.hit_set.filter(authority='tgn').count()>0

  @property
  def public(self):
    return self.dataset.public

  @property
  def repr_geom(self):
    return self.geoms.all()[0].geom

  @property
  def repr_point(self):
    g = self.geoms.all()[0].geom
    gtype = str(type(g))
    if 'MultiPolygon' in gtype:
      return g.coords[0][0][0]
    elif 'Point' in gtype:
      return g.coords

  @property
  def traces(self):
    return TraceAnnotation.objects.filter(place=self.id)

  class Meta:
    managed = True
    db_table = 'places'
    indexes = [
          models.Index(fields=['src_id', 'dataset']),
        ]

class Type(models.Model):
  aat_id = models.IntegerField(unique=True)
  parent_id = models.IntegerField(null=True,blank=True)
  term = models.CharField(max_length=100)
  term_full = models.CharField(max_length=100)
  note = models.TextField(max_length=3000)
  fclass = models.CharField(max_length=1,null=True,blank=True)

  def __str__(self):
    return str(self.aat_id) +':'+self.term

  class Meta:
    managed = True
    db_table = 'types'

# NB in LPF spec but seldom used
class Source(models.Model):
  owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
  # TODO: force unique...turn into slug or integer
  src_id = models.CharField(max_length=30, unique=True)    # contributor's id
  uri = models.URLField(null=True, blank=True)
  label = models.CharField(max_length=255)    # short, e.g. title, author
  citation = models.CharField(max_length=500, null=True, blank=True)

  def __str__(self):
    return self.src_id

  class Meta:
    managed = True
    db_table = 'sources'

class PlaceName(models.Model):
  # {toponym, lang, citation{}, when{}}
  place = models.ForeignKey(Place, related_name='names',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  task_id = models.CharField(max_length=100, blank=True, null=True)

  toponym = models.CharField(max_length=2044)
  name_src = models.ForeignKey(Source, null=True, on_delete=models.SET_NULL)

  def __str__(self):
    return self.toponym

  class Meta:
    managed = True
    db_table = 'place_name'

class PlaceType(models.Model):
  place = models.ForeignKey(Place,related_name='types',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  # identifier, label, sourceLabels[], when{}

  aat_id = models.IntegerField(null=True,blank=True) # Getty AAT identifier
  fclass = models.CharField(max_length=1,choices=FEATURE_CLASSES) # geonames feature class

  def __str__(self):
    return (self.jsonb['sourceLabel'] if 'sourceLabel' in self.jsonb else '') +\
           ('; ' + self.jsonb['label'] if 'label' in self.jsonb else '')

  class Meta:
    managed = True
    db_table = 'place_type'

class PlaceGeom(models.Model):
  place = models.ForeignKey(Place,related_name='geoms',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  task_id = models.CharField(max_length=100, blank=True, null=True)
  # Source is not used
  geom_src = models.ForeignKey(Source, null=True, db_column='geom_src',
                                 to_field='src_id', on_delete=models.SET_NULL)

  geom = geomodels.GeometryField(null=True, blank=True, srid=4326)

  s2 = ArrayField(models.BigIntegerField(), null=True)
  h3 = ArrayField(models.CharField(max_length=15, blank=True,null=True), null=True)

  # informs dataset last_update
  created = models.DateTimeField(null=True, auto_now_add=True)

  @property
  def title(self):
    return self.place.title

  # good to have, but not accessible in values_list queries
  @property
  def minmax(self):
    #tsarr=[]; intarr=[]
    #wg = self.jsonb['when']
    from edtf import parse_edtf    
    def yearPadder(y):
      #print('y',y)
      year = str(y).zfill(5) if str(y)[0] == '-' else str(y).zfill(4)
      return year if int(y) > -9999 else '-9999'
    def getInt(expr):
      #print('expr',expr)
      return int(parse_edtf( yearPadder(list(expr.values())[0])).get_year() )
    
    #when = pg.jsonb['when'] if 'when' in pg.jsonb else None
    when = self.jsonb['when'] if 'when' in self.jsonb else None
    tsarr = when['timespans'] if when and 'timespans' in when else None
    years=[]; nullset=set([None]); intarr=[]
    if when and tsarr:
      #years=[];nullset=set([None]);intarr=[]
      for ts in tsarr:
        start = getInt(ts['start'])
        end = getInt(ts['end']) if 'end' in ts else start
        years += [start,end]
        intarr.append([start,end])
      years = list(set(years)-nullset)
    return [min(years),max(years)] if len(years)>0 else None

  class Meta:
    managed = True
    db_table = 'place_geom'

class PlaceLink(models.Model):
  place = models.ForeignKey(Place,related_name='links',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  task_id = models.CharField(max_length=100, blank=True, null=True)

  review_note = models.CharField(max_length=2044, blank=True, null=True)
  black_parent = models.IntegerField(blank=True, null=True)

  # informs dataset last_update
  created = models.DateTimeField(null=True, auto_now_add=True)

  class Meta:
    managed = True
    db_table = 'place_link'

class PlaceWhen(models.Model):
  place = models.ForeignKey(Place,related_name='whens',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  minmax = ArrayField(models.IntegerField(blank=True,null=True), null=True)

  class Meta:
    managed = True
    db_table = 'place_when'

class PlaceRelated(models.Model):
  place = models.ForeignKey(Place,related_name='related',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  # relation_type, relation_to, label, when{}, citation{label,id}, certainty

  class Meta:
    managed = True
    db_table = 'place_related'

class PlaceDescription(models.Model):
  place = models.ForeignKey(Place,related_name='descriptions',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  task_id = models.CharField(max_length=100, blank=True, null=True)
  # id, value, lang

  class Meta:
    managed = True
    db_table = 'place_description'

class PlaceDepiction(models.Model):
  place = models.ForeignKey(Place,related_name='depictions',
                              default=-1, on_delete=models.CASCADE)
  src_id = models.CharField(max_length=100,default='') # contributor's identifier
  jsonb = JSONField(blank=True, null=True)
  # id, title, license

  class Meta:
    managed = True
    db_table = 'place_depiction'

class CloseMatch(models.Model):
  place_a = models.ForeignKey(Place, on_delete=models.CASCADE, related_name="match_a")
  place_b = models.ForeignKey(Place, on_delete=models.CASCADE, related_name="match_b")
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)
  task = models.ForeignKey(TaskResult, on_delete=models.CASCADE, related_name="task",
                           null=True, blank=True)
  created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name="matcher")
  basis = models.CharField(max_length=200,
      choices=[('authid', 'authority id'), ('reviewed', 'reviewed'), ('imported', 'imported')],
      null=True, blank=True)

  class Meta:
    managed = True