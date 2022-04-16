from django.db import models
from django.db.models import Q
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField #,JSONField
from django.core.validators import URLValidator
from django.urls import reverse
from datasets.models import Dataset
from main.choices import COLLECTIONCLASSES, COLLECTIONTYPES
from places.models import Place
from tinymce.models import HTMLField

def collection_path(instance, filename):
  # upload to MEDIA_ROOT/collections/<coll id>/<filename>
  return 'collections/{0}/{1}'.format(instance.id, filename)

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

class Collection(models.Model):
  owner = models.ForeignKey(User,related_name='collections', on_delete=models.CASCADE)
  title = models.CharField(null=False, max_length=255)
  description = models.CharField( null=False, max_length=2044)
  keywords = ArrayField(models.CharField(max_length=50), null=True)

  # per-collection relation keyword choices, e.g. waypoint, birthplace, battle site
  rel_keywords = ArrayField(models.CharField(max_length=30), null=True)

  # 3 new fields, 20210619
  creator = models.CharField(null=True, blank=True, max_length=500)
  contact = models.CharField(null=True, blank=True, max_length=500)
  webpage = models.URLField(null=True, blank=True)

  # modified, 20220416
  collection_class = models.CharField(choices=COLLECTIONCLASSES, max_length=12, default='dataset')
  type = models.CharField(choices=COLLECTIONTYPES, max_length=12)

  # single representative image
  image_file = models.FileField(upload_to=collection_path, blank=True, null=True)
  # single pdf file
  file = models.FileField(upload_to=collection_path, blank=True, null=True)

  create_date = models.DateTimeField(null=True, auto_now_add=True)
  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)

  # collections can comprise >=0 datasets, >=1 places
  datasets = models.ManyToManyField("datasets.Dataset", blank=True)
  places = models.ManyToManyField("places.Place", blank=True)

  # tinymce field?
  content = HTMLField(null=True, blank=True)

  def get_absolute_url(self):
    #return reverse('datasets:dashboard', kwargs={'id': self.id})
    return reverse('data-collections')

  @property
  def places_ds(self):
    dses = self.datasets.all()
    return Place.objects.filter(dataset__in=dses)

  @property
  def places_all(self):
    all = Place.objects.filter(
      Q(dataset__in=self.datasets.all()) | Q(id__in=self.places.all().values_list('id'))
    )
    return all

  @property
  def ds_list(self):
    dsc = [{"id":d.id, "label":d.label, "title":d.title} for d in self.datasets.all()]
    dsp = [{"id":p.dataset.id, "label":p.dataset.label, "title":p.dataset.title} for p in self.places.all()]
    return list({ item['id'] : item for item in dsp+dsc}.values())

  @property
  def rowcount(self):
    dses = self.datasets.all()
    counts = [ds.places.count() for ds in dses]
    return sum(counts)

  def __str__(self):
    return '%s:%s' % (self.id, self.title)

  class Meta:
    db_table = 'collections'

class CollectionLink(models.Model):
  collection = models.ForeignKey(Collection, default=None,
    on_delete=models.CASCADE, related_name='links')
  label = models.CharField(null=True, blank=True, max_length=200)
  uri = models.TextField(validators=[URLValidator()])
  link_type = models.CharField(default='page', max_length=10, choices=[('page','web page'), ('image','image'),('pdf','pdf')])
  license = models.CharField(null=True, blank=True, max_length=64)

  def __str__(self):
    cap = self.label[:20]+('...' if len(self)>20 else '')
    return '%s:%s' % (self.id, cap)

  class Meta:
      managed = True
      db_table = 'collection_link'

class CollectionImage(models.Model):
  collection = models.ForeignKey(Collection, default=None,
    on_delete=models.CASCADE, related_name='images')
  image = models.FileField(upload_to=collection_path)
  caption = models.CharField(null=True, blank=True, max_length=500)
  uri = models.TextField(validators=[URLValidator()], null=True, blank=True)
  license = models.CharField(null=True, blank=True, max_length=64)

  def __str__(self):
    cap = self.caption[:20]+('...' if len(self)>20 else '')
    return '%s:%s' % (self.id, cap)

  class Meta:
      managed = True
      db_table = 'collection_image'
