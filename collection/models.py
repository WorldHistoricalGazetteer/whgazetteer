from django.db import models
from django.db.models import Q
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField #,JSONField
from django.core.validators import URLValidator
from django.urls import reverse
from datasets.models import Dataset
from main.choices import COLLECTIONCLASSES, LINKTYPES
from places.models import Place
from django_resized import ResizedImageField
from tinymce.models import HTMLField

""" for images """
from io import BytesIO
import sys
from PIL import Image
from django.core.files.uploadedfile import InMemoryUploadedFile
""" end """
def collection_path(instance, filename):
  # upload to MEDIA_ROOT/collections/<coll id>/<filename>
  return 'collections/{0}/{1}'.format(instance.id, filename)

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

def default_relations():
  return 'locale'.split(', ')
# needed b/c collection place_list filters on it
def default_omitted():
  return '{}'

class Collection(models.Model):
  owner = models.ForeignKey(User,related_name='collections', on_delete=models.CASCADE)
  title = models.CharField(null=False, max_length=255)
  description = models.CharField( null=False, max_length=2044)
  keywords = ArrayField(models.CharField(max_length=50), null=True)

  # array of place ids "removed" by user from the collection
  # filtered in collection.places_all and can't be annotated
  omitted = ArrayField(models.IntegerField(), blank=True, default=default_omitted)

  # per-collection relation keyword choices, e.g. waypoint, birthplace, battle site
  # need default or it errors for some reason
  rel_keywords = ArrayField(models.CharField(max_length=30), blank=True, null=True)
  # rel_keywords = ArrayField(models.CharField(max_length=30), blank=True, default=default_relations)

  # 3 new fields, 20210619
  creator = models.CharField(null=True, blank=True, max_length=500)
  contact = models.CharField(null=True, blank=True, max_length=500)
  webpage = models.URLField(null=True, blank=True)

  # modified, 20220416
  collection_class = models.CharField(choices=COLLECTIONCLASSES, max_length=12, default='dataset')
  # type = models.CharField(choices=COLLECTIONTYPES, max_length=12, null=True, blank=True)

  # single representative image
  # image_file = models.FileField(upload_to=collection_path, blank=True, null=True)
  # image_file = models.ImageField(upload_to=collection_path, blank=True, null=True)
  image_file = ResizedImageField(size=[800, 800], upload_to=collection_path, blank=True, null=True)
  # single pdf file
  file = models.FileField(upload_to=collection_path, blank=True, null=True)

  created = models.DateTimeField(null=True, auto_now_add=True)
  # modified = models.DateTimeField(null=True)
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

  # def save(self):
  #   # Opening the uploaded image
  #   print('in Collection.save()')
  #   im = Image.open(self.image_file).convert('RGB')
  #
  #   output = BytesIO()
  #
  #   # Resize/modify the image
  #   im = im.resize((200, 200))
  #
  #   # after modifications, save it to the output
  #   im.save(output, format='JPEG', quality=90, upload_to=collection_path)
  #   output.seek(0)
  #
  #   # change the imagefield value to be the newley modifed image value
  #   self.image = InMemoryUploadedFile(output, 'ImageField', "%s.jpg" % self.image_file.name.split('.')[0], 'image/jpeg',
  #                                     sys.getsizeof(output), None)
  #
  #   super(Collection, self).save()

  @property
  def places_ds(self):
    dses = self.datasets.all()
    return Place.objects.filter(dataset__in=dses)

  @property
  def places_all(self):
    all = Place.objects.filter(
      Q(dataset__in=self.datasets.all()) | Q(id__in=self.places.all().values_list('id'))
    )
    return all.exclude(id__in=self.omitted)

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
  link_type = models.CharField(default='page', max_length=10, choices=LINKTYPES)
  license = models.CharField(null=True, blank=True, max_length=64)

  def __str__(self):
    cap = self.label[:20]+('...' if len(self.label)>20 else '')
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
