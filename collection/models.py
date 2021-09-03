from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField #,JSONField
from django.urls import reverse
from datasets.models import Dataset
from places.models import Place
from tinymce.models import HTMLField

def coll_image_path(instance, filename):
  # upload to MEDIA_ROOT/collections/<id>_<filename>
  return 'collections/{0}_{1}'.format(instance.id, filename)

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

class Collection(models.Model):
  owner = models.ForeignKey(User,related_name='collections', on_delete=models.CASCADE)
  title = models.CharField(null=False, max_length=255)
  description = models.CharField( null=False, max_length=2044)
  keywords = ArrayField(models.CharField(max_length=50))
  image_file = models.FileField(upload_to=coll_image_path)

  # 3 new fields, 20210619
  creator = models.CharField(null=True, blank=True, max_length=500)
  contact = models.CharField(null=True, blank=True, max_length=500)
  webpage = models.URLField(null=True, blank=True)

  image_file = models.FileField(upload_to=coll_image_path, blank=True, null=True)
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)

  datasets = models.ManyToManyField("datasets.Dataset")

  # tinymce field
  content = HTMLField()

  def get_absolute_url(self):
    #return reverse('datasets:dashboard', kwargs={'id': self.id})
    return reverse('datasets:dashboard')

  @property
  def places(self):
    # TODO: gang up indiv & ds places
    dses = self.datasets.all()
    return Place.objects.filter(dataset__in=dses).distinct()

  @property
  def rowcount(self):
    dses = self.datasets.all()
    counts = [ds.places.count() for ds in dses]
    return sum(counts)

  def __str__(self):
    return '%s:%s' % (self.id, self.title)

  class Meta:
    db_table = 'collections'
