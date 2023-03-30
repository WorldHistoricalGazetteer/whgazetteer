from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from main.choices import *

from multiselectfield import MultiSelectField

def resource_file_path(instance, filename):
  # upload to MEDIA_ROOT/resources/<id>_<filename>
  return 'resources/{0}'.format(filename)

class Resource(models.Model):
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  pub_date = models.DateTimeField(null=False)
  owner = models.ForeignKey(
      User, related_name='resources', on_delete=models.CASCADE)
  title = models.CharField(max_length=255, null=False)
  # [lessonplan | syllabus]
  type = models.CharField(max_length=12, null=False, choices=RESOURCE_TYPES)
  description = models.TextField(max_length=2044, null=False)
  subjects = models.CharField(max_length=2044, null=False)
  gradelevels = ArrayField(models.CharField(max_length=24, blank=True))
  keywords = ArrayField(models.CharField(max_length=24, null=False))
  authors = models.CharField(max_length=500, null=False)
  contact = models.CharField(max_length=500, null=True, blank=True)
  webpage = models.URLField(null=True, blank=True)
  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)

  # test commented 28 Mar
  regions = MultiSelectField(choices=REGIONS, null=True, blank=True)
  # regions = models.CharField(max_length=24, choices=REGIONS, null=True, blank=True)

  # [uploaded | published]
  status = models.CharField(
      max_length=12, null=True, blank=True, choices=RESOURCE_STATUS, default='uploaded')

  def __str__(self):
    return self.title
    # return '%d: %s' % (self.id, self.label)

  def title_length(self):
    return -len(self.title)
  
  @property
  def region_ids(self):
    return self.regions

  class Meta:
    managed = True
    db_table = 'resources'


class ResourceFile(models.Model):
  resource = models.ForeignKey(Resource, default=None, on_delete=models.CASCADE)
  file = models.FileField(upload_to=resource_file_path)
  filetype = models.CharField(max_length=12, null=False, blank=False, 
                              choices=RESOURCEFILE_ROLE, default='primary')

  # def __str__(self):
  #   return self.file

  class Meta:
    managed = True
    db_table = 'resource_file'


class ResourceImage(models.Model):
  resource = models.ForeignKey(Resource, default=None, on_delete=models.CASCADE)
  image = models.FileField(upload_to=resource_file_path)

  # def __str__(self):
  #   return self.image

  class Meta:
      managed = True
      db_table = 'resource_image'
