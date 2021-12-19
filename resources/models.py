from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from main.choices import *

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

def resource_file_path(instance, filename):
  # upload to MEDIA_ROOT/resources/<id>_<filename>
  return 'resources/{0}_{1}'.format(instance.id, filename)

class ResourceFile(models.Model):
    file = models.FileField(upload_to=resource_file_path)

    class Meta:
        managed = True
        db_table = 'resource_file'

class ResourceImage(models.Model):
    file = models.FileField(upload_to=resource_file_path)

    class Meta:
        managed = True
        db_table = 'resource_image'

class Resource(models.Model):
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  pub_date = models.DateTimeField(null=False)
  owner = models.ForeignKey(
      User, related_name='resources', on_delete=models.CASCADE)
  title = models.CharField(max_length=255, null=False)
  # [lessonplan | syllabus]
  type = models.CharField(max_length=12, null=False, choices=RESOURCE_TYPES)
  description = models.CharField(max_length=2044, null=False)
  subjects = models.CharField(max_length=2044, null=False)
  gradelevels = ArrayField(models.CharField(max_length=24, blank=True))
  keywords = ArrayField(models.CharField(max_length=24, null=False))
  authors = models.CharField(max_length=500, null=False)
  contact = models.CharField(max_length=500, null=True, blank=True)
  webpage = models.URLField(null=True, blank=True)

  files = models.ManyToManyField(ResourceFile)
  images = models.ManyToManyField(ResourceImage)

  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)

  # [uploaded | published]
  status = models.CharField(
      max_length=12, null=True, blank=True, choices=STATUS_RESOURCE)

  def __str__(self):
    return self.label
    # return '%d: %s' % (self.id, self.label)

  class Meta:
    managed = True
    db_table = 'resources'

