from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from main.choices import *

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

def resource_image_path(instance, filename):
  # upload to MEDIA_ROOT/resources/<id>_<filename>
  return 'resources/{0}_{1}'.format(instance.id, filename)

class Resource(models.Model):
  # record created
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  owner = models.ForeignKey(
      User, related_name='resources', on_delete=models.CASCADE)
  title = models.CharField(max_length=255, null=False)
  # lessonplan, syllabus
  type = models.CharField(max_length=12, null=False, choices=RESOURCE_TYPES)
  description = models.CharField(max_length=2044, null=False)

  subjects = models.CharField(max_length=2044, null=False)
  gradelevels = models.CharField(max_length=255, null=False)
  authors = models.CharField(max_length=500, null=False)
  keywords = models.CharField(max_length=2044, null=False)
  pub_date = models.DateTimeField(null=True, auto_now_add=True)

  webpage = models.URLField(null=True, blank=True)
  image_file = models.FileField(upload_to=resource_image_path, blank=True, null=True)

  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)

  status = models.CharField(
      max_length=12, null=True, blank=True, choices=STATUS_RESOURCE)

  def __str__(self):
    return self.label
    # return '%d: %s' % (self.id, self.label)

  class Meta:
    managed = True
    db_table = 'resources'


class ResourceFile(models.Model):
  resource = models.ForeignKey(Resource, related_name='files',
                                 default=-1, on_delete=models.CASCADE)
  file = models.FileField(upload_to=user_directory_path)
  format = models.CharField(max_length=12, null=False,
                            choices=RESOURCE_FORMATS, default='pdf')
  upload_date = models.DateTimeField(null=True, auto_now_add=True)
  role = models.CharField(max_length=12, null=False,
                            choices=RESOURCEFILE_ROLE)

  #def __str__(self):
  #return 'file_'+str(self.rev)

  class Meta:
    managed = True
    db_table = 'resource_file'
