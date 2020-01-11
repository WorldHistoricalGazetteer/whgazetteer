# datasets.models
from django.conf import settings
#from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField, ArrayField
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.urls import reverse
from django.shortcuts import get_object_or_404
#from django.utils import timezone
#from django.utils.text import slugify

#from django_celery_results.models import TaskResult
from main.choices import AUTHORITIES, FORMATS, DATATYPES, STATUS, TEAMROLES
from places.models import Place

def user_directory_path(instance, filename):
    # upload to MEDIA_ROOT/user_<username>/<filename>
    return 'user_{0}/{1}'.format(instance.owner.username, filename)

class Dataset(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='datasets', on_delete=models.CASCADE)
    label = models.CharField(max_length=20, null=False, unique="True")
    name = models.CharField(max_length=255, null=False)
    description = models.CharField(max_length=2044, null=False)
    file = models.FileField(upload_to=user_directory_path)
    uri_base = models.URLField(blank=True, null=True, default="http://whgazetteer.org/api/places/")
    format = models.CharField(max_length=12, null=False,choices=FORMATS,
        default='lpf')
    datatype = models.CharField(max_length=12, null=False,choices=DATATYPES,
        default='place')
    delimiter = models.CharField(max_length=5, blank=True, null=True)
    status = models.CharField(max_length=12, null=True, blank=True, choices=STATUS)
    upload_date = models.DateTimeField(null=True, auto_now_add=True)
    accepted_date = models.DateTimeField(null=True)
    spine = models.BooleanField(default=False)    
    mapbox_id = models.CharField(max_length=200, null=True, blank=True)

    # backfilled
    header = ArrayField(models.CharField(max_length=30), null=True, blank=True)
    numrows = models.IntegerField(null=True, blank=True)
    numlinked = models.IntegerField(null=True, blank=True)
    total_links = models.IntegerField(null=True, blank=True)

        
    def __str__(self):
        return self.label
        # return '%d: %s' % (self.id, self.label)

    def get_absolute_url(self):
        return reverse('datasets:dataset-detail', kwargs={'id': self.id})
    
    @property
    def tasks(self):
        from django_celery_results.models import TaskResult
        return TaskResult.objects.all().filter(task_args = '['+str(self.id)+']')

    #from django.shortcuts import get_object_or_404
    #from django.contrib.auth.models import User
    #from datasets.models import *
    
    #u=get_object_or_404(User,id=4)
    #ds=get_object_or_404(Dataset,id=104)
    #uids=DatasetUser.objects.filter(dataset_id_id = ds.id).values_list('user_id_id')
    #users = User.objects.filter(id__in=uids)
    
    @property
    def dsusers(self):
        uids=DatasetUser.objects.filter(dataset_id_id = self.id).values_list('user_id_id')
        return User.objects.filter(id__in=uids)
        
    class Meta:
        managed = True
        db_table = 'datasets'


class DatasetUser(models.Model):
    dataset_id = models.ForeignKey(Dataset, related_name='users',
        default=-1, on_delete=models.CASCADE)
    user_id = models.ForeignKey(User, related_name='users',
        default=-1, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES)

    #def __str__(self):
        #return self.toponym

    class Meta:
        managed = True
        db_table = 'dataset_user'

# TODO: operations on entire table
# class DatasetQueryset(models.Queryset):
#     pass
# class DatasetManager(models.Manager):
#     pass

# TODO: multiple files per dataset w/File model and formset
# TODO: linking delimited dataset with sources dataset
class Hit(models.Model):
    place_id = models.ForeignKey(Place, on_delete=models.CASCADE)
    # FK to celery_results_task_result.task_id; TODO: written yet?
    # task_id = models.ForeignKey(TaskResult, related_name='task_id', on_delete=models.CASCADE)
    task_id = models.CharField(max_length=50)
    authority = models.CharField(max_length=12, choices=AUTHORITIES )
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    query_pass = models.CharField(max_length=12, choices=AUTHORITIES )
    src_id = models.CharField(max_length=2044)
    score = models.FloatField()
    reviewed = models.BooleanField(default=False)
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
def remove_file(**kwargs):
    instance = kwargs.get('instance')
    instance.file.delete(save=False)

# raw hits from reconciliation
# [{'place_id', 'task_id', 'authority', 'dataset', 'authrecord_id', 'id'}]
