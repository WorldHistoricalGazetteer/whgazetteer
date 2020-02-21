# datasets.models
from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.urls import reverse
from django.shortcuts import get_object_or_404

from main.choices import AUTHORITIES, FORMATS, DATATYPES, STATUS, TEAMROLES, PASSES
from places.models import Place

def user_directory_path(instance, filename):
    # upload to MEDIA_ROOT/user_<username>/<filename>
    return 'user_{0}/{1}'.format(instance.owner.username, filename)

class Dataset(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='datasets', on_delete=models.CASCADE)
    label = models.CharField(max_length=20, null=False, unique="True")
    title = models.CharField(max_length=255, null=False)
    description = models.CharField(max_length=2044, null=False)
    core = models.BooleanField(default=False)    
    public = models.BooleanField(default=False)    
    ds_status = models.CharField(max_length=12, null=True, blank=True, choices=STATUS)
    create_date = models.DateTimeField(null=True, auto_now_add=True)
    uri_base = models.URLField(blank=True, null=True)

    # TODO: these are updated in both Dataset & DatasetFile  (??)
    datatype = models.CharField(max_length=12, null=False,choices=DATATYPES,
        default='place')
    numrows = models.IntegerField(null=True, blank=True)
    
    # these are back-filled
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
    
    @property
    def collab(self):
        uids=DatasetUser.objects.filter(dataset_id_id = self.id).values_list('user_id_id')
        dus=DatasetUser.objects.filter(dataset_id_id = self.id)
        collabs=[]
        for du in dus:
            u = get_object_or_404(User, id=du.user_id_id)
            r = du.role
            collabs.append({'id':du.user_id_id,'user':u.username,'role':r})
        return collabs
        
    @property
    def placeids(self):
        return Place.objects.filter(dataset=self.label).values_list('id', flat=True)
        
    class Meta:
        managed = True
        db_table = 'datasets'

class DatasetFile(models.Model):
    dataset_id = models.ForeignKey(Dataset, related_name='files',
        default=-1, on_delete=models.CASCADE)
    rev = models.IntegerField(null=True, blank=True)
    file = models.FileField(upload_to=user_directory_path)
    format = models.CharField(max_length=12, null=False,
        choices=FORMATS, default='lpf')
    datatype = models.CharField(max_length=12, null=False,choices=DATATYPES,
        default='place')
    delimiter = models.CharField(max_length=5, blank=True, null=True)
    df_status = models.CharField(max_length=12, null=True, blank=True, choices=STATUS)
    upload_date = models.DateTimeField(null=True, auto_now_add=True)
    header = ArrayField(models.CharField(max_length=30), null=True, blank=True)
    numrows = models.IntegerField(null=True, blank=True)
    
    #def __str__(self):
        #return 'file_'+str(self.rev)

    class Meta:
        managed = True
        db_table = 'dataset_file'
        
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
    query_pass = models.CharField(max_length=12, choices=PASSES )
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
    print('remove_file()',kwargs)
    ds_instance = kwargs.get('instance')
    files = DatasetFile.objects.filter(dataset_id_id=ds_instance.id)
    files.delete()

# raw hits from reconciliation
# [{'place_id', 'task_id', 'authority', 'dataset', 'authrecord_id', 'id'}]
