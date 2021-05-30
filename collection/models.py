from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField #,JSONField
from datasets.models import Dataset
from places.models import Place

def user_directory_path(instance, filename):
  # upload to MEDIA_ROOT/user_<username>/<filename>
  return 'user_{0}/{1}'.format(instance.owner.username, filename)

class Collection(models.Model):
  owner = models.ForeignKey(User,related_name='collections', on_delete=models.CASCADE)
  title = models.CharField(null=False, max_length=255)
  description = models.CharField( null=False, max_length=2044)
  tags = ArrayField(models.CharField(max_length=50))
  image_file = models.FileField(upload_to=user_directory_path, null=True)
  
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  public = models.BooleanField(default=False)

  #@property
  #def datasets(self):
    #return [cd.dataset for cd in self.collection_datasets.all()]

  #@property
  #def places(self):
    ## TODO: gang up indiv & ds places
    #dses = [d for d in self.datasets]
    #return Place.objects.filter(dataset__in=dses)

  def __str__(self):
    return '%s:%s' % (self.id, self.title)

  class Meta:
    db_table = 'collections'

class CollectionPlace(models.Model):
  collection = models.ForeignKey(Collection, related_name='coll_places',
                                   default=-1, on_delete=models.CASCADE)
  place = models.ForeignKey(Place, related_name='places',
                              default=-1, on_delete=models.CASCADE)

  def __str__(self):
    return self.collection + '<>' + self.place

  class Meta:
    managed = True
    db_table = 'collection_place'

class CollectionDataset(models.Model):
  collection = models.ForeignKey(Collection, related_name='collection_datasets',default=-1, on_delete=models.CASCADE)
  dataset = models.ForeignKey(Dataset, related_name='dataset_collections',default=-1, on_delete=models.CASCADE)

  def __str__(self):
    return '%s:%s' % (self.collection, self.dataset)

  class Meta:
    managed = True
    db_table = 'collection_dataset'





