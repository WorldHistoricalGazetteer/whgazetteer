from django.db import models
from django.db.models import Q
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

  datasets = models.ManyToManyField("datasets.Dataset", blank=True)
  # COLL: indiv places
  places = models.ManyToManyField("places.Place", blank=True)

  # tinymce field
  content = HTMLField(null=True, blank=True)

  def get_absolute_url(self):
    #return reverse('datasets:dashboard', kwargs={'id': self.id})
    return reverse('datasets:dashboard')

  @property
  def places_ds(self):
    dses = self.datasets.all()
    return Place.objects.filter(dataset__in=dses)

  @property
  def places_all(self):
    all = Place.objects.filter(Q(dataset__in=self.datasets.all()) | Q(id__in=self.places.all().values_list('id')))
    # dses = self.datasets.all()
    # pids = self.places.all().values_list('id')
    # dses = coll.datasets.all()
    # pd = Place.objects.filter(dataset__in=dses)
    # pc = Place.objects.filter(id__in=self.places.values_list('id', flat=True))
    # all = pd.union(pc)
    # all = Place.objects.filter(Q(dataset__in=dses or id in self.places.values_list('id', flat=True)))
    # all = pd.union(self.places.all())
    return all

  @property
  def ds_list(self):
    dsc = [{"id":d.id, "label":d.label, "title":d.title} for d in self.datasets.all()]
    # dsc = list(self.datasets.values_list('label', flat=True))
    dsp = [{"id":p.dataset.id, "label":p.dataset.label, "title":p.dataset.title} for p in self.places.all()]
    # dsp = [p.dataset.label for p in self.places.all()]
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
