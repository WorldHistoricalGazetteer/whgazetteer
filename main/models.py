from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model
User = get_user_model()
from django.core.validators import URLValidator
from django.urls import reverse
from collection.models import Collection, CollectionGroup
from datasets.models import Dataset
from places.models import Place
from traces.models import TraceAnnotation

from main.choices import COMMENT_TAGS, COMMENT_TAGS_REVIEW, LOG_CATEGORIES, LOG_TYPES, LINKTYPES

# generic links table for collections, collection groups, places?, etc.
class Link(models.Model):
    collection = models.ForeignKey(Collection, default=None,
        on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    collection_group = models.ForeignKey(CollectionGroup, default=None,
        on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    trace_annotation = models.ForeignKey(TraceAnnotation, default=None,
        on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    place = models.ForeignKey(Place, default=None,
        on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)

    uri=models.URLField(max_length=200)
    # uri = models.TextField(validators=[URLValidator()])
    label = models.CharField(null=True, blank=True, max_length=200)
    link_type = models.CharField(default='webpage', max_length=10, choices=LINKTYPES)
    license = models.CharField(null=True, blank=True, max_length=64)

    class Meta:
        managed = True
        db_table = 'links'

# some log entries only user-related; most user- and dataset-related
class Log(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='log', on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, null=True, blank=True, 
        related_name='log', on_delete=models.CASCADE)
    category = models.CharField(max_length=20, choices=LOG_CATEGORIES)
    logtype = models.CharField(max_length=20, choices=LOG_TYPES)
    subtype = models.CharField(max_length=50, null=True, blank=True)
    note = models.CharField(max_length=2044,null=True, blank=True)
    timestamp = models.DateTimeField(null=True, auto_now_add=True)
    
    class Meta:
        managed = True
        db_table = 'log'
    
class Comment(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='comments', on_delete=models.CASCADE)
    place_id = models.ForeignKey(Place, on_delete=models.CASCADE)
    tag = models.CharField(max_length=20, choices=COMMENT_TAGS_REVIEW, default="other")
    note = models.CharField(max_length=2044, null=True, blank=True)
    created = models.DateTimeField(null=True, auto_now_add=True)

    @property
    def dataset(self):
        return self.place_id.dataset
    
    class Meta:
        managed = True
        db_table = 'comments'
    
    
