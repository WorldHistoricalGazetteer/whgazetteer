from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField, ArrayField
from django.core.validators import URLValidator
from main.choices import TRACETYPES, TRACERELATIONS

# annotates a collection (& its subject) with a place
# FKs: Collection, Place
class TraceAnnotation(models.Model):
    # auto id
    src_id = models.CharField(max_length=2044, blank=True, null=True) # if exists
    collection = models.ForeignKey('collection.Collection', db_column='collection',
        related_name='collections', on_delete=models.CASCADE)
    place = models.ForeignKey('places.Place', db_column='place',
        related_name='places', on_delete=models.CASCADE)
    # standard 'when' from LP format
    when = JSONField(blank=True, null=True) # {timespans[[],...], periods[{name, uri}], label, duration}
    start = models.IntegerField(null=True, blank=True)
    end = models.IntegerField(null=True, blank=True)
    sequence = models.IntegerField(blank=True, null=True)
    trace_type = models.CharField(max_length=20, choices = TRACETYPES)
    motivation = models.CharField(max_length=20, default='locating') # choices? locating, describing
    creator = JSONField(blank=True, null=True) # {name, affiliation, orcid, webpage}
    created = models.DateTimeField(null=True, auto_now_add=True)

    def __str__(self):
        return '%s:%d' % (self.collection.id, self.place.id)

    class Meta:
        managed = True
        db_table = 'trace_annotations'
        indexes = [
            models.Index(fields=['collection']),
            models.Index(fields=['place']),
        ]

# lookup for relations, which are collection-specific
class TraceRelation(models.Model):
    # id is auto-generated
    relation = models.CharField(max_length=255)
    collection = models.ForeignKey('collection.Collection', db_column='collection',
        to_field='id', related_name='relations', on_delete=models.CASCADE)

    class Meta:
        managed = True
        db_table = 'trace_relations'