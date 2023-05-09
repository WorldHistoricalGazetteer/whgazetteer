from django.db import models
from django.contrib.auth import get_user_model
User = get_user_model()
from django.contrib.postgres.fields import ArrayField
from django.db.models import JSONField
from django_resized import ResizedImageField

def collection_path(instance, filename):
  # upload to MEDIA_ROOT/collections/<coll id>/<filename>
  return 'collections/anno/{1}'.format(instance.id, filename)

"""
    annotates a place record in a collection
    initial record w/defaults created upon adding place to collection
    FKs: Collection, Place
"""
# TODO: this is redundant to CollPlace; both are created on adding place to Collection; refactor?
class TraceAnnotation(models.Model):
    collection = models.ForeignKey('collection.Collection', db_column='collection',
        related_name='traces', on_delete=models.CASCADE)
    place = models.ForeignKey('places.Place', db_column='place',
        related_name='traces', on_delete=models.CASCADE)
    src_id = models.CharField(max_length=2044, blank=True, null=True) # if exists

    # optional free text note
    note = models.CharField(max_length=2044, blank=True, null=True)
    image_file = ResizedImageField(size=[800, 600],
                                   upload_to=collection_path,
                                   blank=True, null=True)

    # user-defined list of relations
    relation = ArrayField(models.CharField(max_length=30), default=[''], blank=True, null=True)

    start = models.CharField(max_length=11, blank=True, null=True) # ISO8601 date, incl. '-'
    end = models.CharField(max_length=11, blank=True, null=True) # ISO8601 date, incl. '-'
    # TODO: redundant to CollPlace.sequence; this one not used
    sequence = models.IntegerField(blank=True, null=True)
    anno_type = models.CharField(max_length=20, default='place', blank=True, null=True)
    # holdover from W3C anno model
    motivation = models.CharField(max_length=20, default='locating') # choices? locating, describing
    owner = models.ForeignKey(User, related_name='annotations', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True, null=False, blank=True)

    # flagged True when edit is made to initial 'blank'
    saved = models.BooleanField(default=False)
    # failsafe for when place or dataset is removed
    # TODO: logic to manage 'expired' collections
    archived = models.BooleanField(default=False)

    # standard 'when' from LP format; 20220416: not in use
    when = JSONField(blank=True, null=True) # {timespans[[],...], periods[{name, uri}], label, duration}

    @property
    def blank(self):
        return not self.relation and not self.note and not self.start and not self.end

    def __str__(self):
        return '%s:%d' % (self.collection.id, self.place.id)

    class Meta:
        managed = True
        db_table = 'trace_annotations'
        indexes = [
            models.Index(fields=['collection']),
            models.Index(fields=['place']),
        ]

""" lookup for relations, which are collection-specific """
# TODO: not in use; deprecate?
class TraceRelation(models.Model):
    relation = models.CharField(max_length=255)
    collection = models.ForeignKey('collection.Collection', db_column='collection',
        to_field='id', related_name='relations', on_delete=models.CASCADE)

    class Meta:
        managed = True
        db_table = 'trace_relations'