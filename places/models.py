# place.models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import models

from datasets.static.hashes.parents import ccodes as cc
from main.choices import FEATURE_CLASSES
from edtf import parse_edtf

def yearPadder(y):
    year = str(y).zfill(5) if str(y)[0] == '-' else str(y).zfill(4)
    return year

class Place(models.Model):
    # id is auto-maintained, per Django
    title = models.CharField(max_length=255)
    src_id = models.CharField(max_length=2044)
    # note FK is label, not id
    dataset = models.ForeignKey('datasets.Dataset', db_column='dataset',
        to_field='label', related_name='places', on_delete=models.CASCADE)
    ccodes = ArrayField(models.CharField(max_length=2))
    minmax = ArrayField(models.IntegerField(blank=True, null=True),null=True,blank=True)
    timespans = JSONField(blank=True,null=True) # for list of lists
    fclasses = ArrayField(models.CharField(max_length=1,choices=FEATURE_CLASSES),null=True,blank=True)
    indexed = models.BooleanField(default=False)
    
    flag = models.BooleanField(default=False) # not in use

    def __str__(self):
        # return str(self.id)
        return '%s:%d' % (self.dataset, self.id)

    @property
    def idx(self):
        return self.indexed
    
    @property
    def countries(self):
        return [cc[0][x]['gnlabel'] for x in self.ccodes]

    @property
    def geom_count(self):
        return self.geoms.count()
        
    class Meta:
        managed = True
        db_table = 'places'
        indexes = [
            models.Index(fields=['src_id', 'dataset']),
        ]


class Type(models.Model):
    aat_id = models.IntegerField(unique=True)
    parent_id = models.IntegerField(null=True,blank=True)
    term = models.CharField(max_length=100)
    term_full = models.CharField(max_length=100)
    note = models.TextField(max_length=3000)
    fclass = models.CharField(max_length=1,null=True,blank=True)
    
    def __str__(self):
        return str(self.aat_id) +':'+self.term
    
    class Meta:
        managed = True
        db_table = 'types'
    
class Source(models.Model):
    owner = models.ForeignKey(User, on_delete=models.CASCADE)
    # TODO: force unique...turn into slug or integer
    src_id = models.CharField(max_length=30, unique=True)    # contributor's id
    uri = models.URLField(null=True, blank=True)
    label = models.CharField(max_length=255)    # short, e.g. title, author
    citation = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self):
        return self.src_id

    class Meta:
        managed = True
        db_table = 'sources'

class PlaceName(models.Model):
    # {toponym, lang, citation{}, when{}}
    place = models.ForeignKey(Place, related_name='names',
        default=-1, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    toponym = models.CharField(max_length=200)
    name_src = models.ForeignKey(Source, null=True, on_delete=models.SET_NULL)
    jsonb = JSONField(blank=True, null=True)

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    def __str__(self):
        return self.toponym

    class Meta:
        managed = True
        db_table = 'place_name'

class PlaceType(models.Model):
    place = models.ForeignKey(Place,related_name='types',
        default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    jsonb = JSONField(blank=True, null=True)
    # identifier, label, source_label, when{}

    aat_id = models.IntegerField(null=True,blank=True) # Getty AAT identifier
    fclass = models.CharField(max_length=1,choices=FEATURE_CLASSES) # geonames feature class
 
    def __str__(self):
        #return self.jsonb['src_label']
        return self.jsonb['sourceLabel']

    class Meta:
        managed = True
        db_table = 'place_type'

class PlaceGeom(models.Model):
    place = models.ForeignKey(Place,related_name='geoms',
        default=-1, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    geom_src = models.ForeignKey(Source, null=True, db_column='geom_src',
        to_field='src_id', on_delete=models.SET_NULL)
    jsonb = JSONField(blank=True, null=True)

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    @property
    def title(self):
        return self.place.title
    
    class Meta:
        managed = True
        db_table = 'place_geom'

class PlaceLink(models.Model):
    place = models.ForeignKey(Place,related_name='links',
        default=-1, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    review_note = models.CharField(max_length=2044, blank=True, null=True)
    jsonb = JSONField(blank=True, null=True)
    black_parent = models.IntegerField(blank=True, null=True)

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    class Meta:
        managed = True
        db_table = 'place_link'

class PlaceWhen(models.Model):
    place = models.ForeignKey(Place,related_name='whens',
        default=-1, on_delete=models.CASCADE)
    jsonb = JSONField(blank=True, null=True)
    # timespans[{start{}, end{}}], periods[{name,id}], label, duration

    src_id = models.CharField(max_length=100,default='') # contributor's identifier
    minmax = ArrayField(models.IntegerField(blank=True,null=True))

    class Meta:
        managed = True
        db_table = 'place_when'

class PlaceRelated(models.Model):
    place = models.ForeignKey(Place,related_name='related',
        default=-1, on_delete=models.CASCADE)
    jsonb = JSONField(blank=True, null=True)
    # relation_type, relation_to, label, when{}, citation{label,id}, certainty

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    class Meta:
        managed = True
        db_table = 'place_related'

class PlaceDescription(models.Model):
    place = models.ForeignKey(Place,related_name='descriptions',
        default=-1, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    jsonb = JSONField(blank=True, null=True)
    # id, value, lang

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    class Meta:
        managed = True
        db_table = 'place_description'

class PlaceDepiction(models.Model):
    place = models.ForeignKey(Place,related_name='depictions',
        default=-1, on_delete=models.CASCADE)
    jsonb = JSONField(blank=True, null=True)
    # id, title, license

    src_id = models.CharField(max_length=100,default='') # contributor's identifier

    class Meta:
        managed = True
        db_table = 'place_depiction'
