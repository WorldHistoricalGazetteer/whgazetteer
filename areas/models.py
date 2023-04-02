from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
User = get_user_model()

from django.contrib.gis.db import models as geomodels
from django.contrib.postgres.fields import ArrayField
from django.db.models import JSONField
from django.urls import reverse

from datasets.models import Dataset
from main.choices import AREATYPES
from places.models import PlaceGeom
from django.contrib.gis.geos import GEOSGeometry

import json
# from django.contrib.gis.db import models as geomodels
# from djgeojson.fields import PolygonField


class Country(models.Model):
    tgnid = models.IntegerField('Getty TGN id',blank=True, null=True)
    tgnlabel = models.CharField('Getty TGN preferred name',max_length=255, blank=True, null=True)
    iso = models.CharField('2-character code',max_length=2)
    gnlabel = models.CharField('geonames label',max_length=255)
    geonameid = models.IntegerField('geonames id')
    un = models.CharField('UN name',max_length = 3, blank=True, null=True)
    variants = models.CharField(max_length=512, blank=True, null=True)
    
    mpoly = geomodels.MultiPolygonField()
    
    def __str__(self):
        return self.gnlabel

    class Meta:
        managed = True
        db_table = 'countries'


# user-created study area to constrain reconciliation
class Area(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='areas', on_delete=models.CASCADE)
    type = models.CharField(max_length=20, choices=AREATYPES)
    title = models.CharField(max_length=255)
    description = models.CharField(max_length=2044)
    ccodes = ArrayField(models.CharField(max_length=2),blank=True, null=True)
    geojson = JSONField(blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)


    def __str__(self):
        return str(self.id)

    def get_absolute_url(self):
        return reverse('areas:area-update', kwargs={'id': self.id})

    @property
    def count_public(self):
        ds_public = Dataset.objects.filter(public=True)
        areageom = GEOSGeometry(json.dumps(self.geojson))
        places = PlaceGeom.objects.filter(geom__within=areageom, place__dataset__in=ds_public)
        return places.count()

    class Meta:
        managed = True
        db_table = 'areas'
        indexes = [
            # models.Index(fields=['', '']),
        ]
