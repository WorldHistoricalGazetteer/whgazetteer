from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.gis.db import models as geomodels
from django.contrib.postgres.fields import JSONField, ArrayField
from django.urls import reverse
from djgeojson.fields import PolygonField


class Country(models.Model):
    tgnid = models.IntegerField(blank=True, null=True)
    tgnlabel = models.CharField(max_length=255, blank=True, null=True)
    iso = models.CharField(max_length=2)
    gnlabel = models.CharField(max_length=255)
    geonameid = models.IntegerField()
    un = models.CharField(max_length = 3, blank=True, null=True)
    variants = JSONField(blank=True, null=True)
    
    poly = geomodels.PolygonField()
    
    def __str__(self):
        return self.gnlabel    

# user-created study area to constrain reconciliation
class Area(models.Model):
    # id (pk) auto-maintained, per Django
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='areas', on_delete=models.CASCADE)
    type = models.CharField(max_length=20)
    title = models.CharField(max_length=255)
    description = models.CharField(max_length=2044)
    ccodes = ArrayField(models.CharField(max_length=2),blank=True, null=True)
    # geom = geomodels.MultiPolygonField(blank=True, null=True)
    geojson = JSONField(blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)
    # updated = models.DateTimeField(null=True, auto_now_add=True)

    def __str__(self):
        return str(self.id)

    def get_absolute_url(self):
        return reverse('areas:area-update', kwargs={'id': self.id})

    class Meta:
        managed = True
        db_table = 'areas'
        indexes = [
            # models.Index(fields=['', '']),
        ]
