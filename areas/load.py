import os
from django.contrib.gis.utils import LayerMapping
from areas.models import Country

countries_mapping = {
    'tgnid' : 'tgnid',
    'tgnlabel' : 'tgnlabel',
    'iso' : 'iso',
    'gnlabel' : 'gnlabel',
    'geonameid' : 'geonameid',
    'un' : 'un',
    'variants' : 'variants',
    'mpoly' : 'MULTIPOLYGON',
}

shpfile = '/Users/karlg/Desktop/world-shp/countries.shp'


def run(verbose=True):
    lm = LayerMapping(Country, shpfile, countries_mapping, transform=False)
    lm.save(strict=True, verbose=verbose)
    
# tests
#from areas.models import Country
from django.contrib.gis.geos import Point, GEOSGeometry
# Points
pnt = Point(-9.3385, 29.7245)
qs=Country.objects.filter(mpoly__contains = pnt)
print(qs.count(),' countries')
for c in qs:
    print(c.iso, c.gnlabel)

# any geometry from wkt or json
gpointwkt = 'POINT (22.9139937912115 43.9859450581153)'
gpoint = '{ "type": "Point", "coordinates": [ 5.000000, 23.000000 ] }'
gmpoly = GEOSGeometry('MULTIPOLYGON(((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))')
gline = GEOSGeometry('MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))')

qs=Country.objects.filter(mpoly__intersects = gpointwkt)
qs=Country.objects.filter(mpoly__intersects = gpoint)
qs=Country.objects.filter(mpoly__intersects = gmpoly)
qs=Country.objects.filter(mpoly__intersects = gline)
for c in qs:
    print(c.iso, c.gnlabel)