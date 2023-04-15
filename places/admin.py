# places.admin

from django.contrib import admin
from django.contrib.gis import admin as gisadmin
from .models import *

# appear in admin
class PlaceAdmin(admin.ModelAdmin):
    list_display = ('id', 'dataset','title', 'ccodes', 'src_id')
    list_filter = ('dataset',)
admin.site.register(Place,PlaceAdmin)

class SourceAdmin(admin.ModelAdmin):
    list_display = ('owner', 'src_id', 'label', 'uri')
admin.site.register(Source,SourceAdmin)

class PlaceLinkAdmin(admin.ModelAdmin):
    list_display = ('place_id', 'jsonb')
admin.site.register(PlaceLink,PlaceLinkAdmin)

class PlaceNameAdmin(admin.ModelAdmin):
    list_display = ('place_id','jsonb')
admin.site.register(PlaceName,PlaceNameAdmin)

class PlaceTypeAdmin(admin.ModelAdmin):
    list_display = ('place_id','jsonb')
admin.site.register(PlaceType,PlaceTypeAdmin)

class PlaceGeomAdmin(gisadmin.GeoModelAdmin):
# class PlaceGeomAdmin(admin.ModelAdmin):
    list_display = ('place_id','src_id', 'task_id')
    fields = ("src_id", "geom", 'task_id')
admin.site.register(PlaceGeom,PlaceGeomAdmin)

admin.site.register(PlaceWhen)
admin.site.register(PlaceRelated)
admin.site.register(PlaceDescription)
admin.site.register(PlaceDepiction)

