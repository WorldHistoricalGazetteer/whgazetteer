# MOVED to places.admin

# from django.contrib import admin
# from .models import Place, Source, PlaceName, PlaceType, PlaceGeom, PlaceWhen, PlaceRelated, PlaceLink, PlaceDescription, PlaceDepiction
#
# # appear in admin
# class PlaceAdmin(admin.ModelAdmin):
#     list_display = ('id', 'dataset','title', 'ccodes', 'src_id')
# admin.site.register(Place,PlaceAdmin)
#
# class SourceAdmin(admin.ModelAdmin):
#     list_display = ('owner', 'src_id', 'label', 'uri')
# admin.site.register(Source,SourceAdmin)
#
# class PlaceLinkAdmin(admin.ModelAdmin):
#     list_display = ('place_id', 'json')
# admin.site.register(PlaceLink,PlaceLinkAdmin)
#
# class PlaceNameAdmin(admin.ModelAdmin):
#     list_display = ('place_id','json')
# admin.site.register(PlaceName,PlaceNameAdmin)
#
# class PlaceTypeAdmin(admin.ModelAdmin):
#     list_display = ('place_id','json')
# admin.site.register(PlaceType,PlaceTypeAdmin)
#
# class PlaceGeomAdmin(admin.ModelAdmin):
#     list_display = ('place_id','json')
# admin.site.register(PlaceGeom,PlaceGeomAdmin)
#
# admin.site.register(PlaceWhen)
# admin.site.register(PlaceRelated)
# admin.site.register(PlaceDescription)
# admin.site.register(PlaceDepiction)
