from django.contrib import admin
from .models import *

class CollectionAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'description', 'owner', 'status')
    fields = ('id','collection_class','title','owner',('status','public','featured'),
              'description','image_file','keywords','rel_keywords','file','creator')
    readonly_fields = ('id','datasets','places','omitted','collection_class')
    list_filter = ('status','collection_class')
admin.site.register(Collection, CollectionAdmin)

class CollectionLinkAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'link_type', 'uri')
admin.site.register(CollectionLink, CollectionLinkAdmin)

class CollectionUserAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'user', 'role')
admin.site.register(CollectionUser, CollectionUserAdmin)
