from django.contrib import admin
from .models import *

class CollectionAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'description', 'owner')
admin.site.register(Collection, CollectionAdmin)

class CollectionLinkAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'link_type', 'uri')
admin.site.register(CollectionLink, CollectionLinkAdmin)

class CollectionUserAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'user', 'role')
admin.site.register(CollectionUser, CollectionUserAdmin)
