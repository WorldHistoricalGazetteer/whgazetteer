from django.contrib import admin

# Register your models here.
from .models import Resource, ResourceFile, ResourceImage

class ResourceFileAdmin(admin.StackedInline):
    model = ResourceFile

class ResourceImageAdmin(admin.StackedInline):
    model = ResourceImage

# pub_date, owner, title, type, description, subjects, gradelevels, keywords, authors,
# contact, webpage, public, featured, regions, status

@admin.register(Resource)
class ResourceAdmin(admin.ModelAdmin):
    fields = ('pub_date','owner','title','authors','description','keywords',
              'type','subjects','gradelevels', 'public', 'featured', 'status',
              'regions')
    list_display = ('title', 'pub_date', 'authors', 'gradelevels', 'type', 'featured')
    list_filters = ('gradelevels', 'authors')
    # date_hierarchy = 'pub_date'

    inlines = [ResourceFileAdmin, ResourceImageAdmin]

    class Meta:
       model = Resource


@admin.register(ResourceFile)
class ResourceFileAdmin(admin.ModelAdmin):
    pass

@admin.register(ResourceImage)
class ResourceImageAdmin(admin.ModelAdmin):
    pass
