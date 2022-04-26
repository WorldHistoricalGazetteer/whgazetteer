from django.contrib import admin

# Register your models here.
from .models import Resource, ResourceFile, ResourceImage

class ResourceFileAdmin(admin.StackedInline):
    model = ResourceFile

class ResourceImageAdmin(admin.StackedInline):
    model = ResourceImage


@admin.register(Resource)
class ResourceAdmin(admin.ModelAdmin):
    inlines = [ResourceFileAdmin, ResourceImageAdmin]

    class Meta:
       model = Resource


@admin.register(ResourceFile)
class ResourceFileAdmin(admin.ModelAdmin):
    pass

@admin.register(ResourceImage)
class ResourceImageAdmin(admin.ModelAdmin):
    pass
