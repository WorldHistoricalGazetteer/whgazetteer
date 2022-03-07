from django.contrib import admin
from guardian.admin import GuardedModelAdmin

# class DatasetAdmin(GuardedModelAdmin):
#     list_display = ('id', 'label', 'title', 'create_date', 'datatype', 'ds_status')
# admin.site.register(Dataset,DatasetAdmin)
#
# #class DatasetFileAdmin(admin.ModelAdmin):
# class DatasetFileAdmin(GuardedModelAdmin):
#     list_display = ('dataset_id_id', 'file', 'upload_date', 'df_status', 'format', 'datatype')
# admin.site.register(DatasetFile,DatasetFileAdmin)

from .models import Resource, ResourceFile, ResourceImage
# Register your models here.

# class ResourceFileAdmin(admin.StackedInline):
#     list_display = ('','')
#     class Meta:
#         model = ResourceFile
#
# class ResourceImageAdmin(admin.StackedInline):
#     list_display = ('','')
#     class Meta:
#         model = ResourceImage

class ResourceAdmin(GuardedModelAdmin):
    list_display = ('id','create_date','pub_date', 'type', 'title')
    # inlines = [ResourceFileAdmin, ResourceImageAdmin]
    # model = Resource
admin.site.register(Resource, ResourceAdmin)

# @admin.register(ResourceFile)
class ResourceFileAdmin(GuardedModelAdmin):
    list_display = ('resource','file')
    # model = ResourceImage
admin.site.register(ResourceFile, ResourceFileAdmin)

# @admin.register(GuardedModelAdmin)
class ResourceImageAdmin(admin.ModelAdmin):
    list_display = ('resource','image')
    # model = ResourceImage
admin.site.register(ResourceImage, ResourceImageAdmin)

