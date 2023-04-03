from django.contrib import admin
from .models import Dataset, DatasetFile, Hit
from guardian.admin import GuardedModelAdmin

# class DatasetAdmin(GuardedModelAdmin):
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('title', 'label', 'id', 'ds_status', 'public', 'create_date')
    list_filter = ('ds_status',)
    fields = ('id','label','title','owner','ds_status',
              ('public','core',), 'featured',
              'creator', 'numrows','numlinked','total_links')
    readonly_fields = ('id','label','owner','create_date','numrows','numlinked','total_links',)
    search_fields = ('title','label')
admin.site.register(Dataset, DatasetAdmin)
# admin.site.register(Dataset)

#class DatasetFileAdmin(admin.ModelAdmin):
class DatasetFileAdmin(GuardedModelAdmin):
    list_display = ('dataset_id_id', 'file', 'upload_date', 'df_status', 'format', 'datatype')
admin.site.register(DatasetFile, DatasetFileAdmin)

admin.site.register(Hit)
