from django.contrib import admin
from .models import Dataset, DatasetFile, Hit
from guardian.admin import GuardedModelAdmin

# class DatasetAdmin(GuardedModelAdmin):
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('id', 'label', 'title', 'create_date', 'ds_status', 'public')
admin.site.register(Dataset, DatasetAdmin)
# admin.site.register(Dataset)

#class DatasetFileAdmin(admin.ModelAdmin):
class DatasetFileAdmin(GuardedModelAdmin):
    list_display = ('dataset_id_id', 'file', 'upload_date', 'df_status', 'format', 'datatype')
admin.site.register(DatasetFile, DatasetFileAdmin)

admin.site.register(Hit)
