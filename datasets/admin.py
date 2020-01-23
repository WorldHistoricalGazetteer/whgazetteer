from django.contrib import admin
from .models import Dataset, DatasetFile, Hit

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('id', 'label', 'title', 'format', 'datatype')
admin.site.register(Dataset,DatasetAdmin)

class DatasetFileAdmin(admin.ModelAdmin):
    list_display = ('dataset_id_id', 'file', 'upload_date', 'status', 'format', 'datatype')
admin.site.register(DatasetFile,DatasetFileAdmin)

admin.site.register(Hit)
