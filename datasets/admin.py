from django.contrib import admin
from .models import Dataset, Hit

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('id', 'label', 'name', 'format', 'datatype')
admin.site.register(Dataset,DatasetAdmin)

admin.site.register(Hit)
