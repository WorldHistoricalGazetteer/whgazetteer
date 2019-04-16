from django.contrib import admin
from .models import Area

class AreaAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'description')
admin.site.register(Area,AreaAdmin)
