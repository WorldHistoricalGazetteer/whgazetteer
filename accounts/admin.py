from django.contrib import admin
from django.contrib.auth import get_user_model
User = get_user_model()


#from .models import Profile
# Register your models here.

# *should* appear in admin
class UserAdmin(admin.ModelAdmin):
    list_display = ('email', 'id', 'name', 'affiliation', 'role',)
    fields = ('id','email', 'name','affiliation', 'role', 'date_joined',
              ('is_staff', 'is_active', 'is_superuser'))
    readonly_fields = ('id', 'date_joined', )
    list_filter = ('role',)


admin.site.register(User,UserAdmin)
