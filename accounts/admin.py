from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
User = get_user_model()


#from .models import Profile
# Register your models here.
# class GroupInline(admin.StackedInline):
#   model = Group

# *should* appear in admin
class UserAdmin(admin.ModelAdmin):
    list_display = ('email', 'id', 'name', 'affiliation', 'role',)
    fields = ('id','email', 'name', 'affiliation', 'role', 'date_joined',
              'groups', ('is_staff', 'is_active', 'is_superuser'))
    readonly_fields = ('id', 'date_joined', )
    list_filter = ('role',)

    filter_horizontal = ('groups',)
    # fieldsets = (
    #   (None, {
    #     'fields': ('id', 'email', 'name', 'affiliation', 'role', 'date_joined',
    #     ('is_staff', 'is_active', 'is_superuser'))
    #   }),
    #   ('Group Permissions', {
    #       'classes': ('collapse',),
    #       # 'fields': ('groups', )
    #       'fields': ('groups', 'user_permissions', )
    #   })
    # )
#
# @admin.register(User)
# class UserAdmin(BaseUserAdmin):
#     inlines = (GroupInline,)

admin.site.register(User,UserAdmin)
