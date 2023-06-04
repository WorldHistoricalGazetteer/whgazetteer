from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from accounts.models import Profile

class ProfileInline(admin.StackedInline):
    model = Profile
    can_delete = False
    verbose_name_plural = 'Profile'
    fk_name = 'user'

# appear in admin
# class MyUserAdmin(admin.ModelAdmin):
class MyUserAdmin(UserAdmin):
	inlines = (ProfileInline,)
	list_display = ('username', 'id', 'date_joined', 'first_name', 'last_name',)
	# fields = ('id','username',('date_joined','last_login'),'affiliation','first_name','last_name','groups',)
	readonly_fields = ('password',)

	def get_inline_instances(self, request, obj=None):
		if not obj:
			return list()
		return super(MyUserAdmin, self).get_inline_instances(request, obj)

admin.site.unregister(User)
admin.site.register(User,MyUserAdmin)
