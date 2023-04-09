from django.db import models
from django.contrib.auth.models import AbstractUser, PermissionsMixin
from main.choices import USER_ROLE

class User(AbstractUser, PermissionsMixin):

  email = models.EmailField(max_length=255, unique=True)
  name = models.CharField(max_length=255)
  affiliation = models.CharField(max_length=255, null=True)
  role = models.CharField(max_length=24, choices=USER_ROLE, default='normal')
  is_active = models.BooleanField(default=True)
  is_staff = models.BooleanField(default=False)

  # this is a horrible after-effect of switching to a custom user model:
  # old abandoned fields are still embedded in the logic throughout!!
  first_name = models.CharField(max_length=255)
  last_name = models.CharField(max_length=255)
  username = models.CharField(max_length=255)

  USERNAME_FIELD = 'email'
  REQUIRED_FIELDS = ['name']

  class Meta:
      db_table = 'auth_user'

  def __str__(self):
    return '%s: %s' % (self.name, self.id)
