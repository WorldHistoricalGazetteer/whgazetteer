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

  USERNAME_FIELD = 'email'
  REQUIRED_FIELDS = ['name']

  class Meta:
      db_table = 'auth_user'

  def __str__(self):
    return '%s: %s' % (self.name, self.id)
