from django.db import models
from django.contrib.auth.models import AbstractUser, PermissionsMixin


class User(AbstractUser, PermissionsMixin):

  USER_ROLE = (
    ('normal', 'normal'),
    ('group_leader', 'group leader'),
  )
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

