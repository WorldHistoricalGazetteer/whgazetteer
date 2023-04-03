from django.db import models
from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
  
  email = models.EmailField(max_length=255, unique=True)
  username = models.CharField(max_length=50, unique=True)
  name = models.CharField(max_length=255)
  affiliation = models.CharField(max_length=255, null=True)
  is_active = models.BooleanField(default=True)
  is_staff = models.BooleanField(default=False)

  USERNAME_FIELD = 'email'
  REQUIRED_FIELDS = ['username']

  class Meta:
      db_table = 'auth_user'

