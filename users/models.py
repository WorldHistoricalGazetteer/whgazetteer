from django.db import models
from django.contrib.auth.models import AbstractUser, PermissionsMixin
from main.choices import USER_ROLE


# src/users/model.py
from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils.translation import gettext_lazy as _


class UserManager(BaseUserManager):
    """
    Custom user model manager where email is the unique identifiers
    for authentication instead of usernames.
    """
    def create_user(self, email, password, **extra_fields):
        """
        Create and save a User with the given email and password.
        """
        if not email:
            raise ValueError(_('The Email must be set'))
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.set_password(password)
        user.save()
        return user

    def create_superuser(self, email, password, **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError(_('Superuser must have is_staff=True.'))
        if extra_fields.get('is_superuser') is not True:
            raise ValueError(_('Superuser must have is_superuser=True.'))
        return self.create_user(email, password, **extra_fields)


class User(AbstractUser, PermissionsMixin):
    username = None
    name = models.CharField(max_length=255)
    email = models.EmailField(_('email address'), unique=True)
    affiliation = models.CharField(max_length=255, null=True)
    role = models.CharField(max_length=24, choices=USER_ROLE, default='normal')
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['name']

    objects = UserManager()

    def __str__(self):
        return self.email

# class User(AbstractUser, PermissionsMixin):
#
#   email = models.EmailField(max_length=255, unique=True)
#   name = models.CharField(max_length=255)
#   affiliation = models.CharField(max_length=255, null=True)
#   role = models.CharField(max_length=24, choices=USER_ROLE, default='normal')
#   is_active = models.BooleanField(default=True)
#   is_staff = models.BooleanField(default=False)
#
#   # horrible after-effect of switching to a custom user model:
#   # abandoned fields are still embedded in the logic throughout!!
#   # i.e. username is required to create a new User
#   username = models.CharField(max_length=255,null=True,blank=True,unique=False)
#   first_name = models.CharField(max_length=255,null=True,blank=True)
#   last_name = models.CharField(max_length=255,null=True,blank=True)
#
#   USERNAME_FIELD = 'email'
#   REQUIRED_FIELDS = ['username']
#
#   class Meta:
#       # db_table = 'users'
#       db_table = 'auth_user'
#
#   def __str__(self):
#     return '%s: %s' % (self.name, self.id)
