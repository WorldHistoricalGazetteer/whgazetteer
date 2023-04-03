from django.db import models
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
User = get_user_model()
from django.db.models.signals import post_save
from django.dispatch import receiver
from main.choices import USERTYPES


