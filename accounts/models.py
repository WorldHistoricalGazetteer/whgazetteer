from django.db import models
from django.contrib.auth.models import User, Group
from django.db.models.signals import post_save
from django.dispatch import receiver

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    affiliation = models.TextField(max_length=200, blank=True)

def add_user_to_public_group(sender, instance, created, **kwargs):
    """Post-create user signal that adds the user to review group."""
    try:
        if created:
            instance.groups.add(Group.objects.get(pk=5))
    except Group.DoesNotExist:
        pass
post_save.connect(add_user_to_public_group, sender=User)

@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        Profile.objects.create(user=instance)

@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()
