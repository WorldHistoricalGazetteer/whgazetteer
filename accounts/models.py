from django.db import models
from django.contrib.auth.models import User, Group
from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models.signals import post_save
from django.dispatch import receiver

from datasets.views import emailer
from main.choices import USERTYPES
from main.utils import new_emailer

from django.core.mail import send_mail
from allauth.account.models import EmailAddress


# send welcome email to new user; copy to admins; reply-to editorial
@receiver(post_save, sender=EmailAddress)
def send_welcome_email(sender, instance, **kwargs):
    if instance.verified:
        uname = instance.user.username
        full_name = instance.user.first_name + ' ' + instance.user.last_name
        # email_type, subject, from_email, to_email, **kwargs
        new_emailer('welcome',
                    'Welcome to the World Historical Gazetteer',
                    settings.DEFAULT_FROM_EMAIL,
                    [instance.email],
                    name=full_name,
                    username=uname,
                    id=instance.user.id,
                    reply_to=[settings.DEFAULT_FROM_EDITORIAL]
                    )
        new_emailer('new_user',
                    'New WHG user',
                    settings.DEFAULT_FROM_EMAIL,
                    settings.EMAIL_TO_ADMINS,
                    name=full_name,
                    username=uname,
                    id=instance.user.id,
                    )

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    name = models.TextField(max_length=200, null=False, blank=False)
    affiliation = models.TextField(max_length=200, null=True, blank=True)
    web_page = models.URLField(null=True, blank=True)
    user_type = models.CharField(blank=False, null=False, max_length=10, choices=USERTYPES)
    
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
