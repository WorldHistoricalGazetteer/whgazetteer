# signals for the datasets app

from django.conf import settings
from django.db import models, transaction
from django.db.models.signals import pre_delete, pre_save, post_save
from django.dispatch import receiver

from .models import Dataset, DatasetFile
from main.utils import new_emailer

import logging
logger = logging.getLogger(__name__)
@receiver(pre_save, sender=Dataset)
def send_dataset_email(sender, instance, **kwargs):
    print('ds_status, public:', instance.ds_status, instance.public)
    try:
        if instance.pk is not None:  # Check if it's an existing instance, not new
            old_instance = sender.objects.get(pk=instance.pk)

            # Check if 'public' has been changed to True
            if old_instance.public != instance.public and instance.public:
                new_emailer(
                    email_type='dataset_published',
                    subject='Your WHG dataset has been published',
                    from_email=settings.DEFAULT_FROM_EMAIL,
                    to_email=[instance.owner.email],
                    reply_to=[settings.DEFAULT_FROM_EDITORIAL],
                    name=instance.owner.get_full_name(),
                    dataset_title=instance.title,
                    dataset_label=instance.label,
                    dataset_id=instance.id
                )

            # Check if 'ds_status' has been changed to 'indexed'
            if old_instance.ds_status != instance.ds_status and instance.ds_status == 'indexed':
                print('send_dataset_email: ds_status changed to indexed')
                new_emailer(
                    email_type='dataset_indexed',
                    subject='Your WHG dataset is fully indexed',
                    from_email=settings.DEFAULT_FROM_EMAIL,
                    to_email=[instance.owner.email],
                    reply_to=[settings.DEFAULT_FROM_EDITORIAL],
                    bcc=[settings.DEFAULT_FROM_EDITORIAL],
                    name=instance.owner.get_full_name(),
                    dataset_title=instance.title,
                    dataset_label=instance.label,
                    dataset_id=instance.id
                )
    except Exception as e:
        print('send_dataset_email error:', e)
        logger.exception("Error occurred while sending dataset email")

@receiver(post_save, sender=Dataset)
def send_new_dataset_email(sender, instance, created, **kwargs):
  try:
    if created:
        if not instance.owner.groups.filter(name='whg_team').exists():
            from main.utils import new_emailer
            new_emailer(
                email_type='new_dataset',
                subject='New Dataset Created',
                from_email=settings.DEFAULT_FROM_EMAIL,
                to_email=settings.EMAIL_TO_ADMINS,
                name=instance.owner.first_name + ' ' + instance.owner.last_name,
                username=instance.owner.username,
                dataset_title=instance.title,
                dataset_label=instance.label,
                dataset_id=instance.id
            )
  except Exception as e:
    logger.exception("Error occurred while sending new dataset email")

@receiver(pre_delete, sender=Dataset)
def remove_files(**kwargs):
  print('pre_delete remove_files()',kwargs)
  ds_instance = kwargs.get('instance')
  files = DatasetFile.objects.filter(dataset_id_id=ds_instance.id)
  files.delete()


