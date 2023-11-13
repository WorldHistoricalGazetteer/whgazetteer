from django.apps import apps
from django.db.models.signals import pre_save
from django.dispatch import receiver
# from places.models import Place
import logging
logger = logging.getLogger(__name__)
print('in signals.py')

@receiver(pre_save, sender=apps.get_model('places', 'Place'))
def handle_index_change(sender, instance, **kwargs):
    logger.info('in Place signal')
    from datasets.tasks import unindex_from_pub
    Place = apps.get_model('places', 'Place')
    # If the instance is not in the database yet, it's a new record, so no action is needed.
    if instance._state.adding or not instance.pk:
        logger.info("signal: the instance is not in the database yet, it's a new record, so no action is needed.")
        return

    # Get the current value from the database for existing objects
    try:
        current_place = Place.objects.only('indexed').get(pk=instance.pk)
    except Place.DoesNotExist:
        logger.info('from signal: The object does not exist in the database yet.')
        return

    # Check if 'indexed' was False and has changed to True
    if not current_place.indexed and instance.indexed:
        # Perform the unindex operation synchronously or asynchronously based on your choice.
        # Call the task synchronously using the function signature:
        unindex_from_pub(place_id=instance.pk)

        # Update the idx_pub flag to False for the instance.
        instance.idx_pub = False
        # Note: There's no need to save the instance here since the save operation is already in progress.
        # The pre_save signal is just used to perform some action before the actual save happens.
