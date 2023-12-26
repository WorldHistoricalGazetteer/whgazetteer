from datasets.views import failed_upload_notification
from django.conf import settings
from django.contrib.auth.models import User
from datasets.views import emailer


# test status_emailer(ds, task_name) in datasets/utils.py
# (wd, idx)
def test_status_emailer():
	user = User.objects.get(username='whgadmin')
	emailer(user, 'test.txt')
	assert True

# test failed_upload_notification(user, fn, ds=None) in datasets/views.py
def test_failed_upload_notification():
	user = User.objects.get(username='whgadmin')
	failed_upload_notification(user, 'test.txt')
	assert True

user = User.objects.get(username='whgadmin')
emailer('So you know...a new WHG user just registered on the site: {} ({} {}, id {}) '.format(
	user.username, user.first_name, user.last_name, user.id),
                settings.DEFAULT_FROM_EMAIL, settings.EMAIL_TO_ADMINS)


# test emailer() in datasets/views.py
def test_emailer():
	user = User.objects.get(username='whgadmin')
	emailer(user, 'test.txt')
	assert True