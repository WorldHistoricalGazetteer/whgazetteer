from datasets.views import failed_upload_notification
from django.conf import settings
from django.core import mail
from datasets.views import emailer
from django.test import TestCase
from datasets.models import Dataset
from django.contrib.auth.models import User

class SendDatasetEmailTest(TestCase):
		def setUp(self):
				# Create a test user
				self.user = User.objects.create_user(
					username='testuser', password='12345',
					first_name='Test', last_name='User',
				)

				# Create a test dataset
				self.dataset = Dataset.objects.create(
						owner=self.user,
						label='test_dataset',
						title='Test Dataset',
						description='This is a test dataset.',
						public=False,
						ds_status='uploaded'
				)

		def test_send_dataset_email_public(self):
				# Clear the outbox before saving the dataset
				mail.outbox = []

				# Change the 'public' attribute of the dataset to True
				self.dataset.public = True

				# Save the dataset
				# This should trigger the send_dataset_email() function
				self.dataset.save()
				print(self.dataset.id)
				# check if an EmailMessage instance was created with the expected subject, body, and recipient
				self.assertEqual(len(mail.outbox), 1)
				self.assertEqual(mail.outbox[0].subject, 'Your WHG dataset has been published')
				self.assertEqual(mail.outbox[0].body, 'Dear Test User,\n\n'
						'Thank you for publishing your dataset, Test Dataset (test_dataset, {}).\n'
						'It is now publicly viewable, and its records accessible in search and our API .\n\n'
						'regards,\nThe WHG project team'.format(self.dataset.id))

		def test_send_dataset_email_indexed(self):
			# Clear the outbox before saving the dataset
			mail.outbox = []

			# Change the 'ds_status' attribute of the dataset to 'indexed'
			self.dataset.ds_status = 'indexed'
			print(self.dataset.id)
			# Save the dataset
			# This should trigger the send_dataset_email() function
			self.dataset.save()

			# check if an EmailMessage instance was created with the expected subject, body, and recipient
			self.assertEqual(len(mail.outbox), 1)
			self.assertEqual(mail.outbox[0].subject, 'Your WHG dataset is fully indexed')
			self.assertEqual(mail.outbox[0].body, 'Dear Test User,\n\n'
					'Thank you for indexing your dataset, Test Dataset (test_dataset, {}).\n\n'
					'All of its records were already public; now many are linked with those for closely '
					'matched places coming from other projects.\n\n'
					'regards,\nThe WHG project team'.format(self.dataset.id))


# test emailer() in datasets/views.py
def test_emailer():
	user = User.objects.get(username='whgadmin')
	emailer(user, 'test.txt')
	assert True