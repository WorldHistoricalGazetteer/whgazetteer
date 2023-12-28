# main.utils.py

from django.contrib.auth.models import User
from django.core.mail import send_mail, EmailMessage, EmailMultiAlternatives
from django.utils import timezone

from .email_messages import EMAIL_MESSAGES
from datasets.models import DatasetFile
from datasets.views import emailer

from datetime import timedelta

# generic email function, replacing emailer() in datasets/views.py
def new_emailer(email_type, subject, from_email, to_email, **kwargs):
	print('new_emailer() called, to_email', to_email)
	reply_to = kwargs.get('reply_to', None)
	cc = kwargs.get('cc', None)
	bcc = kwargs.get('bcc', None)

	# Get the email body from the dictionary
	email_body = EMAIL_MESSAGES.get(email_type).format(**kwargs)

	# If the email body is not found, raise an error
	if email_body is None:
		raise ValueError('Invalid email type: {}'.format(email_type))

	# Format the email body with the provided kwargs
	html_message = email_body.replace('\n', '<br>')

	# Create and send the email
	email = EmailMultiAlternatives(
		subject,
		email_body,
		from_email,
		to_email,
		reply_to=reply_to,
		cc=cc,
		bcc=bcc,
	)
	email.attach_alternative(html_message, "text/html")
	email.send()

def send_maintenance_email():
	# Fetch recent users.
	recent_users = User.objects.filter(last_login__gte=timezone.now() - timedelta(days=30))
	test_users = User.objects.filter(id=1)
	down_date = '3 September'
	for user in recent_users:
	# for user in test_users:
		send_mail(
			'WHG Scheduled Maintenance Notification',
			'Dear {},\n\n'
			'Because you have logged in to WHG within the last month or so, we are letting you know that'
			'the World Historical Gazetteer site will be undergoing scheduled maintenance on {}, during '
			'the following time window:\n'
				'  CEST: 15:00-19:00 \n'
				'  London: 14:00-19:00 \n'
				'  UTC: 07:00-10:00 \n'
				'  EDT (US): 09:00-13:00 \n'
				'  Tokyo: 22:00-02:00 Mon\n'
				'  Beijing: 21:00-01:00 Mon\n\n'
			'The site might be temporarily unavailable during this period.\n\n'
			'Thank you for your understanding.'.format(user.username, down_date),
			'whg@pitt.edu',
			[user.email],
			fail_silently=False,
		)

def server_downtime():
	# recents = User.objects.filter(username='whgadmin')
	recent_logins = [u.email for u in User.objects.filter(last_login__gt='2022-12-31')]
	recent_uploads = [df.dataset_id.owner.email for df in DatasetFile.objects.filter(upload_date__gte='2022-10-22')]
	# recipients = list(set(recent_uploads+recent_uploads))
	recipients = ['karl.geog@gmail.com']
	subj = 'World Historical Gazetteer maintenance'
	msg = 'Dear World Historical Gazetteer user; \n\n'+\
				'Because you have recently logged in and/or uploaded data, we are letting you know...\n\n'+ \
	      'On Sunday, 23 April we are moving the WHG platform to a new, more capable server. \n'+\
				'For this reason, the system will unavailable for a few hours: \n\n'+\
				'  CEST: 09:00-12:00 \n'+ \
				'  London: 08:00-11:00 \n'+ \
				'  UTC: 07:00-10:00 \n'+ \
				'  EDT (US): 03:00-06:00 \n'+ \
				'  Tokyo: 16:00-19:00 \n'+ \
				'  Beijing: 15:00-18:00 \n\n'+ \
				'Rest assured, your uploaded data (if any) will not be impacted in any way! \n'+ \
				'Please get in touch with any questions, concerns, or issues you might have.\n\n'+\
				'best regards,\n'+\
				'Karl Grossner\n'+\
				'WHG Technical Director\nkarl@kgeographer.org'
	# for email in recipients:
	emailer(subj ,msg ,'whg@pitt.edu' , recipients)

