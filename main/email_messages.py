# email body content used with main.utils.new_emailer() throughout the project
# / = done; * = checked
# from_email = whg@pitt
# admins = [Karl, Ali]
# editor = [Ali]
# developer = [Karl]
# reply_to = editor or developer for fails

# /* welcome: "welcome to WHG -> new user
# /* new_user: "new registration" -> admins
# /* new_dataset: "thanks for uploading" ->  dataset owner, cc editor, bcc developer

# ***  TODO: validate & insert errors ***
# failed_upload: "we'll look into it" -> dataset owner, cc developer

# *** TODO: now in task_emailer() ***
	# wikidata_recon_complete: "thanks for reconciling" -> dataset owner, cc editor
	# wikidata_recon_failed: "we'll look into it" -> dataset owner, cc developer

# signal on dataset save after status
# /*dataset_published: "thanks for publishing" -> dataset owner, cc admins
# /*dataset_indexed: "thanks for indexing" -> dataset owner, cc admins


# /* contact_form: "user says: yada yada" -> admins, reply_to sender
# /* contact_reply: "thanks for contacting us" -> sender, cc editor

EMAIL_MESSAGES = {
	'welcome': ('Greetings! \n\n'
	'Thank you for registering for the World Historical Gazetteer (WHG). You can visit '
	            '<a href="https://whgazetteer.org/tutorials/guide/">the WHG site guide</a> '
	            'to learn more about services and features the platform provides.\n\n'
	'WHG is gathering contributions of place data—large and small, and for all regions and historical periods. '
	            'WHG provides services for geocoding place names, linking records for "closely matched" places, '
	            'and on request, publishing contributed datasets and user-created collections. '
	            'WHG is an excellent resource for teachers and students. Teachers can use its place information '
	            'to develop lessons and to make custom maps for presentation in lectures. \n\n'
	            'The "Place Collection" feature enables building and publishing sets of user-annotated place records, '
	            'accompanied by an explanatory essay, image, and links to external resources. '
	            'The WHG "union index" of place attestations drawn from multiple contributed datasets will over time '
	            'increasingly link the disparate research of contributors on the dimension of place. '
	            'Furthermore, by bringing together all the known references for a place without privileging '
	            'any particular one, it decenters colonial name making.\n\n'
	'We are accepting dataset and place collection contributions, which can be published on the site. '
	'If you would like to set up a consultation to discuss these in more detail, just reply to this message\n\n'
	'regards,\nThe WHG project team'
	 ),
	'new_user': (
		'Hello there,\n\n'
		'So you know...{name} ({username}, id {id}) just registered on the site.\n\n'
		'regards,\nThe WHG auto emailer bot'
	),
	'new_dataset': (
		'So you know...the user {name} ({username}) has created a new dataset, '
		'{dataset_title} ({dataset_label}, {dataset_id}).\n\n'
		'regards,\nThe WHG auto emailer bot'
	),
	'failed_upload': (
		'Dear {name},\n\n'
		'We are sorry to inform you that your upload of {fn} failed. '
		'We will look into it and get back to you soon.\n\n'
		'regards,\nThe WHG project team'
	),
	'wikidata_recon_complete': (
		'Dear {name},\n\n'
		're: {dataset_title} ({dataset_label}, {dataset_id})\n\n'
		'Thank you for reconciling your dataset to Wikidata.\n'
		'If you would like it to be published, please ensure its metadata is complete, then request a review '
		'by WHG editorial staff, in reply to this message, or to ({editorial})\n\n'
		'regards,\nThe WHG project team'
	),
	'wikidata_recon_failed': (
		'Dear {name},\n\n'
		'We are sorry to inform you that your reconciliation task for '
		'{dataset_title} ({dataset_label}, {dataset_id}) failed. '
		'We will look into it and get back to you soon.\n\n'
		'regards,\nThe WHG project team'
	),
	'dataset_published': (
		'Dear {name},\n\n'
		'Thank you for publishing your dataset, {dataset_title} ({dataset_label}, {dataset_id}).\n'
		'It is now publicly viewable, and its records accessible in search and our API .\n\n'
		'regards,\nThe WHG project team'
	),
	'dataset_indexed': (
		'Dear {name},\n\n'
		'Thank you for indexing your dataset, {dataset_title} ({dataset_label}, {dataset_id}).\n\n'
		'All of its records were already public; now many are linked with those for closely '
		'matched places coming from other projects.\n\n'
		'regards,\nThe WHG project team'
	),
	'contact_form': (
		'Hello there,\n\n'
		'{name} ({username}; {user_email}), on the subject of {user_subject} says: \n\n'
		'{user_message}\n\n'
		'regards,\nThe WHG auto emailer bot'
	),
	'contact_reply': (
		'Hello {name},\n\n'
		'We received your message concerning "{user_subject}" and will respond soon.\n\n'
		'regards,\nThe WHG project team'
	),
	'maintenance': (
		'Dear {name},\n\n'
		'Because you have logged in to WHG within the last month or so, we are letting you know that'
		'the World Historical Gazetteer site will be undergoing scheduled maintenance on {downdate}, during '
		'the following time window:\n'
		'  CEST: 15:00-19:00 \n'
		'  London: 14:00-19:00 \n'
		'  UTC: 07:00-10:00 \n'
		'  EDT (US): 09:00-13:00 \n'
		'  Tokyo: 22:00-02:00 Mon\n'
		'  Beijing: 21:00-01:00 Mon\n\n'
		'The site might be temporarily unavailable during this period.\n\n'
		'Thank you for your understanding.'
	),
	# Add more email bodies as needed
}

# (
# 		'Hello {name},\n\n'
# 		'Welcome to the World Historical Gazetteer!\n\n'
# 		'The WHG is a free, open-access resource for researcher, educators, '
# 		'and anyone studying or teaching about the past.\n'
# 		'We hope you will find it useful, and we welcome your contributions.\n\n'
# 		'If you have any questions, please contact our editor at {reply_to}\n\n'
# 		'regards,\nThe WHG project team'
# 	)

