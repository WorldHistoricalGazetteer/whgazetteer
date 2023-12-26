# email body content used with main.utils.new_emailer() throughout the project
# / = done
# from_email = whg@pitt
# admins = [Karl, Ali]
# editor = [Ali]
# developer = [Karl]
# reply_to = editor or developer for fails

# / welcome: "welcome to WHG -> new user
# / new_user: "new registration" -> admins
# / new_dataset: "thanks for uploading" ->  dataset owner, cc editor, bcc developer
# failed_upload: "we'll look into it" -> dataset owner, cc developer
# wikidata_recon_complete: "thanks for reconciling" -> dataset owner, cc editor
# wikidata_recon_failed: "we'll look into it" -> dataset owner, cc developer
# dataset_published: "thanks for publishing" -> dataset owner, cc admins
# dataset_indexed: "thanks for indexing" -> dataset owner, cc admins
# contact_form: "user says: yada yada" -> admins, reply_to sender
# contact_reply: "thanks for contacting us" -> sender, cc editor

EMAIL_MESSAGES = {
	'welcome': (
		'Hello {name},\n\n'
		'Welcome to the World Historical Gazetteer!\n\n'
		'The WHG is a free, open-access resource for researcher, educators, '
		'and anyone studying or teaching about the past.\n'
		'We hope you will find it useful, and we welcome your contributions.\n\n'
		'If you have any questions, please contact our editor at {reply_to}\n\n'
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
		'Thank you for indexing your dataset, {dataset_title} ({dataset_label}, {dataset_id}).\n'
		'All of its records are public, and {index_count} of its records are now linked with those for closely '
		'matched places coming from other projects.\n\n'
		'regards,\nThe WHG project team'
	),
	'contact_form': (
		'Hello there,\n\n'
		'{name} ({username}; {from_email}), on the subject of {subject} says: \n\n'
		'{message}\n\n'
		'regards,\nThe WHG auto emailer bot'
	),
	'contact_reply': (
		'Hello {name},\n\n'
		'We received your message concerning "{subject}" and will respond soon.\n\n'
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
