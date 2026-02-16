import uuid

import frappe


def after_install():
	"""Generate and save the unique site_id into Sync Settings."""
	settings = frappe.get_single("Sync Settings")
	if not settings.site_id:
		settings.site_id = str(uuid.uuid4())
		settings.save()
	frappe.db.commit()
