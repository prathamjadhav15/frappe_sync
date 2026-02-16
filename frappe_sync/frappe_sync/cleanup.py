import frappe


def cleanup_old_sync_logs():
	"""Delete successful sync logs older than the configured retention period.

	Called by daily scheduler.
	"""
	settings = frappe.get_cached_doc("Sync Settings")
	retention_days = settings.log_retention_days or 30

	cutoff = frappe.utils.add_to_date(frappe.utils.now_datetime(), days=-retention_days)

	old_logs = frappe.get_all(
		"Sync Log",
		filters={
			"status": "Success",
			"creation": ["<", cutoff],
		},
		pluck="name",
		limit=1000,
	)

	for name in old_logs:
		frappe.delete_doc("Sync Log", name, ignore_permissions=True)

	if old_logs:
		frappe.db.commit()
