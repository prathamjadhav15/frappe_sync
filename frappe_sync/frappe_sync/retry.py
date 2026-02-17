import frappe

MAX_RETRIES = 5


def process_failed_syncs():
	"""Retry failed sync logs with exponential backoff.

	Called by scheduler every 5 minutes.
	"""
	failed_logs = frappe.get_all(
		"Sync Log",
		filters={
			"status": "Failed",
			"direction": "Outgoing",
			"retry_count": ["<", MAX_RETRIES],
			"next_retry_at": ["<=", frappe.utils.now_datetime()],
		},
		fields=[
			"name",
			"request_payload",
			"sync_connection",
			"event",
			"origin_site_id",
			"modified_timestamp",
			"retry_count",
		],
		limit=50,
	)

	for log_data in failed_logs:
		_retry_sync(log_data)


def _retry_sync(log_data):
	"""Retry a single failed sync."""
	from frappe_sync.frappe_sync.sync_engine import push_to_remote

	try:
		doc_data = frappe.parse_json(log_data.request_payload)
		push_to_remote(
			doc_data=doc_data,
			connection_name=log_data.sync_connection,
			sync_event=log_data.event,
			origin_site_id=log_data.origin_site_id,
			modified_timestamp=log_data.modified_timestamp,
		)
		# push_to_remote creates a new Success log; mark old one resolved
		frappe.db.set_value("Sync Log", log_data.name, "status", "Success")
	except Exception:
		retry_count = log_data.retry_count + 1
		frappe.db.set_value(
			"Sync Log",
			log_data.name,
			{
				"retry_count": retry_count,
				"next_retry_at": _calculate_next_retry(retry_count),
				"error": frappe.get_traceback(),
			},
		)
	frappe.db.commit()


def _calculate_next_retry(retry_count):
	"""Exponential backoff: 1min, 5min, 15min, 1hr, 6hr."""
	delays = [60, 300, 900, 3600, 21600]
	delay = delays[min(retry_count, len(delays) - 1)]
	return frappe.utils.add_to_date(frappe.utils.now_datetime(), seconds=delay)
