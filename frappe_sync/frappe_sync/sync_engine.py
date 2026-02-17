import frappe
from frappe import _

from frappe_sync.frappe_sync.utils import (
	get_enabled_connections,
	get_event_type,
	get_sync_settings,
	is_sync_enabled_for_doctype,
	prepare_doc_payload,
)

# DocTypes that should never be synced (internal/system doctypes)
EXCLUDED_DOCTYPES = {
	"Sync Settings",
	"Sync Connection",
	"Sync DocType",
	"Sync Log",
	"Error Log",
	"Scheduled Job Log",
	"Activity Log",
	"Access Log",
	"Route History",
	"Version",
	"Comment",
	"Communication",
}


def on_document_change(doc, method):
	"""Hook called on after_insert, on_update, on_trash for all doctypes.

	Determines if this doctype is configured for sync, checks the sync-loop
	flag, and enqueues the sync job.
	"""
	# Layer 1: Prevent sync loops
	if getattr(frappe.flags, "in_frappe_sync", False):
		return

	# Skip excluded system doctypes
	if doc.doctype in EXCLUDED_DOCTYPES:
		return

	# Skip if sync is not enabled for this doctype + event
	if not is_sync_enabled_for_doctype(doc.doctype, method):
		return

	settings = get_sync_settings()
	origin_site_id = settings.site_id
	connections = get_enabled_connections()

	if not connections:
		return

	event = get_event_type(method)
	payload = prepare_doc_payload(doc, event)

	for connection in connections:
		# Layer 2: Don't send back to the originator (for forwarded syncs)
		if connection.remote_site_id == origin_site_id:
			continue

		frappe.enqueue(
			"frappe_sync.frappe_sync.sync_engine.push_to_remote",
			queue="short",
			doc_data=payload,
			connection_name=connection.name,
			sync_event=event,
			origin_site_id=origin_site_id,
			modified_timestamp=str(doc.modified),
		)


def push_to_remote(doc_data, connection_name, sync_event, origin_site_id, modified_timestamp):
	"""Background job that pushes a document change to a remote Frappe instance."""
	from frappe.frappeclient import FrappeClient

	log = frappe.get_doc({
		"doctype": "Sync Log",
		"doctype_name": doc_data.get("doctype"),
		"document_name": doc_data.get("name"),
		"sync_connection": connection_name,
		"event": sync_event,
		"direction": "Outgoing",
		"request_payload": frappe.as_json(doc_data),
		"origin_site_id": origin_site_id,
		"modified_timestamp": modified_timestamp,
	})

	try:
		connection = frappe.get_doc("Sync Connection", connection_name)

		client = FrappeClient(
			url=connection.remote_url,
			api_key=connection.api_key,
			api_secret=connection.get_password("api_secret"),
		)

		# For multi-tenant setups, set the Host header to route to the correct site
		if connection.site_name:
			client.headers["Host"] = connection.site_name

		response = client.post_request({
			"cmd": "frappe_sync.frappe_sync.api.receive_sync",
			"doc_data": frappe.as_json(doc_data),
			"event": sync_event,
			"origin_site_id": origin_site_id,
			"modified_timestamp": modified_timestamp,
		})

		log.status = "Success"
		connection.db_set("last_sync_at", frappe.utils.now_datetime())
		connection.db_set("status", "Active")

	except Exception:
		log.status = "Failed"
		log.error = frappe.get_traceback()
		log.retry_count = 0
		log.next_retry_at = _calculate_next_retry(0)

		try:
			frappe.get_doc("Sync Connection", connection_name).db_set("status", "Error")
		except Exception:
			pass

	log.flags.ignore_permissions = True
	log.insert()
	frappe.db.commit()


def _calculate_next_retry(retry_count):
	"""Exponential backoff: 1min, 5min, 15min, 1hr, 6hr."""
	delays = [60, 300, 900, 3600, 21600]
	delay = delays[min(retry_count, len(delays) - 1)]
	return frappe.utils.add_to_date(frappe.utils.now_datetime(), seconds=delay)
