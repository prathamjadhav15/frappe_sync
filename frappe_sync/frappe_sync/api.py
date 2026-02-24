import frappe
from frappe import _

from frappe_sync.frappe_sync.utils import get_conflict_strategy, get_sync_settings


@frappe.whitelist()
def ping():
	"""Health-check endpoint. Returns this site's site_id."""
	settings = get_sync_settings()
	return {"site_id": settings.site_id, "status": "ok"}


@frappe.whitelist()
def receive_sync(doc_data, event, origin_site_id, modified_timestamp):
	"""Receive a sync payload from a remote Frappe instance.

	Args:
		doc_data: JSON string of the document data
		event: "Insert", "Update", or "Delete"
		origin_site_id: The site_id of the instance that originated this change
		modified_timestamp: The `modified` value from the source doc
	"""
	try:
		frappe.flags.in_frappe_sync = True

		if isinstance(doc_data, str):
			doc_data = frappe.parse_json(doc_data)

		doctype = doc_data.get("doctype")
		name = doc_data.get("name")

		# Remove sync-specific metadata before processing
		dependencies = doc_data.pop("_dependencies", [])

		log = _create_sync_log(doctype, name, event, "Incoming", origin_site_id, modified_timestamp)

		# Resolve dependencies first
		_resolve_dependencies(dependencies, origin_site_id)

		if event == "Insert":
			_handle_insert(doc_data, log)
		elif event == "Update":
			_handle_update(doc_data, modified_timestamp, log)
		elif event == "Delete":
			_handle_delete(doctype, name, log)

		frappe.db.commit()
		return {"status": "ok"}

	except Exception as e:
		frappe.db.rollback()
		_create_sync_log(
			doc_data.get("doctype") if isinstance(doc_data, dict) else "",
			doc_data.get("name") if isinstance(doc_data, dict) else "",
			event,
			"Incoming",
			origin_site_id,
			modified_timestamp,
			status="Failed",
			error=frappe.get_traceback(),
		)
		frappe.db.commit()
		frappe.throw(_("Sync failed: {0}").format(str(e)))

	finally:
		frappe.flags.in_frappe_sync = False


@frappe.whitelist()
def get_document(doctype, name):
	"""Fetch a document for dependency resolution by a remote instance."""
	if not frappe.db.exists(doctype, name):
		frappe.throw(_("Document {0} {1} not found").format(doctype, name))

	doc = frappe.get_doc(doctype, name)
	return doc.as_dict()


def _handle_insert(doc_data, log):
	"""Handle an incoming insert event."""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")

	if frappe.db.exists(doctype, name):
		# Document already exists, treat as update
		_handle_update(doc_data, doc_data.get("modified"), log)
		return

	new_doc = frappe.get_doc(doc_data)
	new_doc.flags.ignore_permissions = True
	new_doc.flags.ignore_links = True
	new_doc.flags.ignore_mandatory = True
	new_doc.flags.ignore_validate = True
	# Preserve the original document name from the source site
	if name:
		new_doc.name = name
		new_doc.flags.name_set = True
	new_doc.insert()
	log.db_set("status", "Success")


def _handle_update(doc_data, modified_timestamp, log):
	"""Handle an incoming update event with last-write-wins conflict resolution."""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")

	if not frappe.db.exists(doctype, name):
		# Document doesn't exist locally, treat as insert
		_handle_insert(doc_data, log)
		return

	conflict_strategy = get_conflict_strategy(doctype)
	local_doc = frappe.get_doc(doctype, name)

	if conflict_strategy == "Last Write Wins":
		local_modified = str(local_doc.modified)
		remote_modified = str(modified_timestamp)

		if remote_modified < local_modified:
			# Local is newer, skip
			log.db_set("status", "Skipped")
			return

	elif conflict_strategy == "Skip":
		log.db_set("status", "Skipped")
		return

	# Apply the remote changes
	# Remove fields that shouldn't be overwritten (use a copy to preserve original doc_data)
	update_data = {k: v for k, v in doc_data.items() if k not in ("name", "doctype", "creation", "owner")}

	local_doc.update(update_data)
	local_doc.flags.ignore_permissions = True
	local_doc.flags.ignore_links = True
	local_doc.flags.ignore_version = True
	local_doc.flags.ignore_validate = True
	local_doc.save()
	log.db_set("status", "Success")


def _handle_delete(doctype, name, log):
	"""Handle an incoming delete event."""
	if not frappe.db.exists(doctype, name):
		log.db_set("status", "Skipped")
		return

	frappe.delete_doc(doctype, name, ignore_permissions=True, force=True)
	log.db_set("status", "Success")


def _create_sync_log(doctype_name, document_name, event, direction, origin_site_id,
	modified_timestamp, status="Queued", error=None):
	"""Create a Sync Log entry."""
	log = frappe.get_doc({
		"doctype": "Sync Log",
		"doctype_name": doctype_name,
		"document_name": document_name,
		"event": event,
		"direction": direction,
		"status": status,
		"origin_site_id": origin_site_id,
		"modified_timestamp": modified_timestamp,
		"error": error,
	})
	log.flags.ignore_permissions = True
	log.insert()
	return log


def _resolve_dependencies(dependencies, origin_site_id):
	"""Resolve Link field dependencies before inserting/updating a document."""
	if not dependencies:
		return

	resolving = frappe.flags.get("_sync_resolving_deps") or set()

	for dep in dependencies:
		key = (dep["doctype"], dep["name"])
		if key in resolving:
			continue

		if frappe.db.exists(dep["doctype"], dep["name"]):
			continue

		# Check if this doctype is in our synced list
		settings = get_sync_settings()
		synced_doctypes = [row.doctype_name for row in settings.synced_doctypes]

		if dep["doctype"] not in synced_doctypes:
			frappe.log_error(
				title="Sync Dependency Missing",
				message=f"Missing dependency: {dep['doctype']} {dep['name']} (not in synced doctypes)",
			)
			continue

		# Mark as resolving to prevent circular deps
		resolving.add(key)
		frappe.flags._sync_resolving_deps = resolving

		# Try to fetch from the remote that sent this sync
		# For now, log a warning - the retry mechanism will handle this
		frappe.log_error(
			title="Sync Dependency Missing",
			message=f"Missing dependency: {dep['doctype']} {dep['name']}. Will be resolved on retry.",
		)

		resolving.discard(key)
