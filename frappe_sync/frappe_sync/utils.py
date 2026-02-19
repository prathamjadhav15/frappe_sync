import uuid

import frappe


def generate_site_id():
	return str(uuid.uuid4())


def get_sync_settings():
	"""Cached fetch of Sync Settings single doc."""
	return frappe.get_cached_doc("Sync Settings")


def is_sync_enabled():
	"""Check if sync is globally enabled."""
	settings = get_sync_settings()
	return bool(settings.enabled)


def is_sync_enabled_for_doctype(doctype_name, event):
	"""Check if a doctype + event combination is configured for sync."""
	settings = get_sync_settings()
	if not settings.enabled:
		return False

	event_field_map = {
		"after_insert": "sync_insert",
		"on_update": "sync_update",
		"on_trash": "sync_delete",
	}
	field = event_field_map.get(event)
	if not field:
		return False

	for row in settings.synced_doctypes:
		if row.doctype_name == doctype_name and row.get(field):
			return True

	return False


def get_conflict_strategy(doctype_name):
	"""Get the conflict strategy for a given doctype."""
	settings = get_sync_settings()
	for row in settings.synced_doctypes:
		if row.doctype_name == doctype_name:
			return row.conflict_strategy or "Last Write Wins"
	return "Last Write Wins"


def get_enabled_connections():
	"""Return list of enabled Sync Connection docs."""
	return frappe.get_all(
		"Sync Connection",
		filters={"enabled": 1},
		fields=["name", "remote_url", "api_key", "remote_site_id"],
	)


def prepare_doc_payload(doc, event):
	"""Serialize a document for sync transmission.

	Strips internal fields and includes child table data.
	"""
	internal_fields = {
		"_liked_by",
		"_comments",
		"_assign",
		"_user_tags",
		"_seen",
		"docstatus",
		"modified",
		"modified_by",
	}

	payload = doc.as_dict()

	# Remove internal fields
	for field in internal_fields:
		payload.pop(field, None)

	# Include dependency info for Link fields
	dependencies = []
	meta = frappe.get_meta(doc.doctype)
	for df in meta.get_link_fields():
		value = doc.get(df.fieldname)
		if value and df.options not in ("DocType",):
			dependencies.append({
				"doctype": df.options,
				"name": value,
			})

	# Also check child tables for link fields
	for table_field in meta.get_table_fields():
		child_meta = frappe.get_meta(table_field.options)
		for row in doc.get(table_field.fieldname) or []:
			for df in child_meta.get_link_fields():
				value = row.get(df.fieldname)
				if value and df.options not in ("DocType",):
					dependencies.append({
						"doctype": df.options,
						"name": value,
					})

	# Deduplicate
	seen = set()
	unique_deps = []
	for dep in dependencies:
		key = (dep["doctype"], dep["name"])
		if key not in seen:
			seen.add(key)
			unique_deps.append(dep)

	payload["_dependencies"] = unique_deps
	return payload


def get_event_type(method):
	"""Map Frappe doc_event method name to sync event type."""
	mapping = {
		"after_insert": "Insert",
		"on_update": "Update",
		"on_trash": "Delete",
	}
	return mapping.get(method, "Update")
