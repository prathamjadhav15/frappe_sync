import requests as _requests

import frappe
from frappe import _
from frappe.model.document import Document


class SyncConnection(Document):
	def validate(self):
		if not self.enabled:
			self.status = "Disabled"
		elif self.status == "Disabled" and self.enabled:
			self.status = "Active" if self.remote_site_id else "Disabled"

	@frappe.whitelist()
	def test_connection(self):
		"""Entry point for Frappe v15 frm.call (run_doc_method) compatibility."""
		_do_test_connection(self)


@frappe.whitelist()
def test_connection(doc_name=None):
	"""Entry point for Frappe v16 frappe.call (full module path) compatibility."""
	if not doc_name:
		frappe.throw(_("doc_name is required to test connection."))
	doc = frappe.get_doc("Sync Connection", doc_name)
	_do_test_connection(doc)


def _do_test_connection(doc):
	"""Shared implementation — tests connectivity and updates status."""
	try:
		base_url = doc.remote_url.rstrip("/")
		api_secret = doc.get_password("api_secret")

		headers = {
			"Authorization": f"token {doc.api_key}:{api_secret}",
			"Content-Type": "application/x-www-form-urlencoded",
			"Accept": "application/json",
		}

		if doc.site_name:
			headers["Host"] = doc.site_name

		resp = _requests.post(
			f"{base_url}/api/method/frappe_sync.frappe_sync.api.ping",
			headers=headers,
			timeout=15,
		)

		if resp.status_code != 200:
			frappe.log_error(
				title="Sync Test Connection Failed",
				message=f"HTTP {resp.status_code}: {resp.text[:500]}",
			)
			frappe.throw(_("Connection failed: HTTP {0} — {1}").format(resp.status_code, resp.text[:200]))

		data = resp.json()
		result = data.get("message") or {}
		site_id = result.get("site_id")

		if site_id:
			doc.db_set("remote_site_id", site_id)
			doc.db_set("status", "Active")
			frappe.msgprint(
				_("Connection successful. Remote Site ID: {0}").format(site_id),
				title=_("Success"),
				indicator="green",
			)
		else:
			frappe.throw(_("Invalid response from remote site: {0}").format(str(data)))

	except frappe.exceptions.ValidationError:
		raise
	except Exception as e:
		frappe.log_error(title="Sync Test Connection Error", message=frappe.get_traceback())
		doc.db_set("status", "Error")
		frappe.throw(_("Connection failed: {0}").format(str(e)))
