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
		"""Test connectivity to the remote Frappe instance and fetch its site_id."""
		from frappe.frappeclient import FrappeClient

		try:
			client = FrappeClient(
				url=self.remote_url,
				api_key=self.api_key,
				api_secret=self.get_password("api_secret"),
			)

			if self.site_name:
				client.headers["Host"] = self.site_name

			response = client.post_request({
				"cmd": "frappe_sync.frappe_sync.api.ping",
			})

			if response.get("site_id"):
				self.db_set("remote_site_id", response["site_id"])
				self.db_set("status", "Active")
				frappe.msgprint(
					_("Connection successful. Remote Site ID: {0}").format(response["site_id"]),
					title=_("Success"),
					indicator="green",
				)
			else:
				frappe.throw(_("Invalid response from remote site."))

		except Exception as e:
			self.db_set("status", "Error")
			frappe.throw(_("Connection failed: {0}").format(str(e)))
