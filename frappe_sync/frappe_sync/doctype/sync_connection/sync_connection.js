frappe.ui.form.on("Sync Connection", {
	test_connection_button(frm) {
		frappe.show_alert({ message: __("Testing connection..."), indicator: "blue" });
		frappe.call({
			method: "frappe_sync.frappe_sync.doctype.sync_connection.sync_connection.test_connection",
			args: { doc_name: frm.doc.name },
			freeze: true,
			freeze_message: __("Testing connection..."),
			callback() {
				frm.reload_doc();
			},
		});
	},
});
