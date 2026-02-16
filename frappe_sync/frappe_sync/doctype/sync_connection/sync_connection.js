frappe.ui.form.on("Sync Connection", {
	test_connection_button(frm) {
		frm.call("test_connection").then(() => {
			frm.reload_doc();
		});
	},
});
