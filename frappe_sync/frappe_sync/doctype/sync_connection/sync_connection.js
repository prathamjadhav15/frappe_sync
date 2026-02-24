frappe.ui.form.on("Sync Connection", {
	test_connection_button(frm) {
		frm.call({
			method: "test_connection",
			btn: frm.get_field("test_connection_button").$input,
			callback() {
				frm.reload_doc();
			},
			error() {
				frm.reload_doc();
			},
		});
	},
});
