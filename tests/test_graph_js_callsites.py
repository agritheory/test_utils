"""Tests for tree-sitter based JS call site extraction."""

import unittest

from test_utils.utils.graph.js_callsites import extract_callsites_from_source


class TestJsCallSites(unittest.TestCase):
	def test_string_arg_no_loop(self) -> None:
		src = "frappe.call('myapp.module.fn');\n"
		sites = extract_callsites_from_source("t.js", src)
		self.assertEqual(len(sites), 1)
		self.assertEqual(sites[0].target_path, "myapp.module.fn")
		self.assertFalse(sites[0].loop_context)

	def test_object_method_form(self) -> None:
		src = """
		frappe.call({
			method: 'erpnext.selling.get_data',
			callback(r) { }
		});
		"""
		sites = extract_callsites_from_source("t.js", src)
		self.assertEqual(len(sites), 1)
		self.assertEqual(sites[0].target_path, "erpnext.selling.get_data")

	def test_for_each_loop(self) -> None:
		src = """
		rows.forEach(r => {
			frappe.xcall('a.b.c');
		});
		"""
		sites = extract_callsites_from_source("t.js", src)
		self.assertEqual(len(sites), 1)
		self.assertTrue(sites[0].loop_context)
		self.assertEqual(sites[0].loop_type, "forEach")

	def test_for_statement_loop(self) -> None:
		src = """
		for (const x of items) {
			frappe.call('x.y.z');
		}
		"""
		sites = extract_callsites_from_source("t.js", src)
		self.assertEqual(len(sites), 1)
		self.assertTrue(sites[0].loop_context)
		self.assertEqual(sites[0].loop_type, "for_in_statement")

	def test_jquery_each(self) -> None:
		src = """
		$.each(data, function(i, row) {
			frappe.call('app.api.run');
		});
		"""
		sites = extract_callsites_from_source("t.js", src)
		self.assertEqual(len(sites), 1)
		self.assertTrue(sites[0].loop_context)
		self.assertEqual(sites[0].loop_type, "$.each")


if __name__ == "__main__":
	unittest.main()
