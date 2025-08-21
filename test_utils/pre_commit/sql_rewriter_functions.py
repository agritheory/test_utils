import ast
import shutil
import sys
import subprocess
import os
from pathlib import Path
from typing import List, Optional
import difflib
import tempfile
from test_utils.utils.sql_registry import SQLRegistry


# ANSI color codes
class Colors:
	RED = "\033[91m"
	GREEN = "\033[92m"
	YELLOW = "\033[93m"
	BLUE = "\033[94m"
	MAGENTA = "\033[95m"
	CYAN = "\033[96m"
	WHITE = "\033[97m"
	RESET = "\033[0m"
	BOLD = "\033[1m"

	@classmethod
	def disable(cls):
		"""Disable colors for file output"""
		cls.RED = ""
		cls.GREEN = ""
		cls.YELLOW = ""
		cls.BLUE = ""
		cls.MAGENTA = ""
		cls.CYAN = ""
		cls.WHITE = ""
		cls.RESET = ""
		cls.BOLD = ""


class SQLRewriter:
	"""SQL to Query Builder rewriter functionality"""

	def __init__(self, registry_file: str = ".sql_registry.pkl", use_colors: bool = True):
		self.registry = SQLRegistry(registry_file)
		if not use_colors or not sys.stdout.isatty():
			# Disable colors if not in terminal or explicitly disabled
			Colors.disable()

	def list_sql_calls(self, file_path: str | None = None):
		"""List all SQL calls, optionally filtered by file"""
		calls = self.registry.data["calls"]

		if not calls:
			print(
				f"{Colors.YELLOW}No SQL calls found in registry. Run 'sql_registry scan' first.{Colors.RESET}"
			)
			return

		filtered_calls = []
		for call in calls.values():
			if file_path and not call.file_path.endswith(file_path):
				continue
			filtered_calls.append(call)

		if not filtered_calls:
			print(f"{Colors.YELLOW}No SQL calls match the specified criteria.{Colors.RESET}")
			return

		print(f"\n{Colors.BOLD}Found {len(filtered_calls)} SQL calls:{Colors.RESET}")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			print(
				f"\n{Colors.CYAN}{call.call_id[:8]}{Colors.RESET}  {file_name}:{call.line_number}"
			)
			print(f"   Function: {call.function_context}")
			print(f"   Variable: {call.variable_name or 'None'}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

	def show_sql_details(self, call_id: str):
		"""Show detailed information about a specific SQL call"""
		matching_calls = [
			call
			for call in self.registry.data["calls"].values()
			if call.call_id.startswith(call_id)
		]

		if not matching_calls:
			print(
				f"{Colors.RED}No SQL call found with ID starting with '{call_id}'{Colors.RESET}"
			)
			return

		if len(matching_calls) > 1:
			print(
				f"{Colors.YELLOW}Multiple calls match '{call_id}'. Please be more specific:{Colors.RESET}"
			)
			for call in matching_calls:
				print(f"  {call.call_id[:12]} - {Path(call.file_path).name}:{call.line_number}")
			return

		call = matching_calls[0]

		print(f"\n{Colors.BOLD}SQL Call Details: {call.call_id}{Colors.RESET}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Variable: {call.variable_name or 'None'}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		print(f"\n{Colors.BOLD}Original SQL:{Colors.RESET}")
		print("-" * 40)
		print(call.sql_query)

		print(f"\n{Colors.BOLD}Query Builder Equivalent:{Colors.RESET}")
		print("-" * 40)
		print(call.query_builder_equivalent)

		if call.notes:
			print(f"\n{Colors.BOLD}Notes:{Colors.RESET}")
			print("-" * 40)
			print(call.notes)

	def rewrite_sql(self, call_id: str, dry_run: bool = True, backup: bool = True):
		"""Rewrite a specific SQL call to Query Builder"""
		matching_calls = [
			call
			for call in self.registry.data["calls"].values()
			if call.call_id.startswith(call_id)
		]

		if not matching_calls:
			print(
				f"{Colors.RED}No SQL call found with ID starting with '{call_id}'{Colors.RESET}"
			)
			return False

		if len(matching_calls) > 1:
			print(
				f"{Colors.YELLOW}Multiple calls match '{call_id}'. Please be more specific.{Colors.RESET}"
			)
			return False

		call = matching_calls[0]

		print(f"\n{Colors.BOLD}Rewriting SQL call: {call.call_id[:12]}{Colors.RESET}")
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Variable: {call.variable_name or 'None'}")

		file_path = Path(call.file_path)
		if not file_path.exists():
			print(f"{Colors.RED}Error: File not found: {file_path}{Colors.RESET}")
			return False

		try:
			original_content = file_path.read_text(encoding="utf-8")
			lines = original_content.splitlines()

			# Find the SQL call and replace it
			modified_content = self.replace_sql_in_content(original_content, call)

			if modified_content == original_content:
				print(
					f"{Colors.YELLOW}Warning: No changes were made. The SQL might not be easily replaceable.{Colors.RESET}"
				)
				return False

			# Apply Black formatting to the modified content
			formatted_content = self.apply_black_formatting(modified_content)

			if dry_run:
				print(f"\n{Colors.BOLD}DRY RUN - Changes that would be made:{Colors.RESET}")
				print("=" * 50)
				self.show_diff(original_content, formatted_content, call.line_number)
				print(
					f"\n{Colors.CYAN}To apply: poetry run sql_registry rewrite {call_id[:8]} --apply{Colors.RESET}"
				)
				return True

			# Create backup if requested
			if backup:
				backup_path = file_path.with_suffix(f"{file_path.suffix}.bak")
				shutil.copy2(file_path, backup_path)
				print(f"{Colors.BLUE}Backup created: {backup_path}{Colors.RESET}")

			# Write the modified content
			file_path.write_text(formatted_content, encoding="utf-8")

			# Update registry
			call.implementation_type = "query_builder"
			call.notes = f"Converted by sql_rewriter on {call.updated_at}"
			self.registry.save_registry()

			print(f"{Colors.GREEN}Successfully converted SQL to Query Builder{Colors.RESET}")
			print(f"Modified: {file_path}")

			return True

		except Exception as e:
			print(f"{Colors.RED}Error during rewrite: {e}{Colors.RESET}")
			return False

	def replace_sql_in_content(self, content: str, call) -> str:
		"""Replace SQL call with Query Builder equivalent in file content"""
		lines = content.splitlines()

		# Find the full extent of the frappe.db.sql call
		start_line = call.line_number - 1  # Convert to 0-based index
		end_line = start_line

		# Find the complete statement by tracking parentheses
		paren_count = 0
		found_start = False

		for i in range(start_line, len(lines)):
			line = lines[i]

			# Check if this line contains frappe.db.sql
			if i == start_line and "frappe.db.sql" in line:
				found_start = True
				# Count opening parens after frappe.db.sql
				sql_start = line.index("frappe.db.sql")
				after_sql = line[sql_start:]
				paren_count = after_sql.count("(") - after_sql.count(")")
			elif found_start:
				paren_count += line.count("(") - line.count(")")

			if found_start and paren_count == 0:
				end_line = i
				break

		# Get indentation from the original line (use tabs)
		original_line = lines[start_line]
		indent_level = len(original_line) - len(original_line.lstrip("\t"))
		indent_str = "\t" * indent_level

		# Prepare the Query Builder replacement
		qb_lines = call.query_builder_equivalent.split("\n")

		# If the call has a variable name, ensure it's used
		if call.variable_name and call.variable_name not in [
			"__return__",
			"__yield__",
			"__expr__",
		]:
			# Replace any 'result = ' with the actual variable name
			qb_lines = [
				line.replace("result =", f"{call.variable_name} =") if "result =" in line else line
				for line in qb_lines
			]

		# Apply proper tab indentation to all lines
		indented_replacement = []
		for line in qb_lines:
			if line.strip():  # Non-empty lines
				# Count the leading tabs/spaces in the generated code
				leading_whitespace = len(line) - len(line.lstrip())
				# Convert to tabs (assuming 4 spaces = 1 tab in generated code)
				if line.startswith("    "):
					# This is indented content
					additional_tabs = leading_whitespace // 4
					indented_replacement.append(indent_str + "\t" * additional_tabs + line.lstrip())
				elif line.startswith("\t"):
					# Already using tabs
					indented_replacement.append(indent_str + line)
				else:
					# No indentation in generated line
					indented_replacement.append(indent_str + line.lstrip())
			else:
				# Empty lines
				indented_replacement.append("")

		# Replace the lines
		new_lines = lines[:start_line] + indented_replacement + lines[end_line + 1 :]

		return "\n".join(new_lines)

	def apply_black_formatting(self, content: str) -> str:
		"""Apply Black formatting to Python code"""
		try:
			with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
				tmp.write(content)
				tmp_path = tmp.name

			# Run black on the file with tab indentation
			result = subprocess.run(
				["black", "--use-tabs", "--tab-width=4", tmp_path], capture_output=True, text=True
			)

			# Read the formatted content
			with open(tmp_path) as f:
				formatted_content = f.read()

			# Clean up
			Path(tmp_path).unlink()

			return formatted_content
		except (subprocess.CalledProcessError, FileNotFoundError):
			# Black not available or failed, return original content
			print(
				f"{Colors.YELLOW}Warning: Black formatter not available, skipping formatting{Colors.RESET}"
			)
			return content

	def show_diff(self, original: str, modified: str, around_line: int):
		"""Show a diff of the changes around the specified line"""
		orig_lines = original.splitlines()
		mod_lines = modified.splitlines()

		# Show context around the changed line
		start = max(0, around_line - 3)
		end = min(
			len(orig_lines), around_line + 10
		)  # Show more context for multi-line replacements

		print(f"Lines {start + 1}-{end}:")

		diff = difflib.unified_diff(
			orig_lines[start:end],
			mod_lines[start:end] if start < len(mod_lines) else [],
			lineterm="",
			n=0,
		)

		for line in diff:
			if line.startswith("+++") or line.startswith("---"):
				continue
			elif line.startswith("@@"):
				print(f"\n{Colors.CYAN}{line}{Colors.RESET}")
			elif line.startswith("+"):
				print(f"{Colors.GREEN}+ {line[1:]}{Colors.RESET}")
			elif line.startswith("-"):
				print(f"{Colors.RED}- {line[1:]}{Colors.RESET}")
			else:
				print(f"  {line}")
