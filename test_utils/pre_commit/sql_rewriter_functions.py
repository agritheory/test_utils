import ast
import shutil
import sys
import subprocess
import os
from datetime import datetime
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

		# Check if this call is flagged for manual review
		if call.query_builder_equivalent and "# MANUAL:" in call.query_builder_equivalent:
			print(
				f"\n{Colors.YELLOW}Skipping {call.call_id[:12]} - flagged for manual review:{Colors.RESET}"
			)
			print(f"  {call.query_builder_equivalent}")
			return False

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

	def rewrite_batch(
		self, call_ids: list[str], dry_run: bool = True, backup: bool = True
	) -> tuple[int, list[str]]:
		"""
		Rewrite multiple SQL calls, handling multiple calls per file correctly.

		Groups calls by file and applies them in reverse line order to avoid
		line number invalidation.

		Returns: (success_count, failed_call_ids)
		"""
		# Collect all matching calls, skip those flagged for manual review
		calls_to_rewrite = []
		skipped_manual = []
		for call_id in call_ids:
			matching = [
				call
				for call in self.registry.data["calls"].values()
				if call.call_id.startswith(call_id)
			]
			if matching:
				call = matching[0]
				# Skip calls flagged for manual review
				if call.query_builder_equivalent and "# MANUAL:" in call.query_builder_equivalent:
					skipped_manual.append(call)
					continue
				calls_to_rewrite.append(call)

		if skipped_manual:
			print(
				f"\n{Colors.YELLOW}Skipping {len(skipped_manual)} calls flagged for manual review:{Colors.RESET}"
			)
			for call in skipped_manual:
				print(f"  - {call.call_id[:12]}: {call.file_path}:{call.line_number}")

		if not calls_to_rewrite:
			return 0, call_ids

		# Group calls by file
		calls_by_file: dict[str, list] = {}
		for call in calls_to_rewrite:
			if call.file_path not in calls_by_file:
				calls_by_file[call.file_path] = []
			calls_by_file[call.file_path].append(call)

		success_count = 0
		failed_ids = []

		for file_path, file_calls in calls_by_file.items():
			# Sort by line number in REVERSE order (bottom to top)
			# This ensures earlier line numbers remain valid as we modify
			file_calls_sorted = sorted(file_calls, key=lambda c: c.line_number, reverse=True)

			path = Path(file_path)
			if not path.exists():
				print(f"{Colors.RED}File not found: {file_path}{Colors.RESET}")
				failed_ids.extend([c.call_id for c in file_calls])
				continue

			try:
				content = path.read_text(encoding="utf-8")
				original_content = content

				# Create backup once per file
				if backup and not dry_run:
					backup_path = path.with_suffix(f"{path.suffix}.bak")
					shutil.copy2(path, backup_path)
					print(f"{Colors.BLUE}Backup created: {backup_path}{Colors.RESET}")

				# Apply each rewrite in reverse line order
				for call in file_calls_sorted:
					print(f"\n{Colors.BOLD}Rewriting SQL call: {call.call_id[:12]}{Colors.RESET}")
					print(f"File: {call.file_path}")
					print(f"Line: {call.line_number}")
					print(f"Variable: {call.variable_name or 'None'}")

					new_content = self.replace_sql_in_content(content, call)

					if new_content == content:
						print(f"{Colors.YELLOW}Warning: No changes for {call.call_id[:8]}{Colors.RESET}")
						failed_ids.append(call.call_id)
					else:
						content = new_content
						success_count += 1

						if dry_run:
							print(f"\n{Colors.BOLD}DRY RUN - Changes that would be made:{Colors.RESET}")
							print("=" * 50)
							self.show_diff(original_content, content, call.line_number)
							# Update original for next diff display
							original_content = content

				if not dry_run and content != path.read_text(encoding="utf-8"):
					# Apply Black formatting once at the end
					formatted_content = self.apply_black_formatting(content)
					path.write_text(formatted_content, encoding="utf-8")

					# Update registry for successful rewrites
					for call in file_calls_sorted:
						if call.call_id not in failed_ids:
							call.implementation_type = "query_builder"
							call.notes = f"Converted by sql_rewriter batch on {datetime.now()}"

					self.registry.save_registry()
					print(f"{Colors.GREEN}Successfully modified: {file_path}{Colors.RESET}")

			except Exception as e:
				print(f"{Colors.RED}Error processing {file_path}: {e}{Colors.RESET}")
				failed_ids.extend([c.call_id for c in file_calls])

		return success_count, failed_ids

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

		# Generate full unified diff with context
		diff = list(
			difflib.unified_diff(
				orig_lines,
				mod_lines,
				fromfile="original",
				tofile="modified",
				lineterm="",
				n=3,  # 3 lines of context
			)
		)

		if not diff:
			print("No changes detected.")
			return

		# Find and display only the hunks that are near our target line
		in_relevant_hunk = False
		hunk_start_line = 0

		for line in diff:
			if line.startswith("@@"):
				# Parse the hunk header to get line numbers
				# Format: @@ -start,count +start,count @@
				import re

				match = re.match(r"@@ -(\d+)", line)
				if match:
					hunk_start_line = int(match.group(1))
					# Show hunks within reasonable range of our target line
					in_relevant_hunk = abs(hunk_start_line - around_line) < 20
				if in_relevant_hunk:
					print(f"\n{Colors.CYAN}{line}{Colors.RESET}")
			elif line.startswith("+++") or line.startswith("---"):
				continue
			elif in_relevant_hunk:
				if line.startswith("+"):
					print(f"{Colors.GREEN}+ {line[1:]}{Colors.RESET}")
				elif line.startswith("-"):
					print(f"{Colors.RED}- {line[1:]}{Colors.RESET}")
				else:
					print(f"  {line}")
