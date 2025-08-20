import ast
import shutil
import sys
from pathlib import Path
from typing import List, Optional

try:
	from sql_registry import SQLRegistry
except ImportError:
	sys.path.append(str(Path(__file__).parent.parent / "utils"))
	from sql_registry import SQLRegistry


class SQLRewriter:
	"""SQL to Query Builder rewriter functionality"""

	def __init__(self, registry_file: str = ".sql_registry.pkl"):
		self.registry = SQLRegistry(registry_file)

	def list_sql_calls(
		self, file_path: str | None = None, show_complex_only: bool = False
	):
		"""List all SQL calls, optionally filtered by file or complexity"""
		calls = self.registry.data["calls"]

		if not calls:
			print(
				"No SQL calls found in registry. Run 'pre-commit run sql_registry --scan-all' first."
			)
			return

		filtered_calls = []
		for call in calls.values():
			# Filter by file if specified
			if file_path and not call.file_path.endswith(file_path):
				continue

			# Filter by complexity if specified
			if show_complex_only and call.complexity_score <= 0.7:
				continue

			filtered_calls.append(call)

		if not filtered_calls:
			print("No SQL calls match the specified criteria.")
			return

		print(f"\n📋 Found {len(filtered_calls)} SQL calls:")
		print("=" * 80)

		for call in sorted(filtered_calls, key=lambda x: (x.file_path, x.line_number)):
			file_name = Path(call.file_path).name
			complexity_indicator = (
				"🔴" if call.complexity_score > 0.7 else "🟡" if call.complexity_score > 0.3 else "🟢"
			)

			print(f"\n{complexity_indicator} {call.call_id[:8]}  {file_name}:{call.line_number}")
			print(f"   Function: {call.function_context}")
			print(f"   Complexity: {call.complexity_score:.2f}")
			print(f"   SQL: {call.sql_query[:100]}{'...' if len(call.sql_query) > 100 else ''}")

	def show_sql_details(self, call_id: str):
		"""Show detailed information about a specific SQL call"""
		# Find call by ID (support partial IDs)
		matching_calls = [
			call
			for call in self.registry.data["calls"].values()
			if call.call_id.startswith(call_id)
		]

		if not matching_calls:
			print(f"No SQL call found with ID starting with '{call_id}'")
			return

		if len(matching_calls) > 1:
			print(f"Multiple calls match '{call_id}'. Please be more specific:")
			for call in matching_calls:
				print(f"  {call.call_id[:12]} - {Path(call.file_path).name}:{call.line_number}")
			return

		call = matching_calls[0]

		print(f"\n📝 SQL Call Details: {call.call_id}")
		print("=" * 60)
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")
		print(f"Function: {call.function_context}")
		print(f"Implementation: {call.implementation_type}")
		print(f"Complexity: {call.complexity_score:.2f}")
		print(f"Created: {call.created_at}")
		print(f"Updated: {call.updated_at}")

		print("\n🔍 Original SQL:")
		print("-" * 40)
		print(call.sql_query)

		print("\n🏗️ Query Builder Equivalent:")
		print("-" * 40)
		print(call.query_builder_equivalent)

		if call.notes:
			print("\n📋 Notes:")
			print("-" * 40)
			print(call.notes)

	def rewrite_sql(self, call_id: str, dry_run: bool = True, backup: bool = True):
		"""Rewrite a specific SQL call to Query Builder"""
		# Find the call
		matching_calls = [
			call
			for call in self.registry.data["calls"].values()
			if call.call_id.startswith(call_id)
		]

		if not matching_calls:
			print(f"No SQL call found with ID starting with '{call_id}'")
			return False

		if len(matching_calls) > 1:
			print(f"Multiple calls match '{call_id}'. Please be more specific.")
			return False

		call = matching_calls[0]

		print(f"\n🔄 Rewriting SQL call: {call.call_id[:12]}")
		print(f"File: {call.file_path}")
		print(f"Line: {call.line_number}")

		# Load the file
		file_path = Path(call.file_path)
		if not file_path.exists():
			print(f"❌ File not found: {file_path}")
			return False

		try:
			# Read original content
			original_content = file_path.read_text(encoding="utf-8")
			lines = original_content.splitlines()

			# Find the SQL call line
			target_line_idx = call.line_number - 1  # Convert to 0-based index
			if target_line_idx >= len(lines):
				print(f"❌ Line {call.line_number} not found in file")
				return False

			# Get the function context and SQL replacement
			modified_content = self._replace_sql_in_content(
				original_content, call, target_line_idx
			)

			if modified_content == original_content:
				print("⚠️ No changes were made. The SQL might not be easily replaceable.")
				return False

			if dry_run:
				print("\n🔍 DRY RUN - Changes that would be made:")
				print("=" * 50)
				self._show_diff(original_content, modified_content, call.line_number)
				print(
					f"\nTo apply: pre-commit run sql_registry --rewriter-mode --rewrite {call_id[:8]} --apply"
				)
				return True

			# Create backup if requested
			if backup:
				backup_path = file_path.with_suffix(f"{file_path.suffix}.bak")
				shutil.copy2(file_path, backup_path)
				print(f"📄 Backup created: {backup_path}")

			# Write the modified content
			file_path.write_text(modified_content, encoding="utf-8")

			# Update registry
			call.implementation_type = "query_builder"
			call.notes = f"Converted by sql_rewriter on {call.updated_at}"
			self.registry.save_registry()

			print("✅ Successfully converted SQL to Query Builder")
			print(f"📄 Modified: {file_path}")

			return True

		except Exception as e:
			print(f"❌ Error during rewrite: {e}")
			return False

	def _replace_sql_in_content(self, content: str, call, target_line_idx: int) -> str:
		"""Replace SQL call with Query Builder equivalent in file content"""
		lines = content.splitlines()

		# For now, simple line replacement - can be enhanced for multi-line calls
		replacement_lines = call.query_builder_equivalent.split("\n")

		# Calculate indentation from the original line
		original_line = lines[target_line_idx]
		indent = len(original_line) - len(original_line.lstrip())

		# Apply indentation to all replacement lines
		indented_replacement = [
			" " * indent + line if line.strip() else line for line in replacement_lines
		]

		# Replace the original line with the first replacement line
		lines[target_line_idx] = indented_replacement[0]

		# Insert additional lines if needed
		if len(indented_replacement) > 1:
			for i, extra_line in enumerate(indented_replacement[1:], 1):
				lines.insert(target_line_idx + i, extra_line)

		return "\n".join(lines)

	def _show_diff(self, original: str, modified: str, around_line: int):
		"""Show a diff of the changes around the specified line"""
		orig_lines = original.splitlines()
		mod_lines = modified.splitlines()

		# Show context around the changed line
		start = max(0, around_line - 3)
		end = min(len(orig_lines), around_line + 3)

		print(f"Lines {start + 1}-{end}:")
		for i in range(start, end):
			line_num = i + 1
			if i < len(orig_lines):
				if line_num == around_line:
					print(f"- {line_num:3d}: {orig_lines[i]}")
					if i < len(mod_lines):
						print(f"+ {line_num:3d}: {mod_lines[i]}")
				else:
					print(f"  {line_num:3d}: {orig_lines[i]}")
