#!/usr/bin/env python3
"""
Tool to automatically update bylines in markdown files based on git history.
Can be used as a pre-commit hook or as a standalone CLI tool.
Updates bylines within <div class="byline"> sections with contributors' real names.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from collections import OrderedDict


def build_global_identity_map(file_paths, alias_map=None):
	"""Build identity map with optional manual aliases."""
	all_identities = {}

	for file_path in file_paths:
		try:
			file_path = Path(file_path).resolve()
			git_root = find_git_root(file_path)
			if not git_root:
				continue

			try:
				relative_path = file_path.relative_to(git_root)
			except ValueError:
				continue

			result = subprocess.run(
				["git", "log", "--follow", "--format=%aN|%aE", "--", str(relative_path)],
				capture_output=True,
				text=True,
				check=True,
				cwd=git_root,
			)

			if result.stdout:
				for line in result.stdout.strip().split("\n"):
					if line:
						parts = line.split("|")
						if len(parts) >= 2:
							name = parts[0].strip()
							email = parts[1].strip().lower()

							if email and name:
								if email not in all_identities:
									all_identities[email] = set()
								all_identities[email].add(name)

			result = subprocess.run(
				["git", "log", "--follow", "--format=%b", "--", str(relative_path)],
				capture_output=True,
				text=True,
				check=True,
				cwd=git_root,
			)

			if result.stdout:
				co_author_pattern = r"^Co-authored-by:\s*([^<]+)\s*<([^>]+)>"
				for line in result.stdout.split("\n"):
					match = re.match(co_author_pattern, line, re.IGNORECASE)
					if match:
						name = match.group(1).strip()
						email = match.group(2).strip().lower()

						if email and name:
							if email not in all_identities:
								all_identities[email] = set()
							all_identities[email].add(name)

		except subprocess.CalledProcessError:
			continue

	identity_groups = []
	processed_emails = set()

	for email, names in all_identities.items():
		if email in processed_emails:
			continue

		group = {"emails": {email}, "names": set(names)}
		processed_emails.add(email)

		for other_email, other_names in all_identities.items():
			if other_email in processed_emails:
				continue

			should_merge = False
			for name in group["names"]:
				for other_name in other_names:
					if names_match(name, other_name):
						should_merge = True
						break
				if should_merge:
					break

			if should_merge:
				group["emails"].add(other_email)
				group["names"].update(other_names)
				processed_emails.add(other_email)

		identity_groups.append(group)

	name_map = {}

	for group in identity_groups:
		canonical = select_best_name(list(group["names"]))

		for name in group["names"]:
			name_map[name] = canonical

	# Apply manual aliases
	if alias_map:
		for alias, real_name in alias_map.items():
			name_map[alias] = real_name
			# Also update any existing mappings that point to the alias
			for key in list(name_map.keys()):
				if name_map[key] == alias:
					name_map[key] = real_name

	return name_map


def get_git_contributors(file_path, global_name_map=None):
	"""
	Get contributors for a file from git history.
	Uses the global name map to ensure consistent naming across files.
	"""
	try:
		file_path = Path(file_path).resolve()

		git_root = find_git_root(file_path)
		if not git_root:
			return []

		try:
			relative_path = file_path.relative_to(git_root)
		except ValueError:
			return []

		file_names = set()
		result = subprocess.run(
			["git", "log", "--follow", "--format=%aN", "--", str(relative_path)],
			capture_output=True,
			text=True,
			check=True,
			cwd=git_root,
		)

		if result.stdout:
			for line in result.stdout.strip().split("\n"):
				if line:
					name = line.strip()
					if name:
						file_names.add(name)

		result = subprocess.run(
			["git", "log", "--follow", "--format=%b", "--", str(relative_path)],
			capture_output=True,
			text=True,
			check=True,
			cwd=git_root,
		)

		if result.stdout:
			co_author_pattern = r"^Co-authored-by:\s*([^<]+)\s*<"
			for line in result.stdout.split("\n"):
				match = re.match(co_author_pattern, line, re.IGNORECASE)
				if match:
					name = match.group(1).strip()
					if name:
						file_names.add(name)

		canonical_names = set()
		for name in file_names:
			if global_name_map and name in global_name_map:
				canonical_names.add(global_name_map[name])
			else:
				canonical_names.add(name)

		def sort_key(name):
			parts = name.split()
			if len(parts) > 1:
				return parts[-1].lower() + " " + " ".join(parts[:-1]).lower()
			else:
				return name.lower()

		return sorted(list(canonical_names), key=sort_key)

	except subprocess.CalledProcessError:
		return []


def names_match(name1, name2):
	"""
	Check if two names likely refer to the same person.
	Handles cases like:
	- "fproldan" and "Francisco Roldán"
	- "Rohan" and "Rohan Bansal"
	- Exact matches with different capitalization
	"""
	n1_lower = name1.lower().strip()
	n2_lower = name2.lower().strip()

	if n1_lower == n2_lower:
		return True

	n1_parts = n1_lower.replace(".", " ").replace("_", " ").replace("-", " ").split()
	n2_parts = n2_lower.replace(".", " ").replace("_", " ").replace("-", " ").split()

	if len(n1_parts) < len(n2_parts):
		if all(part in n2_parts for part in n1_parts):
			return True
	elif len(n2_parts) < len(n1_parts):
		if all(part in n1_parts for part in n2_parts):
			return True

	n1_clean = "".join(n1_parts)
	n2_clean = "".join(n2_parts)

	if len(n1_clean) < len(n2_clean):
		for part in n2_parts:
			if n1_clean.startswith(part[:1]):
				remaining = n1_clean[1:]
				for other_part in n2_parts:
					if other_part != part and remaining.startswith(
						other_part[: min(3, len(other_part))]
					):
						return True
		if n1_clean in n2_clean:
			return True
	elif len(n2_clean) < len(n1_clean):
		for part in n1_parts:
			if n2_clean.startswith(part[:1]):
				remaining = n2_clean[1:]
				for other_part in n1_parts:
					if other_part != part and remaining.startswith(
						other_part[: min(3, len(other_part))]
					):
						return True
		if n2_clean in n1_clean:
			return True

	return False


def select_best_name(names):
	"""
	Select the best canonical name from a list of name variations. Prefers full names over partial names or usernames.
	"""
	if not names:
		return ""

	if len(names) == 1:
		return names[0]

	scored_names = []
	for name in names:
		score = 0

		if " " in name:
			score += 100

		score += len(name.split()) * 50

		if any(c.isupper() for c in name):
			score += 25

		if name.islower():
			score -= 50

		vowel_ratio = sum(1 for c in name.lower() if c in "aeiou") / max(len(name), 1)
		if vowel_ratio < 0.25:
			score -= 30

		score += len(name)

		scored_names.append((score, name))

	scored_names.sort(reverse=True)
	return scored_names[0][1]


def find_git_root(path):
	path = Path(path)
	if path.is_file():
		path = path.parent

	current = path
	while current != current.parent:
		if (current / ".git").exists():
			return current
		current = current.parent

	try:
		result = subprocess.run(
			["git", "rev-parse", "--show-toplevel"],
			capture_output=True,
			text=True,
			check=True,
			cwd=path,
		)
		return Path(result.stdout.strip())
	except subprocess.CalledProcessError:
		return None


def get_last_modified_date(file_path):
	try:
		file_path = Path(file_path).resolve()

		git_root = find_git_root(file_path)
		if not git_root:
			return datetime.now().strftime("%Y-%m-%d")

		try:
			relative_path = file_path.relative_to(git_root)
		except ValueError:
			return datetime.now().strftime("%Y-%m-%d")

		result = subprocess.run(
			["git", "log", "-1", "--format=%at", "--", str(relative_path)],
			capture_output=True,
			text=True,
			check=True,
			cwd=git_root,
		)

		if result.stdout:
			timestamp = int(result.stdout.strip())
			return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

	except (subprocess.CalledProcessError, ValueError):
		pass

	return datetime.now().strftime("%Y-%m-%d")


def find_byline_div(content):
	"""
	Find the <div class="byline"> section in markdown content.
	Returns (start_index, end_index, indent, manual_authors) or (None, None, '', []) if not found.
	"""
	lines = content.split("\n")

	opening_pattern = r'^(\s*)<div\s+class\s*=\s*["\']byline["\'](?:\s+data-manual-authors\s*=\s*["\']([^"\']*)["\'])?\s*>'
	closing_pattern = r"^(\s*)</div>"

	for i, line in enumerate(lines):
		match = re.match(opening_pattern, line, re.IGNORECASE)
		if match:
			start = i
			indent = match.group(1)
			manual_authors = match.group(2) if match.group(2) else ""

			for j in range(start + 1, len(lines)):
				if re.match(closing_pattern, lines[j], re.IGNORECASE):
					return start, j + 1, indent, manual_authors

			return start, len(lines), indent, manual_authors

	return None, None, "", ""


def format_byline_content(contributors, last_modified, indent="", manual_authors=""):
	"""Format the byline content for inside the div."""
	all_contributors = list(contributors)

	if manual_authors:
		manual_list = [a.strip() for a in manual_authors.split(",") if a.strip()]
		for author in manual_list:
			if author not in all_contributors:
				all_contributors.append(author)

		def sort_key(name):
			parts = name.split()
			if len(parts) > 1:
				return parts[-1].lower() + " " + " ".join(parts[:-1]).lower()
			else:
				return name.lower()

		all_contributors.sort(key=sort_key)

	if not all_contributors:
		return f"{indent}"

	if len(all_contributors) == 1:
		byline = f"{indent}  {all_contributors[0]}"
	elif len(all_contributors) == 2:
		byline = f"{indent}  {all_contributors[0]} and {all_contributors[1]}"
	else:
		byline = f"{indent}  {', '.join(all_contributors[:-1])}, and {all_contributors[-1]}"

	byline += f" {last_modified}"

	return byline


def create_byline_div(contributors, last_modified, indent="", manual_authors=""):
	"""Create a complete byline div section."""
	content = format_byline_content(contributors, last_modified, indent, manual_authors)
	div_attrs = 'class="byline"'
	if manual_authors:
		div_attrs += f' data-manual-authors="{manual_authors}"'
	return f"{indent}<div {div_attrs}>\n{content}\n{indent}</div>"


def update_markdown_byline(
	file_path,
	dry_run=False,
	verbose=False,
	global_name_map=None,
	file_specific_authors=None,
):
	"""Update the byline in a markdown file using the global name map."""

	with open(file_path, encoding="utf-8") as f:
		content = f.read()

	contributors = get_git_contributors(file_path, global_name_map)
	last_modified = get_last_modified_date(file_path)

	if not contributors:
		if verbose:
			print(f"  No git history found for {file_path}, skipping...")
		return False

	start_idx, end_idx, indent, manual_authors = find_byline_div(content)

	# Add file-specific authors if provided
	if file_specific_authors:
		if manual_authors:
			manual_authors += ", " + file_specific_authors
		else:
			manual_authors = file_specific_authors

	lines = content.split("\n")

	if verbose:
		print(f"    Found {len(contributors)} contributor(s): {', '.join(contributors)}")
		print(f"    Byline div found: {start_idx is not None}")

	if start_idx is not None:
		new_content = format_byline_content(
			contributors, last_modified, indent, manual_authors
		)
		div_attrs = 'class="byline"'
		if manual_authors:
			div_attrs += f' data-manual-authors="{manual_authors}"'
		new_lines = (
			lines[:start_idx]
			+ [f"{indent}<div {div_attrs}>"]
			+ [new_content]
			+ lines[end_idx - 1 :]
		)
		new_content_str = "\n".join(new_lines)
	else:
		title_idx = None
		for i, line in enumerate(lines):
			if re.match(r"^#+\s+", line):
				title_idx = i
				if verbose:
					print(f"    Found heading at line {i + 1}: {line[:50]}")
				break

		byline_div = create_byline_div(
			contributors, last_modified, manual_authors=manual_authors
		)

		if title_idx is not None:
			new_lines = lines[: title_idx + 1] + ["", byline_div, ""] + lines[title_idx + 1 :]
			if verbose:
				print(f"    Inserting byline after heading at line {title_idx + 1}")
		else:
			new_lines = [byline_div, ""] + lines
			if verbose:
				print("    No heading found, inserting byline at beginning")

		new_content_str = "\n".join(new_lines)

	if new_content_str != content:
		if not dry_run:
			with open(file_path, "w", encoding="utf-8") as f:
				f.write(new_content_str)
		if verbose:
			print("    File updated successfully")
		return True
	else:
		if verbose:
			print("    No changes needed (content unchanged)")

	return False


def get_staged_files():
	"""Get list of staged markdown files."""
	try:
		result = subprocess.run(
			["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
			capture_output=True,
			text=True,
			check=True,
		)

		files = result.stdout.strip().split("\n") if result.stdout else []
		return [f for f in files if f and (f.endswith(".md") or f.endswith(".markdown"))]

	except subprocess.CalledProcessError:
		return []


def find_markdown_files(path):
	"""Find all markdown files in a directory or return single file if path is a file."""
	path = Path(path)

	if path.is_file():
		if path.suffix in [".md", ".markdown"]:
			return [path]
		else:
			return []
	elif path.is_dir():
		md_files = list(path.glob("**/*.md")) + list(path.glob("**/*.markdown"))
		return md_files
	else:
		return []


def should_skip_file(file_path, skip_patterns):
	"""Check if a file should be skipped based on skip patterns."""
	if not skip_patterns:
		return False

	file_path = Path(file_path)
	for pattern in skip_patterns:
		# Match against filename
		if file_path.name == pattern:
			return True
		# Match against relative path
		if file_path.match(pattern):
			return True

	return False


def parse_specify_args(specify_args):
	"""Parse --specify arguments into a dictionary mapping file patterns to author lists."""
	file_authors = {}

	i = 0
	while i < len(specify_args):
		if i + 1 < len(specify_args):
			file_pattern = specify_args[i]
			authors = specify_args[i + 1]
			file_authors[file_pattern] = authors
			i += 2
		else:
			i += 1

	return file_authors


def parse_alias_args(alias_args):
	"""Parse --alias arguments into a dictionary mapping aliases to real names."""
	alias_map = {}

	i = 0
	while i < len(alias_args):
		if i + 1 < len(alias_args):
			alias = alias_args[i]
			real_name = alias_args[i + 1]
			alias_map[alias] = real_name
			i += 2
		else:
			i += 1

	return alias_map


def get_file_specific_authors(file_path, file_authors_map):
	"""Get file-specific authors if the file matches any pattern."""
	if not file_authors_map:
		return None

	file_path = Path(file_path)
	for pattern, authors in file_authors_map.items():
		# Match against filename
		if file_path.name == pattern:
			return authors
		# Match against relative path
		if file_path.match(pattern):
			return authors

	return None


def run_as_cli(args):
	"""Run as a standalone CLI tool."""
	# Parse aliases
	alias_map = parse_alias_args(args.alias) if args.alias else {}

	# Parse file-specific authors
	file_authors_map = parse_specify_args(args.specify) if args.specify else {}

	paths = []

	for path_str in args.paths:
		paths.extend(find_markdown_files(path_str))

	# Filter out skipped files
	if args.skip:
		original_count = len(paths)
		paths = [p for p in paths if not should_skip_file(p, args.skip)]
		if args.verbose and original_count > len(paths):
			print(f"Skipped {original_count - len(paths)} file(s) based on --skip patterns")

	if not paths:
		print("No markdown files found.")
		return 0

	print("Building global identity map across all files...")
	global_name_map = build_global_identity_map(paths, alias_map)

	if args.verbose and global_name_map:
		print("\nIdentity mappings found:")
		unique_mappings = {}
		for variant, canonical in global_name_map.items():
			if variant != canonical:
				if canonical not in unique_mappings:
					unique_mappings[canonical] = []
				unique_mappings[canonical].append(variant)
		for canonical, variants in unique_mappings.items():
			if variants:
				print(f"  {canonical}: {', '.join(variants)}")

	modified_count = 0

	print(f"\nProcessing {len(paths)} markdown file(s)...")

	for file_path in paths:
		if args.verbose:
			print(f"  Processing {file_path}...")

		try:
			file_specific_authors = get_file_specific_authors(file_path, file_authors_map)

			if update_markdown_byline(
				file_path,
				dry_run=args.dry_run,
				verbose=args.verbose,
				global_name_map=global_name_map,
				file_specific_authors=file_specific_authors,
			):
				modified_count += 1
				action = "Would update" if args.dry_run else "Updated"
				print(f"  ✓ {action} byline in {file_path}")
			elif args.verbose:
				print(f"  - No changes needed for {file_path}")
		except Exception as e:
			print(f"  ✗ Error processing {file_path}: {e}", file=sys.stderr)
			if args.verbose:
				import traceback

				traceback.print_exc()

	if args.dry_run and modified_count > 0:
		print(f"\nDry run: Would update {modified_count} file(s)")
	elif modified_count > 0:
		print(f"\n✓ Updated {modified_count} file(s)")
	else:
		print("\nNo byline updates needed")

	return 0


def run_as_precommit():
	"""Run as a pre-commit hook."""
	staged_files = get_staged_files()

	if not staged_files:
		sys.exit(0)

	print("Building global identity map...")
	global_name_map = build_global_identity_map(staged_files)

	modified_files = []

	print("Checking markdown files for byline updates...")

	for file_path in staged_files:
		if Path(file_path).exists():
			print(f"  Processing {file_path}...")
			try:
				if update_markdown_byline(file_path, verbose=True, global_name_map=global_name_map):
					modified_files.append(file_path)
					print("    ✓ Updated byline")
				else:
					print("    - No changes needed")
			except Exception as e:
				print(f"    ✗ Error: {e}")
				import traceback

				traceback.print_exc()

	if modified_files:
		try:
			subprocess.run(["git", "add"] + modified_files, check=True)
			print(f"\n✓ Re-staged {len(modified_files)} file(s) with updated bylines")
		except subprocess.CalledProcessError as e:
			print(f"Error re-staging files: {e}", file=sys.stderr)
			sys.exit(1)
	else:
		print("\nNo byline updates needed")

	sys.exit(0)


def main():
	if len(sys.argv) == 1:
		if "GIT_DIR" in os.environ or "GIT_INDEX_FILE" in os.environ:
			run_as_precommit()
			return

	parser = argparse.ArgumentParser(
		description="Update bylines in markdown files based on git history",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
Examples:
  # Process all markdown files in current directory
  %(prog)s .

  # Skip README.md files
  %(prog)s . --skip README.md

  # Add specific authors to index.md
  %(prog)s . --specify index.md "John Doe, Jane Smith"

  # Map GitHub username to real name
  %(prog)s . --alias jdoe123 "John Doe"

  # Combine multiple options
  %(prog)s . --skip README.md --alias jsmith "Jane Smith" --specify docs/guide.md "Expert Reviewer"
		""",
	)
	parser.add_argument(
		"paths",
		nargs="+",
		help="Path(s) to markdown files or directories containing markdown files",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Show what would be changed without actually modifying files",
	)
	parser.add_argument("-v", "--verbose", action="store_true", help="Show verbose output")
	parser.add_argument(
		"--skip",
		action="append",
		metavar="PATTERN",
		help="Skip files matching this pattern (can be used multiple times). "
		"Matches against filename or path pattern (e.g., 'README.md', '*/docs/*.md')",
	)
	parser.add_argument(
		"--specify",
		nargs="+",
		action="append",
		metavar=("FILE_PATTERN", "AUTHORS"),
		help="Specify additional authors for files matching a pattern. "
		'Usage: --specify FILE_PATTERN "Author1, Author2". '
		"Can be used multiple times for different files.",
	)
	parser.add_argument(
		"--alias",
		nargs="+",
		action="append",
		metavar=("USERNAME", "REAL_NAME"),
		help="Map a git username/alias to a real name. "
		'Usage: --alias gh_username "Real Name". '
		"Can be used multiple times for different aliases.",
	)

	args = parser.parse_args()

	# Flatten the nested lists from action="append" with nargs="+"
	if args.specify:
		args.specify = [item for sublist in args.specify for item in sublist]
	if args.alias:
		args.alias = [item for sublist in args.alias for item in sublist]

	sys.exit(run_as_cli(args))


if __name__ == "__main__":
	main()
