import argparse
import os
import pathlib
import re
import sys
from collections.abc import Sequence

import requests

from test_utils.utils.track_overrides import (
	compare_method_diff,
	download_file_from_commit,
	extract_method,
)


def get_all_python_files(repo_path):
	"""Yield (file_path, content) for all .py files in repo_path. file_path is relative to repo_path."""
	for root, _, files in os.walk(repo_path):
		for file_name in files:
			if not file_name.endswith(".py"):
				continue
			abs_path = os.path.join(root, file_name)
			relative_path = os.path.relpath(abs_path, repo_path)
			try:
				with open(abs_path, encoding="utf-8", errors="replace") as f:
					content = f.read()
				yield relative_path, content
			except OSError as e:
				print(f"Failed to read {relative_path}: {e}")


def get_last_commit_hash_for_file_in_branch(repo_url, file_path, base_branch):
	repo_url_split = repo_url.strip("/").split("/")
	username, repo_name = repo_url_split[-2], repo_url_split[-1]
	api_url = f"https://api.github.com/repos/{username}/{repo_name}/commits"
	params = {"path": file_path, "sha": base_branch}
	response = requests.get(api_url, params=params)

	if response.status_code != 200:
		print(f"Failed to fetch commits: {response.status_code} - {response.reason}")
		return None

	commits = response.json()
	return commits[0]["sha"] if commits else None


def print_diff(diff_text):
	diff_lines = diff_text.splitlines()
	for line in diff_lines:
		if line.startswith("+++") or line.startswith("---"):
			print("\033[32m" + line)
		elif line.startswith("@@"):
			print("\033[36m" + line)
		elif line.startswith("+"):
			print("\033[32m" + line)
		elif line.startswith("-"):
			print("\033[31m" + line)
		else:
			print("\033[37m" + line)


def check_tracked_methods(repo_directory, base_branch, apps_filter=None):
	changed_methods = []
	pattern = (
		r"(?:APP:\s*([\w_-]+)\s+)?"
		r"HASH:\s*(\w+)\s*REPO:\s*([\w\/:.]+)\s*PATH:\s*([\w\/.]+)\s*METHOD:\s*([\w]+)"
	)
	for file_path, source_code in get_all_python_files(repo_directory):
		for match in re.finditer(pattern, source_code, re.DOTALL):
			app_name = match.group(1)
			commit_hash = match.group(2)
			repo_url = match.group(3)
			original_file_path = match.group(4)
			method_name = match.group(5)
			# Infer app from file path if APP not in annotation (backward compat)
			if not app_name and "/" in file_path:
				parts = file_path.replace("\\", "/").split("/")
				app_name = parts[1] if parts[0] == "apps" and len(parts) > 1 else parts[0]
			elif not app_name:
				app_name = os.path.basename(repo_directory) if repo_directory else ""

			latest_commit_hash = get_last_commit_hash_for_file_in_branch(
				repo_url, original_file_path, base_branch
			)
			if not latest_commit_hash or latest_commit_hash == commit_hash:
				continue

			old_file_content = download_file_from_commit(
				repo_url, commit_hash, original_file_path
			)
			new_file_content = download_file_from_commit(
				repo_url, latest_commit_hash, original_file_path
			)

			method_at_hash = extract_method(old_file_content, method_name)
			latest_method = extract_method(new_file_content, method_name)

			diff = compare_method_diff(method_at_hash, latest_method)
			if diff:
				if apps_filter is None or app_name in apps_filter:
					changed_methods.append(
						{
							"title": f"{app_name}: `{method_name}` in `{original_file_path}` has changed:",
							"diff": diff,
						}
					)

	return changed_methods


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"--app", action="append", help="App name(s) to scan (resolves to app directory)"
	)
	parser.add_argument(
		"--directory",
		help="Directory to scan (e.g. workspace root); overrides --app when set. Use for monorepos to match GHA behavior.",
	)
	parser.add_argument(
		"--base-branch", action="append", help="Base branch to compare against"
	)
	args = parser.parse_args(argv)

	if not args.base_branch:
		sys.exit(0)
	base_branch = args.base_branch[0]

	if args.directory:
		# Scan entire directory (like GHA with github.workspace)
		directories = [str(pathlib.Path(args.directory).resolve())]
		apps_filter = set(args.app) if args.app else None
	elif args.app:
		# Scan app directory(ies); supports multiple apps for monorepos
		directories = [str(pathlib.Path(app).resolve().parent) for app in args.app]
		apps_filter = None  # Already scoped by directory
	else:
		sys.exit(0)

	if base_branch:
		changed_methods = []
		for repo_directory in directories:
			changed_methods.extend(
				check_tracked_methods(repo_directory, base_branch, apps_filter=apps_filter)
			)
		if changed_methods:
			print(
				"\n[PRE-COMMIT HOOK] Method changes detected! Please review before committing."
			)
			for change in changed_methods:
				print(change["title"])
				print_diff(change["diff"])
				print("")
			sys.exit(1)
	sys.exit(0)
