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
				with open(abs_path) as f:
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


def check_tracked_methods(app, base_branch):
	repo_directory = str(pathlib.Path(app).resolve().parent)
	changed_methods = []
	pattern = (
		r"HASH:\s*(\w+)\s*REPO:\s*([\w\/:.]+)\s*PATH:\s*([\w\/.]+)\s*METHOD:\s*([\w]+)"
	)
	for file_path, source_code in get_all_python_files(repo_directory):
		for match in re.finditer(pattern, source_code, re.DOTALL):
			commit_hash = match.group(1)
			repo_url = match.group(2)
			original_file_path = match.group(3)
			method_name = match.group(4)

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
				changed_methods.append(
					{
						"title": f"`{method_name}` in `{file_path}` has changed:",
						"diff": diff,
					}
				)

	return changed_methods


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("--app", action="append", help="An argument for the hook")
	parser.add_argument("--base-branch", action="append", help="An argument for the hook")
	args = parser.parse_args(argv)
	app = args.app[0]
	base_branch = args.base_branch[0]

	if app and base_branch:
		changed_methods = check_tracked_methods(app, base_branch)
		if changed_methods:
			print(
				"\n[PRE-COMMIT HOOK] Method changes detected! Please review before committing."
			)
			for change in changed_methods:
				print(change["title"])
				print_diff(change["diff"])
				print("")
	sys.exit(0)
