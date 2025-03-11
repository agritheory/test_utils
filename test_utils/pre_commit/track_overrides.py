import os
import sys
import re
import difflib
import subprocess
import argparse
import pathlib
import requests
from typing import Sequence

sys.path.append(os.path.abspath(".github/actions/track_overrides/src"))
from track_overrides import (
	download_file_from_commit,
	extract_method,
	compare_method_diff,
)


def get_staged_python_files(repo_path):
	result = subprocess.run(
		["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
		capture_output=True,
		text=True,
		cwd=repo_path,
	)
	return [f.strip() for f in result.stdout.split("\n") if f.strip().endswith(".py")]


def get_staged_file_content(file_path, repo_path):
	try:
		file_path = f"{repo_path}/{file_path}"
		abs_file_path = os.path.abspath(file_path)
		relative_path = os.path.relpath(abs_file_path, repo_path)

		staged_content = subprocess.check_output(
			["git", "show", f":{relative_path}"], text=True, cwd=repo_path
		)
		return staged_content
	except subprocess.CalledProcessError:
		print(f"Failed to get staged content of {file_path}")
		return None
	except Exception as e:
		print(f"Error: {e}")
		return None


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


def get_current_git_branch(repo_path):
	if not os.path.isdir(os.path.join(repo_path, ".git")):
		raise Exception(f"Directory {repo_path} is not a Git repository.")

	result = subprocess.run(
		["git", "rev-parse", "--abbrev-ref", "HEAD"],
		capture_output=True,
		text=True,
		cwd=repo_path,
	)

	if result.returncode == 0:
		return result.stdout.strip()
	raise Exception("Failed to get current git branch")


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
	repo_directory = str(pathlib.Path(app).resolve().parent / app)
	changed_methods = []
	pattern = (
		r"HASH:\s*(\w+)\s*REPO:\s*([\w\/:.]+)\s*PATH:\s*([\w\/.]+)\s*METHOD:\s*([\w]+)"
	)
	staged_files = get_staged_python_files(repo_directory)
	for file_path in staged_files:
		staged_content = get_staged_file_content(file_path, repo_directory)
		if not staged_content:
			continue

		for match in re.finditer(pattern, staged_content, re.DOTALL):
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
					{"title": f"`{method_name}` in `{file_path}` has changed:", "diff": diff}
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
