import os
import sys
import re
import difflib
import subprocess
import pathlib
import requests
from typing import Sequence


def get_staged_python_files():
	result = subprocess.run(
		["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
		capture_output=True,
		text=True,
	)
	return [f.strip() for f in result.stdout.split("\n") if f.strip().endswith(".py")]


def get_staged_file_content(file_path):
	try:
		staged_content = subprocess.check_output(["git", "show", f":{file_path}"], text=True)
		return staged_content
	except subprocess.CalledProcessError:
		print(f"Failed to get staged content of {file_path}")
		return None


def extract_method(source_code, method_name):
	method_pattern = re.compile(
		rf"def\s+{re.escape(method_name)}\s*\(.*?\):.*?(?=^\s*def\s+|\Z)",
		re.DOTALL | re.MULTILINE,
	)
	match = method_pattern.search(source_code)
	return match.group(0) if match else None


def compare_method_diff(old_method, new_method):
	if not old_method or not new_method:
		return None

	diff = difflib.unified_diff(
		old_method.splitlines(),
		new_method.splitlines(),
		lineterm="",
		fromfile="old",
		tofile="new",
	)
	return "\n".join(diff)


def download_file_from_commit(repo_url, commit_hash, file_path):
	repo_url_split = repo_url.strip("/").split("/")
	username, repo_name = repo_url_split[-2], repo_url_split[-1]

	raw_url = (
		f"https://raw.githubusercontent.com/{username}/{repo_name}/{commit_hash}/{file_path}"
	)
	response = requests.get(raw_url)

	if response.status_code == 200:
		return response.text
	print(
		f"Failed to fetch file {file_path} from commit {commit_hash}: {response.status_code} - {response.reason}"
	)
	return None


def get_last_commit_hash_for_file_in_branch(repo_url, file_path):
	repo_url_split = repo_url.strip("/").split("/")
	username, repo_name = repo_url_split[-2], repo_url_split[-1]
	api_url = f"https://api.github.com/repos/{username}/{repo_name}/commits"
	repo_path = pathlib.Path().resolve().parent / repo_url.split("/")[-2]
	branch = get_current_git_branch(repo_path)
	params = {"path": file_path, "sha": branch}
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


def check_tracked_methods():
	changed_methods = []
	pattern = (
		r"HASH:\s*(\w+)\s*REPO:\s*([\w\/:.]+)\s*PATH:\s*([\w\/.]+)\s*METHOD:\s*([\w]+)"
	)

	staged_files = get_staged_python_files()
	print(staged_files)
	for file_path in staged_files:
		staged_content = get_staged_file_content(file_path)
		if not staged_content:
			continue

		for match in re.finditer(pattern, staged_content, re.DOTALL):
			commit_hash = match.group(1)
			repo_url = match.group(2)
			original_file_path = match.group(3)
			method_name = match.group(4)

			latest_commit_hash = get_last_commit_hash_for_file_in_branch(
				repo_url, original_file_path
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
	changed_methods = check_tracked_methods()
	if changed_methods:
		print("\n[PRE-COMMIT HOOK] Method changes detected! Please review before committing.")
		for change in changed_methods:
			print(change["title"])
			print_diff(change["diff"])
			print("")
		sys.exit(1)
	else:
		sys.exit(0)
