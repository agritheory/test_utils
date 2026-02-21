"""Shared utilities for tracking method overrides across repositories."""

import difflib
import re

import requests


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
