"""Shared utilities for tracking method overrides across repositories."""

import difflib
import os
import re

import requests


class UpstreamFetchError(Exception):
	"""GitHub could not be reached. Distinct from an override actually having changed."""


def download_file_from_commit(repo_url, commit_hash, file_path):
	repo_url_split = repo_url.strip("/").split("/")
	username, repo_name = repo_url_split[-2], repo_url_split[-1]
	token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")

	if token:
		# raw.githubusercontent.com rate limits per IP and ignores the token, which 429s on
		# shared CI runners. The contents API honours it.
		response = requests.get(
			f"https://api.github.com/repos/{username}/{repo_name}/contents/{file_path}",
			params={"ref": commit_hash},
			headers={
				"Authorization": f"token {token}",
				"Accept": "application/vnd.github.raw",
			},
		)
	else:
		response = requests.get(
			f"https://raw.githubusercontent.com/{username}/{repo_name}/{commit_hash}/{file_path}"
		)

	if response.status_code == 200:
		return response.text
	raise UpstreamFetchError(
		f"Failed to fetch file {file_path} from commit {commit_hash}: "
		f"{response.status_code} - {response.reason}"
	)


def extract_method(source_code, method_name):
	if not source_code:
		return None

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
