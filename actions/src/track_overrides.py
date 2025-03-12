import requests
import os
import re
import sys
import difflib


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


def get_last_commit_hash_for_file_in_branch(repo_url, file_path):
	repo_url_split = repo_url.strip("/").split("/")
	username, repo_name = repo_url_split[-2], repo_url_split[-1]
	api_url = f"https://api.github.com/repos/{username}/{repo_name}/commits"
	branch = os.getenv("GITHUB_BASE_REF", "develop")
	params = {"path": file_path, "sha": branch}
	response = requests.get(
		api_url,
		params=params,
		headers={"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"},
	)

	if response.status_code != 200:
		print(f"Failed to fetch commits: {response.status_code} - {response.reason}")
		return None

	commits = response.json()
	return commits[0]["sha"] if commits else None


def compare_commit_hashes(directory):
	changed_methods = []
	pattern = (
		r"HASH:\s*(\w+)\s*REPO:\s*([\w\/:.]+)\s*PATH:\s*([\w\/.]+)\s*METHOD:\s*([\w]+)"
	)

	for root, _, files in os.walk(directory):
		for file_name in files:
			if not file_name.endswith(".py"):
				continue

			file_path = os.path.join(root, file_name)
			with open(file_path) as file:
				source_code = file.read()

				for match in re.finditer(pattern, source_code, re.DOTALL):
					commit_hash = match.group(1)
					repo = match.group(2)
					file_path = match.group(3)
					method_name = match.group(4)

					latest_commit_hash = get_last_commit_hash_for_file_in_branch(repo, file_path)
					if not latest_commit_hash or latest_commit_hash == commit_hash:
						continue

					old_file_content = download_file_from_commit(repo, commit_hash, file_path)
					new_file_content = download_file_from_commit(repo, latest_commit_hash, file_path)

					if not old_file_content or not new_file_content:
						changed_methods.append(
							f"- `{method_name}` in file `{file_path}` changed, but could not fetch file contents."
						)
						continue

					old_method = extract_method(old_file_content, method_name)
					new_method = extract_method(new_file_content, method_name)

					diff = compare_method_diff(old_method, new_method)
					if diff:
						changed_methods.append(
							f"### `{method_name}` in file `{file_path}` has changed:\n\n```diff\n{diff}\n```"
						)

	return changed_methods


if __name__ == "__main__":
	changed_methods = compare_commit_hashes(sys.argv[1])
	if changed_methods:
		print("\n".join(changed_methods))
