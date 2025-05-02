#!/usr/bin/env python
import os
import sys
import json
import requests
from anthropic import Anthropic


def get_github_token():
	"""Retrieve the GitHub token from environment variables."""
	token = os.environ.get("GITHUB_TOKEN")
	if not token:
		print("Error: GITHUB_TOKEN environment variable is not set")
		sys.exit(1)
	return token


def get_anthropic_api_key():
	"""Retrieve the Anthropic API key from environment variables."""
	api_key = os.environ.get("ANTHROPIC_API_KEY")
	if not api_key:
		print("Error: ANTHROPIC_API_KEY environment variable is not set")
		sys.exit(1)
	return api_key


def get_model():
	"""Retrieve the model from environment variables or use default."""
	return os.environ.get("MODEL", "claude-3-haiku-20240307")


def get_pr_data():
	"""Fetch PR data including commits, description, and comments."""
	token = get_github_token()
	repo_full_name = os.environ.get("REPO_FULL_NAME")
	pr_number = os.environ.get("PR_NUMBER")

	if not repo_full_name or not pr_number:
		print("Error: Repository name or PR number not provided")
		sys.exit(1)

	headers = {
		"Accept": "application/vnd.github.v3+json",
		"Authorization": f"token {token}",
	}

	# Fetch PR details
	pr_url = f"https://api.github.com/repos/{repo_full_name}/pulls/{pr_number}"
	pr_response = requests.get(pr_url, headers=headers)
	pr_response.raise_for_status()
	pr_data = pr_response.json()

	# Fetch PR commits
	commits_url = f"{pr_url}/commits"
	commits_response = requests.get(commits_url, headers=headers)
	commits_response.raise_for_status()
	commits_data = commits_response.json()

	# Fetch PR comments
	comments_url = f"{pr_url}/comments"
	comments_response = requests.get(comments_url, headers=headers)
	comments_response.raise_for_status()
	comments_data = comments_response.json()

	# Fetch PR review comments
	review_comments_url = f"{pr_url}/comments"
	review_comments_response = requests.get(review_comments_url, headers=headers)
	review_comments_response.raise_for_status()
	review_comments_data = review_comments_response.json()

	# Fetch PR issues
	issue_url = f"https://api.github.com/repos/{repo_full_name}/issues/{pr_number}"
	issue_response = requests.get(issue_url, headers=headers)
	issue_response.raise_for_status()
	issue_data = issue_response.json()

	# Check for existing changelog comment
	issue_comments_url = f"{issue_url}/comments"
	issue_comments_response = requests.get(issue_comments_url, headers=headers)
	issue_comments_response.raise_for_status()
	issue_comments = issue_comments_response.json()

	comment_header = os.environ.get("COMMENT_HEADER", "## 📝 Draft Changelog Entry")
	existing_changelog_comment = None
	for comment in issue_comments:
		if comment_header in comment.get("body", ""):
			existing_changelog_comment = comment
			break

	# Get changed files
	files_url = f"{pr_url}/files"
	files_response = requests.get(files_url, headers=headers)
	files_response.raise_for_status()
	files_data = files_response.json()

	return {
		"pr": pr_data,
		"commits": commits_data,
		"comments": comments_data + review_comments_data,
		"issue": issue_data,
		"files": files_data,
		"existing_changelog_comment": existing_changelog_comment,
	}


def check_should_regenerate(existing_comment_id):
	"""Check if the comment was deleted and we should regenerate."""
	token = get_github_token()
	repo_full_name = os.environ.get("REPO_FULL_NAME")

	headers = {
		"Accept": "application/vnd.github.v3+json",
		"Authorization": f"token {token}",
	}

	comment_url = (
		f"https://api.github.com/repos/{repo_full_name}/issues/comments/{existing_comment_id}"
	)
	response = requests.get(comment_url, headers=headers)

	return response.status_code == 404  # Comment was deleted


def get_custom_prompt_template():
	"""Load custom prompt template if provided."""
	prompt_path = os.environ.get("PROMPT_TEMPLATE_PATH")
	default_prompt = """
    You are an expert at analyzing Pull Requests and generating changelog entries.
    Analyze the following PR data and generate a comprehensive, user-friendly changelog entry.

    Focus on:
    - Breaking changes (API modifications, dependency updates)
    - New features vs bug fixes vs performance improvements, using conventional commits
    - Security-relevant changes
    - Infrastructure/tooling updates
    - User impact ("Users can now...")
    - Required actions by users ("Requires updating...")
    - Context for why changes were made

    Format your response as markdown with appropriate sections and bullet points.
    Be concise but informative.

    PR Data:
    {pr_data}
    """

	try:
		if prompt_path and os.path.exists(prompt_path):
			with open(prompt_path) as f:
				return f.read()
	except Exception as e:
		print(f"Warning: Could not load custom prompt template: {e}")

	return default_prompt


def format_pr_data_for_prompt(pr_data):
	"""Format the PR data for inclusion in the prompt."""
	formatted_data = {
		"title": pr_data["pr"]["title"],
		"description": pr_data["pr"]["body"],
		"author": pr_data["pr"]["user"]["login"],
		"commits": [
			{
				"sha": commit["sha"][:7],
				"message": commit["commit"]["message"],
				"author": commit["commit"]["author"]["name"],
			}
			for commit in pr_data["commits"]
		],
		"changed_files": [
			{
				"filename": file["filename"],
				"changes": f"+{file.get('additions', 0)}/-{file.get('deletions', 0)}",
				"status": file["status"],
			}
			for file in pr_data["files"]
		],
	}

	return json.dumps(formatted_data, indent=2)


def generate_changelog_with_anthropic(pr_data):
	"""Generate a changelog entry using Anthropic's Claude."""
	api_key = get_anthropic_api_key()
	model = get_model()
	client = Anthropic(api_key=api_key)

	prompt_template = get_custom_prompt_template()
	formatted_pr_data = format_pr_data_for_prompt(pr_data)
	prompt = prompt_template.format(pr_data=formatted_pr_data)

	try:
		response = client.messages.create(
			model=model,  # Use the model from environment variables
			max_tokens=1500,
			temperature=0.2,  # Lower temperature for more factual outputs
			messages=[{"role": "user", "content": prompt}],
		)

		return response.content[0].text
	except Exception as e:
		print(f"Error generating changelog with Anthropic: {e}")
		return None


def post_or_update_comment(changelog_text, existing_comment=None):
	"""Post a new comment or update the existing comment with the generated changelog."""
	token = get_github_token()
	repo_full_name = os.environ.get("REPO_FULL_NAME")
	pr_number = os.environ.get("PR_NUMBER")
	comment_header = os.environ.get("COMMENT_HEADER", "## 📝 Draft Changelog Entry")

	headers = {
		"Accept": "application/vnd.github.v3+json",
		"Authorization": f"token {token}",
	}

	comment_body = f"{comment_header}\n\n{changelog_text}\n\n_This changelog entry was automatically generated by the Changelog Generator Action._"

	if existing_comment:
		# Update existing comment
		comment_url = f"https://api.github.com/repos/{repo_full_name}/issues/comments/{existing_comment['id']}"
		response = requests.patch(comment_url, headers=headers, json={"body": comment_body})
	else:
		# Create new comment
		comments_url = (
			f"https://api.github.com/repos/{repo_full_name}/issues/{pr_number}/comments"
		)
		response = requests.post(comments_url, headers=headers, json={"body": comment_body})

	response.raise_for_status()
	return response.json()


def main():
	"""Main function to generate and post changelog."""
	try:
		pr_data = get_pr_data()

		# Check if there's already a changelog comment
		existing_comment = pr_data.get("existing_changelog_comment")

		if existing_comment and not check_should_regenerate(existing_comment["id"]):
			print("Draft changelog comment already exists. Taking no action.")
			return

		# Generate the changelog text
		changelog_text = generate_changelog_with_anthropic(pr_data)

		if not changelog_text:
			print("Failed to generate changelog text")
			sys.exit(1)

		# Post or update the comment
		comment = post_or_update_comment(changelog_text, existing_comment)
		print(f"Successfully {'updated' if existing_comment else 'posted'} changelog comment")

	except Exception as e:
		print(f"Error in changelog generation: {e}")
		sys.exit(1)


if __name__ == "__main__":
	main()
