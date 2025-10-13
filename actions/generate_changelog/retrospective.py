import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from anthropic import Anthropic
from github import Github
from github.PaginatedList import PaginatedList


class ChangelogEntry:
	"""Represents a single changelog entry with metadata."""

	def __init__(self, date: datetime, content: str, source_type: str, source_id: str):
		self.date = date
		self.content = content
		self.source_type = source_type  # 'pr' or 'commit'
		self.source_id = source_id


class RetrospectiveChangelogGenerator:
	"""Generate changelog from historical GitHub data."""

	def __init__(self, github_token: str, anthropic_api_key: str, repo_name: str):
		self.github = Github(github_token)
		self.anthropic = Anthropic(api_key=anthropic_api_key)
		self.repo = self.github.get_repo(repo_name)
		self.anthropic_model = "claude-3-7-sonnet-latest"
		self.max_tokens = 1500
		self.temperature = 0.2

	def get_merged_prs(
		self,
		branch: str,
		since_date: datetime | None = None,
		until_date: datetime | None = None,
	) -> list[dict[str, Any]]:
		"""Get all merged PRs for a branch within date range."""
		print(f"Fetching merged PRs for branch: {branch}")

		# Get closed PRs for the branch
		prs = self.repo.get_pulls(
			state="closed", base=branch, sort="updated", direction="desc"
		)

		merged_prs = []
		for pr in prs:
			# Skip if not merged
			if not pr.merged:
				continue

			# Apply date filters
			if since_date and pr.merged_at < since_date:
				continue
			if until_date and pr.merged_at > until_date:
				break  # Since sorted by updated desc, we can break here

			merged_prs.append(
				{
					"pr": pr,
					"commits": list(pr.get_commits()),
					"files": list(pr.get_files()),
					"comments": list(pr.get_comments()),
					"review_comments": list(pr.get_review_comments()),
				}
			)

			print(f"  Found PR #{pr.number}: {pr.title}")

		return merged_prs

	def get_direct_commits(
		self,
		branch: str,
		since_date: datetime | None = None,
		until_date: datetime | None = None,
	) -> list[dict[str, Any]]:
		"""Get commits that weren't part of PRs (direct pushes)."""
		print(f"Fetching direct commits for branch: {branch}")

		commits = self.repo.get_commits(sha=branch, since=since_date, until=until_date)

		direct_commits = []
		for commit in commits:
			# Skip merge commits (they're associated with PRs)
			if len(commit.parents) > 1:
				continue

			# TODO: Could add logic here to verify commit isn't part of a PR
			# For now, we'll include all non-merge commits

			direct_commits.append(
				{"commit": commit, "files": commit.files if hasattr(commit, "files") else []}
			)

			print(
				f"  Found commit {commit.sha[:7]}: {commit.commit.message.split()[0] if commit.commit.message else 'No message'}"
			)

		return direct_commits

	def format_pr_data_for_prompt(self, pr_data: dict[str, Any]) -> str:
		"""Format PR data for the LLM prompt (reused from GitHub Action)."""
		pr = pr_data["pr"]
		commits = pr_data.get("commits", [])
		files = pr_data.get("files", [])

		formatted_data = {
			"title": pr.title,
			"description": pr.body or "",
			"author": pr.user.login,
			"number": pr.number,
			"merged_at": pr.merged_at.isoformat() if pr.merged_at else None,
			"commits": [
				{
					"sha": commit.sha[:7],
					"message": commit.commit.message,
					"author": commit.commit.author.name if commit.commit.author else "Unknown",
				}
				for commit in commits
			],
			"changed_files": [
				{
					"filename": file.filename,
					"changes": f"+{file.additions}/-{file.deletions}",
					"status": file.status,
				}
				for file in files
			],
		}

		return json.dumps(formatted_data, indent=2)

	def format_commit_data_for_prompt(self, commit_data: dict[str, Any]) -> str:
		"""Format direct commit data to look like PR data for the LLM."""
		commit = commit_data["commit"]
		files = commit_data.get("files", [])

		formatted_data = {
			"title": commit.commit.message.split("\n")[0]
			if commit.commit.message
			else "Direct commit",
			"description": "\n".join(commit.commit.message.split("\n")[1:]).strip()
			if commit.commit.message
			else "",
			"author": commit.commit.author.name if commit.commit.author else "Unknown",
			"number": f"commit-{commit.sha[:7]}",
			"merged_at": commit.commit.author.date.isoformat() if commit.commit.author else None,
			"commits": [
				{
					"sha": commit.sha[:7],
					"message": commit.commit.message or "",
					"author": commit.commit.author.name if commit.commit.author else "Unknown",
				}
			],
			"changed_files": [
				{
					"filename": file.filename,
					"changes": f"+{file.additions}/-{file.deletions}",
					"status": file.status,
				}
				for file in files
			],
		}

		return json.dumps(formatted_data, indent=2)

	def get_prompt_template(self) -> str:
		"""Load the prompt template (reused from GitHub Action logic)."""
		# Try to find the template from the GitHub Action
		action_template_path = (
			Path(__file__).parent.parent.parent
			/ "actions"
			/ "generate_changelog"
			/ "default-prompt.txt"
		)

		if action_template_path.exists():
			return action_template_path.read_text()

		# Fallback to inline template
		return """You are an expert at analyzing Pull Requests and generating changelog entries for non-technical end-users.
Analyze the following PR data and generate a user-friendly changelog entry that explains how the changes will benefit them.

<data>
PR Data:
{pr_data}
</data>

<instructions>
Use the pull request's commits, linked issues, description and related comments to generate the changelog. Focus on:
- Explaining changes in plain language that non-technical users will understand
- Describing new features in terms of what users can now do
- Explaining fixes in terms of problems that have been solved
- Mentioning any changes that users need to be aware of
- Document any actions that may be required by the users
- Being concise and strictly factual
- Avoiding technical jargon, implementation details, and developer-centric information
</instructions>

<formatting>
Ensure the following considerations for generating the changelog:
- Format your response as Github-flavoured markdown.
- Do NOT categorize changes by type (features/fixes/etc).
- Present information in a strictly factual manner ("This update adds...", "Fixed an issue where...")
- Use straightforward, neutral language without marketing or promotional tone
- Avoid subjective qualifiers like "improved", "enhanced", "better" unless quantifiable
- Present changes as factual prose statements without bulleted lists, subheadings or emojis
- Do NOT use phrases that use second-person language like "you will now be able to..."
- Do NOT include statements about "commitment to users", "ongoing efforts", or other company intentions
- Focus exclusively on what was changed, not why it matters or how it represents company values
- Avoid mentioning users' dependencies on features or making claims about user experience
</formatting>
"""

	def generate_changelog_entry(
		self, data: dict[str, Any], is_pr: bool = True
	) -> str | None:
		"""Generate a changelog entry using Anthropic's Claude."""
		try:
			prompt_template = self.get_prompt_template()

			if is_pr:
				formatted_data = self.format_pr_data_for_prompt(data)
			else:
				formatted_data = self.format_commit_data_for_prompt(data)

			prompt = prompt_template.format(pr_data=formatted_data)

			response = self.anthropic.messages.create(
				model=self.anthropic_model,
				max_tokens=self.max_tokens,
				temperature=self.temperature,
				messages=[{"role": "user", "content": prompt}],
			)

			return response.content[0].text

		except Exception as e:
			print(f"Error generating changelog entry: {e}")
			return None

	def generate_changelog_file(self, entries: list[ChangelogEntry], output_path: str):
		"""Generate the final CHANGELOG.md file."""
		# Sort entries by date (newest first)
		entries.sort(key=lambda x: x.date, reverse=True)

		content = ["# Changelog", ""]
		content.append(
			"This changelog was automatically generated from historical GitHub data."
		)
		content.append("")

		current_date = None
		for entry in entries:
			entry_date = entry.date.strftime("%Y-%m-%d")

			if entry_date != current_date:
				current_date = entry_date
				content.append(f"## {entry_date}")
				content.append("")

			# Add the changelog content
			content.append(entry.content)
			content.append("")
			content.append(f"_Source: {entry.source_type.upper()} {entry.source_id}_")
			content.append("")

		# Write to file
		Path(output_path).write_text("\n".join(content), encoding="utf-8")
		print(f"Generated changelog: {output_path}")


def main():
	"""Main entry point for the retrospective changelog generator."""
	parser = argparse.ArgumentParser(
		description="Generate retrospective changelog from GitHub data"
	)
	parser.add_argument("repo", help="Repository name in format owner/repo")
	parser.add_argument("--github-token", required=True, help="GitHub API token")
	parser.add_argument("--anthropic-api-key", required=True, help="Anthropic API key")
	parser.add_argument(
		"--branches", nargs="+", default=["main"], help="Branches to analyze (default: main)"
	)
	parser.add_argument("--since", help="Start date (ISO format: 2023-01-01)")
	parser.add_argument("--until", help="End date (ISO format: 2023-12-31)")
	parser.add_argument("--output", default="CHANGELOG.md", help="Output file path")
	parser.add_argument(
		"--include-direct-commits",
		action="store_true",
		help="Include direct commits (not just PRs)",
	)

	args = parser.parse_args()

	# Parse dates
	since_date = (
		datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
		if args.since
		else None
	)
	until_date = (
		datetime.fromisoformat(args.until).replace(tzinfo=timezone.utc)
		if args.until
		else None
	)

	# Initialize generator
	generator = RetrospectiveChangelogGenerator(
		github_token=args.github_token,
		anthropic_api_key=args.anthropic_api_key,
		repo_name=args.repo,
	)

	all_entries = []

	# Process each branch
	for branch in args.branches:
		print(f"Processing branch: {branch}")

		# Get merged PRs
		merged_prs = generator.get_merged_prs(branch, since_date, until_date)

		for pr_data in merged_prs:
			print(f"Generating changelog for PR #{pr_data['pr'].number}")
			content = generator.generate_changelog_entry(pr_data, is_pr=True)

			if content:
				entry = ChangelogEntry(
					date=pr_data["pr"].merged_at,
					content=content,
					source_type="pr",
					source_id=f"#{pr_data['pr'].number}",
				)
				all_entries.append(entry)

		# Get direct commits if requested
		if args.include_direct_commits:
			direct_commits = generator.get_direct_commits(branch, since_date, until_date)

			for commit_data in direct_commits:
				print(f"Generating changelog for commit {commit_data['commit'].sha[:7]}")
				content = generator.generate_changelog_entry(commit_data, is_pr=False)

				if content:
					entry = ChangelogEntry(
						date=commit_data["commit"].commit.author.date,
						content=content,
						source_type="commit",
						source_id=commit_data["commit"].sha[:7],
					)
					all_entries.append(entry)

	# Generate the changelog file
	if all_entries:
		generator.generate_changelog_file(all_entries, args.output)
		print(f"Successfully generated changelog with {len(all_entries)} entries")
	else:
		print("No entries found to generate changelog")


if __name__ == "__main__":
	main()
