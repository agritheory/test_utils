import argparse
import json
import os
import sys
import requests
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from anthropic import Anthropic
from github import Github, RateLimitExceededException
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

	def __init__(
		self,
		github_token: str,
		anthropic_api_key: str = None,
		repo_name: str = None,
		use_ollama: bool = False,
		ollama_model: str = None,
	):
		self.github = Github(github_token)
		self.use_ollama = use_ollama

		# Track GitHub rate limits
		self.check_github_rate_limit()

		if not use_ollama:
			if not anthropic_api_key:
				raise ValueError("Anthropic API key required when not using Ollama")
			self.anthropic = Anthropic(api_key=anthropic_api_key)
			self.anthropic_model = "claude-haiku-4-5"
			print(f"Using Anthropic API with model: {self.anthropic_model}")
		else:
			self.ollama_url = "http://localhost:11434/api/generate"
			self.ollama_model = ollama_model or "mistral:7b-instruct-q4_K_M"
			print(f"Using Ollama with model: {self.ollama_model}")

			# Test Ollama connection
			try:
				test_response = requests.get("http://localhost:11434/api/tags")
				if test_response.status_code != 200:
					print(
						f"Warning: Ollama may not be running properly. Status: {test_response.status_code}"
					)
				else:
					models = test_response.json().get("models", [])
					model_names = [m["name"] for m in models]
					print(f"Available Ollama models: {model_names}")
					if self.ollama_model not in model_names:
						print(
							f"WARNING: Model {self.ollama_model} not found. Please run: ollama pull {self.ollama_model}"
						)
			except Exception as e:
				print(f"Error connecting to Ollama: {e}")
				print("Make sure Ollama is running: ollama serve")
				sys.exit(1)

		self.repo = self.github.get_repo(repo_name)
		self.max_tokens = 1500
		self.temperature = 0.2

		# Timing and rate limit tracking
		self.request_times = []
		self.github_requests_made = 0
		self.llm_requests_made = 0

	def check_github_rate_limit(self):
		"""Check and display GitHub API rate limit status."""
		try:
			rate_limit = self.github.get_rate_limit()

			# Handle different PyGithub versions
			if hasattr(rate_limit, "core"):
				core = rate_limit.core
			elif hasattr(rate_limit, "rate"):
				core = rate_limit.rate
			else:
				# Try accessing it directly
				core = rate_limit

			print("\n=== GitHub Rate Limit Status ===")
			print(f"  Remaining: {core.remaining}/{core.limit}")
			print(f"  Resets at: {core.reset.strftime('%H:%M:%S %Z')}")

			if core.remaining < 100:
				print("  ⚠️  WARNING: Low rate limit remaining!")
				if core.remaining < 10:
					wait_time = (core.reset - datetime.now(timezone.utc)).total_seconds()
					if wait_time > 0:
						print(f"  ⏰ Rate limit nearly exhausted. Waiting {wait_time:.0f} seconds...")
						time.sleep(wait_time + 1)

			return core.remaining
		except Exception as e:
			# Silently continue if we can't check rate limit
			return None

	def wait_for_rate_limit(self, minimum_remaining: int = 10):
		"""Wait if rate limit is too low."""
		remaining = self.check_github_rate_limit()
		if remaining and remaining < minimum_remaining:
			try:
				rate_limit = self.github.get_rate_limit()
				if hasattr(rate_limit, "core"):
					reset_time = rate_limit.core.reset
				elif hasattr(rate_limit, "rate"):
					reset_time = rate_limit.rate.reset
				else:
					reset_time = rate_limit.reset

				wait_time = (reset_time - datetime.now(timezone.utc)).total_seconds()
				if wait_time > 0:
					print(f"⏰ Waiting {wait_time:.0f} seconds for rate limit reset...")
					time.sleep(wait_time + 1)
			except Exception:
				pass  # Continue if rate limit check fails

	def get_releases(
		self,
		since_date: datetime | None = None,
		until_date: datetime | None = None,
	) -> list[dict[str, Any]]:
		"""Get all releases within date range."""
		print("\nFetching releases...")
		start_time = time.time()

		releases = self.repo.get_releases()

		filtered_releases = []
		for release in releases:
			# Skip draft releases
			if release.draft:
				continue

			# Apply date filters
			if since_date and release.created_at < since_date:
				break  # Releases are ordered newest first
			if until_date and release.created_at > until_date:
				continue

			filtered_releases.append(
				{
					"release": release,
					"tag": release.tag_name,
					"name": release.name or release.tag_name,
					"body": release.body,
					"date": release.published_at or release.created_at,
					"created_at": release.created_at,
					"published_at": release.published_at,
					"is_prerelease": release.prerelease,
					"is_draft": release.draft,
					"target_commitish": release.target_commitish,
				}
			)

			print(f"  Found release: {release.tag_name} - {release.name or 'Unnamed'}")

		elapsed = time.time() - start_time
		print(f"  ✓ Fetched {len(filtered_releases)} releases in {elapsed:.1f} seconds")
		return filtered_releases

	def get_merged_prs(
		self,
		branch: str,
		since_date: datetime | None = None,
		until_date: datetime | None = None,
		limit: int | None = None,
	) -> list[dict[str, Any]]:
		"""Get all merged PRs for a branch within date range."""
		print(f"\nFetching merged PRs for branch: {branch}")
		if limit:
			print(f"  Limiting to {limit} PRs")

		start_time = time.time()

		# Check rate limit before starting
		self.wait_for_rate_limit(minimum_remaining=50)

		# Get closed PRs for the branch
		prs = self.repo.get_pulls(
			state="closed", base=branch, sort="updated", direction="desc"
		)

		merged_prs = []
		pr_count = 0

		for pr in prs:
			# Stop if we've hit the limit
			if limit and len(merged_prs) >= limit:
				print(f"  Reached limit of {limit} PRs")
				break

			# Rate limit check every 10 PRs
			if pr_count % 10 == 0 and pr_count > 0:
				print(f"  Processed {pr_count} PRs so far...")

			pr_count += 1

			# Skip if not merged
			if not pr.merged:
				continue

			# Apply date filters
			if since_date and pr.merged_at < since_date:
				continue
			if until_date and pr.merged_at > until_date:
				break  # Since sorted by updated desc, we can break here

			print(f"  Found PR #{pr.number}: {pr.title}")

			# Store minimal PR data first
			merged_prs.append(
				{
					"pr": pr,
					"commits": None,  # Fetch lazily later
					"files": None,  # Fetch lazily later
				}
			)

			# Stop if we've hit the limit after adding
			if limit and len(merged_prs) >= limit:
				print(f"  Reached limit of {limit} PRs")
				break

		elapsed = time.time() - start_time
		print(f"  ✓ Found {len(merged_prs)} merged PRs in {elapsed:.1f} seconds")

		# Now fetch details for the PRs we're actually processing
		print(f"\nFetching details for {len(merged_prs)} PRs...")
		for i, pr_data in enumerate(merged_prs, 1):
			try:
				print(
					f"  [{i}/{len(merged_prs)}] Fetching PR #{pr_data['pr'].number} details...",
					end="",
					flush=True,
				)
				# Add small delay between API calls to avoid hitting rate limits
				time.sleep(0.3)

				pr_data["commits"] = list(pr_data["pr"].get_commits())[:5]  # Limit commits
				pr_data["files"] = list(pr_data["pr"].get_files())[:20]  # Limit files

				print(" ✓")
				self.github_requests_made += 2  # Approximate API calls

			except RateLimitExceededException:
				print(" ⚠️  Rate limit exceeded")
				self.wait_for_rate_limit(minimum_remaining=50)
				# Retry this PR
				pr_data["commits"] = list(pr_data["pr"].get_commits())[:5]
				pr_data["files"] = list(pr_data["pr"].get_files())[:20]
			except Exception as e:
				print(f" ✗ Error: {e}")
				# Use empty lists if we can't fetch
				pr_data["commits"] = []
				pr_data["files"] = []

		return merged_prs

	def get_direct_commits(
		self,
		branch: str,
		since_date: datetime | None = None,
		until_date: datetime | None = None,
		limit: int | None = None,
	) -> list[dict[str, Any]]:
		"""Get commits that weren't part of PRs (direct pushes)."""
		print(f"\nFetching direct commits for branch: {branch}")
		start_time = time.time()

		commits = self.repo.get_commits(sha=branch, since=since_date, until=until_date)

		direct_commits = []
		commit_count = 0

		for commit in commits:
			if limit and commit_count >= limit:
				break

			# Skip merge commits (they're associated with PRs)
			if len(commit.parents) > 1:
				continue

			direct_commits.append(
				{"commit": commit, "files": commit.files if hasattr(commit, "files") else []}
			)

			print(
				f"  Found commit {commit.sha[:7]}: {commit.commit.message.split()[0] if commit.commit.message else 'No message'}"
			)
			commit_count += 1

		elapsed = time.time() - start_time
		print(f"  ✓ Fetched {len(direct_commits)} commits in {elapsed:.1f} seconds")
		return direct_commits

	def associate_prs_with_releases(
		self, merged_prs: list[dict[str, Any]], releases: list[dict[str, Any]]
	) -> dict[str, list[dict[str, Any]]]:
		"""Group PRs by their associated release based on merge dates."""

		# Sort releases by date (oldest first for easier comparison)
		sorted_releases = sorted(releases, key=lambda x: x["date"])

		# Create a dict to group PRs by release
		prs_by_release = {}
		for release in sorted_releases:
			prs_by_release[release["tag"]] = []
		prs_by_release["unreleased"] = []

		# Debug: Print release dates
		print("\nRelease timeline:")
		for r in sorted_releases:
			print(f"  {r['tag']}: {r['date'].strftime('%Y-%m-%d')}")

		# Assign each PR to the appropriate release
		for pr_data in merged_prs:
			pr_date = pr_data["pr"].merged_at
			pr_number = pr_data["pr"].number
			assigned = False

			# Find the first release that comes AFTER this PR was merged
			for i, release in enumerate(sorted_releases):
				if pr_date <= release["date"]:
					# This PR was merged before this release, so it belongs to this release
					prs_by_release[release["tag"]].append(pr_data)
					assigned = True
					print(f"  PR #{pr_number} ({pr_date.strftime('%Y-%m-%d')}) -> {release['tag']}")
					break

			# If not assigned to any release, it's unreleased (merged after latest release)
			if not assigned:
				prs_by_release["unreleased"].append(pr_data)
				print(f"  PR #{pr_number} ({pr_date.strftime('%Y-%m-%d')}) -> unreleased")

		# Sort PRs within each release by merge date (newest first)
		for tag in prs_by_release:
			prs_by_release[tag].sort(key=lambda x: x["pr"].merged_at, reverse=True)

		return prs_by_release

	def format_pr_data_for_prompt(self, pr_data: dict[str, Any]) -> str:
		"""Format PR data for the LLM prompt (reused from GitHub Action)."""
		pr = pr_data["pr"]
		commits = pr_data.get("commits", []) or []
		files = pr_data.get("files", []) or []

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
				for commit in commits[:5]  # Limit commits to reduce prompt size
			],
			"changed_files": [
				{
					"filename": file.filename,
					"changes": f"+{file.additions}/-{file.deletions}",
					"status": file.status,
				}
				for file in files[:20]  # Limit files to reduce prompt size
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
				for file in files[:20]
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
		return """You are an expert at analyzing code changes and generating user-friendly changelog entries.

	<data>
	{pr_data}
	</data>

	Generate a concise changelog entry (2-3 sentences max) that explains changes in plain language for non-technical users.

	Focus on:
	- What users can now do (new features)
	- What problems were fixed
	- Important changes users should know

	Rules:
	- NO technical jargon or implementation details
	- NO marketing language or subjective qualifiers
	- NO lists
	- Use professional prose without hyperbole
	- Use factual statements only
	- Be extremely concise
	- Do NOT include any prefixes like "Changelog Entry:" or headers
	- Start directly with the description of changes
	- Do not include call to action or create other closing statements

	Output ONLY the changelog text itself, with no emojis, labels or prefixes."""

	def generate_changelog_entry(
		self, data: dict[str, Any], is_pr: bool = True
	) -> str | None:
		"""Generate a changelog entry using Anthropic's Claude or Ollama."""
		start_time = time.time()

		try:
			prompt_template = self.get_prompt_template()

			if is_pr:
				formatted_data = self.format_pr_data_for_prompt(data)
				item_id = f"PR #{data['pr'].number}"
			else:
				formatted_data = self.format_commit_data_for_prompt(data)
				item_id = f"commit {data['commit'].sha[:7]}"

			prompt = prompt_template.format(pr_data=formatted_data)

			if self.use_ollama:
				print(f"  Generating with Ollama ({self.ollama_model})...", end="", flush=True)

				response = requests.post(
					self.ollama_url,
					json={
						"model": self.ollama_model,
						"prompt": prompt,
						"stream": False,
						"options": {
							"temperature": self.temperature,
							"num_predict": 500,
							"num_ctx": 4096,
						},
					},
					timeout=300,
				)

				elapsed = time.time() - start_time

				if response.status_code == 200:
					result = response.json()
					print(f" ✓ ({elapsed:.1f}s)")
					self.llm_requests_made += 1

					# Clean up the response - remove common prefixes
					response_text = result.get("response", "")

					# Remove common prefixes that models might add
					prefixes_to_remove = [
						"Changelog Entry:",
						"**Changelog Entry:**",
						"Changelog entry:",
						"**Changelog entry:**",
						"## Changelog Entry",
						"### Changelog Entry",
					]

					for prefix in prefixes_to_remove:
						if response_text.strip().startswith(prefix):
							response_text = response_text[len(prefix) :].strip()
							break

					return response_text
				else:
					print(f" ✗ Error: {response.status_code}")
					print(f"  Response: {response.text}")
					return None
			else:
				# Similar cleanup for Anthropic responses
				print("  Generating with Anthropic...", end="", flush=True)
				response = self.anthropic.messages.create(
					model=self.anthropic_model,
					max_tokens=500,
					temperature=self.temperature,
					messages=[{"role": "user", "content": prompt}],
				)
				elapsed = time.time() - start_time
				print(f" ✓ ({elapsed:.1f}s)")
				self.llm_requests_made += 1

				response_text = response.content[0].text

				# Clean up prefixes
				prefixes_to_remove = [
					"Changelog Entry:",
					"**Changelog Entry:**",
					"Changelog entry:",
					"**Changelog entry:**",
					"## Changelog Entry",
					"### Changelog Entry",
				]

				for prefix in prefixes_to_remove:
					if response_text.strip().startswith(prefix):
						response_text = response_text[len(prefix) :].strip()
						break

				return response_text

		except requests.exceptions.ReadTimeout:
			elapsed = time.time() - start_time
			print(f" ✗ Timeout after {elapsed:.1f}s")
			return None
		except Exception as e:
			elapsed = time.time() - start_time
			print(f" ✗ Error after {elapsed:.1f}s: {e}")
			return None

	def generate_changelog_with_releases(
		self,
		prs_by_release: dict[str, list[dict[str, Any]]],
		releases: list[dict[str, Any]],
		output_path: str,
		use_release_notes: bool = True,
	):
		"""Generate changelog organized by releases."""
		content = ["# Changelog", ""]
		content.append(
			"This changelog was automatically generated from GitHub releases and pull requests."
		)
		content.append("")

		# Create a release lookup dict
		release_dict = {r["tag"]: r for r in releases}

		# Sort releases by date (newest first)
		sorted_releases = sorted(releases, key=lambda x: x["date"], reverse=True)

		# Start with unreleased if it has content
		if prs_by_release.get("unreleased"):
			content.append("## Unreleased")
			content.append("")

			for pr_data in prs_by_release["unreleased"]:
				if "changelog_entry" in pr_data and pr_data["changelog_entry"]:
					content.append(pr_data["changelog_entry"])
				else:
					content.append(f"- PR #{pr_data['pr'].number}: {pr_data['pr'].title}")
				content.append("")

		# Add each release
		for release in sorted_releases:
			tag = release["tag"]
			prs = prs_by_release.get(tag, [])

			# Skip empty releases unless they have release notes
			if not prs and not (use_release_notes and release["body"]):
				continue

			# Add release header
			date_str = release["date"].strftime("%Y-%m-%d")
			release_name = release["name"] or tag

			content.append(f"## [{tag}] - {date_str}")
			if release_name != tag:
				content.append(f"**{release_name}**")
			content.append("")

			# Include release notes if available and enabled
			if use_release_notes and release["body"]:
				content.append("### Release Notes")
				content.append("")
				content.append(release["body"])
				content.append("")

				if prs:  # Only add PR section if there are PRs
					content.append("### Changes from Pull Requests")
					content.append("")

			# Add PR changelogs
			for pr_data in prs:
				if "changelog_entry" in pr_data and pr_data["changelog_entry"]:
					content.append(pr_data["changelog_entry"])
				else:
					content.append(f"- PR #{pr_data['pr'].number}: {pr_data['pr'].title}")
				content.append(f"  _Source: PR #{pr_data['pr'].number}_")
				content.append("")

		# Write to file
		Path(output_path).parent.mkdir(parents=True, exist_ok=True)
		Path(output_path).write_text("\n".join(content), encoding="utf-8")
		print(f"\n✓ Generated changelog: {output_path}")

	def generate_changelog_file(self, entries: list[ChangelogEntry], output_path: str):
		"""Generate the final CHANGELOG.md file (date-based, legacy format)."""
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
		Path(output_path).parent.mkdir(parents=True, exist_ok=True)
		Path(output_path).write_text("\n".join(content), encoding="utf-8")
		print(f"\n✓ Generated changelog: {output_path}")

	def print_summary(self, start_time: float, entries_count: int):
		"""Print execution summary."""
		elapsed = time.time() - start_time

		print("\n" + "=" * 50)
		print("EXECUTION SUMMARY")
		print("=" * 50)
		print(f"Total time: {elapsed:.1f} seconds")
		print(f"Entries generated: {entries_count}")
		print(f"GitHub API calls: ~{self.github_requests_made}")
		print(f"LLM calls: {self.llm_requests_made}")

		if self.llm_requests_made > 0:
			avg_time = elapsed / self.llm_requests_made
			print(f"Average time per changelog: {avg_time:.1f}s")

		# Final rate limit check
		self.check_github_rate_limit()


def main():
	"""Main entry point for the retrospective changelog generator."""
	parser = argparse.ArgumentParser(
		description="Generate retrospective changelog from GitHub data"
	)
	parser.add_argument("repo", help="Repository name in format owner/repo")
	parser.add_argument("--github-token", required=True, help="GitHub API token")
	parser.add_argument(
		"--anthropic-api-key", help="Anthropic API key (not required if using Ollama)"
	)
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
	parser.add_argument(
		"--use-ollama", action="store_true", help="Use local Ollama instead of Anthropic API"
	)
	parser.add_argument(
		"--ollama-model",
		default="mistral:7b-instruct-q4_K_M",
		help="Ollama model to use (default: mistral:7b-instruct-q4_K_M)",
	)
	parser.add_argument(
		"--limit", type=int, help="Limit number of PRs to process (useful for testing)"
	)
	parser.add_argument(
		"--use-releases",
		action="store_true",
		default=True,
		help="Organize changelog by GitHub releases",
	)
	parser.add_argument(
		"--include-release-notes",
		action="store_true",
		default=True,
		help="Include GitHub release notes in changelog (default: True)",
	)
	parser.add_argument(
		"--no-release-notes",
		action="store_false",
		dest="include_release_notes",
		help="Exclude GitHub release notes from changelog",
	)

	args = parser.parse_args()

	# Validate API key requirement
	if not args.use_ollama and not args.anthropic_api_key:
		parser.error("--anthropic-api-key is required when not using --use-ollama")

	print(f"Starting changelog generation for {args.repo}")
	print(f"Using {'Ollama' if args.use_ollama else 'Anthropic API'}")
	if args.use_releases:
		print("Organizing by GitHub releases")

	overall_start = time.time()

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

	try:
		# Initialize generator
		generator = RetrospectiveChangelogGenerator(
			github_token=args.github_token,
			anthropic_api_key=args.anthropic_api_key,
			repo_name=args.repo,
			use_ollama=args.use_ollama,
			ollama_model=args.ollama_model if args.use_ollama else None,
		)

		all_entries = []

		# Process each branch
		for branch in args.branches:
			print(f"\nProcessing branch: {branch}")

			# Get merged PRs - pass limit here
			merged_prs = generator.get_merged_prs(
				branch, since_date, until_date, limit=args.limit
			)

			# Handle release-based organization
			if args.use_releases:
				# Fetch releases
				releases = generator.get_releases(since_date, until_date)

				if releases:
					print(f"\nOrganizing {len(merged_prs)} PRs into {len(releases)} releases...")
					prs_by_release = generator.associate_prs_with_releases(merged_prs, releases)

					# Print summary of organization
					for tag, prs in prs_by_release.items():
						if prs:
							print(f"  {tag}: {len(prs)} PRs")

					# Generate changelog for each PR
					print("\nGenerating changelog entries...")
					total_prs = sum(len(prs) for prs in prs_by_release.values())
					pr_count = 0

					for tag, prs in prs_by_release.items():
						for pr_data in prs:
							pr_count += 1
							print(f"\n[{pr_count}/{total_prs}] PR #{pr_data['pr'].number} (Release: {tag})")
							content = generator.generate_changelog_entry(pr_data, is_pr=True)
							if content:
								pr_data["changelog_entry"] = content

					# Generate the release-based changelog
					generator.generate_changelog_with_releases(
						prs_by_release,
						releases,
						args.output,
						use_release_notes=args.include_release_notes,
					)

					# Count entries for summary
					all_entries_count = sum(
						len([pr for pr in prs if "changelog_entry" in pr and pr["changelog_entry"]])
						for prs in prs_by_release.values()
					)
					generator.print_summary(overall_start, all_entries_count)
					continue  # Skip date-based generation
				else:
					print("No releases found, falling back to date-based changelog")

			# Traditional date-based changelog (fallback or default)
			print(f"\nGenerating {len(merged_prs)} changelog entries...")

			for i, pr_data in enumerate(merged_prs, 1):
				print(f"\n[{i}/{len(merged_prs)}] PR #{pr_data['pr'].number}")
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
				direct_commits = generator.get_direct_commits(
					branch, since_date, until_date, limit=args.limit
				)

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

		# Generate the date-based changelog file (if not using releases)
		if not args.use_releases and all_entries:
			generator.generate_changelog_file(all_entries, args.output)
			generator.print_summary(overall_start, len(all_entries))
		elif not args.use_releases:
			print("No entries found to generate changelog")

	except Exception as e:
		print(f"\nFatal error: {e}")
		import traceback

		traceback.print_exc()
		sys.exit(1)


if __name__ == "__main__":
	main()
