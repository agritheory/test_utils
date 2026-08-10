#!/usr/bin/env python3
"""Register and submit Frappe Cloud Marketplace app releases (update-only, no bench deploy)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

import requests


class FrappeCloudError(Exception):
	pass


@dataclass(frozen=True)
class AppSourceMatch:
	name: str
	app: str
	branch: str
	repository_owner: str
	repository: str


@dataclass(frozen=True)
class PublishResult:
	skipped: bool
	reason: str
	app: str | None = None
	source: str | None = None
	release: str | None = None
	release_status: str | None = None
	approval_submitted: bool = False


def parse_repository(repository: str) -> tuple[str, str]:
	repository = repository.strip().strip("/")
	if "/" not in repository:
		raise FrappeCloudError(
			f"Invalid repository slug: {repository!r} (expected owner/repo)"
		)
	owner, repo = repository.split("/", 1)
	if not owner or not repo:
		raise FrappeCloudError(f"Invalid repository slug: {repository!r}")
	return owner, repo.removesuffix(".git")


def pick_app_source(
	sources: list[dict[str, Any]],
	app_name: str | None,
) -> AppSourceMatch:
	if not sources:
		raise FrappeCloudError("No App Source records provided")

	if app_name:
		matches = [s for s in sources if s.get("app") == app_name]
		if not matches:
			available = ", ".join(sorted({s.get("app", "") for s in sources}))
			raise FrappeCloudError(
				f"No App Source for app {app_name!r} (available: {available})"
			)
		if len(matches) > 1:
			raise FrappeCloudError(
				f"Multiple App Sources for app {app_name!r}; set app-name explicitly"
			)
		source = matches[0]
	elif len(sources) == 1:
		source = sources[0]
	else:
		apps = ", ".join(sorted({s.get("app", "") for s in sources}))
		raise FrappeCloudError(
			f"Multiple App Sources match this repo/branch ({apps}); set app-name input"
		)

	return AppSourceMatch(
		name=source["name"],
		app=source["app"],
		branch=source.get("branch") or "",
		repository_owner=source.get("repository_owner") or "",
		repository=source.get("repository") or "",
	)


class FrappeCloudClient:
	def __init__(
		self,
		base_url: str,
		api_key: str,
		api_secret: str,
		team: str | None = None,
	) -> None:
		self.base_url = base_url.rstrip("/")
		if self.base_url.rstrip("/") == "https://frappecloud.com":
			raise FrappeCloudError(
				"Use https://cloud.frappe.io as fc-base-url. https://frappecloud.com "
				"redirects and strips Authorization headers, which breaks API token auth."
			)
		self.team = team.strip() if team else None
		self.session = requests.Session()
		self.session.headers.update(
			{
				"Authorization": f"token {api_key}:{api_secret}",
				"Content-Type": "application/json",
				"Accept": "application/json",
			}
		)
		if self.team:
			self.session.headers["X-Press-Team"] = self.team

	def list_teams(self) -> list[dict[str, str]]:
		account = self.call_method("press.api.account.get")
		if not isinstance(account, dict):
			return []

		teams: list[dict[str, str]] = []
		for team in account.get("teams") or []:
			if isinstance(team, dict):
				teams.append(
					{
						"name": team.get("name") or "",
						"title": team.get("team_title") or team.get("name") or "",
					}
				)
			elif isinstance(team, str):
				teams.append({"name": team, "title": team})
		return [team for team in teams if team["name"]]

	def ensure_team(self) -> str:
		if self.team:
			return self.team

		teams = self.list_teams()
		if len(teams) == 1:
			self.team = teams[0]["name"]
			self.session.headers["X-Press-Team"] = self.team
			return self.team

		if len(teams) > 1:
			options = ", ".join(
				f"{team['name']} ({team['title']})"
				if team["title"] != team["name"]
				else team["name"]
				for team in teams
			)
			raise FrappeCloudError(
				"Multiple Frappe Cloud teams available. Set FC_TEAM or pass --fc-team. "
				f"Options: {options}. Run with --list-teams for details."
			)

		me = self.call_method("press.api.account.me")
		if not isinstance(me, dict) or not me.get("team"):
			raise FrappeCloudError(
				"Could not resolve Frappe Cloud team; set FC_TEAM or pass --fc-team"
			)

		self.team = me["team"]
		self.session.headers["X-Press-Team"] = self.team
		return self.team

	def call_method(self, method: str, data: dict[str, Any] | None = None) -> Any:
		url = f"{self.base_url}/api/method/{method}"
		try:
			response = self.session.post(
				url, json=data or {}, timeout=120, allow_redirects=False
			)
		except requests.RequestException as exc:
			raise FrappeCloudError(f"Network error calling {method}: {exc}") from exc

		if response.status_code in (301, 302, 303, 307, 308):
			location = response.headers.get("Location", "")
			raise FrappeCloudError(
				f"Unexpected redirect from {url} to {location}. "
				"Use https://cloud.frappe.io as fc-base-url."
			)

		if not response.ok:
			raise FrappeCloudError(
				f"HTTP {response.status_code} calling {method}: {response.text[:500]}"
			)

		try:
			payload = response.json()
		except json.JSONDecodeError as exc:
			raise FrappeCloudError(f"Invalid JSON from {method}: {response.text[:500]}") from exc

		if payload.get("exc_type") or payload.get("exception"):
			message = payload.get("message")
			if not message and payload.get("_server_messages"):
				try:
					message = json.loads(payload["_server_messages"])[0]
					if isinstance(message, str):
						message = json.loads(message).get("message", message)
				except (json.JSONDecodeError, IndexError, TypeError):
					message = payload["_server_messages"]
			raise FrappeCloudError(message or str(payload))
		if "message" in payload:
			return payload["message"]
		return payload

	def verify_auth(self) -> dict[str, Any]:
		me = self.call_method("press.api.account.me")
		if not isinstance(me, dict) or not me.get("user") or me.get("user") == "Guest":
			raise FrappeCloudError(
				"Frappe Cloud API authentication failed. Regenerate API keys from "
				"Settings → Profile & Team → API Access."
			)
		team = self.ensure_team()
		return {"user": me["user"], "team": team}

	def find_app_sources(
		self,
		repository_owner: str,
		repository: str,
		branch: str,
	) -> list[dict[str, Any]]:
		self.ensure_team()
		apps = self.call_method("press.api.marketplace.get_apps")
		if not isinstance(apps, list):
			return []

		matches: list[dict[str, Any]] = []
		for app_summary in apps:
			marketplace_app_name = app_summary.get("name")
			if not marketplace_app_name:
				continue

			app_detail = self.call_method(
				"press.api.marketplace.get_app",
				{"name": marketplace_app_name},
			)
			if not isinstance(app_detail, dict):
				continue

			for source_row in app_detail.get("sources") or []:
				source_info = source_row.get("source_information") or {}
				if (
					source_info.get("repository_owner") == repository_owner
					and source_info.get("repository") == repository
					and source_info.get("branch") == branch
					and source_info.get("enabled")
				):
					matches.append(
						{
							"name": source_info.get("name"),
							"app": source_info.get("app"),
							"branch": source_info.get("branch"),
							"repository_owner": source_info.get("repository_owner"),
							"repository": source_info.get("repository"),
							"marketplace_app": marketplace_app_name,
							"marketplace_app_status": app_detail.get("status"),
						}
					)

		return [match for match in matches if match.get("name") and match.get("app")]

	def find_marketplace_app(
		self, app: str, source_match: dict[str, Any]
	) -> dict[str, Any] | None:
		marketplace_app_name = source_match.get("marketplace_app")
		if not marketplace_app_name:
			return None

		return {
			"name": marketplace_app_name,
			"app": app,
			"status": source_match.get("marketplace_app_status"),
		}

	def create_release(self, source_name: str, commit_hash: str | None = None) -> Any:
		self.ensure_team()
		args: dict[str, Any] = {"force": True}
		if commit_hash:
			args["commit_hash"] = commit_hash
		return self.call_method(
			"press.api.client.run_doc_method",
			{
				"dt": "App Source",
				"dn": source_name,
				"method": "create_release",
				"args": args,
			},
		)

	def latest_release(self, source_name: str) -> dict[str, Any] | None:
		releases = self.call_method(
			"press.api.marketplace.releases",
			{
				"filters": {"source": source_name},
				"limit_page_length": 1,
				"order_by": "creation desc",
			},
		)
		if isinstance(releases, list) and releases:
			return releases[0]
		return None

	def submit_for_approval(self, marketplace_app: str, app_release: str) -> None:
		self.call_method(
			"press.api.marketplace.create_approval_request",
			{"name": marketplace_app, "app_release": app_release},
		)


def publish_marketplace_release(
	client: FrappeCloudClient,
	repository: str,
	branch: str,
	app_name: str | None = None,
	commit_hash: str | None = None,
	dry_run: bool = False,
) -> PublishResult:
	owner, repo = parse_repository(repository)
	sources = client.find_app_sources(owner, repo, branch)
	if not sources:
		return PublishResult(
			skipped=True,
			reason=f"No enabled App Source on Frappe Cloud for {owner}/{repo} @ {branch}",
		)

	source = pick_app_source(sources, app_name)
	source_row = next(match for match in sources if match["name"] == source.name)
	marketplace_app = client.find_marketplace_app(source.app, source_row)
	if not marketplace_app:
		return PublishResult(
			skipped=True,
			reason=f"No Marketplace App on Frappe Cloud for app {source.app!r}",
			app=source.app,
			source=source.name,
		)

	if dry_run:
		return PublishResult(
			skipped=False,
			reason="dry-run",
			app=source.app,
			source=source.name,
			release_status="(dry-run)",
		)

	try:
		client.create_release(source.name, commit_hash=commit_hash)
	except FrappeCloudError as exc:
		print(
			f"Note: could not force-poll App Source ({exc}); "
			"relying on GitHub webhook to create the App Release",
			file=sys.stderr,
		)

	release = client.latest_release(source.name)
	if not release:
		raise FrappeCloudError(
			f"No App Release found for source {source.name}. "
			"Confirm the GitHub App/webhook is connected on Frappe Cloud and retry."
		)

	release_name = release.get("name")
	release_status = release.get("status")
	approval_submitted = False

	if release_status == "Draft":
		try:
			client.submit_for_approval(marketplace_app["name"], release_name)
			approval_submitted = True
			release = client.latest_release(source.name) or release
			release_status = release.get("status", release_status)
		except FrappeCloudError as exc:
			message = str(exc).lower()
			if "active request" in message or "already exists" in message:
				approval_submitted = False
			else:
				raise

	return PublishResult(
		skipped=False,
		reason="published",
		app=source.app,
		source=source.name,
		release=release_name,
		release_status=release_status,
		approval_submitted=approval_submitted,
	)


def write_step_summary(result: PublishResult, repository: str, branch: str) -> None:
	summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
	if not summary_path:
		return

	lines = [
		"## Frappe Cloud Marketplace Publish",
		"",
		f"- **Repository:** `{repository}`",
		f"- **Branch:** `{branch}`",
	]
	if result.skipped:
		lines.append(f"- **Result:** skipped — {result.reason}")
	else:
		lines.extend(
			[
				f"- **App:** `{result.app}`",
				f"- **App Source:** `{result.source}`",
				f"- **Release:** `{result.release}`",
				f"- **Status:** `{result.release_status}`",
				f"- **Approval submitted:** `{result.approval_submitted}`",
			]
		)
	lines.append("")

	with open(summary_path, "a", encoding="utf-8") as handle:
		handle.write("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--repository", help="GitHub owner/repo")
	parser.add_argument("--branch", help="Publish branch (App Source.branch)")
	parser.add_argument("--app-name", default="", help="FC app name override")
	parser.add_argument("--fc-base-url", default="https://cloud.frappe.io")
	parser.add_argument("--fc-api-key", default=os.environ.get("FC_API_KEY", ""))
	parser.add_argument("--fc-api-secret", default=os.environ.get("FC_API_SECRET", ""))
	parser.add_argument(
		"--fc-team",
		default=os.environ.get("FC_TEAM", ""),
		help="Frappe Cloud team name (required when user belongs to multiple teams)",
	)
	parser.add_argument(
		"--list-teams",
		action="store_true",
		help="List Frappe Cloud teams for this API key and exit",
	)
	parser.add_argument("--commit-hash", default="", help="Optional commit hash pin")
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Resolve FC config only; do not create release or submit approval",
	)
	args = parser.parse_args(argv)

	if not args.list_teams and (not args.repository or not args.branch):
		parser.error("--repository and --branch are required unless using --list-teams")

	if not args.fc_api_key or not args.fc_api_secret:
		print("FC_API_KEY and FC_API_SECRET are required", file=sys.stderr)
		return 1

	client = FrappeCloudClient(
		args.fc_base_url,
		args.fc_api_key,
		args.fc_api_secret,
		team=args.fc_team.strip() or None,
	)
	app_name = args.app_name.strip() or None
	commit_hash = args.commit_hash.strip() or None

	try:
		if args.list_teams:
			me = client.call_method("press.api.account.me")
			user = me.get("user") if isinstance(me, dict) else me
			print(f"Authenticated as {user}")
			for team in client.list_teams():
				default = ""
				if isinstance(me, dict) and me.get("team") == team["name"]:
					default = " (current default)"
				title = team["title"]
				if title and title != team["name"]:
					print(f"- {team['name']} — {title}{default}")
				else:
					print(f"- {team['name']}{default}")
			return 0

		auth = client.verify_auth()
		print(f"Authenticated as {auth['user']} (team {auth['team']})")
		result = publish_marketplace_release(
			client,
			repository=args.repository,
			branch=args.branch,
			app_name=app_name,
			commit_hash=commit_hash,
			dry_run=args.dry_run,
		)
	except FrappeCloudError as exc:
		print(f"::error::{exc}", file=sys.stderr)
		return 1

	write_step_summary(result, args.repository, args.branch)

	if result.skipped:
		print(result.reason)
		return 0

	if args.dry_run:
		print(
			f"dry-run: would publish {result.app} from source {result.source} "
			f"({args.repository} @ {args.branch})"
		)
		return 0

	print(
		f"Published {result.app}: release {result.release} ({result.release_status})"
		+ ("; approval submitted" if result.approval_submitted else "")
	)
	return 0


if __name__ == "__main__":
	sys.exit(main())
