#!/usr/bin/env python
import os
import sys
import json
import boto3
from anthropic import Anthropic
from botocore.exceptions import ClientError, NoCredentialsError
from github import Github, Auth
from openai import OpenAI, AuthenticationError, RateLimitError


PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_OPENAI = "openai"
PROVIDER_BEDROCK = "bedrock"
SUPPORTED_PROVIDERS = (PROVIDER_ANTHROPIC, PROVIDER_OPENAI, PROVIDER_BEDROCK)


def get_env_vars():
	"""Get all required environment variables in one function."""
	provider = os.environ.get("LLM_PROVIDER", PROVIDER_ANTHROPIC).lower()

	env_vars = {
		"github_token": os.environ.get("GITHUB_TOKEN"),
		"llm_provider": provider,
		# Anthropic
		"anthropic_api_key": os.environ.get("ANTHROPIC_API_KEY"),
		"anthropic_model": os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5"),
		# OpenAI
		"openai_api_key": os.environ.get("OPENAI_API_KEY"),
		"openai_model": os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
		# Amazon Bedrock
		"aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
		"aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
		"aws_region": os.environ.get("AWS_REGION", "us-east-1"),
		"bedrock_model": os.environ.get("BEDROCK_MODEL", "amazon.nova-lite-v1:0"),
		# Shared
		"prompt_template_path": os.environ.get("PROMPT_TEMPLATE_PATH"),
		"comment_header": os.environ.get("COMMENT_HEADER", "## Draft Changelog Entry"),
		"repo_full_name": os.environ.get("REPO_FULL_NAME"),
		"pr_number": os.environ.get("PR_NUMBER"),
		"max_tokens": int(os.environ.get("MAX_TOKENS", 1500)),
		"temperature": float(os.environ.get("TEMPERATURE", 0.2)),
		"is_regeneration": os.environ.get("IS_REGENERATION", "false").lower() == "true",
	}

	missing_vars = []
	if not env_vars["github_token"]:
		missing_vars.append("GITHUB_TOKEN")

	if provider not in SUPPORTED_PROVIDERS:
		print(
			f"Error: Unsupported LLM_PROVIDER '{provider}'. Must be one of: {', '.join(SUPPORTED_PROVIDERS)}"
		)
		sys.exit(1)

	if provider == PROVIDER_ANTHROPIC and not env_vars["anthropic_api_key"]:
		missing_vars.append("ANTHROPIC_API_KEY")
	elif provider == PROVIDER_OPENAI and not env_vars["openai_api_key"]:
		missing_vars.append("OPENAI_API_KEY")
	elif provider == PROVIDER_BEDROCK:
		if not env_vars["aws_access_key_id"]:
			missing_vars.append("AWS_ACCESS_KEY_ID")
		if not env_vars["aws_secret_access_key"]:
			missing_vars.append("AWS_SECRET_ACCESS_KEY")

	if missing_vars:
		print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
		sys.exit(1)

	return env_vars


def get_pr_data(env_vars):
	"""Fetch PR data including commits, description, and comments using PyGithub."""
	try:
		g = Github(auth=Auth.Token(env_vars["github_token"]))
		repo = g.get_repo(env_vars["repo_full_name"])
		pr = repo.get_pull(int(env_vars["pr_number"]))

		commits = list(pr.get_commits())
		comments = list(pr.get_comments())
		review_comments = list(pr.get_review_comments())
		files = list(pr.get_files())
		issue = repo.get_issue(int(env_vars["pr_number"]))
		issue_comments = list(issue.get_comments())

		existing_changelog_comment = None
		for comment in issue_comments:
			if env_vars["comment_header"] in comment.body:
				existing_changelog_comment = comment
				break

		return {
			"pr": pr,
			"commits": commits,
			"comments": comments + review_comments,
			"issue": issue,
			"files": files,
			"existing_changelog_comment": existing_changelog_comment,
		}
	except Exception as e:
		print(f"Error fetching PR data: {e}")
		sys.exit(1)


def get_custom_prompt_template(env_vars):
	"""Load custom prompt template if provided, otherwise use the default template file."""
	try:
		if env_vars["prompt_template_path"] and os.path.exists(
			env_vars["prompt_template_path"]
		):
			with open(env_vars["prompt_template_path"]) as f:
				return f.read()
	except Exception as e:
		print(f"Warning: Could not load custom prompt template: {e}")

	default_prompt_path = os.path.join(os.path.dirname(__file__), "default-prompt.txt")
	try:
		with open(default_prompt_path) as f:
			return f.read()
	except Exception as e:
		print(f"Error: Could not load default prompt template: {e}")
		return """Analyze the PR and generate a changelog entry.

		PR Data:
		{pr_data}
		"""


def format_pr_data_for_prompt(pr_data):
	"""Format the PR data for inclusion in the prompt."""
	pr = pr_data["pr"]
	commits = pr_data["commits"]
	files = pr_data["files"]

	formatted_data = {
		"title": pr.title,
		"description": pr.body,
		"author": pr.user.login,
		"commits": [
			{
				"sha": commit.sha[:7],
				"message": commit.commit.message,
				"author": commit.commit.author.name,
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


def generate_changelog_with_anthropic(pr_data, env_vars):
	"""Generate a changelog entry using Anthropic's Claude."""
	client = Anthropic(api_key=env_vars["anthropic_api_key"])

	prompt_template = get_custom_prompt_template(env_vars)
	formatted_pr_data = format_pr_data_for_prompt(pr_data)
	prompt = prompt_template.format(pr_data=formatted_pr_data)

	try:
		response = client.messages.create(
			model=env_vars["anthropic_model"],
			max_tokens=env_vars["max_tokens"],
			temperature=env_vars["temperature"],
			messages=[{"role": "user", "content": prompt}],
		)

		return response.content[0].text
	except Exception as e:
		error_type = None
		error_msg = str(e).lower()

		if hasattr(e, "type"):
			error_type = e.type
		elif "invalid_request_error" in error_msg:
			error_type = "invalid_request_error"
		elif "authentication_error" in error_msg:
			error_type = "authentication_error"
		elif "permission_error" in error_msg:
			error_type = "permission_error"
		elif "not_found_error" in error_msg:
			error_type = "not_found_error"
		elif "rate_limit_error" in error_msg:
			error_type = "rate_limit_error"
		elif "api_error" in error_msg:
			error_type = "api_error"

		if error_type == "invalid_request_error" and "credit balance is too low" in error_msg:
			print("ERROR: Anthropic API account has insufficient credits.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the Anthropic API account has insufficient credits.\n\n"
				"Please check your Anthropic account billing status and ensure you have available credits."
			)
			post_error_comment(comment, pr_data, env_vars)
		elif error_type == "rate_limit_error":
			print("ERROR: Anthropic API rate limit exceeded.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the Anthropic API rate limit was exceeded.\n\n"
				"Please try again later. See https://docs.anthropic.com/en/api/rate-limits for more information."
			)
			post_error_comment(comment, pr_data, env_vars)
		elif error_type == "authentication_error":
			print("ERROR: Invalid Anthropic API key.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the Anthropic API key is invalid or has expired.\n\n"
				"Please check your API key configuration."
			)
			post_error_comment(comment, pr_data, env_vars)
		else:
			print(f"Error generating changelog with Anthropic: {e}")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated due to an error with the Anthropic API.\n\n"
				f"Error details: {str(e)}"
			)
			post_error_comment(comment, pr_data, env_vars)
		return None


def generate_changelog_with_openai(pr_data, env_vars):
	"""Generate a changelog entry using OpenAI's API."""
	client = OpenAI(api_key=env_vars["openai_api_key"])

	prompt_template = get_custom_prompt_template(env_vars)
	formatted_pr_data = format_pr_data_for_prompt(pr_data)
	prompt = prompt_template.format(pr_data=formatted_pr_data)

	try:
		response = client.chat.completions.create(
			model=env_vars["openai_model"],
			max_tokens=env_vars["max_tokens"],
			temperature=env_vars["temperature"],
			messages=[{"role": "user", "content": prompt}],
		)

		return response.choices[0].message.content
	except AuthenticationError as e:
		print("ERROR: Invalid OpenAI API key.")
		comment = (
			"## Changelog Generation Error\n\n"
			"The changelog could not be generated because the OpenAI API key is invalid or has expired.\n\n"
			"Please check your API key configuration."
		)
		post_error_comment(comment, pr_data, env_vars)
	except RateLimitError as e:
		error_msg = str(e).lower()
		if "insufficient_quota" in error_msg or "quota" in error_msg:
			print("ERROR: OpenAI API account has insufficient quota.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the OpenAI API account has insufficient quota.\n\n"
				"Please check your OpenAI account billing status and ensure you have available credits."
			)
		else:
			print("ERROR: OpenAI API rate limit exceeded.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the OpenAI API rate limit was exceeded.\n\n"
				"Please try again later. See https://platform.openai.com/docs/guides/rate-limits for more information."
			)
		post_error_comment(comment, pr_data, env_vars)
	except Exception as e:
		print(f"Error generating changelog with OpenAI: {e}")
		comment = (
			"## Changelog Generation Error\n\n"
			"The changelog could not be generated due to an error with the OpenAI API.\n\n"
			f"Error details: {str(e)}"
		)
		post_error_comment(comment, pr_data, env_vars)
	return None


def generate_changelog_with_bedrock(pr_data, env_vars):
	"""Generate a changelog entry using Amazon Bedrock's Converse API."""
	prompt_template = get_custom_prompt_template(env_vars)
	formatted_pr_data = format_pr_data_for_prompt(pr_data)
	prompt = prompt_template.format(pr_data=formatted_pr_data)

	try:
		client = boto3.client(
			service_name="bedrock-runtime",
			region_name=env_vars["aws_region"],
			aws_access_key_id=env_vars["aws_access_key_id"],
			aws_secret_access_key=env_vars["aws_secret_access_key"],
		)

		response = client.converse(
			modelId=env_vars["bedrock_model"],
			messages=[{"role": "user", "content": [{"text": prompt}]}],
			inferenceConfig={
				"maxTokens": env_vars["max_tokens"],
				"temperature": env_vars["temperature"],
			},
		)

		return response["output"]["message"]["content"][0]["text"]
	except NoCredentialsError:
		print("ERROR: AWS credentials are missing or invalid.")
		comment = (
			"## Changelog Generation Error\n\n"
			"The changelog could not be generated because the AWS credentials are missing or invalid.\n\n"
			"Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY configuration."
		)
		post_error_comment(comment, pr_data, env_vars)
	except ClientError as e:
		error_code = e.response["Error"]["Code"]
		if error_code in ("AccessDeniedException", "UnauthorizedException"):
			print(f"ERROR: AWS access denied for Bedrock model '{env_vars['bedrock_model']}'.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because access to the Amazon Bedrock model was denied.\n\n"
				"Please ensure your AWS credentials have the `bedrock:InvokeModel` permission and that "
				f"model access is enabled for `{env_vars['bedrock_model']}` in your AWS account."
			)
		elif error_code == "ThrottlingException":
			print("ERROR: Amazon Bedrock request was throttled.")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated because the Amazon Bedrock request was throttled.\n\n"
				"Please try again later."
			)
		elif error_code == "ValidationException":
			print(f"ERROR: Invalid request to Amazon Bedrock: {e}")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated due to an invalid request to Amazon Bedrock.\n\n"
				f"Please verify the model ID `{env_vars['bedrock_model']}` is correct and supported in region "
				f"`{env_vars['aws_region']}`.\n\nError details: {str(e)}"
			)
		else:
			print(f"Error generating changelog with Amazon Bedrock: {e}")
			comment = (
				"## Changelog Generation Error\n\n"
				"The changelog could not be generated due to an error with Amazon Bedrock.\n\n"
				f"Error details: {str(e)}"
			)
		post_error_comment(comment, pr_data, env_vars)
	except Exception as e:
		print(f"Error generating changelog with Amazon Bedrock: {e}")
		comment = (
			"## Changelog Generation Error\n\n"
			"The changelog could not be generated due to an unexpected error with Amazon Bedrock.\n\n"
			f"Error details: {str(e)}"
		)
		post_error_comment(comment, pr_data, env_vars)
	return None


def generate_changelog(pr_data, env_vars):
	"""Route changelog generation to the configured LLM provider."""
	provider = env_vars["llm_provider"]
	if provider == PROVIDER_ANTHROPIC:
		return generate_changelog_with_anthropic(pr_data, env_vars)
	elif provider == PROVIDER_OPENAI:
		return generate_changelog_with_openai(pr_data, env_vars)
	elif provider == PROVIDER_BEDROCK:
		return generate_changelog_with_bedrock(pr_data, env_vars)


def post_error_comment(error_message, pr_data, env_vars):
	"""Post an error comment explaining why changelog generation failed."""
	try:
		issue = pr_data["issue"]
		issue.create_comment(error_message)
		return True
	except Exception as e:
		print(f"Error posting error comment: {e}")
		return False


def post_comment(changelog_text, pr_data, env_vars):
	"""Post a new comment with the generated changelog."""
	comment_body = f"{env_vars['comment_header']}\n\n{changelog_text}\n\n_This changelog entry was automatically generated by the Changelog Generator Action._"

	try:
		issue = pr_data["issue"]
		issue.create_comment(comment_body)
		return True
	except Exception as e:
		print(f"Error posting comment: {e}")
		return False


def main():
	try:
		env_vars = get_env_vars()
		pr_data = get_pr_data(env_vars)
		is_regeneration = env_vars["is_regeneration"]

		existing_comment = pr_data.get("existing_changelog_comment")
		if existing_comment and not is_regeneration:
			print("Draft changelog comment already exists. Taking no action.")
			return

		changelog_text = generate_changelog(pr_data, env_vars)
		if not changelog_text:
			print("Failed to generate changelog text")
			sys.exit(1)

		success = post_comment(changelog_text, pr_data, env_vars)
		if success:
			print("Successfully posted changelog comment")
		else:
			print("Failed to post changelog comment")
			sys.exit(1)
	except Exception as e:
		print(f"Error in changelog generation: {e}")
		sys.exit(1)


if __name__ == "__main__":
	main()
