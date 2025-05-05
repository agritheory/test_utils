# Changelog Generator Action

This GitHub Action automatically generates a changelog entry as a PR comment by analyzing PR changes using Anthropic's Claude AI model.

## Features

- Automatically generates detailed changelog entries when a PR is opened or updated
- Analyzes PR details including commits, files changed, and PR description
- Identifies breaking changes, new features, bug fixes, and more
- Adds context on user impact and required actions
- Creates changelog as a PR comment that can be edited if needed
- Only generates one changelog per PR (no duplicate comments on updates)
- Provides clear error messages when API issues occur
- Reuses changelog content for GitHub releases

## Requirements

- GitHub repository with Actions enabled
- Anthropic API key (for Claude AI model)

## Setup

### 1. Configure GitHub Secrets

In your repository settings, add the following secrets:
- `ANTHROPIC_API_KEY`: Your Anthropic API key

### 2. Create a Workflow File

Create a workflow file at `.github/workflows/generate-changelog.yml`:

```yaml
name: Generate Changelog

on:
  pull_request:
    types: [opened, reopened, synchronize]

jobs:
  generate-changelog:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write

    steps:
      - uses: actions/checkout@v4

      - name: Generate Changelog
        uses: agritheory/test_utils/actions/generate_changelog@main
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          anthropic-api-key: ${{ secrets.ANTHROPIC_API_KEY }}
          # Optional: Use custom prompt template
          # prompt-template: '.github/changelog-prompt.txt'
```

That's it! The action automatically handles different event types and determines when to run.

### 3. (Optional) Create a Custom Prompt Template

Create a file `.github/changelog-prompt.txt` to customize the prompt sent to the AI model:

```
You are an expert at analyzing Pull Requests and generating changelog entries.
Analyze the following PR data and generate a comprehensive, user-friendly changelog entry.

Focus on:
- Breaking changes (API modifications, dependency updates)
- New features vs bug fixes vs performance improvements
- Security-relevant changes
- Infrastructure/tooling updates
- User impact ("Users can now...")
- Required actions by users ("Requires updating...")
- Context for why changes were made

Format your response as markdown with appropriate sections and bullet points.
Be concise but informative.

PR Data:
{pr_data}
```

### 4. Set Up Release Integration (Optional)

For integrating with release workflows, see the [Release Integration Guide](./docs/release-integration.md).

This covers:
- Integration with the GitHub Releases API
- Integration with [`python-semantic-release`](https://github.com/python-semantic-release/python-semantic-release)
- Integration with other release tools

## Usage

Once installed and configured:

1. When a PR is opened, the action will automatically generate a changelog entry as a comment.
2. If the PR is updated, you can delete the comment to regenerate it with updated content.
3. When the PR is merged, the release workflow (if configured) will use the changelog content for the release notes.

## Configuration Options

The action accepts the following inputs:

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `github-token` | GitHub token with permissions to read PRs and create comments | Yes | N/A |
| `anthropic-api-key` | API key for Anthropic | Yes | N/A |
| `anthropic-model` | Anthropic model to use for generating the changelog | No | `claude-3-7-sonnet-latest` |
| `prompt-template` | Path to a custom prompt template file | No | `.github/changelog-prompt.txt` |
| `comment-header` | Header text for the changelog comment | No | `## 📝 Draft Changelog Entry` |
| `max_tokens` | Maximum number of tokens to generate in the response | No | `1500` |
| `temperature` | Temperature for the model response (lower is more analytical) | No | `0.2` |

All event-specific parameters are handled automatically by the action.

## How It Works

1. The action automatically detects relevant events:
   - When a PR is opened or reopened
   - When new commits are pushed to the PR
2. When triggered, the action:
   - Checks if a changelog comment already exists
   - If one exists, takes no action (preserving any manual edits)
   - If none exists, analyzes PR title, description, commits, and files
   - Generates a comprehensive changelog with Claude AI
   - Posts the changelog as a PR comment
3. The comment can be edited manually if needed
4. If API errors occur (like insufficient credits), the action posts a clear error comment
5. When the PR is merged, the release workflow (if configured) finds the changelog comment and uses its content for the release notes
