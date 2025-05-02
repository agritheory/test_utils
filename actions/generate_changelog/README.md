# Changelog Generator Action

This GitHub Action automatically generates a changelog entry as a PR comment by analyzing PR changes using Anthropic's Claude AI model.

## Features

- Automatically generates detailed changelog entries when a PR is opened
- Analyzes PR details including commits, files changed, and PR description
- Identifies breaking changes, new features, bug fixes, and more
- Adds context on user impact and required actions
- Creates changelog as a PR comment that can be edited if needed
- Regenerates changelog if the comment is deleted
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
    types: [opened, reopened]
  issue_comment:
    types: [deleted]

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

### 4. Set Up Release Workflow (Optional)

If you want to use the generated changelog for releases, create a workflow file at `.github/workflows/release.yml`:

```yaml
name: Create Release

on:
  pull_request:
    types: [closed]
    branches:
      - main
      - master

jobs:
  create-release:
    # Only run when PR is merged (not just closed)
    if: github.event.pull_request.merged == true
    runs-on: ubuntu-latest
    permissions:
      contents: write
      pull-requests: read

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Get version
        id: get-version
        # This example assumes version is in a file like package.json, pyproject.toml, etc.
        # Modify as needed for your project
        run: |
          # Example for package.json
          VERSION=$(grep -m1 '"version":' package.json | cut -d'"' -f4)
          echo "version=v${VERSION}" >> $GITHUB_OUTPUT

      - name: Find changelog comment
        id: find-changelog
        uses: actions/github-script@v6
        with:
          script: |
            const prNumber = context.payload.pull_request.number;
            const comments = await github.rest.issues.listComments({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: prNumber
            });

            const changelogHeaderPattern = /## 📝 Draft Changelog Entry/;
            const changelogComment = comments.data.find(comment =>
              changelogHeaderPattern.test(comment.body)
            );

            if (!changelogComment) {
              console.log('No changelog comment found');
              return null;
            }

            // Extract just the changelog content (without header and footer)
            const commentBody = changelogComment.body;
            const headerEndIndex = commentBody.indexOf('\n\n') + 2;
            const footerStartIndex = commentBody.lastIndexOf('\n\n_This changelog');

            const changelogText = commentBody.substring(
              headerEndIndex,
              footerStartIndex > 0 ? footerStartIndex : undefined
            ).trim();

            return changelogText;

      - name: Create Release
        uses: actions/github-script@v6
        with:
          script: |
            const version = '${{ steps.get-version.outputs.version }}';
            const changelogText = ${{ steps.find-changelog.outputs.result != null ?
              format('"{0}"', steps.find-changelog.outputs.result) : '""' }};

            let releaseBody;
            if (changelogText) {
              releaseBody = changelogText;
            } else {
              releaseBody = 'Release ' + version;
            }

            await github.rest.repos.createRelease({
              owner: context.repo.owner,
              repo: context.repo.repo,
              tag_name: version,
              name: version,
              body: releaseBody,
              draft: false,
              prerelease: false
            });
```

## Usage

Once installed and configured:

1. When a PR is opened, the action will automatically generate a changelog entry as a comment
2. If the PR is updated, you can delete the comment to regenerate it with updated content
3. When the PR is merged, the release workflow (if enabled) will use the changelog content for the release

## Configuration Options

The action accepts the following inputs:

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `github-token` | GitHub token with permissions to read PRs and create comments | Yes | N/A |
| `anthropic-api-key` | API key for Anthropic | Yes | N/A |
| `prompt-template` | Path to a custom prompt template file | No | `.github/changelog-prompt.txt` |
| `comment-header` | Header text for the changelog comment | No | `## 📝 Draft Changelog Entry` |
| `model` | Anthropic model to use for generating the changelog | No | `claude-3-haiku-20240307` |

All event-specific parameters are handled automatically by the action.

## How It Works

1. The action automatically detects relevant events:
   - When a PR is opened or reopened
   - When a changelog comment is deleted (to regenerate it)
2. When triggered, the action:
   - Analyzes PR title, description, commits, and files
   - Generates a comprehensive changelog with Claude AI
   - Posts the changelog as a PR comment
3. The comment can be edited manually if needed
4. If the comment is deleted, the action will regenerate it automatically
5. When the PR is merged, the release workflow (if configured) finds the changelog comment and uses its content for the release notes
