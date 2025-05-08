# Integration with Release Tools

This document covers how to integrate the Changelog Generator Action with various release tools.

## GitHub Releases Integration

If you want to use the generated changelog for GitHub releases, create a workflow file at `.github/workflows/release.yml`:

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

## Python Semantic Release Integration

If your repository uses [python-semantic-release](https://python-semantic-release.readthedocs.io/), you can integrate the generated changelog as follows:

### Workflow Setup

Create a release workflow file:

```yaml
name: Release

on:
  push:
    branches:
      - main
      - master

jobs:
  release:
    runs-on: ubuntu-latest
    concurrency: release
    permissions:
      id-token: write
      contents: write

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      # Optional step: extract changelog from recent PR
      - name: Find PR and extract changelog
        id: extract-changelog
        uses: actions/github-script@v6
        with:
          script: |
            // Find the PR that was just merged
            const { data: commits } = await github.rest.repos.listCommits({
              owner: context.repo.owner,
              repo: context.repo.repo,
              per_page: 1
            });

            const commitMessage = commits[0].commit.message;
            const prMatch = commitMessage.match(/Merge pull request #(\d+)/);

            if (!prMatch) {
              console.log('No PR found in commit message');
              return '';
            }

            const prNumber = prMatch[1];

            // Get PR comments
            const { data: comments } = await github.rest.issues.listComments({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: prNumber
            });

            // Find changelog comment
            const changelogComment = comments.find(comment =>
              comment.body.includes('## 📝 Draft Changelog Entry')
            );

            if (!changelogComment) {
              console.log('No changelog comment found');
              return '';
            }

            // Extract changelog content
            const commentBody = changelogComment.body;
            const headerEndIndex = commentBody.indexOf('\n\n') + 2;
            const footerStartIndex = commentBody.lastIndexOf('\n\n_This changelog');

            const changelogContent = commentBody.substring(
              headerEndIndex,
              footerStartIndex > 0 ? footerStartIndex : undefined
            ).trim();

            return changelogContent;

      # Set up Python and python-semantic-release
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install python-semantic-release

      # Run python-semantic-release
      - name: Python Semantic Release
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          # Pass the extracted changelog to be used by semantic-release
          EXTRA_CHANGELOG: ${{ steps.extract-changelog.outputs.result }}
        run: |
          semantic-release publish
```

### Configuration Setup

To use the generated changelog with python-semantic-release, customize your configuration:

1. In your `pyproject.toml`:

```toml
[tool.semantic_release]
branch = "main"
version_variable = "your_package/__init__.py:__version__"
changelog_file = "CHANGELOG.md"

# Use a custom changelog template
changelog_template = ".github/changelog_template.md"
```

2. Create a custom template file `.github/changelog_template.md`:

```md
# Changelog

{% for version in versions %}
## {{ version.version }} ({{ version.date }})

{% if version.env.EXTRA_CHANGELOG %}
{{ version.env.EXTRA_CHANGELOG }}
{% else %}
{% for commit in version.commits %}
* {{ commit.subject }} ([`{{ commit.hash }}`]({{ commit.url }}))
{% endfor %}
{% endif %}

{% endfor %}
```

This approach uses the AI-generated changelog when available, but falls back to the standard commit log when it's not.

## Other Release Tools

For other release management tools, you can follow a similar pattern:

1. Extract the changelog content from PR comments
2. Pass the content to your release tool via environment variables
3. Customize your release tool's templates to use this content

The key is using GitHub's API to find the PR comment containing the changelog and extracting its contents.
