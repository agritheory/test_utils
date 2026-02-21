# Translate Docs Action

This GitHub Action translates markdown files under `docs/*/en/` to other languages using Google Cloud Translation. When a PR modifies English docs, the action creates translation PRs for each configured language.

## Requirements

- GitHub repository with Actions enabled
- Google Cloud Translation API credentials (JSON key file)
- Repository with a `hooks` module exposing `docs_languages`

## Repository Configuration

### 1. Add GitHub Secrets

In your repository settings (Settings > Secrets and variables > Actions), add:

| Secret | Description |
|-------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | The full JSON content of your Google Cloud service account credentials file |

### 2. Workflow Permissions

Go to Settings > Actions > General > Workflow permissions and enable:

- **Read and write permissions** (required for creating branches and PRs)
- **Allow GitHub Actions to create and approve pull requests** (optional, for auto-merge)

### 3. Configure Languages

In your app's `hooks` module, define `docs_languages` as a list of target language codes:

```python
docs_languages = ['es', 'fr']  # Spanish and French
```

If the folder for a language does not exist under `docs/{version}/{language}/`, it will be created.

## Usage

Create a workflow file at `.github/workflows/translate-docs.yml`:

```yaml
name: Translate Docs

on:
  pull_request:
    paths:
      - 'docs/**/en/**/*.md'

jobs:
  translate:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      pull-requests: write

    steps:
      - uses: actions/checkout@v4

      - name: Translate Docs
        uses: agritheory/test_utils/actions/translate_docs@main
        with:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GITHUB_REPOSITORY: ${{ github.repository }}
          GITHUB_BASE_REF: ${{ github.base_ref }}
          GITHUB_REF: ${{ github.ref }}
```

## How It Works

1. When a PR modifies any `.md` file under `docs/*/en/`, the action runs.
2. The action fetches the modified files from the PR and translates them using Google Cloud Translation.
3. For each language in `docs_languages`, it:
   - Creates translated files under `docs/{version}/{language}/`
   - Creates a new branch `translate-{language}`
   - Pushes the branch and opens a PR against the base branch
4. The resulting PRs contain only the translated files for that language.

## Inputs

| Input | Description | Required |
|-------|-------------|----------|
| `GITHUB_TOKEN` | GitHub token (default: `github.token`) | Yes |
| `GOOGLE_APPLICATION_CREDENTIALS` | Google Cloud credentials JSON | Yes |
| `GITHUB_REPOSITORY` | Repository `owner/repo` | Yes |
| `GITHUB_BASE_REF` | Base branch (e.g. `main`) | Yes |
| `GITHUB_REF` | The ref that triggered the workflow (e.g. `refs/pull/123/merge`) | Yes |

## Directory Structure

The action expects this structure:

```
docs/
├── {version}/
│   ├── en/           # Source English files (modified in PR)
│   │   └── *.md
│   ├── es/           # Spanish translations (created/updated)
│   └── fr/           # French translations (created/updated)
```
