# Publish Docs

This action is for AgriTheory apps only. When a push to the default branch changes markdown, it starts a rebuild of the AgriTheory website so the site can refetch that app's documentation and reindex search.

A `docs:` commit does not publish a version. The action compares the push itself, not the semantic-release tag.

Other projects that use test_utils should leave this action unused. It is not a general documentation publisher.

## Usage

Run it on every push to the default branch:

```yaml
- name: Publish docs
  uses: agritheory/test_utils/actions/publish_docs@main
  with:
    before: ${{ github.event.before }}
    sha: ${{ github.sha }}
    token: ${{ secrets.ATSITE_WORKFLOW_TOKEN }}
```

`ATSITE_WORKFLOW_TOKEN` needs permission to start workflows on `agritheory/atsite`. This repository calls the same action by local path from [`.github/workflows/release.yaml`](../../.github/workflows/release.yaml).
