# Publish Docs

This action is for AgriTheory apps only. After a release that changes markdown, it starts a rebuild of the AgriTheory website so the site can refetch that app's documentation and reindex search.

Other projects that use test_utils should leave this action unused. It is not a general documentation publisher.

## Usage

Add this step to an AgriTheory app's release workflow, after semantic-release publishes a version:

```yaml
- name: Publish docs
  if: steps.release.outputs.released == 'true'
  uses: agritheory/test_utils/actions/publish_docs@main
  with:
    tag: ${{ steps.release.outputs.tag }}
    previous_version: ${{ steps.release.outputs.previous_version }}
    token: ${{ secrets.ATSITE_WORKFLOW_TOKEN }}
```

`ATSITE_WORKFLOW_TOKEN` needs permission to start workflows on `agritheory/atsite`. This repository calls the same action by local path from [`.github/workflows/release.yaml`](../../.github/workflows/release.yaml).
