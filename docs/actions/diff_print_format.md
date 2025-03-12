# Print Format Diff

This GitHub Action helps automate the process of comparing changes made to print formats in a Frappe app. It checks for changes in .json files within the print_format directory, compares them to the base branch, and then comments the diff on the pull request.


## Usage

To use the `diff-print-format` GitHub Action in your workflow, follow these steps:

### 1. Create a Workflow File

Create a new GitHub Actions workflow file in your repository, e.g., `.github/workflows/diff_print_format.yml`, and add the following content:

```yaml
name: Diff Print Formats

on:
  pull_request:

jobs:
  diff_print_format:
    runs-on: ubuntu-latest
    name: Diff Print Formats
    steps:
      - name: Checkout the code
        uses: actions/checkout@v3
        with:
          fetch-depth: 0

      - name: Checkout test_utils
        uses: actions/checkout@v2
        with:
          repository: agritheory/test_utils
          ref: main
          path: test_utils

      - name: Diff Print Formats
        uses: ./test_utils/actions/diff_print_format
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
```
