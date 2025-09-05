# Translate Docs

### Repository Configuration

1. Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable:

- Go to Settings > Secrets and variables > Actions > New repository settings
  - Name: `GOOGLE_APPLICATION_CREDENTIALS`
  - Value: the content of the JSON credentials file

2. Configuration for Actions:

- Go to Settings > Actions > General > Workflow permissions and enable the following:
  - Read and write permission.
  - Allow GitHub Actions to create and approve pull requests.

### Usage

Every Pull Request that makes changes in any .md file under `docs/*/en/` will trigger the action. The action will make a Pull Request with the translated files for every language configured in `docs_languages` in hooks under `docs/*/{LANGUAGE}/`, if the folder for the language doesn't exists it will be created.

`docs_languages` should be a list of languages i.e:
`docs_languages = ['es', 'fr']`
