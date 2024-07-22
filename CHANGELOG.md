# CHANGELOG

## v0.13.1 (2024-07-13)

### Fix

* fix: add exit codes, other cleanup (#64) ([`4531acc`](https://github.com/agritheory/test_utils/commit/4531acc225652a3b91d51d38874d0ed7c6f9a400))

## v0.13.0 (2024-07-13)

### Feature

* feat: auto update pre-commit config (#59)

* feat: auto update pre-commit config

* docs: update_pre_commit_config ([`8ee41dd`](https://github.com/agritheory/test_utils/commit/8ee41dda35e150e53666bd0f85970a382919ee2f))

## v0.12.0 (2024-07-13)

### Feature

* feat(fixtures): fixtures data for bank, bank accounts and mode of payments (#53) ([`42ecda0`](https://github.com/agritheory/test_utils/commit/42ecda0e2d4f716faf58f10b948469a388cf9bfd))

* feat: add alternative chart of accounts (#49)

* feat: start chart of accounts

* wip: create alternative coa json files

* feat: update farm COA

* feat: update IFRS COA

* feat: update COA for consistent accounts

* feat: update for bank, bank account and setup flow

* refactor: remove app-specific MOP creation

* refactor: only change A/R and A/P acct names with numbers

* refactor: fix spelling, add error string

* refactor: check value exists for invalid accout links

* fix: duplicate account numbers

* chore: shorten too-long account name ([`61c37c1`](https://github.com/agritheory/test_utils/commit/61c37c104a9aa2a7e30534db50f709b6fc760bd6))

## v0.11.0 (2024-06-21)

### Feature

* feat(fixtures): fixtures data for employees (#52)

* feat(fixtures): fixtures data for employees

* fix: set company for employee from settings ([`579b93c`](https://github.com/agritheory/test_utils/commit/579b93cc14394223a8c735228837a98f213083d2))

* feat: setup company fixtures (#55)

* feat: setup company fixtures

* fix: rename setup.json to company.json

* fix: change path for fixtures file

* fix: remove extra db.commit() ([`99ecb1b`](https://github.com/agritheory/test_utils/commit/99ecb1ba53a3bd2d20b753f130276757e7772493))

## v0.10.0 (2024-06-06)

### Feature

* feat(fixtures): fixtures data for boms (#51)

* feat(fixtures): fixtures data for boms

* fix: remove check for existing bom ([`5eeac11`](https://github.com/agritheory/test_utils/commit/5eeac112b7f64698757abfe75b2b735e0dd74769))

### Unknown

* pre-commit: added flynt (#50) ([`1a1c30a`](https://github.com/agritheory/test_utils/commit/1a1c30a39f9f92551b4803b24cda9a1a97abddc7))

## v0.9.0 (2024-06-04)

### Feature

* feat(fixtures): fixtures data for customers (#39)

* feat(fixtures): fixtures data for customers

* fix: update create_customers and fixtures

* chore: spelling for phoenix

---------

Co-authored-by: Rohan Bansal &lt;rohan@agritheory.dev&gt; ([`335501f`](https://github.com/agritheory/test_utils/commit/335501f61b01c29445b2c4e9ede2234e54e77b04))

## v0.8.0 (2024-06-04)

### Feature

* feat(fixtures): fixtures data for suppliers (#38)

* feat(fixtures): fixtures data for supplier

* feat: added supplier groups fixtures and builder functions

* fix: change default price list to standard buying

* fix: create address while creating suppliers

---------

Co-authored-by: Rohan Bansal &lt;rohan@agritheory.dev&gt; ([`01cacc5`](https://github.com/agritheory/test_utils/commit/01cacc52a50d481cc5ef9b7e330855264d77a1d8))

### Fix

* fix: progress bar for load_customizations (#48) ([`cd5fe25`](https://github.com/agritheory/test_utils/commit/cd5fe25339c7abdd34e3dde9775f807f68d3c298))

## v0.7.1 (2024-06-04)

### Fix

* fix: ignore spelling issues in commit messages (#47)

Co-authored-by: Rohan Bansal &lt;rohan@agritheory.dev&gt; ([`934c926`](https://github.com/agritheory/test_utils/commit/934c9262d8981113cb7e545d3d5d19f7a3fc1d78))

## v0.7.0 (2024-06-04)

### Feature

* feat: update validate_customizations to work as pre-commit hook (#46) ([`13f70dc`](https://github.com/agritheory/test_utils/commit/13f70dc8f46df89cfa75c58780bf36dc2fd27ef1))

## v0.6.0 (2024-06-03)

### Feature

* feat: update clean_customized_doctypes to work as pre-commit hook (#42) ([`cbd1628`](https://github.com/agritheory/test_utils/commit/cbd1628c1277f9a13f888037e5aaa47dd592f1d8))

## v0.5.0 (2024-06-03)

### Feature

* feat: update validate_javascript_dependencies to work as pre-commit hook (#43) ([`49d7eed`](https://github.com/agritheory/test_utils/commit/49d7eed55daf3e67a716d21c0872e541d62040f7))

## v0.4.0 (2024-06-03)

### Ci

* ci: update workflows ([`31e5071`](https://github.com/agritheory/test_utils/commit/31e50712217f45c6652567cf2ddfdfd0e2659ac5))

### Feature

* feat: update validate_python_dependencies to work as pre-commit hook (#45) ([`55cf2b1`](https://github.com/agritheory/test_utils/commit/55cf2b12bcbdfe97ab6d76c6a9ddd2cac4e35ae9))

### Unknown

* Merge pull request #44 from agritheory/fix-ci ([`93a4f7f`](https://github.com/agritheory/test_utils/commit/93a4f7f5ddd9bcb65bb6c79f7c2b4b6d3a571e9f))

## v0.3.0 (2024-05-29)

### Feature

* feat: update validate_copyright to work as pre-commit hook (#41)

* feat: update validate_copyright to work as pre-commit hook

* docs: add docs for pre-commit config ([`16be96d`](https://github.com/agritheory/test_utils/commit/16be96d102e32659b4452a261187204792e4bad2))

* feat(fixtures): fixtures data for items (#36)

* feat(fixtures): fixtures data for items

* feat(fixtures): fixtures data for items_groups

* fix: update item fixtures from inventory_tools

* feat: added builder functions to create item and item group

* fix: change create_item_group variable

* feat(fixtures): fixtures data for items from check_run

* fix: move all data to json

* fix: update builder function to directly update dict

* fix: add shortlist api

* fix: add only_create api and change function ([`fabb59d`](https://github.com/agritheory/test_utils/commit/fabb59de188a96dc00a2d910f837e8af73d5f2a9))

## v0.2.2 (2024-05-04)

### Fix

* fix: sync hrms customization if hrms in installed apps (#33) ([`5fec2bd`](https://github.com/agritheory/test_utils/commit/5fec2bd0722206018dc094dd409e62ffdae97442))

## v0.2.1 (2024-05-01)

### Ci

* ci: build project wheel (#29)

* ci: build project wheel

* chore: better naming for GHA steps

* fix: event name ([`60182d6`](https://github.com/agritheory/test_utils/commit/60182d679272af0f33e82869b2735fd0cf91d81c))

### Fix

* fix: package name ([`2921c57`](https://github.com/agritheory/test_utils/commit/2921c572549b92e5a03a6bff00acdcede5336768))

### Unknown

* Restructure (#30)

* wip: restructure

* feat: added customize.py

* format: formate customize.py with black

* fix: compute hash for concatenated data for standard and custom json

* wip: restruacture

---------

Co-authored-by: Myuddin khatri &lt;khatrimayu111@gmail.com&gt; ([`75c5367`](https://github.com/agritheory/test_utils/commit/75c5367235eaf648197caceb362915b289ed917f))

## v0.2.0 (2024-04-26)

### Feature

* feat: added release.yaml to github workflows ([`57f868f`](https://github.com/agritheory/test_utils/commit/57f868f1246328115a667776e4834e7803665177))

### Unknown

* Merge pull request #27 from MyuddinKhatri/add-release.yaml

feat: added release.yaml to github workflows ([`d7b51db`](https://github.com/agritheory/test_utils/commit/d7b51db932ce98fb441d8a18cdf9bea77a310a21))

## v0.1.0 (2024-04-25)

### Documentation

* docs: correction in docs ([`327d02f`](https://github.com/agritheory/test_utils/commit/327d02fae0ad7a42438d72e4a6cc46e0c2ccd715))

* docs: convert pyproject.toml to use poetry instead of flit_core ([`cd13dd2`](https://github.com/agritheory/test_utils/commit/cd13dd2ffba5b04be73e3d6a844abf06ade09766))

### Feature

* feat: conftest.py boilerplate ([`b8c6490`](https://github.com/agritheory/test_utils/commit/b8c6490ff2f778e1a228fd39ff6d76abdb46f18a))

* feat: added clean customization pre-commit config ([`6aedce9`](https://github.com/agritheory/test_utils/commit/6aedce9e4ef238107b9a82cc5dcbc3cee572affe))

* feat: validate copyright and add if it does not exist pre-commit config ([`1383cb9`](https://github.com/agritheory/test_utils/commit/1383cb95db7972fbd60926f867e2c023235fe294))

* feat: validate customization pre commit config ([`5c7e50a`](https://github.com/agritheory/test_utils/commit/5c7e50a37492ec28b68cd4335187399973e50f8b))

* feat: pre-commit hook configurations ([`b5a955c`](https://github.com/agritheory/test_utils/commit/b5a955cf55a369fc6de7e24c514b6c8ceff88f12))

### Fix

* fix: update modified timestamp when json is modified ([`6c6179c`](https://github.com/agritheory/test_utils/commit/6c6179c9d0f062eaf778490ad00c15ee46c90649))

* fix: import scrub from validate_customizations ([`6b70da2`](https://github.com/agritheory/test_utils/commit/6b70da2d5a17f4692c085f9881529b84d91a815c))

* fix: get app_publisher from hooks.py ([`73faa06`](https://github.com/agritheory/test_utils/commit/73faa067ef9cf7c125c6735edba0d7f8f835be52))

* fix: exclude directories to validate copyright nodemodules and dist ([`85b5949`](https://github.com/agritheory/test_utils/commit/85b5949086c7f952c06b2fe86a6c072d363ca838))

* fix: move files to pre-commit ([`490d677`](https://github.com/agritheory/test_utils/commit/490d6779e7a2492442bae3daa9c1e6a52f79c840))

### Unknown

* Merge pull request #26 from MyuddinKhatri/use-poetry-docs

docs: convert pyproject.toml to use poetry instead of flit_core ([`ad34183`](https://github.com/agritheory/test_utils/commit/ad34183113e0597541bad7fe52cbabe2aa1c5bd6))

* Merge pull request #25 from MyuddinKhatri/conftest.py-boilerplate

feat: conftest.py boilerplate ([`a4f90db`](https://github.com/agritheory/test_utils/commit/a4f90db8892bb3a22587840f1c079afac320708c))

* Merge pull request #24 from MyuddinKhatri/clean-customized-doctypes

feat: added clean customization pre-commit config ([`edbf8b9`](https://github.com/agritheory/test_utils/commit/edbf8b9ce29081fd180286629b43668623ed7563))

* Merge pull request #22 from MyuddinKhatri/add-validate-copyright

feat: validate copyright and add if it does not exist pre-commit config ([`6f34931`](https://github.com/agritheory/test_utils/commit/6f34931beaefdae2be470b929ee61f7d3c2bb3fb))

* Merge pull request #18 from MyuddinKhatri/add-validate-customizations

feat: validate customization pre commit config ([`e75bda4`](https://github.com/agritheory/test_utils/commit/e75bda43c25bb25ee6e13f53547e33a096e76515))

* Merge pull request #15 from agritheory/pre-commit-config

feat: pre-commit hook configurations ([`f17e7e6`](https://github.com/agritheory/test_utils/commit/f17e7e61581cf77ec024190c3d4502529b93b56e))

* wip: readme ([`7b0d32a`](https://github.com/agritheory/test_utils/commit/7b0d32a3f0f7d4562aecc61cc5f0202325fe091a))

* wip: add readme with outline ([`2eba9c9`](https://github.com/agritheory/test_utils/commit/2eba9c959a8ee6a5877f06cdb648216fd9ef9df2))

* initial commit ([`8b87d1e`](https://github.com/agritheory/test_utils/commit/8b87d1e33c803fb1c6d4149aa3c0676d93a32c9e))
