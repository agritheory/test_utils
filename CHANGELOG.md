# CHANGELOG

## v0.17.0 (2024-12-11)

### Features

- Create partition helper ([#75](https://github.com/agritheory/test_utils/pull/75),
  [`ab2d871`](https://github.com/agritheory/test_utils/commit/ab2d8711626161007da56108f5c3785d676c73a8))

* feat: create partition helper

* feat: partitioning

* feat: partition by month and quarter

* feat: partition by field

* feat: partition for child doctypes

* wip: add partition helper script to pyproject.toml

* wip: add create partition import

* wip: v13 meta.\_fields fix

* fix: skip standard fields

* wip: restore partitions

* wip: rm debug

* wip: improve get_partitions_to_backup

* feat: bkp and restore for partitions in the same machine

* feat: compress and path

* feat: option to exclude non partitioned tables

* fix: add_custom_field

* frix: csv file size limit (#83)

---

Co-authored-by: Francisco Roldan <franciscoproldan@gmail.com>

Co-authored-by: Tyler Matteson <tyler@agritheory.com>

## v0.16.1 (2024-10-26)

## v0.16.0 (2024-09-04)

### Features

- Create warehouses ([#57](https://github.com/agritheory/test_utils/pull/57),
  [`1d6817d`](https://github.com/agritheory/test_utils/commit/1d6817d110ff4e0007365d434b38b56857e280a8))

## v0.15.0 (2024-08-05)

### Features

- Add quarantine warehouse ([#72](https://github.com/agritheory/test_utils/pull/72),
  [`54995f4`](https://github.com/agritheory/test_utils/commit/54995f4e5c978cd573253bf40e00cd3764e6ba11))

## v0.14.1 (2024-07-26)

### Bug Fixes

- Remove set-module from validate customizations
  ([#69](https://github.com/agritheory/test_utils/pull/69),
  [`6c1ea61`](https://github.com/agritheory/test_utils/commit/6c1ea61191509759c84ba45a992a34a108225aad))

* fix: remove set-module from validate customizations

* fix: update modified timestamp when json is modified

* fix: remove timestamp modify code

## v0.14.0 (2024-07-22)

### Features

- Holiday list fixtures ([#58](https://github.com/agritheory/test_utils/pull/58),
  [`c84d6b2`](https://github.com/agritheory/test_utils/commit/c84d6b23779e3d6cf59874da369d2316a46dc688))

* feat: holiday list fixtures

* fix: add country to json and filter data based on country

* fix: format setup_fixtures.py

## v0.13.4 (2024-07-22)

### Bug Fixes

- Add mypy hook with default args ([#62](https://github.com/agritheory/test_utils/pull/62),
  [`801e6ac`](https://github.com/agritheory/test_utils/commit/801e6ac693416eed36860a6fda72f8b5e66d44ff))

Co-authored-by: Rohan Bansal <rohan@agritheory.dev>

## v0.13.3 (2024-07-13)

### Chores

- Add more pre-commit configs ([#67](https://github.com/agritheory/test_utils/pull/67),
  [`873c36f`](https://github.com/agritheory/test_utils/commit/873c36f8e3116db7ddbede001d732a36775ef1ed))

## v0.13.2 (2024-07-13)

## v0.13.1 (2024-07-13)

### Bug Fixes

- Add exit codes, other cleanup ([#64](https://github.com/agritheory/test_utils/pull/64),
  [`4531acc`](https://github.com/agritheory/test_utils/commit/4531acc225652a3b91d51d38874d0ed7c6f9a400))

## v0.13.0 (2024-07-13)

### Features

- Auto update pre-commit config ([#59](https://github.com/agritheory/test_utils/pull/59),
  [`8ee41dd`](https://github.com/agritheory/test_utils/commit/8ee41dda35e150e53666bd0f85970a382919ee2f))

* feat: auto update pre-commit config

* docs: update_pre_commit_config

## v0.12.0 (2024-07-13)

### Features

- Add alternative chart of accounts ([#49](https://github.com/agritheory/test_utils/pull/49),
  [`61c37c1`](https://github.com/agritheory/test_utils/commit/61c37c104a9aa2a7e30534db50f709b6fc760bd6))

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

* chore: shorten too-long account name

- **fixtures**: Fixtures data for bank, bank accounts and mode of payments
  ([#53](https://github.com/agritheory/test_utils/pull/53),
  [`42ecda0`](https://github.com/agritheory/test_utils/commit/42ecda0e2d4f716faf58f10b948469a388cf9bfd))

## v0.11.0 (2024-06-21)

### Features

- Setup company fixtures ([#55](https://github.com/agritheory/test_utils/pull/55),
  [`99ecb1b`](https://github.com/agritheory/test_utils/commit/99ecb1ba53a3bd2d20b753f130276757e7772493))

* feat: setup company fixtures

* fix: rename setup.json to company.json

* fix: change path for fixtures file

* fix: remove extra db.commit()

- **fixtures**: Fixtures data for employees
  ([#52](https://github.com/agritheory/test_utils/pull/52),
  [`579b93c`](https://github.com/agritheory/test_utils/commit/579b93cc14394223a8c735228837a98f213083d2))

* feat(fixtures): fixtures data for employees

* fix: set company for employee from settings

## v0.10.0 (2024-06-06)

### Features

- **fixtures**: Fixtures data for boms ([#51](https://github.com/agritheory/test_utils/pull/51),
  [`5eeac11`](https://github.com/agritheory/test_utils/commit/5eeac112b7f64698757abfe75b2b735e0dd74769))

* feat(fixtures): fixtures data for boms

* fix: remove check for existing bom

## v0.9.0 (2024-06-04)

### Features

- **fixtures**: Fixtures data for customers
  ([#39](https://github.com/agritheory/test_utils/pull/39),
  [`335501f`](https://github.com/agritheory/test_utils/commit/335501f61b01c29445b2c4e9ede2234e54e77b04))

* feat(fixtures): fixtures data for customers

* fix: update create_customers and fixtures

* chore: spelling for phoenix

---

Co-authored-by: Rohan Bansal <rohan@agritheory.dev>

## v0.8.0 (2024-06-04)

### Bug Fixes

- Progress bar for load_customizations ([#48](https://github.com/agritheory/test_utils/pull/48),
  [`cd5fe25`](https://github.com/agritheory/test_utils/commit/cd5fe25339c7abdd34e3dde9775f807f68d3c298))

### Features

- **fixtures**: Fixtures data for suppliers
  ([#38](https://github.com/agritheory/test_utils/pull/38),
  [`01cacc5`](https://github.com/agritheory/test_utils/commit/01cacc52a50d481cc5ef9b7e330855264d77a1d8))

* feat(fixtures): fixtures data for supplier

* feat: added supplier groups fixtures and builder functions

* fix: change default price list to standard buying

* fix: create address while creating suppliers

---

Co-authored-by: Rohan Bansal <rohan@agritheory.dev>

## v0.7.1 (2024-06-04)

### Bug Fixes

- Ignore spelling issues in commit messages
  ([#47](https://github.com/agritheory/test_utils/pull/47),
  [`934c926`](https://github.com/agritheory/test_utils/commit/934c9262d8981113cb7e545d3d5d19f7a3fc1d78))

Co-authored-by: Rohan Bansal <rohan@agritheory.dev>

## v0.7.0 (2024-06-04)

### Features

- Update validate_customizations to work as pre-commit hook
  ([#46](https://github.com/agritheory/test_utils/pull/46),
  [`13f70dc`](https://github.com/agritheory/test_utils/commit/13f70dc8f46df89cfa75c58780bf36dc2fd27ef1))

## v0.6.0 (2024-06-03)

### Features

- Update clean_customized_doctypes to work as pre-commit hook
  ([#42](https://github.com/agritheory/test_utils/pull/42),
  [`cbd1628`](https://github.com/agritheory/test_utils/commit/cbd1628c1277f9a13f888037e5aaa47dd592f1d8))

## v0.5.0 (2024-06-03)

### Features

- Update validate_javascript_dependencies to work as pre-commit hook
  ([#43](https://github.com/agritheory/test_utils/pull/43),
  [`49d7eed`](https://github.com/agritheory/test_utils/commit/49d7eed55daf3e67a716d21c0872e541d62040f7))

## v0.4.0 (2024-06-03)

### Continuous Integration

- Update workflows
  ([`31e5071`](https://github.com/agritheory/test_utils/commit/31e50712217f45c6652567cf2ddfdfd0e2659ac5))

### Features

- Update validate_python_dependencies to work as pre-commit hook
  ([#45](https://github.com/agritheory/test_utils/pull/45),
  [`55cf2b1`](https://github.com/agritheory/test_utils/commit/55cf2b12bcbdfe97ab6d76c6a9ddd2cac4e35ae9))

## v0.3.0 (2024-05-29)

### Features

- Update validate_copyright to work as pre-commit hook
  ([#41](https://github.com/agritheory/test_utils/pull/41),
  [`16be96d`](https://github.com/agritheory/test_utils/commit/16be96d102e32659b4452a261187204792e4bad2))

* feat: update validate_copyright to work as pre-commit hook

* docs: add docs for pre-commit config

- **fixtures**: Fixtures data for items ([#36](https://github.com/agritheory/test_utils/pull/36),
  [`fabb59d`](https://github.com/agritheory/test_utils/commit/fabb59de188a96dc00a2d910f837e8af73d5f2a9))

* feat(fixtures): fixtures data for items

* feat(fixtures): fixtures data for items_groups

* fix: update item fixtures from inventory_tools

* feat: added builder functions to create item and item group

* fix: change create_item_group variable

* feat(fixtures): fixtures data for items from check_run

* fix: move all data to json

* fix: update builder function to directly update dict

* fix: add shortlist api

* fix: add only_create api and change function

## v0.2.2 (2024-05-04)

### Bug Fixes

- Sync hrms customization if hrms in installed apps
  ([#33](https://github.com/agritheory/test_utils/pull/33),
  [`5fec2bd`](https://github.com/agritheory/test_utils/commit/5fec2bd0722206018dc094dd409e62ffdae97442))

## v0.2.1 (2024-05-01)

### Bug Fixes

- Package name
  ([`2921c57`](https://github.com/agritheory/test_utils/commit/2921c572549b92e5a03a6bff00acdcede5336768))

### Continuous Integration

- Build project wheel ([#29](https://github.com/agritheory/test_utils/pull/29),
  [`60182d6`](https://github.com/agritheory/test_utils/commit/60182d679272af0f33e82869b2735fd0cf91d81c))

* ci: build project wheel

* chore: better naming for GHA steps

* fix: event name

## v0.2.0 (2024-04-26)

### Features

- Added release.yaml to github workflows
  ([`57f868f`](https://github.com/agritheory/test_utils/commit/57f868f1246328115a667776e4834e7803665177))

## v0.1.0 (2024-04-25)

### Bug Fixes

- Exclude directories to validate copyright nodemodules and dist
  ([`85b5949`](https://github.com/agritheory/test_utils/commit/85b5949086c7f952c06b2fe86a6c072d363ca838))

- Get app_publisher from hooks.py
  ([`73faa06`](https://github.com/agritheory/test_utils/commit/73faa067ef9cf7c125c6735edba0d7f8f835be52))

- Import scrub from validate_customizations
  ([`6b70da2`](https://github.com/agritheory/test_utils/commit/6b70da2d5a17f4692c085f9881529b84d91a815c))

- Move files to pre-commit
  ([`490d677`](https://github.com/agritheory/test_utils/commit/490d6779e7a2492442bae3daa9c1e6a52f79c840))

- Update modified timestamp when json is modified
  ([`6c6179c`](https://github.com/agritheory/test_utils/commit/6c6179c9d0f062eaf778490ad00c15ee46c90649))

### Documentation

- Convert pyproject.toml to use poetry instead of flit_core
  ([`cd13dd2`](https://github.com/agritheory/test_utils/commit/cd13dd2ffba5b04be73e3d6a844abf06ade09766))

- Correction in docs
  ([`327d02f`](https://github.com/agritheory/test_utils/commit/327d02fae0ad7a42438d72e4a6cc46e0c2ccd715))

### Features

- Added clean customization pre-commit config
  ([`6aedce9`](https://github.com/agritheory/test_utils/commit/6aedce9e4ef238107b9a82cc5dcbc3cee572affe))

- Conftest.py boilerplate
  ([`b8c6490`](https://github.com/agritheory/test_utils/commit/b8c6490ff2f778e1a228fd39ff6d76abdb46f18a))

- Pre-commit hook configurations
  ([`b5a955c`](https://github.com/agritheory/test_utils/commit/b5a955cf55a369fc6de7e24c514b6c8ceff88f12))

- Validate copyright and add if it does not exist pre-commit config
  ([`1383cb9`](https://github.com/agritheory/test_utils/commit/1383cb95db7972fbd60926f867e2c023235fe294))

- Validate customization pre commit config
  ([`5c7e50a`](https://github.com/agritheory/test_utils/commit/5c7e50a37492ec28b68cd4335187399973e50f8b))
