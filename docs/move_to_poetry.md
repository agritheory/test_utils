## Convert pyproject.toml to use poetry from flit_core

The following is a guide on converting the pyproject.toml file of the Beam app to use poetry instead of flit_core. Changes made in the file to adapt it for poetry are outlined below:

 - Changed `[project]` to `[tool.poetry]`
 - Added version key to `[tool.poetry]`
 - Removed `requires-python` and `dynamic`
 - Changed format to specify Authors

### Required Fields for Frappe Apps with Poetry

When using Poetry with Frappe apps, your `pyproject.toml` must include certain fields for proper bench compatibility:

#### Add [project] Section for Better Compatibility

For improved bench compatibility (especially with `bench get-app`), you can add both `[project]` and `[tool.poetry]` sections:

```toml
[project]
name = "your_app_name"
authors = [
    { name = "Your Name", email = "your.email@example.com" }
]
description = "Your app description"
requires-python = ">=3.10"
readme = "README.md"
authors = [
    { name = "Your Name", email = "example@email.com" }
]

[tool.poetry]
name = "your_app_name"
version = "1.0.0"
description = "Your app description"
authors = ["Your Name <example@email.com>"]
readme = "README.md"
```

#### Changed build System

| flit_core | poetry |
| --- | --- |
| ```[build-system]```<br>```requires = ["flit_core >=3.4,<4"]```<br>```build-backend = "flit_core.buildapi"``` | ```[build-system]```<br>```requires = ["poetry-core"]```<br>```build-backend = "poetry.core.masonry.api"```|

#### To add dependency
```
$ poetry add {python-package}
Eg.
$ poetry add pytest-cov
```

#### To add dev dependency
```
$ poetry add --group dev {python-package}
Eg.
$ poetry add --group dev pytest-cov
```

#### To remove dependency
```
$ poetry remove {python-package}
Eg.
$ poetry remove pytest-cov
```

In the above cases, it updates pyproject.toml.

#### To validate pyproject.toml
```
$ poetry check
```

The above command validates pyproject.toml and reports if any error

#### To rebuild poetry.lock file

After making changes to `pyproject.toml`, you need to update the lock file:

```bash
# Update dependencies and rebuild lock file
$ poetry lock
```

**When to rebuild:**
- After adding or removing dependencies
- After changing version constraints
- After modifying `pyproject.toml` structure
- When `poetry.lock` is out of sync with `pyproject.toml`

**Note:** Always commit both `pyproject.toml` and `poetry.lock` together to ensure reproducible installs.

After implementing the mentioned changes, the app was able to uninstall and install without any errors on the site. The following commands were executed successfully:
```
$ bench setup requirements
$ bench migrate
$ pip3 install pyproject.toml
$ bench pip install pyproject.toml
```
