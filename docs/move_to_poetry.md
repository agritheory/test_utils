## Convert pyproject.toml to use poetry from flit_core

The following is a guide on converting the pyproject.toml file of the Beam app to use poetry instead of flit_core. Changes made in the file to adapt it for poetry are outlined below:

 - Changed `[project]` to `[tool.poetry]`
 - Added version key to `[tool.poetry]`
 - Removed `requires-python` and `dynamic`
 - Changed format to specify Authors

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

After implementing the mentioned changes, the app was able to uninstall and install without any errors on the site. The following commands were executed successfully:
```
$ bench setup requirements
$ bench migrate
$ pip3 install pyproject.toml
$ bench pip install pyproject.toml
```
