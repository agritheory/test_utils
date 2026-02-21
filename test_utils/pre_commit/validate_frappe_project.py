import argparse
import pathlib
import re
import sys
from collections.abc import Sequence
from enum import Enum

import toml

LOWER_BOUND_PATTERN = re.compile(r">=?\s*\S+")
UPPER_BOUND_PATTERN = re.compile(r"<=?\s*\S+|!=\s*\S+")


class BuildBackend(Enum):
	"""Supported build backends for Frappe applications."""

	FLIT = "flit_core.buildapi"
	POETRY = "poetry.core.masonry.api"
	UNKNOWN = "unknown"


def find_pyproject_toml():
	current = pathlib.Path().resolve()
	for path in [current] + list(current.parents):
		candidate = path / "pyproject.toml"
		if candidate.exists():
			return candidate
	return None


def detect_build_backend(data: dict) -> BuildBackend:
	"""
	Detect which build backend is being used.

	Args:
	    data: Parsed pyproject.toml data

	Returns:
	    BuildBackend enum value
	"""
	build_backend = data.get("build-system", {}).get("build-backend", "")

	if "flit_core" in build_backend:
		return BuildBackend.FLIT
	elif "poetry" in build_backend:
		return BuildBackend.POETRY
	else:
		return BuildBackend.UNKNOWN


def validate_toml_syntax(pyproject_path: pathlib.Path) -> tuple[dict | None, list[str]]:
	"""
	Parse and validate TOML syntax.

	Returns:
	    Tuple of (parsed_data, errors)
	"""
	errors = []

	try:
		with open(pyproject_path) as f:
			data = toml.load(f)
		return data, errors
	except Exception as e:
		errors.append(f"Failed to parse {pyproject_path}: {e}")
		return None, errors


def validate_required_sections(data: dict, app_name: str) -> list[str]:
	"""
	Validate that required sections exist in pyproject.toml.

	Args:
	    data: Parsed pyproject.toml data
	    app_name: Name of the application

	Returns:
	    List of error messages
	"""
	errors = []

	if "project" not in data:
		errors.append(f"{app_name}: pyproject.toml is missing required [project] section")

	if "build-system" not in data:
		errors.append(
			f"{app_name}: pyproject.toml is missing required [build-system] section"
		)

	return errors


def validate_project_metadata(data: dict, app_name: str) -> list[str]:
	"""
	Validate required metadata fields in [project] section.

	Args:
	    data: Parsed pyproject.toml data
	    app_name: Name of the application

	Returns:
	    List of error messages
	"""
	errors = []
	project = data.get("project", {})

	# Required fields
	if "name" not in project:
		errors.append(f"{app_name}: [project] section is missing required 'name' field")

	# Warnings for missing optional but recommended fields
	warnings = []
	if "authors" not in project:
		warnings.append(
			f"{app_name}: [project] section is missing recommended 'authors' field"
		)
	if "description" not in project:
		warnings.append(
			f"{app_name}: [project] section is missing recommended 'description' field"
		)
	if "readme" not in project:
		warnings.append(
			f"{app_name}: [project] section is missing recommended 'readme' field"
		)
	if "requires-python" not in project:
		warnings.append(
			f"{app_name}: [project] section is missing recommended 'requires-python' field"
		)

	# Print warnings but don't add to errors
	for warning in warnings:
		print(f"Warning: {warning}")

	return errors


def validate_build_system(
	data: dict, app_name: str, backend: BuildBackend
) -> list[str]:
	"""
	Validate build system configuration and check for hybrid configurations.

	Args:
	    data: Parsed pyproject.toml data
	    app_name: Name of the application
	    backend: Detected build backend

	Returns:
	    List of error messages
	"""
	errors = []
	build_system = data.get("build-system", {})
	build_backend = build_system.get("build-backend", "")
	requires = build_system.get("requires", [])

	if backend == BuildBackend.FLIT:
		# Validate flit configuration
		if build_backend != "flit_core.buildapi":
			errors.append(
				f'{app_name}: build-backend is "{build_backend}" but expected "flit_core.buildapi" '
				f"for flit-based projects"
			)

		# Check that requires includes flit_core
		has_flit = any("flit_core" in req for req in requires)
		if not has_flit:
			errors.append(
				f'{app_name}: build-system.requires must include "flit_core" for flit-based projects. '
				f'Example: requires = ["flit_core >=3.4,<4"]'
			)

		# REJECT: Check for hybrid configuration (flit with poetry sections)
		if "tool" in data and "poetry" in data["tool"]:
			if "dependencies" in data["tool"]["poetry"]:
				errors.append(
					f"{app_name}: Invalid hybrid configuration detected. "
					f"Using flit_core build backend but has [tool.poetry.dependencies] section. "
					f"For flit projects, use [project].dependencies instead. "
					f"For poetry projects, use poetry-core build backend."
				)

	elif backend == BuildBackend.POETRY:
		# Validate poetry configuration
		if "poetry.core.masonry.api" not in build_backend:
			errors.append(
				f'{app_name}: build-backend is "{build_backend}" but expected "poetry.core.masonry.api" '
				f"for poetry-based projects"
			)

		# Check that requires includes poetry-core
		has_poetry = any("poetry-core" in req for req in requires)
		if not has_poetry:
			errors.append(
				f'{app_name}: build-system.requires must include "poetry-core" for poetry-based projects. '
				f'Example: requires = ["poetry-core>=2.0.0,<3.0.0"]'
			)

		# REJECT: Check for hybrid configuration (poetry with populated project.dependencies)
		project = data.get("project", {})
		if "dependencies" in project and project["dependencies"]:
			errors.append(
				f"{app_name}: Invalid hybrid configuration detected. "
				f"Using poetry-core build backend but has populated [project].dependencies section. "
				f"For poetry projects, use [tool.poetry.dependencies] instead. "
				f"For flit projects, use flit_core build backend."
			)

	elif backend == BuildBackend.UNKNOWN:
		errors.append(
			f'{app_name}: Unknown or unsupported build backend "{build_backend}". '
			f'Expected "flit_core.buildapi" or "poetry.core.masonry.api"'
		)

	return errors


def validate_version_consistency(
	data: dict, app_name: str, backend: BuildBackend
) -> list[str]:
	"""
	Validate version consistency for Poetry projects.

	Args:
	    data: Parsed pyproject.toml data
	    app_name: Name of the application
	    backend: Detected build backend

	Returns:
	    List of error messages
	"""
	errors = []

	if backend == BuildBackend.POETRY:
		project = data.get("project", {})
		tool_poetry = data.get("tool", {}).get("poetry", {})

		project_version = project.get("version")
		poetry_version = tool_poetry.get("version")

		if project_version and poetry_version and project_version != poetry_version:
			print(
				f"Warning: {app_name}: Version mismatch - "
				f"[project].version = {project_version} but "
				f"[tool.poetry].version = {poetry_version}"
			)

	return errors


def is_bounded(version_spec: str) -> bool:
	"""
	Check if a version specifier string is bounded, i.e. has both
	a lower bound (>= or >) and an upper bound (< or <= or !=).

	Examples:
	    ">=16.0.0-dev <17.0.0-dev"  -> True
	    ">=16.0.0-dev"              -> False (no upper bound)
	    "<17.0.0-dev"               -> False (no lower bound)
	    ""                          -> False
	    "~=16.0"                    -> True (compatible release, implies both bounds)
	    "==16.0.0"                  -> True (exact pin implies both bounds)
	"""
	version_spec = version_spec.strip()
	if not version_spec:
		return False

	if "~=" in version_spec:
		return True

	if "==" in version_spec:
		return True

	has_lower = bool(LOWER_BOUND_PATTERN.search(version_spec))
	has_upper = bool(UPPER_BOUND_PATTERN.search(version_spec))

	return has_lower and has_upper


def validate_frappe_dependencies(data: dict, app_name: str) -> list[str]:
	"""
	Validate that the pyproject.toml declares bounded frappe (and optionally erpnext)
	dependencies under [tool.bench.frappe-dependencies].

	Args:
	    data: Parsed pyproject.toml data
	    app_name: Name of the application

	Returns:
	    List of error messages
	"""
	errors = []

	# Skip frappe-dependencies validation for the frappe framework itself
	if app_name == "frappe":
		return errors

	frappe_deps = data.get("tool", {}).get("bench", {}).get("frappe-dependencies", None)

	if frappe_deps is None:
		errors.append(
			f"{app_name}: pyproject.toml is missing [tool.bench.frappe-dependencies] section. "
			f"See https://discuss.frappe.io/t/versioning-and-custom-app-rules/160252"
		)
		return errors

	# Validate frappe dependency
	if "frappe" not in frappe_deps:
		errors.append(
			f'{app_name}: [tool.bench.frappe-dependencies] must declare a "frappe" dependency. '
			f'Example: frappe = ">=16.0.0-dev <17.0.0-dev"'
		)
	elif not is_bounded(str(frappe_deps["frappe"])):
		errors.append(
			f'{app_name}: frappe dependency "{frappe_deps["frappe"]}" is not bounded. '
			f"It must specify both a lower bound (>=) and an upper bound (<). "
			f'Example: frappe = ">=16.0.0-dev <17.0.0-dev"'
		)

	# Validate erpnext dependency if declared
	if "erpnext" in frappe_deps and not is_bounded(str(frappe_deps["erpnext"])):
		errors.append(
			f'{app_name}: erpnext dependency "{frappe_deps["erpnext"]}" is not bounded. '
			f"It must specify both a lower bound (>=) and an upper bound (<). "
			f'Example: erpnext = ">=16.0.0-dev <17.0.0-dev"'
		)

	return errors


def validate_frappe_project(pyproject_path: pathlib.Path) -> list[str]:
	"""
	Comprehensively validate a Frappe application's pyproject.toml file.

	Validates:
	- TOML syntax
	- Required sections ([project], [build-system])
	- Build backend consistency (flit vs poetry)
	- Rejects hybrid configurations
	- Version consistency (for poetry)
	- Project metadata
	- Frappe dependencies (bounded versions)

	Args:
	    pyproject_path: Path to pyproject.toml file

	Returns:
	    List of error messages (empty if all validations pass)
	"""
	all_errors = []

	# 1. Validate TOML syntax
	data, syntax_errors = validate_toml_syntax(pyproject_path)
	all_errors.extend(syntax_errors)
	if syntax_errors:
		return all_errors  # Can't continue if TOML is invalid

	# Get app name for error messages
	app_name = data.get("project", {}).get("name", pyproject_path.parent.name)

	# 2. Validate required sections
	all_errors.extend(validate_required_sections(data, app_name))
	if all_errors:
		return all_errors  # Can't continue without required sections

	# 3. Detect build backend
	backend = detect_build_backend(data)

	# 4. Validate build system and check for hybrid configurations
	all_errors.extend(validate_build_system(data, app_name, backend))

	# 5. Validate project metadata
	all_errors.extend(validate_project_metadata(data, app_name))

	# 6. Validate version consistency (for Poetry projects)
	all_errors.extend(validate_version_consistency(data, app_name, backend))

	# 7. Validate frappe dependencies (bounded versions)
	all_errors.extend(validate_frappe_dependencies(data, app_name))

	return all_errors


def main(argv: Sequence[str] | None = None):
	parser = argparse.ArgumentParser(
		description="Validate pyproject.toml files for Frappe applications (structure, dependencies, build backend)"
	)
	parser.add_argument("filenames", nargs="*")
	parser.add_argument(
		"--pyproject",
		type=str,
		default=None,
		help="Path to pyproject.toml (auto-detected if not provided)",
	)
	args = parser.parse_args(argv)

	pyproject_path = None
	if args.pyproject:
		pyproject_path = pathlib.Path(args.pyproject)
	else:
		for filename in args.filenames:
			if pathlib.Path(filename).name == "pyproject.toml":
				pyproject_path = pathlib.Path(filename).resolve()
				break

	if pyproject_path is None:
		pyproject_path = find_pyproject_toml()

	if pyproject_path is None or not pyproject_path.exists():
		print("Error: pyproject.toml not found")
		sys.exit(1)

	errors = validate_frappe_project(pyproject_path)

	if errors:
		for error in errors:
			print(f"\n❌ {error}")
		print()
		sys.exit(1)

	sys.exit(0)
