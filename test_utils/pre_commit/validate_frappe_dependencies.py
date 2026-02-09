import argparse
import pathlib
import re
import sys
from collections.abc import Sequence

import toml

LOWER_BOUND_PATTERN = re.compile(r">=?\s*\S+")
UPPER_BOUND_PATTERN = re.compile(r"<=?\s*\S+|!=\s*\S+")


def find_pyproject_toml():
	current = pathlib.Path().resolve()
	for path in [current] + list(current.parents):
		candidate = path / "pyproject.toml"
		if candidate.exists():
			return candidate
	return None


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


def validate_frappe_dependencies(pyproject_path: pathlib.Path) -> list[str]:
	"""
	Validate that the pyproject.toml declares bounded frappe (and optionally erpnext)
	dependencies under [tool.bench.frappe-dependencies].

	Returns a list of error messages. Empty list means all validations passed.
	"""
	errors = []

	try:
		with open(pyproject_path) as f:
			data = toml.load(f)
	except Exception as e:
		errors.append(f"Failed to parse {pyproject_path}: {e}")
		return errors

	app_name = data.get("project", {}).get("name", pyproject_path.parent.name)
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


def main(argv: Sequence[str] | None = None):
	parser = argparse.ArgumentParser(
		description="Validate that pyproject.toml declares bounded frappe/erpnext dependencies"
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

	errors = validate_frappe_dependencies(pyproject_path)

	if errors:
		for error in errors:
			print(f"\n {error}")
		print()
		sys.exit(1)

	sys.exit(0)
