import argparse
import pathlib
import sys
from collections.abc import Sequence

import toml


def get_dependencies(app):
	apps_dir = pathlib.Path().resolve().parent
	if pathlib.Path.exists(apps_dir / app / "pyproject.toml"):
		with open(apps_dir / app / "pyproject.toml") as f:
			return toml.load(f)


def get_versions(app):
	pyproject_toml = get_dependencies(app)
	if not pyproject_toml:
		return {}

	if pyproject_toml.get("build-system", {}).get("build-backend") == "flit_core.buildapi":
		dependencies = pyproject_toml.get("project", {}).get("dependencies", [])
	elif (
		pyproject_toml.get("build-system", {}).get("build-backend")
		== "poetry.core.masonry.api"
	):
		dependencies = (
			pyproject_toml.get("tool", {}).get("poetry", {}).get("dependencies", [])
		)

	dependency_objects = {}
	for dep in dependencies:
		if "==" in dep:
			package, version = dep.split("==")
		elif "~=" in dep:
			package, version = dep.split("~=")
			version = "~" + version
		elif ">=" in dep:
			package, version = dep.split(">=")
			version = ">=" + version
		elif "<=" in dep:
			package, version = dep.split("<=")
			version = "<=" + version
		else:
			package = dep
			version = ""

		dependency_objects[package] = version

	return dependency_objects


def get_mismatched_versions():
	apps_order = pathlib.Path().resolve().parent.parent / "sites" / "apps.txt"
	apps_order = apps_order.read_text().split("\n")
	exceptions = []
	app_packages = {app: get_versions(app) for app in apps_order}
	for app, packages in app_packages.items():
		for package, package_version in packages.items():
			for app2, app2_packages in app_packages.items():
				if app == app2:
					continue

				if package in app2_packages and app2_packages[package] != package_version:
					# Check if exception already exists
					existing_exception = next(
						(exception for exception in exceptions if package in exception), None
					)
					if existing_exception:
						existing_exception[package][app] = package_version
					else:
						exceptions.append({package: {app: package_version}})
	return exceptions


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	args = parser.parse_args(argv)

	exceptions = get_mismatched_versions()
	if exceptions:
		for exception in exceptions:
			for package, apps in exception.items():
				print(f"\nVersion mismatch for {package} in:")
				for app, version in apps.items():
					print(f"{app}: {version}")

	sys.exit(1) if exceptions else sys.exit(0)
