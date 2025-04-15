import argparse
import json
import pathlib
import sys
from collections.abc import Sequence


def get_package_json(app):
	apps_dir = pathlib.Path().resolve().parent
	if pathlib.Path.exists(apps_dir / app / "package.json"):
		with open(apps_dir / app / "package.json") as f:
			return json.load(f)


def get_versions(app):
	package_json = get_package_json(app)
	if package_json:
		return {
			**package_json.get("dependencies", {}),
			**package_json.get("devDependencies", {}),
		}


def get_mismatched_versions():
	apps_order = pathlib.Path().resolve().parent.parent / "sites" / "apps.txt"
	apps_order = apps_order.read_text().split("\n")
	exceptions = []
	app_packages = {app: get_versions(app) for app in apps_order}
	for app, packages in app_packages.items():
		if not packages:
			continue

		for package, package_version in packages.items():
			for app2, app2_packages in app_packages.items():
				if not app2_packages:
					continue

				if app == app2:
					continue

				if package in app2_packages and app2_packages[package] != package_version:
					# Check if exception already exists
					existing_exception = next(
						(exception for exception in exceptions if package in exception),
						None,
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
