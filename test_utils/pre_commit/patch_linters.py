#!/usr/bin/env python3
"""
Patch Linter - Validates and manages patch files for ERPNext/Frappe applications

Usage:
    python patch_linter.py --app inventory_tools [--check]
    python patch_linter.py --app inventory_tools --list

For pre-commit (run from bench root):
    python patch_linter.py --app inventory_tools --check
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Set, Dict, Tuple


class PatchLinter:
	def __init__(self, app_name: str):
		self.app_name = app_name
		self.base_path = self._find_app_base_path()

		if self.base_path:
			self.patches_dir = self.base_path / "patches"
			self.patches_txt_path = self.base_path / "patches.txt"
		else:
			self.patches_dir = None
			self.patches_txt_path = None

		self.errors: list[str] = []
		self.warnings: list[str] = []

	def _find_app_base_path(self) -> Path:
		"""Find the app base path for ERPNext custom apps"""
		possible_paths = [
			Path(f"apps/{self.app_name}/{self.app_name}"),
			Path(f"{self.app_name}/{self.app_name}"),
			Path(self.app_name),
		]

		for path in possible_paths:
			if path.exists() and (path / "patches").exists():
				return path

		return None

	def find_patch_files(self) -> dict[str, list[str]]:
		"""Recursively find all patch files in the patches directory"""
		patches = {}

		if not self.patches_dir or not self.patches_dir.exists():
			self.errors.append(f"Patches directory not found for app: {self.app_name}")
			return patches

		for py_file in self.patches_dir.rglob("*.py"):
			if py_file.name == "__init__.py":
				continue

			rel_path = py_file.relative_to(self.patches_dir)

			module_parts = [self.app_name, "patches"]
			if rel_path.parent != Path("."):
				module_parts.extend(rel_path.parent.parts)
			module_parts.append(rel_path.stem)

			module_path = ".".join(module_parts)

			subfolder = str(rel_path.parent) if rel_path.parent != Path(".") else "root"

			if subfolder not in patches:
				patches[subfolder] = []
			patches[subfolder].append(module_path)

		return patches

	def parse_patches_txt(self) -> tuple[list[str], list[str]]:
		"""Parse patches.txt and return active and commented patches"""
		active_patches = []
		commented_patches = []

		if not self.patches_txt_path or not self.patches_txt_path.exists():
			self.warnings.append(f"patches.txt not found at: {self.patches_txt_path}")
			return active_patches, commented_patches

		with open(self.patches_txt_path) as f:
			in_section = False
			has_sections = False
			current_section = None

			content = f.read()
			f.seek(0)
			has_sections = "[pre-model]" in content or "[post-model]" in content

			for line_num, line in enumerate(f, 1):
				original_line = line
				line = line.strip()

				if not line:
					continue

				if line.startswith("[") and line.endswith("]"):
					current_section = line[1:-1]
					in_section = current_section in ["pre-model", "post-model"]
					continue

				if has_sections and not in_section:
					continue

				if line.startswith("#"):
					patch_name = line.lstrip("#").strip()
					if "#" in patch_name:
						patch_name = patch_name.split("#")[0].strip()
					if patch_name and not patch_name.startswith("["):
						commented_patches.append(patch_name)
				else:
					patch_name = line.split("#")[0].strip()
					if patch_name:
						active_patches.append(patch_name)

		return active_patches, commented_patches

	def validate_patch_file_exists(self, patch_name: str) -> bool:
		"""Check if a patch file exists on disk"""
		if not self.base_path:
			return False

		parts = patch_name.split(".")

		if len(parts) >= 2 and parts[0] == self.app_name and parts[1] == "patches":
			parts = parts[2:]

		file_path = self.patches_dir / Path(*parts[:-1]) / f"{parts[-1]}.py"
		return file_path.exists()

	def lint(self) -> bool:
		"""Run linting checks on patch configuration"""
		print(f"Linting patches for ERPNext app: {self.app_name}")

		if not self.base_path:
			print(f"ERROR: Could not find app structure for: {self.app_name}")
			print(f"       Tried looking in: apps/{self.app_name}/{self.app_name}")
			return False

		print(f"App location: {self.base_path}")
		print(f"Patches dir: {self.patches_dir}")
		print(f"patches.txt: {self.patches_txt_path}\n")

		all_patches = self.find_patch_files()

		if self.errors:
			for error in self.errors:
				print(f"ERROR: {error}")
			return False

		if not all_patches:
			print("WARNING: No patch files found")
			return True

		print(f"Found patches in {len(all_patches)} location(s):")
		total_patches = 0
		for folder, patches in sorted(all_patches.items()):
			print(f"  - {folder}: {len(patches)} patch(es)")
			total_patches += len(patches)
		print(f"  Total: {total_patches} patch file(s)\n")

		active_patches, commented_patches = self.parse_patches_txt()

		print(f"Active patches in patches.txt: {len(active_patches)}")
		print(f"Commented patches: {len(commented_patches)}\n")

		if active_patches:
			print("Validating active patches...")
			for patch in active_patches:
				if not self.validate_patch_file_exists(patch):
					self.errors.append(f"Active patch file not found: {patch}")
					print(f"  [FAIL] {patch}")
				else:
					print(f"  [OK] {patch}")
			print()

		all_patch_modules = set()
		for patches in all_patches.values():
			all_patch_modules.update(patches)

		listed_patches = set(active_patches + commented_patches)
		unlisted_patches = all_patch_modules - listed_patches

		if unlisted_patches:
			print("WARNING: Patches not listed in patches.txt:")
			for patch in sorted(unlisted_patches):
				print(f"  - {patch}")
				self.warnings.append(f"Patch exists but not listed: {patch}")
			print()

		if self.errors:
			print(f"FAILED: Linting failed with {len(self.errors)} error(s):")
			for error in self.errors:
				print(f"  - {error}")
		else:
			print("PASSED: All patches validated!")

		if self.warnings:
			print(f"\nWARNINGS: {len(self.warnings)} warning(s):")
			for warning in self.warnings:
				print(f"  - {warning}")

		return len(self.errors) == 0

	def list_patches(self):
		"""List all available patches with their status"""
		if not self.base_path:
			print(f"ERROR: Could not find app structure for: {self.app_name}")
			return

		all_patches = self.find_patch_files()
		active_patches, commented_patches = self.parse_patches_txt()

		print(f"All patches for {self.app_name}:\n")
		print(f"Location: {self.patches_dir}\n")

		if not all_patches:
			print("No patches found")
			return

		for folder in sorted(all_patches.keys()):
			print(f"[{folder}]")
			for patch in sorted(all_patches[folder]):
				status = (
					"[ACTIVE]"
					if patch in active_patches
					else "[COMMENTED]"
					if patch in commented_patches
					else "[UNLISTED]"
				)
				print(f"  {status} {patch}")
			print()


def main():
	parser = argparse.ArgumentParser(
		description="Lint and validate patch files for ERPNext/Frappe applications"
	)
	parser.add_argument(
		"--app", required=True, help="Application name (e.g., inventory_tools)"
	)
	parser.add_argument(
		"--check", action="store_true", help="Run linting checks (exit code 1 on failure)"
	)
	parser.add_argument(
		"--list", action="store_true", help="List all patches with their status"
	)

	args = parser.parse_args()

	if not args.list:
		args.check = True

	linter = PatchLinter(args.app)

	if args.list:
		linter.list_patches()
		sys.exit(0)
	elif args.check:
		success = linter.lint()
		sys.exit(0 if success else 1)


if __name__ == "__main__":
	main()
