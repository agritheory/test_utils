"""Validate template paths referenced via frappe.get_template / render_template / {% include %}."""

import re
from dataclasses import dataclass, field
from pathlib import Path

GET_TEMPLATE_RE = re.compile(
	r"""frappe\.(?:get_template|render_template)\s*\(\s*['"]([^'"]+)['"]""",
)
JINJA_INCLUDE_RE = re.compile(
	r"""\{%-?\s*(?:include|extends)\s+['"]([^'"]+)['"]""",
)

TEMPLATE_EXTENSIONS = {".html", ".jinja", ".jinja2", ".j2"}
SKIP_DIRS = {"node_modules", "__pycache__", "dist", ".git"}


@dataclass
class JinjaValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	calls_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"calls_checked": self.calls_checked,
		}


def should_skip(filepath: Path) -> bool:
	return any(part in SKIP_DIRS for part in filepath.parts)


def resolve_template_path(
	template_path: str,
	app_path: Path,
	dependency_paths: list[Path] | None = None,
) -> Path | None:
	"""Locate a Frappe template across the app and dependency apps."""
	for root in [app_path] + list(dependency_paths or []):
		app_name = root.name
		for candidate in (root / app_name / template_path, root / template_path):
			if candidate.exists():
				return candidate
	return None


def looks_like_file_path(s: str) -> bool:
	if not s or any(c in s for c in ("$", "`", "{")):
		return False
	return "/" in s or any(s.endswith(ext) for ext in TEMPLATE_EXTENSIONS)


class JinjaValidator:
	"""
	Validate that template paths referenced in Python and HTML files exist on disk.

	Checks ``frappe.get_template``, ``frappe.render_template``, ``{% include %}``,
	and ``{% extends %}`` — across the app and all dependency apps.
	"""

	def __init__(self, app_path: Path, dependency_paths: list[Path] | None = None) -> None:
		self.app_path = app_path
		self.dependency_paths = list(dependency_paths or [])

	def validate(self) -> JinjaValidationResult:
		result = JinjaValidationResult()
		self.check_python_files(result)
		self.check_template_files(result)
		return result

	def check_python_files(self, result: JinjaValidationResult) -> None:
		for filepath in self.app_path.rglob("*.py"):
			if should_skip(filepath):
				continue
			try:
				content = filepath.read_text(encoding="utf-8", errors="replace")
			except OSError:
				continue
			if "frappe.get_template" not in content and "frappe.render_template" not in content:
				continue

			for lineno, line in enumerate(content.splitlines(), 1):
				if "frappe-vulture:ignore" in line:
					continue
				for m in GET_TEMPLATE_RE.finditer(line):
					tpath = m.group(1)
					if not looks_like_file_path(tpath):
						continue
					result.calls_checked += 1
					if resolve_template_path(tpath, self.app_path, self.dependency_paths) is None:
						result.errors.append(f"{filepath}:{lineno}: template '{tpath}' not found")

	def check_template_files(self, result: JinjaValidationResult) -> None:
		for ext in TEMPLATE_EXTENSIONS:
			for filepath in self.app_path.rglob(f"*{ext}"):
				if should_skip(filepath):
					continue
				try:
					content = filepath.read_text(encoding="utf-8", errors="replace")
				except OSError:
					continue

				for lineno, line in enumerate(content.splitlines(), 1):
					if "frappe-vulture:ignore" in line:
						continue
					for m in JINJA_INCLUDE_RE.finditer(line):
						tpath = m.group(1)
						if not looks_like_file_path(tpath):
							continue
						result.calls_checked += 1
						if resolve_template_path(tpath, self.app_path, self.dependency_paths) is None:
							result.errors.append(f"{filepath}:{lineno}: template '{tpath}' not found")
