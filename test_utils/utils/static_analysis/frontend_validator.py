"""Validate frappe.call / frappe.xcall paths found in JS/TS/Vue/JSX/TSX files."""

import re
from dataclasses import dataclass, field
from pathlib import Path

from .path_resolver import PathResolver, is_potential_dotted_path, is_dynamic_path

FRONTEND_EXTENSIONS = {".js", ".ts", ".vue", ".jsx", ".tsx"}

DIRECT_CALL_RE = re.compile(
	r"""frappe\.x?call\s*\(\s*['"]([A-Za-z_][A-Za-z0-9_.]+)['"]""",
	re.VERBOSE,
)
METHOD_KEY_RE = re.compile(r"""\bmethod\s*:\s*['"]([A-Za-z_][A-Za-z0-9_.]+)['"]""")
JS_COMMENT_RE = re.compile(r"//.*$")


@dataclass
class FrontendValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	calls_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"calls_checked": self.calls_checked,
		}


def extract_calls(content: str, filepath: Path) -> list[tuple[str, int]]:
	"""Return (dotted_path, line_number) for every frappe.call / frappe.xcall found."""
	file_uses_frappe = "frappe.call" in content or "frappe.xcall" in content

	found: list[tuple[str, int]] = []
	seen: set[tuple[str, int]] = set()

	for lineno, raw_line in enumerate(content.splitlines(), 1):
		if "frappe-vulture:ignore" in raw_line:
			continue

		code_line = JS_COMMENT_RE.sub("", raw_line)

		for m in DIRECT_CALL_RE.finditer(code_line):
			path = m.group(1)
			if not is_dynamic_path(path) and is_potential_dotted_path(path):
				key = (path, lineno)
				if key not in seen:
					seen.add(key)
					found.append(key)

		if file_uses_frappe:
			for m in METHOD_KEY_RE.finditer(code_line):
				path = m.group(1)
				if not is_dynamic_path(path) and is_potential_dotted_path(path):
					key = (path, lineno)
					if key not in seen:
						seen.add(key)
						found.append(key)

	return found


class FrontendValidator:
	"""
	Validate every ``frappe.call`` / ``frappe.xcall`` path in JS/TS/Vue source files.

	Errors when the target Python function does not exist or lacks ``@frappe.whitelist()``.
	"""

	def __init__(self, resolver: PathResolver) -> None:
		self.resolver = resolver

	def validate(self, app_path: Path) -> FrontendValidationResult:
		result = FrontendValidationResult()

		for ext in FRONTEND_EXTENSIONS:
			for filepath in app_path.rglob(f"*{ext}"):
				if "node_modules" in filepath.parts or "dist" in filepath.parts:
					continue

				try:
					content = filepath.read_text(encoding="utf-8", errors="replace")
				except OSError:
					continue

				for path, lineno in extract_calls(content, filepath):
					resolved = self.resolver.resolve(path)
					result.calls_checked += 1
					loc = f"{filepath}:{lineno}"

					if not resolved.exists:
						result.errors.append(f"{loc}: '{path}' — {resolved.error}")
					elif resolved.kind == "whitelisted":
						pass
					elif not resolved.is_whitelisted:
						result.errors.append(
							f"{loc}: '{path}' exists but is not decorated with @frappe.whitelist()"
						)

		return result
