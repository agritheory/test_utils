"""Validate dotted Python paths referenced inside a Frappe app's hooks.py."""

import ast
from dataclasses import dataclass, field
from pathlib import Path

from .path_resolver import PathResolver, is_potential_dotted_path

NON_PATH_KEYS: frozenset[str] = frozenset(
	{
		"app_include_js",
		"app_include_css",
		"web_include_js",
		"web_include_css",
		"website_theme_scss",
		"webform_include_js",
		"webform_include_css",
		"page_js",
		"doctype_js",
		"doctype_list_js",
		"doctype_tree_js",
		"doctype_calendar_js",
		"website_route_rules",
		"portal_menu_items",
		"website_redirects",
		"home_page",
		"role_home_page",
		"website_generators",
		"app_name",
		"app_title",
		"app_publisher",
		"app_description",
		"app_email",
		"app_license",
		"app_logo_url",
		"app_version",
		"required_apps",
		"fixtures",
		"default_mail_footer",
		"calendars",
		"domains",
		"export_python_ignore_modules",
		"ignore_links_on_delete",
		"auto_cancel_exempted_doctypes",
	}
)


@dataclass
class HooksValidationResult:
	app_name: str
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	paths_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"app_name": self.app_name,
			"errors": self.errors,
			"warnings": self.warnings,
			"paths_checked": self.paths_checked,
		}


class PathExtractor(ast.NodeVisitor):
	"""Walk a hooks.py AST and collect (path, lineno, context) triples."""

	def __init__(self) -> None:
		self.found: list[tuple[str, int, str]] = []
		self.top_key: str | None = None

	def visit_Module(self, node: ast.Module) -> None:
		for stmt in node.body:
			self.visit_top_level(stmt)

	def visit_top_level(self, node: ast.stmt) -> None:
		if isinstance(node, ast.Assign):
			for target in node.targets:
				if isinstance(target, ast.Name):
					old = self.top_key
					self.top_key = target.id
					self.generic_visit(node)
					self.top_key = old
					return
		elif isinstance(node, ast.AugAssign):
			if isinstance(node.target, ast.Name):
				old = self.top_key
				self.top_key = node.target.id
				self.generic_visit(node)
				self.top_key = old
				return
		self.generic_visit(node)

	def visit_Constant(self, node: ast.Constant) -> None:
		if self.top_key in NON_PATH_KEYS:
			return
		if isinstance(node.value, str) and is_potential_dotted_path(node.value):
			self.found.append((node.value, node.lineno, self.top_key or "hooks"))


def find_hooks_file(app_path: Path) -> Path | None:
	candidate = app_path / app_path.name / "hooks.py"
	if candidate.exists():
		return candidate
	for subdir in sorted(app_path.iterdir()):
		if subdir.is_dir() and (subdir / "hooks.py").exists():
			return subdir / "hooks.py"
	direct = app_path / "hooks.py"
	if direct.exists():
		return direct
	return None


class HooksValidator:
	"""Validate all dotted-path strings found in a Frappe app's hooks.py."""

	def __init__(self, resolver: PathResolver) -> None:
		self.resolver = resolver

	def validate(self, app_path: Path) -> HooksValidationResult:
		app_name = app_path.name
		result = HooksValidationResult(app_name=app_name)

		hooks_file = find_hooks_file(app_path)
		if hooks_file is None:
			result.warnings.append(f"hooks.py not found under '{app_path}'")
			return result

		try:
			source = hooks_file.read_text(encoding="utf-8")
			tree = ast.parse(source, filename=str(hooks_file))
		except SyntaxError as exc:
			result.errors.append(f"{hooks_file}: syntax error: {exc}")
			return result

		extractor = PathExtractor()
		extractor.visit(tree)

		for path, lineno, context in extractor.found:
			resolved = self.resolver.resolve(path)
			result.paths_checked += 1
			loc = f"{hooks_file}:{lineno}"
			if not resolved.exists:
				result.errors.append(f"{loc} [{context}]: '{path}' — {resolved.error}")
			elif resolved.kind in (
				"function",
				"async_function",
				"class",
				"attribute",
				"whitelisted",
			):
				pass
			else:
				result.warnings.append(
					f"{loc} [{context}]: '{path}' resolves to a module, not a callable"
				)

		return result
