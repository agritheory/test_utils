"""Resolve dotted Python paths to source locations via AST, without importing."""

import ast
import re
from dataclasses import dataclass
from pathlib import Path

import toml

IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
DOTTED_PATH_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)+$")

WEB_EXTENSIONS = frozenset(
	"js mjs cjs jsx ts tsx css less scss sass vue html htm json jsonc "
	"wasm map svg png jpg jpeg gif webp ico ttf woff woff2 eot".split()
)


def is_potential_dotted_path(s: str) -> bool:
	"""Return True if *s* looks like a dotted Python module path."""
	if not isinstance(s, str) or len(s) > 300:
		return False
	if "." not in s:
		return False
	if s.startswith(("/", "http://", "https://", "ws://", "wss://")):
		return False
	if not DOTTED_PATH_RE.match(s):
		return False
	if s.rsplit(".", 1)[-1].lower() in WEB_EXTENSIONS:
		return False
	return True


def is_dynamic_path(s: str) -> bool:
	"""Return True if *s* contains template/interpolation markers."""
	return any(c in s for c in ("$", "`", "{", " "))


@dataclass
class ResolvedPath:
	dotted_path: str
	exists: bool
	kind: str | None = (
		None  # "function", "async_function", "class", "attribute", "module", "whitelisted"
	)
	location: tuple[Path, int] | None = None
	is_whitelisted: bool = False
	error: str | None = None


def load_whitelist(app_path: Path) -> list[str]:
	for candidate in (app_path / "pyproject.toml", app_path.parent / "pyproject.toml"):
		if candidate.exists():
			try:
				data = toml.load(candidate)
				return (
					data.get("tool", {})
					.get("test_utils", {})
					.get("static-analysis", {})
					.get("whitelist", [])
				)
			except Exception:
				pass
	return []


class PathResolver:
	"""Resolve dotted Python paths to source files via filesystem + AST traversal."""

	def __init__(self, app_paths: list[Path], whitelist: list[str] | None = None):
		self.app_paths = list(app_paths)
		wl = set(whitelist or [])
		self.exact_whitelist: set[str] = {p for p in wl if not p.endswith(".*")}
		self.prefix_whitelist: list[str] = [p[:-2] for p in wl if p.endswith(".*")]

	@classmethod
	def from_app(
		cls, app_path: Path, dependency_paths: list[Path] | None = None
	) -> "PathResolver":
		"""Create a resolver, loading the whitelist from the app's pyproject.toml."""
		whitelist = load_whitelist(app_path)
		return cls([app_path] + (dependency_paths or []), whitelist=whitelist)

	def is_whitelisted(self, dotted_path: str) -> bool:
		if dotted_path in self.exact_whitelist:
			return True
		return any(
			dotted_path == prefix or dotted_path.startswith(prefix + ".")
			for prefix in self.prefix_whitelist
		)

	def find_module_file(self, parts: list[str]) -> tuple[Path | None, int]:
		for i in range(len(parts), 0, -1):
			module_parts = parts[:i]
			for app_path in self.app_paths:
				pkg = app_path.joinpath(*module_parts) / "__init__.py"
				if pkg.exists():
					return pkg, i
				if len(module_parts) > 1:
					mod = app_path.joinpath(*module_parts[:-1]) / (module_parts[-1] + ".py")
				else:
					mod = app_path / (module_parts[0] + ".py")
				if mod.exists():
					return mod, i
		return None, 0

	def has_whitelist_decorator(
		self, node: ast.FunctionDef | ast.AsyncFunctionDef
	) -> bool:
		for dec in node.decorator_list:
			if isinstance(dec, ast.Call):
				func = dec.func
				if isinstance(func, ast.Attribute) and func.attr == "whitelist":
					return True
				if isinstance(func, ast.Name) and func.id == "whitelist":
					return True
			elif isinstance(dec, ast.Attribute) and dec.attr == "whitelist":
				return True
			elif isinstance(dec, ast.Name) and dec.id == "whitelist":
				return True
		return False

	def find_in_ast(
		self, tree: ast.Module, attr_parts: list[str]
	) -> tuple[str | None, int | None, bool]:
		"""Locate the first element of *attr_parts* inside *tree*. Returns (kind, lineno, is_whitelisted)."""
		if not attr_parts:
			return "module", 1, False
		name = attr_parts[0]
		for node in ast.walk(tree):
			if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
				kind = "async_function" if isinstance(node, ast.AsyncFunctionDef) else "function"
				return kind, node.lineno, self.has_whitelist_decorator(node)
			if isinstance(node, ast.ClassDef) and node.name == name:
				return "class", node.lineno, False
			if isinstance(node, ast.Assign):
				for target in node.targets:
					if isinstance(target, ast.Name) and target.id == name:
						return "attribute", node.lineno, False
		return None, None, False

	def resolve(self, dotted_path: str) -> ResolvedPath:
		"""Resolve *dotted_path* to a :class:`ResolvedPath` without importing."""
		if self.is_whitelisted(dotted_path):
			return ResolvedPath(
				dotted_path=dotted_path,
				exists=True,
				kind="whitelisted",
				is_whitelisted=True,
			)

		if not is_potential_dotted_path(dotted_path):
			return ResolvedPath(
				dotted_path=dotted_path,
				exists=False,
				error=f"'{dotted_path}' is not a valid dotted Python path",
			)

		parts = dotted_path.split(".")
		file_path, module_end = self.find_module_file(parts)
		if file_path is None:
			return ResolvedPath(
				dotted_path=dotted_path,
				exists=False,
				error=f"Cannot find module for '{dotted_path}'",
			)

		attr_parts = parts[module_end:]

		try:
			source = file_path.read_text(encoding="utf-8")
			tree = ast.parse(source, filename=str(file_path))
		except SyntaxError as exc:
			return ResolvedPath(
				dotted_path=dotted_path,
				exists=False,
				error=f"Syntax error in {file_path}: {exc}",
			)

		kind, lineno, is_wl = self.find_in_ast(tree, attr_parts)
		if kind is None:
			attr_name = attr_parts[0] if attr_parts else dotted_path
			return ResolvedPath(
				dotted_path=dotted_path,
				exists=False,
				error=f"'{attr_name}' not found in {file_path}",
			)

		return ResolvedPath(
			dotted_path=dotted_path,
			exists=True,
			kind=kind,
			location=(file_path, lineno),
			is_whitelisted=is_wl,
		)
