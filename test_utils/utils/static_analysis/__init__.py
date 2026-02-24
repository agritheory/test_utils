"""
Static analysis for Frappe apps.

Public API::

    from test_utils.utils.static_analysis import StaticAnalyzer, StaticAnalysisConfig, analyze_app

    result = analyze_app("/path/to/my_app")
    if result.has_errors:
        for msg in result.all_errors:
            print(msg)
"""

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path

import toml

from .frontend_validator import FrontendValidationResult, FrontendValidator
from .hooks_validator import HooksValidationResult, HooksValidator
from .jinja_validator import JinjaValidationResult, JinjaValidator
from .orphan_detector import OrphanDetector, OrphanResult
from .patches_validator import PatchesValidationResult, PatchesValidator
from .path_resolver import PathResolver
from .python_call_validator import PythonCallValidationResult, PythonCallValidator
from .report_validator import ReportValidationResult, ReportValidator

__all__ = [
	"StaticAnalysisConfig",
	"StaticAnalysisResult",
	"StaticAnalyzer",
	"analyze_app",
]


@dataclass
class StaticAnalysisConfig:
	validate_hooks: bool = True
	validate_patches: bool = True
	validate_frontend: bool = True
	validate_python_calls: bool = True
	validate_jinja: bool = True
	validate_reports: bool = True
	detect_orphans: bool = True
	min_confidence: int = 80
	ignore_patterns: list[str] = field(default_factory=list)
	ignore_paths: list[str] = field(default_factory=list)


@dataclass
class StaticAnalysisResult:
	app_path: Path
	hooks_result: HooksValidationResult | None = None
	patches_result: PatchesValidationResult | None = None
	frontend_result: FrontendValidationResult | None = None
	python_call_result: PythonCallValidationResult | None = None
	jinja_result: JinjaValidationResult | None = None
	report_result: ReportValidationResult | None = None
	orphan_result: OrphanResult | None = None

	@property
	def has_errors(self) -> bool:
		for r in (
			self.hooks_result,
			self.patches_result,
			self.frontend_result,
			self.python_call_result,
			self.jinja_result,
		):
			if r is not None and r.errors:
				return True
		if self.orphan_result is not None:
			if self.orphan_result.error or self.orphan_result.unreachable:
				return True
		return False

	@property
	def all_errors(self) -> list[str]:
		msgs: list[str] = []
		for r in (
			self.hooks_result,
			self.patches_result,
			self.frontend_result,
			self.python_call_result,
			self.jinja_result,
		):
			if r is not None:
				msgs.extend(r.errors)
		if self.orphan_result is not None:
			if self.orphan_result.error:
				msgs.append(f"orphan_detector: {self.orphan_result.error}")
			msgs.extend(str(item) for item in self.orphan_result.unreachable)
		return msgs

	@property
	def all_warnings(self) -> list[str]:
		msgs: list[str] = []
		for r in (
			self.hooks_result,
			self.patches_result,
			self.frontend_result,
			self.python_call_result,
			self.jinja_result,
			self.report_result,
		):
			if r is not None:
				msgs.extend(r.warnings)
		return msgs

	def to_dict(self) -> dict:
		def r(obj) -> dict | None:
			return obj.to_dict() if obj is not None else None

		return {
			"app_path": str(self.app_path),
			"has_errors": self.has_errors,
			"hooks": r(self.hooks_result),
			"patches": r(self.patches_result),
			"frontend": r(self.frontend_result),
			"python_calls": r(self.python_call_result),
			"jinja": r(self.jinja_result),
			"reports": r(self.report_result),
			"orphans": r(self.orphan_result),
		}

	def to_json(self, indent: int = 2) -> str:
		return json.dumps(self.to_dict(), indent=indent)


def collect_whitelisted_functions(app_path: Path) -> list[str]:
	"""Return dotted paths for every ``@frappe.whitelist()`` function in the app."""
	app_name = app_path.name
	results: list[str] = []

	for py_file in app_path.rglob("*.py"):
		if any(p in py_file.parts for p in ("__pycache__", "node_modules")):
			continue
		try:
			source = py_file.read_text(encoding="utf-8", errors="replace")
		except OSError:
			continue
		if "whitelist" not in source:
			continue

		try:
			tree = ast.parse(source, filename=str(py_file))
		except SyntaxError:
			continue

		for node in ast.walk(tree):
			if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
				continue
			for dec in node.decorator_list:
				is_wl = (
					(
						isinstance(dec, ast.Call)
						and isinstance(dec.func, ast.Attribute)
						and dec.func.attr == "whitelist"
					)
					or (
						isinstance(dec, ast.Call)
						and isinstance(dec.func, ast.Name)
						and dec.func.id == "whitelist"
					)
					or (isinstance(dec, ast.Attribute) and dec.attr == "whitelist")
					or (isinstance(dec, ast.Name) and dec.id == "whitelist")
				)
				if is_wl:
					try:
						rel = py_file.relative_to(app_path)
					except ValueError:
						continue
					parts = list(rel.with_suffix("").parts)
					if parts and parts[-1] == "__init__":
						parts = parts[:-1]
					results.append(".".join(parts) + "." + node.name)
					break

	return results


def collect_doctype_controllers(app_path: Path) -> list[str]:
	"""Return dotted module paths for every Frappe doctype controller."""
	app_name = app_path.name
	controllers: list[str] = []
	doctype_root = app_path / app_name / "doctype"
	if not doctype_root.is_dir():
		return controllers
	for doctype_dir in doctype_root.iterdir():
		if not doctype_dir.is_dir():
			continue
		if (doctype_dir / (doctype_dir.name + ".py")).exists():
			controllers.append(f"{app_name}.doctype.{doctype_dir.name}.{doctype_dir.name}")
	return controllers


def read_required_apps(app_path: Path) -> list[str]:
	"""Return app names listed in ``required_apps`` in hooks.py."""
	for hooks_file in (app_path / app_path.name / "hooks.py", app_path / "hooks.py"):
		if not hooks_file.exists():
			continue
		try:
			tree = ast.parse(hooks_file.read_text(encoding="utf-8"))
		except Exception:
			continue
		for node in ast.walk(tree):
			if not isinstance(node, ast.Assign):
				continue
			for target in node.targets:
				if isinstance(target, ast.Name) and target.id == "required_apps":
					if isinstance(node.value, ast.List):
						return [
							elt.value
							for elt in node.value.elts
							if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
						]
	return []


def discover_dependency_paths(app_path: Path, extra: list[Path]) -> list[Path]:
	"""
	Auto-discover dependency app paths for the resolver.

	Checks the bench ``apps/`` sibling directory for ``frappe`` (always) and
	any app declared in ``required_apps`` in hooks.py. Frappe's ``owner/app``
	format is handled by taking only the trailing app name.
	"""
	seen: set[Path] = set()
	result: list[Path] = []

	def add(p: Path) -> None:
		p = p.resolve()
		if p not in seen and p.is_dir():
			seen.add(p)
			result.append(p)

	for p in extra:
		add(p)

	apps_dir = app_path.parent
	if apps_dir.name != "apps":
		return result

	required = {name.rsplit("/", 1)[-1] for name in read_required_apps(app_path)}
	for name in sorted({"frappe"} | required):
		candidate = apps_dir / name
		if candidate.is_dir():
			add(candidate)

	return result


class StaticAnalyzer:
	"""
	Orchestrate all static-analysis passes for a single Frappe app.

	Parameters
	----------
	app_path:
	    Root directory of the Frappe app (contains ``pyproject.toml``).
	dependency_paths:
	    Additional directories to search when resolving dotted paths.
	config:
	    Analysis configuration. Defaults to :class:`StaticAnalysisConfig`.
	"""

	def __init__(
		self,
		app_path: Path,
		dependency_paths: list[Path] | None = None,
		config: StaticAnalysisConfig | None = None,
	) -> None:
		self.app_path = Path(app_path).resolve()
		self.dependency_paths = discover_dependency_paths(
			self.app_path,
			[Path(p).resolve() for p in (dependency_paths or [])],
		)
		self.config = config or StaticAnalysisConfig()

	def analyze(self) -> StaticAnalysisResult:
		result = StaticAnalysisResult(app_path=self.app_path)
		resolver = PathResolver.from_app(self.app_path, self.dependency_paths)

		if self.config.validate_hooks:
			result.hooks_result = HooksValidator(resolver).validate(self.app_path)

		if self.config.validate_patches:
			result.patches_result = PatchesValidator(resolver).validate(self.app_path)

		if self.config.validate_frontend:
			result.frontend_result = FrontendValidator(resolver).validate(self.app_path)

		if self.config.validate_python_calls:
			result.python_call_result = PythonCallValidator(resolver).validate(self.app_path)

		if self.config.validate_jinja:
			result.jinja_result = JinjaValidator(self.app_path, self.dependency_paths).validate()

		if self.config.validate_reports:
			result.report_result = ReportValidator(self.app_path).validate()

		if self.config.detect_orphans:
			from .hooks_validator import PathExtractor

			entry_points: list[str] = []
			entry_points.extend(collect_whitelisted_functions(self.app_path))
			entry_points.extend(collect_doctype_controllers(self.app_path))

			hooks_file = self.app_path / self.app_path.name / "hooks.py"
			if not hooks_file.exists():
				hooks_file = self.app_path / "hooks.py"
			if hooks_file.exists():
				try:
					tree = ast.parse(hooks_file.read_text(encoding="utf-8"))
					extractor = PathExtractor()
					extractor.visit(tree)
					entry_points.extend(p for p, _, _ in extractor.found)
				except Exception:
					pass

			result.orphan_result = OrphanDetector(
				self.app_path,
				min_confidence=self.config.min_confidence,
				ignore_patterns=self.config.ignore_patterns or None,
			).detect(entry_points)

		return result


def analyze_app(
	app_path: str | Path,
	dependency_paths: list[Path] | None = None,
	**config_kwargs,
) -> StaticAnalysisResult:
	"""Convenience wrapper around :class:`StaticAnalyzer`."""
	config = StaticAnalysisConfig(**config_kwargs)
	return StaticAnalyzer(
		Path(app_path), dependency_paths=dependency_paths, config=config
	).analyze()
