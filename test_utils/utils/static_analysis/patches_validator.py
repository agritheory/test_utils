"""Validate that each patch path in patches.txt resolves to a real Python function."""

from dataclasses import dataclass, field
from pathlib import Path

from .path_resolver import PathResolver, is_potential_dotted_path


@dataclass
class PatchesValidationResult:
	errors: list[str] = field(default_factory=list)
	warnings: list[str] = field(default_factory=list)
	paths_checked: int = 0

	def to_dict(self) -> dict:
		return {
			"errors": self.errors,
			"warnings": self.warnings,
			"paths_checked": self.paths_checked,
		}


def parse_patches_txt(patches_txt: Path) -> list[tuple[str, int]]:
	"""Return (dotted_path, line_number) pairs from patches.txt."""
	results: list[tuple[str, int]] = []
	for lineno, raw in enumerate(patches_txt.read_text(encoding="utf-8").splitlines(), 1):
		line = raw.strip()
		if not line or line.startswith("#"):
			continue
		line = line.split("#")[0].strip()
		if line.startswith("execute:"):
			line = line[len("execute:") :].strip()
		token = line.split()[0] if line.split() else ""
		if token and is_potential_dotted_path(token):
			results.append((token, lineno))
	return results


def find_patches_txt(app_path: Path) -> Path | None:
	candidate = app_path / app_path.name / "patches.txt"
	if candidate.exists():
		return candidate
	for subdir in sorted(app_path.iterdir()):
		if subdir.is_dir() and (subdir / "patches.txt").exists():
			return subdir / "patches.txt"
	direct = app_path / "patches.txt"
	if direct.exists():
		return direct
	return None


class PatchesValidator:
	"""Validate that patch paths in patches.txt resolve to importable functions."""

	def __init__(self, resolver: PathResolver) -> None:
		self.resolver = resolver

	def validate(self, app_path: Path) -> PatchesValidationResult:
		result = PatchesValidationResult()

		patches_txt = find_patches_txt(app_path)
		if patches_txt is None:
			result.warnings.append(f"patches.txt not found under '{app_path}'")
			return result

		for path, lineno in parse_patches_txt(patches_txt):
			resolved = self.resolver.resolve(path)
			result.paths_checked += 1
			loc = f"{patches_txt}:{lineno}"
			if not resolved.exists:
				result.errors.append(f"{loc}: '{path}' — {resolved.error}")
			elif resolved.kind == "whitelisted":
				pass
			elif resolved.kind == "module":
				# Frappe calls execute() on plain-module patches automatically.
				if not self.resolver.resolve(path + ".execute").exists:
					result.warnings.append(
						f"{loc}: '{path}' resolves to a module with no execute() function"
					)
			elif resolved.kind not in ("function", "async_function"):
				result.warnings.append(
					f"{loc}: '{path}' resolves to a {resolved.kind}, expected a function"
				)

		return result
