"""Path exclusion patterns (e.g. skip tests) when scanning an app tree."""

from __future__ import annotations

import fnmatch
import re
from pathlib import PurePosixPath

DEFAULT_TEST_EXCLUDE_GLOBS: tuple[str, ...] = (
	"**/tests/**",
	"**/test_*.py",
	"**/*_test.py",
)


def _seg_glob_to_regex(segment: str) -> str:
	"""Glob for one path segment (* and ? do not cross ``/``)."""
	out: list[str] = []
	for ch in segment:
		if ch == "*":
			out.append("[^/]*")
		elif ch == "?":
			out.append("[^/]")
		else:
			out.append(re.escape(ch))
	return "".join(out)


def _glob_with_double_star_to_regex(pattern: str) -> re.Pattern[str]:
	"""Compile ``**/foo/bar/**``-style patterns (POSIX path, no leading slash)."""
	pat = pattern.replace("\\", "/")
	assert pat.startswith("**/")
	rest = pat[3:]
	if rest.endswith("/**"):
		rest = rest[:-3]
	rest = rest.strip("/")
	if not rest:
		return re.compile("^.*$")
	segs = [s for s in rest.split("/") if s]
	if not segs:
		return re.compile("^.*$")
	parts = [r"^(?:.*/)?"]
	for k, seg in enumerate(segs):
		if k > 0:
			parts.append(r"/")
		parts.append(_seg_glob_to_regex(seg))
	parts.append(r"(?:/.*)?$")
	return re.compile("".join(parts))


def rel_path_matches_glob(relative_posix_path: str, pattern: str) -> bool:
	path = relative_posix_path.replace("\\", "/").lstrip("/")
	pat = pattern.replace("\\", "/")
	pp = PurePosixPath(path)
	match = getattr(pp, "match", None)
	if callable(match):
		try:
			if pp.match(pat):
				return True
		except Exception:
			pass
	if "**" in pat:
		if pat.startswith("**/"):
			if _glob_with_double_star_to_regex(pat).fullmatch(path):
				return True
		return fnmatch.fnmatch(path, pat) or fnmatch.fnmatch(PurePosixPath(path).name, pat)
	return fnmatch.fnmatch(path, pat) or fnmatch.fnmatch(PurePosixPath(path).name, pat)


def is_excluded_relative_path(relative_posix_path: str, patterns: list[str]) -> bool:
	for pat in patterns:
		if not pat:
			continue
		if rel_path_matches_glob(relative_posix_path, pat):
			return True
	return False


def merge_exclude_globs(
	extra: list[str] | None,
	*,
	exclude_tests: bool,
) -> list[str]:
	out: list[str] = []
	if exclude_tests:
		out.extend(DEFAULT_TEST_EXCLUDE_GLOBS)
	if extra:
		out.extend(extra)
	return out
