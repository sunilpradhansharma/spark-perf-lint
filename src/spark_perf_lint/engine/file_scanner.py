"""File discovery and PySpark detection for spark-perf-lint.

Four entry points cover every invocation mode:

    FileScanner.from_paths(paths, config)
        CLI ``scan`` command — accepts files and/or directories, recurses.

    FileScanner.from_staged_files(paths, config)
        Pre-commit hook — receives the list of staged file paths from the
        pre-commit framework; filters to .py and applies ignore rules.

    FileScanner.from_git_diff(base_ref, config, repo_root)
        CI / PR mode — shells out to git to discover files changed relative
        to *base_ref* (e.g. ``"origin/main"``).

    FileScanner.from_glob_patterns(patterns, config, root)
        Utility entry point — accepts shell-style glob patterns.

All four return an iterable of ``ScanTarget`` objects.  Callers never deal
with path filtering, encoding, or PySpark detection directly.

PySpark detection
-----------------
A fast text scan (no AST parsing) is used to classify each file before the
full ``ASTAnalyzer`` pipeline is invoked.  Files that do not import pyspark
and do not reference any Spark-related symbols are marked
``is_pyspark = False`` so the orchestrator can skip them cheaply.

The detection is intentionally permissive: a file that *might* use Spark
passes.  False negatives (missing a Spark file) are worse than false
positives (scanning a non-Spark file).
"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from spark_perf_lint.config import LintConfig

logger = logging.getLogger(__name__)


# =============================================================================
# ScanTarget
# =============================================================================


@dataclass
class ScanTarget:
    """A single Python file ready to be analysed.

    Attributes:
        path: Absolute path to the source file.
        content: Full UTF-8 source text (read once here; not re-read by
            rules).
        is_pyspark: ``True`` when the fast PySpark detector found at least
            one Spark indicator.  ``False`` means the file almost certainly
            has no PySpark code and may be skipped by the orchestrator.
        relative_path: Path relative to *root* supplied at scan time.  Used
            in ``Finding.file_path`` so reports show project-relative paths
            rather than absolute system paths.
        size_bytes: File size in bytes (populated from ``len(content)``).
        skip_reason: When not ``None``, the orchestrator should skip this
            file and record the reason (e.g. ``"ignored by config"``).
    """

    path: Path
    content: str
    is_pyspark: bool
    relative_path: str
    size_bytes: int
    skip_reason: str | None = None

    def __repr__(self) -> str:
        flag = "spark" if self.is_pyspark else "plain"
        return f"ScanTarget({self.relative_path!r}, {flag}, {self.size_bytes} B)"


@dataclass
class ScanSummary:
    """Counts collected during a ``FileScanner`` scan pass.

    Attributes:
        total_found: Total ``.py`` files found before filtering.
        ignored: Files skipped due to ignore rules.
        non_pyspark: Files that passed ignore rules but failed the PySpark
            detector.
        to_scan: Files queued for full analysis (``len(targets)``).
        read_errors: Paths that could not be read (permission denied, etc.).
    """

    total_found: int = 0
    ignored: int = 0
    non_pyspark: int = 0
    to_scan: int = 0
    read_errors: list[str] = field(default_factory=list)


# =============================================================================
# PySpark fast detector
# =============================================================================

# Substrings and patterns that strongly indicate PySpark usage.
# Ordered approximately by specificity (more specific first avoids
# re-scanning the whole file for common names like "spark").
_PYSPARK_IMPORT_MARKERS: tuple[str, ...] = (
    "from pyspark",
    "import pyspark",
)

_PYSPARK_USAGE_MARKERS: tuple[str, ...] = (
    "SparkSession",
    "SparkContext",
    "SQLContext",
    "HiveContext",
    "SparkConf",
    "pyspark",
    "spark.read",
    "spark.sql",
    "spark.table",
    "spark.createDataFrame",
    ".repartition(",
    ".coalesce(",
    ".groupByKey(",
    ".reduceByKey(",
    ".join(",
    ".cache()",
    ".persist(",
    ".unpersist(",
    ".broadcast(",
    "sc.parallelize(",
    "sc.textFile(",
)

# If the file is larger than this, only scan the first N bytes for the quick
# check (avoids loading huge auto-generated files into memory just to skip them).
_QUICK_SCAN_BYTES = 8_192  # 8 KB


def is_pyspark_file(content: str) -> bool:
    """Return ``True`` when *content* almost certainly contains PySpark code.

    Uses a two-stage check:

    1. Look for ``from pyspark`` / ``import pyspark`` — the most reliable
       signal.  If found, return immediately.
    2. Scan the first 8 KB for any of the usage markers.  These are common
       enough to catch files that import Spark objects via ``*`` imports or
       utility wrappers.

    The check is *case-sensitive* because Python identifiers are.

    Args:
        content: Full source text of the file.

    Returns:
        ``True`` when at least one PySpark indicator is found.
    """
    # Stage 1 — import lines (always near the top; cheap to scan in full)
    for marker in _PYSPARK_IMPORT_MARKERS:
        if marker in content:
            return True

    # Stage 2 — usage markers in the first 8 KB
    head = content[:_QUICK_SCAN_BYTES]
    for marker in _PYSPARK_USAGE_MARKERS:
        if marker in head:
            return True

    return False


# =============================================================================
# Internal helpers
# =============================================================================


def _read_file(path: Path) -> tuple[str, str | None]:
    """Read *path* as UTF-8 text, falling back to latin-1.

    Args:
        path: File to read.

    Returns:
        ``(content, None)`` on success or ``("", error_message)`` on failure.
    """
    try:
        return path.read_text(encoding="utf-8", errors="replace"), None
    except OSError as exc:
        return "", f"{path}: {exc}"


def _is_python_file(path: Path) -> bool:
    """Return ``True`` for ``.py`` files (case-insensitive on Windows)."""
    return path.suffix.lower() == ".py"


def _resolve_relative(path: Path, root: Path) -> str:
    """Return *path* relative to *root*, or the absolute string if outside root."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _apply_ignore(
    path: Path,
    config: LintConfig,
    root: Path,
) -> str | None:
    """Return a skip reason string if *path* should be ignored, else ``None``.

    Delegates to ``LintConfig.should_ignore_file`` but resolves the path
    relative to *root* first so glob patterns like ``**/*_test.py`` match
    properly regardless of the caller's working directory.

    Args:
        path: Absolute path to test.
        config: Resolved ``LintConfig``.
        root: Project root used for relative-path matching.

    Returns:
        A non-empty reason string, or ``None`` when the file should be scanned.
    """
    rel = _resolve_relative(path, root)
    if config.should_ignore_file(rel):
        return f"ignored by config pattern (rel={rel!r})"
    # Also test the absolute path in case patterns include full path components
    if config.should_ignore_file(str(path)):
        return "ignored by config pattern (abs)"
    return None


def _collect_py_files(root_path: Path) -> list[Path]:
    """Recursively collect all ``.py`` files under *root_path*.

    Follows symlinks.  Does not apply ignore patterns (caller's job).

    Args:
        root_path: Directory to walk.

    Returns:
        Sorted list of absolute paths.
    """
    return sorted(root_path.rglob("*.py"))


def _build_target(
    path: Path,
    root: Path,
    config: LintConfig,
    *,
    force_pyspark: bool = False,
) -> ScanTarget | None:
    """Read *path* and produce a ``ScanTarget``, or ``None`` on read error.

    Applies ignore rules and the PySpark detector.  Returns a target with
    ``skip_reason`` set when the file should be skipped, so callers can
    surface the reason in verbose output.

    Args:
        path: Absolute path to the source file.
        root: Project root for relative-path computation.
        config: Resolved ``LintConfig``.
        force_pyspark: Skip the PySpark detector and mark the file as
            PySpark regardless (used when the caller already knows).

    Returns:
        A ``ScanTarget`` (possibly with ``skip_reason`` set), or ``None``
        when the file could not be read.
    """
    content, err = _read_file(path)
    if err:
        logger.warning("Could not read %s: %s", path, err)
        return None

    rel = _resolve_relative(path, root)
    size = len(content.encode("utf-8", errors="replace"))
    skip_reason = _apply_ignore(path, config, root)
    spark = force_pyspark or is_pyspark_file(content)

    return ScanTarget(
        path=path,
        content=content,
        is_pyspark=spark,
        relative_path=rel,
        size_bytes=size,
        skip_reason=skip_reason,
    )


# =============================================================================
# FileScanner
# =============================================================================


class FileScanner:
    """Discovers and classifies Python source files for spark-perf-lint.

    Do not instantiate directly.  Use one of the class-method factories:

    - :meth:`from_paths` — CLI scan
    - :meth:`from_staged_files` — pre-commit hook
    - :meth:`from_git_diff` — CI / PR mode
    - :meth:`from_glob_patterns` — glob-pattern utility

    All factories return a ``(targets, summary)`` tuple.

    Example::

        targets, summary = FileScanner.from_paths(
            [Path("src/"), Path("jobs/etl.py")],
            config=LintConfig.load(),
        )
        for t in targets:
            if t.skip_reason:
                continue
            analyzer = ASTAnalyzer.from_source(t.content, t.relative_path)
    """

    # ------------------------------------------------------------------
    # CLI scan mode
    # ------------------------------------------------------------------

    @classmethod
    def from_paths(
        cls,
        paths: list[Path | str],
        config: LintConfig,
        root: Path | None = None,
    ) -> tuple[list[ScanTarget], ScanSummary]:
        """Discover files from a mix of file and directory paths.

        Directories are scanned recursively.  Non-``.py`` files are silently
        skipped.  Glob patterns embedded in path strings (``*``, ``**``,
        ``?``, ``[seq]``) are expanded.

        Args:
            paths: File paths, directory paths, or glob patterns.  Strings
                and ``Path`` objects are both accepted.
            config: Resolved ``LintConfig``.
            root: Base directory for relative-path computation.  Defaults to
                ``Path.cwd()``.

        Returns:
            ``(targets, summary)`` tuple.
        """
        root = (root or Path.cwd()).resolve()
        summary = ScanSummary()
        targets: list[ScanTarget] = []
        seen: set[Path] = set()

        for raw in paths:
            raw_path = Path(raw)

            # Glob expansion for patterns like "src/**/*.py"
            if any(c in str(raw) for c in ("*", "?", "[")):
                expanded = list(root.glob(str(raw_path)))
                if not expanded:
                    logger.debug("Glob pattern %r matched no files", raw)
                candidates = expanded
            elif raw_path.is_dir():
                candidates = _collect_py_files(raw_path.resolve())
            elif raw_path.is_file():
                candidates = [raw_path.resolve()]
            else:
                logger.warning("Path does not exist: %s", raw_path)
                candidates = []

            for candidate in candidates:
                abs_path = candidate.resolve()
                if not _is_python_file(abs_path):
                    continue
                if abs_path in seen:
                    continue
                seen.add(abs_path)
                summary.total_found += 1

                target = _build_target(abs_path, root, config)
                if target is None:
                    summary.read_errors.append(str(abs_path))
                    continue
                if target.skip_reason:
                    summary.ignored += 1
                elif not target.is_pyspark:
                    summary.non_pyspark += 1
                else:
                    summary.to_scan += 1
                targets.append(target)

        return targets, summary

    # ------------------------------------------------------------------
    # Pre-commit mode
    # ------------------------------------------------------------------

    @classmethod
    def from_staged_files(
        cls,
        staged_paths: list[str | Path],
        config: LintConfig,
        root: Path | None = None,
    ) -> tuple[list[ScanTarget], ScanSummary]:
        """Build scan targets from the list of staged files.

        The pre-commit framework passes the list of staged file paths as
        positional arguments to the hook script.  Pass them here verbatim.

        Only ``.py`` files are processed; everything else is silently
        discarded.  Deleted files (those that no longer exist on disk) are
        also skipped.

        Args:
            staged_paths: Paths as received from the pre-commit framework.
                May be relative to the repo root or absolute.
            config: Resolved ``LintConfig``.
            root: Repo root directory.  Defaults to ``Path.cwd()``.

        Returns:
            ``(targets, summary)`` tuple.
        """
        root = (root or Path.cwd()).resolve()
        summary = ScanSummary()
        targets: list[ScanTarget] = []

        for raw in staged_paths:
            path = Path(raw)
            if not path.is_absolute():
                path = (root / path).resolve()
            else:
                path = path.resolve()

            if not _is_python_file(path):
                continue
            if not path.exists():
                logger.debug("Staged file no longer on disk (deleted?): %s", path)
                continue

            summary.total_found += 1
            target = _build_target(path, root, config)
            if target is None:
                summary.read_errors.append(str(path))
                continue
            if target.skip_reason:
                summary.ignored += 1
            elif not target.is_pyspark:
                summary.non_pyspark += 1
            else:
                summary.to_scan += 1
            targets.append(target)

        return targets, summary

    # ------------------------------------------------------------------
    # CI / git diff mode
    # ------------------------------------------------------------------

    @classmethod
    def from_git_diff(
        cls,
        base_ref: str,
        config: LintConfig,
        repo_root: Path | None = None,
        *,
        include_untracked: bool = False,
    ) -> tuple[list[ScanTarget], ScanSummary]:
        """Discover files changed relative to *base_ref* via ``git diff``.

        Runs ``git diff --name-only --diff-filter=ACMR <base_ref>`` to
        enumerate Added, Copied, Modified, and Renamed files.  Deleted
        files are excluded (``D`` not in filter).

        Args:
            base_ref: Git ref to compare against, e.g. ``"origin/main"`` or
                ``"HEAD~1"``.
            config: Resolved ``LintConfig``.
            repo_root: Path to the git repository root.  Defaults to
                ``Path.cwd()``.
            include_untracked: When ``True``, also include untracked ``.py``
                files that ``git status`` reports.

        Returns:
            ``(targets, summary)`` tuple.

        Raises:
            RuntimeError: If the ``git diff`` command fails (not inside a
                git repo, or *base_ref* is not a valid ref).
        """
        repo_root = (repo_root or Path.cwd()).resolve()
        summary = ScanSummary()
        targets: list[ScanTarget] = []

        changed_paths = _git_diff_files(base_ref, repo_root)

        if include_untracked:
            changed_paths.extend(_git_untracked_files(repo_root))

        seen: set[Path] = set()
        for rel_str in changed_paths:
            abs_path = (repo_root / rel_str).resolve()
            if not _is_python_file(abs_path):
                continue
            if abs_path in seen:
                continue
            seen.add(abs_path)
            if not abs_path.exists():
                continue

            summary.total_found += 1
            target = _build_target(abs_path, repo_root, config)
            if target is None:
                summary.read_errors.append(str(abs_path))
                continue
            if target.skip_reason:
                summary.ignored += 1
            elif not target.is_pyspark:
                summary.non_pyspark += 1
            else:
                summary.to_scan += 1
            targets.append(target)

        return targets, summary

    # ------------------------------------------------------------------
    # Glob-pattern utility
    # ------------------------------------------------------------------

    @classmethod
    def from_glob_patterns(
        cls,
        patterns: list[str],
        config: LintConfig,
        root: Path | None = None,
    ) -> tuple[list[ScanTarget], ScanSummary]:
        """Discover files that match one or more glob patterns.

        Patterns are resolved relative to *root*.  Both ``fnmatch``-style
        (``*.py``) and ``pathlib``-style (``**/*.py``) patterns are
        supported.

        Args:
            patterns: Shell-style glob patterns, e.g.
                ``["src/**/*.py", "jobs/*.py"]``.
            config: Resolved ``LintConfig``.
            root: Base directory for glob expansion.  Defaults to
                ``Path.cwd()``.

        Returns:
            ``(targets, summary)`` tuple.
        """
        root = (root or Path.cwd()).resolve()
        summary = ScanSummary()
        targets: list[ScanTarget] = []
        seen: set[Path] = set()

        for pattern in patterns:
            matched = list(root.glob(pattern))
            if not matched:
                logger.debug("Glob pattern %r matched no files under %s", pattern, root)

            for candidate in matched:
                abs_path = candidate.resolve()
                if not _is_python_file(abs_path) or not abs_path.is_file():
                    continue
                if abs_path in seen:
                    continue
                seen.add(abs_path)
                summary.total_found += 1

                target = _build_target(abs_path, root, config)
                if target is None:
                    summary.read_errors.append(str(abs_path))
                    continue
                if target.skip_reason:
                    summary.ignored += 1
                elif not target.is_pyspark:
                    summary.non_pyspark += 1
                else:
                    summary.to_scan += 1
                targets.append(target)

        return targets, summary

    # ------------------------------------------------------------------
    # Filtering helpers (public, for external use)
    # ------------------------------------------------------------------

    @staticmethod
    def scannable(targets: list[ScanTarget]) -> list[ScanTarget]:
        """Return only targets that should be fully analysed.

        A target is scannable when it has no ``skip_reason`` and
        ``is_pyspark`` is ``True``.

        Args:
            targets: Full list of ``ScanTarget`` objects as returned by
                any factory.

        Returns:
            Filtered list in original order.
        """
        return [t for t in targets if not t.skip_reason and t.is_pyspark]

    @staticmethod
    def skipped(targets: list[ScanTarget]) -> list[ScanTarget]:
        """Return targets that were excluded by ignore rules.

        Args:
            targets: Full list of ``ScanTarget`` objects.

        Returns:
            Targets with a non-``None`` ``skip_reason``.
        """
        return [t for t in targets if t.skip_reason]

    @staticmethod
    def non_pyspark(targets: list[ScanTarget]) -> list[ScanTarget]:
        """Return targets that passed ignore rules but failed PySpark detection.

        Args:
            targets: Full list of ``ScanTarget`` objects.

        Returns:
            Targets with ``skip_reason is None`` and ``is_pyspark == False``.
        """
        return [t for t in targets if not t.skip_reason and not t.is_pyspark]


# =============================================================================
# Git helpers
# =============================================================================


def _git_diff_files(base_ref: str, cwd: Path) -> list[str]:
    """Return repo-relative paths changed vs *base_ref*.

    Uses ``--diff-filter=ACMR`` to exclude deletions.

    Args:
        base_ref: Git ref to compare against.
        cwd: Working directory for the git command (must be inside the repo).

    Returns:
        List of repo-relative path strings.

    Raises:
        RuntimeError: When git exits non-zero.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR", base_ref],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"git diff against {base_ref!r} failed "
            f"(exit {exc.returncode}): {exc.stderr.strip()}"
        ) from exc
    except FileNotFoundError as exc:
        raise RuntimeError("git executable not found in PATH") from exc


def _git_untracked_files(cwd: Path) -> list[str]:
    """Return repo-relative paths of untracked files.

    Args:
        cwd: Working directory for the git command.

    Returns:
        List of repo-relative path strings.  Empty list on any error.
    """
    try:
        result = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.debug("Could not list untracked files; skipping.")
        return []


# =============================================================================
# Convenience top-level functions
# =============================================================================


def scan_paths(
    paths: list[Path | str],
    config: LintConfig,
    root: Path | None = None,
) -> list[ScanTarget]:
    """Shorthand: scan paths and return only scannable targets.

    Equivalent to ``FileScanner.scannable(FileScanner.from_paths(...)[0])``.

    Args:
        paths: File or directory paths to scan.
        config: Resolved ``LintConfig``.
        root: Project root for relative-path computation.

    Returns:
        Scannable (PySpark, not-ignored) ``ScanTarget`` objects.
    """
    targets, _ = FileScanner.from_paths(paths, config, root)
    return FileScanner.scannable(targets)


def scan_staged(
    staged_paths: list[str | Path],
    config: LintConfig,
    root: Path | None = None,
) -> list[ScanTarget]:
    """Shorthand: process staged files and return only scannable targets.

    Args:
        staged_paths: Paths as received from the pre-commit framework.
        config: Resolved ``LintConfig``.
        root: Repo root directory.

    Returns:
        Scannable ``ScanTarget`` objects.
    """
    targets, _ = FileScanner.from_staged_files(staged_paths, config, root)
    return FileScanner.scannable(targets)
