"""GitHub PR reporter for spark-perf-lint.

Integrates with GitHub Actions to provide:

1. **Inline annotations** — workflow commands printed to stdout that GitHub
   renders as inline file annotations on the PR diff::

       ::error   file=jobs/etl.py,line=42,title=SPL-D03-001::Cartesian product detected
       ::warning file=jobs/etl.py,line=10,title=SPL-D08-001::AQE not enabled
       ::notice  file=jobs/load.py,line=55,title=SPL-D06-002::DataFrame cached once

2. **Job summary** — Markdown written to ``$GITHUB_STEP_SUMMARY`` so the
   Actions run page shows a formatted findings table without needing a
   separate PR comment.

3. **PR comment** — posts (or updates) a collapsible Markdown comment on the
   pull request via the GitHub REST API, using ``GITHUB_TOKEN`` from the
   environment.  Requires the workflow to have ``pull-requests: write``
   permission.

Severity → annotation level mapping:

+----------+-----------+
| Severity | Level     |
+==========+===========+
| CRITICAL | ``error`` |
+----------+-----------+
| WARNING  | ``warning``|
+----------+-----------+
| INFO     | ``notice``|
+----------+-----------+

Usage in a GitHub Actions workflow::

    - name: Run spark-perf-lint
      run: spark-perf-lint scan . --format github-pr
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

Outside GitHub Actions (e.g. local ``--format github-pr``), annotations are
still printed as plain text and ``write_step_summary`` / ``post_pr_comment``
are no-ops that log a warning.

All methods are safe to call unconditionally — they check the required
environment variables and degrade gracefully when they are absent.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import IO, Any

from spark_perf_lint import __version__
from spark_perf_lint.config import LintConfig
from spark_perf_lint.reporters.markdown_reporter import MarkdownReporter
from spark_perf_lint.types import AuditReport, Finding, Severity

# ---------------------------------------------------------------------------
# GitHub Actions workflow-command helpers
# ---------------------------------------------------------------------------

# Characters that must be percent-encoded inside annotation *property* values
# (the comma-separated key=value section before the final `::message`).
_PROP_ESCAPE: dict[int, str] = {
    ord("%"): "%25",
    ord(","): "%2C",
    ord(":"): "%3A",
    ord("]"): "%5D",
    ord("\r"): "%0D",
    ord("\n"): "%0A",
}

# Characters that must be percent-encoded inside the annotation *message*
# (the text after the final `::`).
_MSG_ESCAPE: dict[int, str] = {
    ord("%"): "%25",
    ord("\r"): "%0D",
    ord("\n"): "%0A",
}


def _encode_prop(value: str) -> str:
    """Percent-encode a workflow-command property value.

    Args:
        value: Raw property value string.

    Returns:
        Encoded string safe for use as a workflow-command property value.
    """
    return value.translate(_PROP_ESCAPE)


def _encode_msg(value: str) -> str:
    """Percent-encode a workflow-command message string.

    Args:
        value: Raw message string (may contain newlines for multi-line output).

    Returns:
        Encoded string safe for use as a workflow-command message.
    """
    return value.translate(_MSG_ESCAPE)


def _workflow_command(
    level: str,
    message: str,
    *,
    file: str | None = None,
    line: int | None = None,
    col: int | None = None,
    end_line: int | None = None,
    end_column: int | None = None,
    title: str | None = None,
) -> str:
    """Build a GitHub Actions workflow command string.

    Format::

        ::{level} {properties}::{message}

    Args:
        level: Command name — ``"error"``, ``"warning"``, or ``"notice"``.
        message: Human-readable annotation text.
        file: Repo-relative file path.
        line: 1-based start line number.
        col: 0-based start column offset.
        end_line: 1-based end line (defaults to *line* when omitted).
        end_column: 0-based end column.
        title: Short title shown in the annotation badge.

    Returns:
        A ready-to-print workflow command string (no trailing newline).
    """
    props: list[str] = []
    if file is not None:
        props.append(f"file={_encode_prop(file)}")
    if line is not None:
        props.append(f"line={line}")
    if end_line is not None:
        props.append(f"endLine={end_line}")
    if col is not None:
        props.append(f"col={col}")
    if end_column is not None:
        props.append(f"endColumn={end_column}")
    if title is not None:
        props.append(f"title={_encode_prop(title)}")

    props_str = ",".join(props)
    sep = " " if props_str else ""
    return f"::{level}{sep}{props_str}::{_encode_msg(message)}"


# ---------------------------------------------------------------------------
# GitHub environment snapshot
# ---------------------------------------------------------------------------

_COMMENT_MARKER = "<!-- spark-perf-lint:summary -->"


@dataclass
class _GitHubEnv:
    """Snapshot of relevant GitHub Actions environment variables.

    All fields default to empty string / ``False`` when not running inside
    GitHub Actions so callers can test them without special-casing.
    """

    is_github_actions: bool = False
    repository: str = ""  # "owner/repo"
    token: str = ""  # GITHUB_TOKEN
    step_summary_path: str = ""  # GITHUB_STEP_SUMMARY
    event_name: str = ""  # pull_request, push, …
    event_path: str = ""  # path to event JSON payload
    server_url: str = "https://github.com"
    sha: str = ""
    run_id: str = ""
    workflow: str = ""
    api_url: str = "https://api.github.com"


def _read_github_env() -> _GitHubEnv:
    """Read GitHub Actions environment variables into a ``_GitHubEnv``."""
    e = os.environ
    return _GitHubEnv(
        is_github_actions=e.get("GITHUB_ACTIONS", "").lower() == "true",
        repository=e.get("GITHUB_REPOSITORY", ""),
        token=e.get("GITHUB_TOKEN", ""),
        step_summary_path=e.get("GITHUB_STEP_SUMMARY", ""),
        event_name=e.get("GITHUB_EVENT_NAME", ""),
        event_path=e.get("GITHUB_EVENT_PATH", ""),
        server_url=e.get("GITHUB_SERVER_URL", "https://github.com"),
        sha=e.get("GITHUB_SHA", ""),
        run_id=e.get("GITHUB_RUN_ID", ""),
        workflow=e.get("GITHUB_WORKFLOW", ""),
        api_url=e.get("GITHUB_API_URL", "https://api.github.com"),
    )


# ---------------------------------------------------------------------------
# Severity → annotation level
# ---------------------------------------------------------------------------

_ANNOTATION_LEVEL: dict[Severity, str] = {
    Severity.CRITICAL: "error",
    Severity.WARNING: "warning",
    Severity.INFO: "notice",
}


# ---------------------------------------------------------------------------
# Public reporter class
# ---------------------------------------------------------------------------


class GitHubPRReporter:
    """Emit GitHub Actions annotations and post / update a PR summary comment.

    Args:
        report: The completed scan result.
        config: Resolved ``LintConfig`` used during the scan.
        show_fix: Include before / after code blocks in the PR comment body.
        compact: Use ``compact=True`` on the embedded ``MarkdownReporter``
            to stay within GitHub's ~65 000-character PR comment limit.
    """

    def __init__(
        self,
        report: AuditReport,
        config: LintConfig,
        *,
        show_fix: bool = True,
        compact: bool = False,
    ) -> None:
        self.report = report
        self.config = config
        self.show_fix = show_fix
        self.compact = compact
        self._env = _read_github_env()

    # ------------------------------------------------------------------
    # 1. Inline annotations
    # ------------------------------------------------------------------

    def emit_annotations(self, file: IO[str] | None = None) -> None:
        """Print GitHub Actions workflow-command annotation lines to *file*.

        Each finding at or above ``config.severity_threshold`` is emitted as
        one ``::error``/``::warning``/``::notice`` command.  The findings are
        bracketed inside a ``::group::`` / ``::endgroup::`` so they collapse
        in the Actions log.

        When *file* is ``None`` (default), writes to ``sys.stdout`` — which
        is what GitHub Actions reads for workflow commands.

        Args:
            file: Writable text-mode file.  ``None`` → ``sys.stdout``.
        """
        out = file if file is not None else sys.stdout
        threshold = self.config.severity_threshold
        visible = [f for f in self.report.findings if f.severity >= threshold]

        if not visible:
            return

        out.write("::group::spark-perf-lint findings\n")
        for finding in visible:
            out.write(self._annotation_line(finding) + "\n")
        out.write("::endgroup::\n")

    def _annotation_line(self, finding: Finding) -> str:
        """Build a single workflow-command line for *finding*.

        The annotation title is the rule ID.  The message combines the short
        message with the recommendation (truncated if together they exceed
        the practical 1 000-character message limit).

        Args:
            finding: The finding to annotate.

        Returns:
            A ``::level …::message`` workflow-command string.
        """
        level = _ANNOTATION_LEVEL[finding.severity]
        title = f"{finding.rule_id} — {finding.dimension.display_name}"

        # Build a concise message: short description + recommendation hint
        msg_parts = [finding.message]
        if finding.recommendation:
            msg_parts.append(f"Fix: {finding.recommendation}")
        message = "  ".join(msg_parts)
        if len(message) > 1000:
            message = message[:997] + "…"

        return _workflow_command(
            level,
            message,
            file=finding.file_path,
            line=finding.line_number,
            col=finding.column_offset,
            title=title,
        )

    # ------------------------------------------------------------------
    # 2. Job summary (GITHUB_STEP_SUMMARY)
    # ------------------------------------------------------------------

    def write_step_summary(self) -> bool:
        """Append a Markdown summary to ``$GITHUB_STEP_SUMMARY``.

        Writes the full MarkdownReporter output (compact mode) to the file
        pointed at by ``GITHUB_STEP_SUMMARY``.  The Actions run page renders
        this as a formatted job summary.

        Returns:
            ``True`` on success; ``False`` if ``GITHUB_STEP_SUMMARY`` is not
            set or the file cannot be written.
        """
        path = self._env.step_summary_path
        if not path:
            self._warn("GITHUB_STEP_SUMMARY is not set — skipping job summary.")
            return False

        md = MarkdownReporter(
            self.report,
            self.config,
            show_fix=False,  # keep step summary concise
            compact=self.compact,
        ).to_str()

        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(md)
                fh.write("\n")
        except OSError as exc:
            self._warn(f"Could not write to GITHUB_STEP_SUMMARY ({path}): {exc}")
            return False

        return True

    # ------------------------------------------------------------------
    # 3. PR comment
    # ------------------------------------------------------------------

    def post_pr_comment(self) -> bool:
        """Post or update a spark-perf-lint summary comment on the PR.

        Searches existing comments for one containing the sentinel marker
        ``<!-- spark-perf-lint:summary -->``.  If found, the comment is
        updated in place so repeated runs do not flood the PR timeline.
        If not found, a new comment is created.

        Requires the environment variables:

        * ``GITHUB_TOKEN`` — personal access token or ``secrets.GITHUB_TOKEN``
          with ``pull-requests: write`` permission.
        * ``GITHUB_REPOSITORY`` — ``owner/repo`` set automatically by Actions.
        * The workflow event must be ``pull_request`` (or ``pull_request_target``).

        Returns:
            ``True`` if the comment was posted or updated successfully.
            ``False`` on any error (API failure, missing token, non-PR event).
        """
        if not self._env.token:
            self._warn("GITHUB_TOKEN is not set — skipping PR comment.")
            return False

        pr_number = self._get_pr_number()
        if pr_number is None:
            self._warn(
                f"Could not determine PR number "
                f"(event_name={self._env.event_name!r}) — skipping PR comment."
            )
            return False

        body = self._build_comment_body()
        existing_id = self._find_existing_comment(pr_number)

        if existing_id is not None:
            return self._update_comment(existing_id, body)
        return self._create_comment(pr_number, body)

    def _build_comment_body(self) -> str:
        """Build the full Markdown comment body with the hidden sentinel marker.

        Returns:
            A Markdown string that starts with the ``<!-- sentinel -->`` line
            so future runs can identify and update this specific comment.
        """
        md = MarkdownReporter(
            self.report,
            self.config,
            show_fix=self.show_fix,
            compact=self.compact,
        ).to_str()
        return f"{_COMMENT_MARKER}\n{md}"

    def _get_pr_number(self) -> int | None:
        """Extract the pull-request number from the Actions event payload.

        Reads ``GITHUB_EVENT_PATH`` (a JSON file GitHub writes before the job
        starts) and returns ``event["number"]``.  Returns ``None`` when the
        event is not a pull-request or the file cannot be parsed.

        Returns:
            Integer PR number, or ``None``.
        """
        if self._env.event_name not in ("pull_request", "pull_request_target"):
            return None

        event_path = self._env.event_path
        if not event_path:
            return None

        try:
            with open(event_path, encoding="utf-8") as fh:
                event = json.load(fh)
            return int(event["number"])
        except (OSError, KeyError, ValueError, TypeError):
            return None

    def _api_headers(self) -> dict[str, str]:
        """Return headers required for authenticated GitHub API requests.

        Returns:
            Dictionary with ``Authorization``, ``Accept``, and ``User-Agent``
            headers.
        """
        return {
            "Authorization": f"Bearer {self._env.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": f"spark-perf-lint/{__version__}",
            "Content-Type": "application/json",
        }

    def _find_existing_comment(self, pr_number: int) -> int | None:
        """Search the PR's comment list for a previous spark-perf-lint comment.

        Paginates through all issue comments (up to 10 pages / 1 000 comments)
        looking for the hidden sentinel marker.

        Args:
            pr_number: GitHub pull-request number.

        Returns:
            Comment ID (integer) if found, otherwise ``None``.
        """
        repo = self._env.repository
        base = self._env.api_url
        page = 1
        per_page = 100

        while page <= 10:
            url = (
                f"{base}/repos/{repo}/issues/{pr_number}/comments"
                f"?per_page={per_page}&page={page}"
            )
            try:
                data = self._api_get(url)
            except urllib.error.URLError as exc:
                self._warn(f"GitHub API GET failed: {exc}")
                return None

            if not data:
                break

            for comment in data:
                body = comment.get("body", "")
                if _COMMENT_MARKER in body:
                    return int(comment["id"])

            if len(data) < per_page:
                break
            page += 1

        return None

    def _create_comment(self, pr_number: int, body: str) -> bool:
        """POST a new comment to the pull request.

        Args:
            pr_number: GitHub pull-request number.
            body: Comment body (Markdown string).

        Returns:
            ``True`` on HTTP 201; ``False`` on any error.
        """
        repo = self._env.repository
        url = f"{self._env.api_url}/repos/{repo}/issues/{pr_number}/comments"
        try:
            self._api_post(url, {"body": body}, expected_status=201)
            return True
        except (urllib.error.URLError, _APIError) as exc:
            self._warn(f"Failed to create PR comment: {exc}")
            return False

    def _update_comment(self, comment_id: int, body: str) -> bool:
        """PATCH an existing comment with a new body.

        Args:
            comment_id: GitHub comment ID.
            body: Updated comment body (Markdown string).

        Returns:
            ``True`` on HTTP 200; ``False`` on any error.
        """
        repo = self._env.repository
        url = f"{self._env.api_url}/repos/{repo}/comments/{comment_id}"
        try:
            self._api_patch(url, {"body": body}, expected_status=200)
            return True
        except (urllib.error.URLError, _APIError) as exc:
            self._warn(f"Failed to update PR comment {comment_id}: {exc}")
            return False

    # ------------------------------------------------------------------
    # 4. Full pipeline
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        annotations_file: IO[str] | None = None,
        post_comment: bool = True,
        write_summary: bool = True,
    ) -> int:
        """Execute the full GitHub PR reporting pipeline.

        Steps executed in order:

        1. Emit inline annotations to *annotations_file* (or stdout).
        2. Write job summary to ``$GITHUB_STEP_SUMMARY`` (if *write_summary*).
        3. Post / update PR comment (if *post_comment* and in a PR context).

        Args:
            annotations_file: Override for annotation output (default: stdout).
            post_comment: Whether to attempt a PR comment API call.
            write_summary: Whether to append to ``$GITHUB_STEP_SUMMARY``.

        Returns:
            ``0`` if no findings match the ``fail_on`` severity levels,
            ``1`` if one or more findings trigger failure.
        """
        self.emit_annotations(file=annotations_file)

        if write_summary:
            self.write_step_summary()

        if post_comment:
            self.post_pr_comment()

        # Exit code
        fail_severities = set(self.config.fail_on)
        threshold = self.config.severity_threshold
        visible = [f for f in self.report.findings if f.severity >= threshold]
        should_fail = any(f.severity in fail_severities for f in visible)
        return 1 if should_fail else 0

    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------

    def _api_get(self, url: str) -> Any:
        """Issue an authenticated GET and return the decoded JSON body.

        Args:
            url: Full GitHub API URL.

        Returns:
            Decoded JSON (list or dict).

        Raises:
            urllib.error.URLError: On network or HTTP error.
        """
        req = urllib.request.Request(url, headers=self._api_headers(), method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _api_post(self, url: str, payload: dict[str, Any], *, expected_status: int = 200) -> Any:
        """Issue an authenticated POST with a JSON payload.

        Args:
            url: Full GitHub API URL.
            payload: Dictionary to serialise as the request body.
            expected_status: Expected HTTP status code.

        Returns:
            Decoded JSON response body.

        Raises:
            urllib.error.URLError: On network error.
            _APIError: If the response status differs from *expected_status*.
        """
        return self._api_request("POST", url, payload, expected_status=expected_status)

    def _api_patch(self, url: str, payload: dict[str, Any], *, expected_status: int = 200) -> Any:
        """Issue an authenticated PATCH with a JSON payload.

        Args:
            url: Full GitHub API URL.
            payload: Dictionary to serialise as the request body.
            expected_status: Expected HTTP status code.

        Returns:
            Decoded JSON response body.

        Raises:
            urllib.error.URLError: On network error.
            _APIError: If the response status differs from *expected_status*.
        """
        return self._api_request("PATCH", url, payload, expected_status=expected_status)

    def _api_request(
        self,
        method: str,
        url: str,
        payload: dict[str, Any],
        *,
        expected_status: int,
    ) -> Any:
        """Generic authenticated JSON API request.

        Args:
            method: HTTP method string (``"POST"``, ``"PATCH"``, etc.).
            url: Full API URL.
            payload: Request body dictionary.
            expected_status: HTTP status code that indicates success.

        Returns:
            Decoded JSON response body.

        Raises:
            urllib.error.URLError: On network-level failure.
            _APIError: If the HTTP status code does not match *expected_status*.
        """
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=self._api_headers(), method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.status
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise _APIError(f"HTTP {exc.code} {exc.reason} from {method} {url}") from exc

        if status != expected_status:
            raise _APIError(f"Expected HTTP {expected_status}, got {status} from {method} {url}")
        return body

    # ------------------------------------------------------------------
    # Logging helper
    # ------------------------------------------------------------------

    @staticmethod
    def _warn(message: str) -> None:
        """Write a diagnostic warning to stderr.

        Args:
            message: Human-readable warning text.
        """
        print(f"[spark-perf-lint] WARNING: {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Internal error type
# ---------------------------------------------------------------------------


class _APIError(Exception):
    """Raised when the GitHub API returns an unexpected HTTP status."""


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def run_github_reporter(
    report: AuditReport,
    config: LintConfig,
    *,
    show_fix: bool = True,
    compact: bool = False,
    annotations_file: IO[str] | None = None,
    post_comment: bool = True,
    write_summary: bool = True,
) -> int:
    """Run the full GitHub PR reporting pipeline and return an exit code.

    This is a thin wrapper around :class:`GitHubPRReporter.run` for callers
    that do not need to keep a reporter instance.

    Args:
        report: The completed scan result.
        config: Resolved configuration used during the scan.
        show_fix: Include before / after code blocks in the PR comment.
        compact: Use compact Markdown to stay within PR comment size limits.
        annotations_file: Override for annotation output (default: stdout).
        post_comment: Attempt to post / update a PR comment via the API.
        write_summary: Append to ``$GITHUB_STEP_SUMMARY``.

    Returns:
        ``0`` (pass) or ``1`` (fail) based on ``config.fail_on``.

    Example::

        from spark_perf_lint.reporters.github_pr import run_github_reporter
        sys.exit(run_github_reporter(report, config))
    """
    return GitHubPRReporter(
        report,
        config,
        show_fix=show_fix,
        compact=compact,
    ).run(
        annotations_file=annotations_file,
        post_comment=post_comment,
        write_summary=write_summary,
    )
