#!/usr/bin/env python3
"""MCP server that connects GitHub pull requests to Slack.

Design constraint: this server never summarizes anything. It returns structured
PR data and posts whatever text it is handed, so the digest format can change in
the prompt without touching this file.

Transport is stdio, so stdout belongs to the MCP protocol. All logging goes to
stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

try:  # mcp >= 2.0 renamed FastMCP to MCPServer
    from mcp.server.mcpserver import MCPServer as McpServer
except ImportError:  # pragma: no cover - mcp 1.x
    from mcp.server.fastmcp import FastMCP as McpServer

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

GITHUB_API = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
GITHUB_TIMEOUT = float(os.environ.get("GITHUB_TIMEOUT_SECONDS", "30"))

RATE_LIMIT_FLOOR = 100          # below this many remaining calls we stop and warn
LIST_CACHE_TTL = 60.0           # seconds
DIFF_CHAR_BUDGET = 50_000
LARGE_FILE_LINE_LIMIT = 1000
MAX_LIST_PAGES = 5              # 5 x 100 PRs is plenty for a digest
MAX_FILE_PAGES = 3              # 300 files
MAX_COMMENTS = 100
DETAIL_CONCURRENCY = 5

LOCKFILE_NAMES = {
    "package-lock.json",
    "npm-shrinkwrap.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "bun.lockb",
    "bun.lock",
    "poetry.lock",
    "uv.lock",
    "pdm.lock",
    "Pipfile.lock",
    "Gemfile.lock",
    "composer.lock",
    "Cargo.lock",
    "go.sum",
    "mix.lock",
    "flake.lock",
    "gradle.lockfile",
    "packages.lock.json",
    "pubspec.lock",
    "conan.lock",
}
LOCKFILE_SUFFIXES = (".lock", "-lock.json", ".lockfile")

logging.basicConfig(
    stream=sys.stderr,
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [gh-pr-slack] %(message)s",
)
log = logging.getLogger("gh-pr-slack")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class ToolError(Exception):
    """An API failure that should be reported to the model, not raised."""

    def __init__(self, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class RateLimitLow(Exception):
    """Raised instead of making a call when the GitHub budget is nearly spent."""

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(payload.get("message", "rate limit low"))
        self.payload = payload


def tool_result(fn):
    """Turn every exception into a structured result instead of a traceback."""

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except RateLimitLow as exc:
            log.warning("refusing call: %s", exc.payload.get("message"))
            return exc.payload
        except ToolError as exc:
            log.warning("%s failed: %s", fn.__name__, exc)
            return {"error": str(exc), "retryable": exc.retryable}
        except SlackApiError as exc:
            return slack_error(exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the boundary is the point
            log.exception("%s raised", fn.__name__)
            return {"error": f"{type(exc).__name__}: {exc}", "retryable": False}

    return wrapper


def slack_error(exc: SlackApiError) -> dict[str, Any]:
    code = "unknown"
    try:
        code = exc.response.get("error") or "unknown"
    except Exception:  # noqa: BLE001
        pass
    retryable = code in {"ratelimited", "service_unavailable", "internal_error", "fatal_error"}
    hint = {
        "not_in_channel": "Invite the bot to the channel first: /invite @your-bot",
        "channel_not_found": "Use a channel ID (C...) or a name the bot can see; needs channels:read.",
        "invalid_auth": "SLACK_BOT_TOKEN is invalid or revoked.",
        "missing_scope": "The bot token is missing a required scope (chat:write, channels:read).",
        "not_allowed_token_type": "Use a bot token (xoxb-...), not a user or app token.",
    }.get(code)
    message = f"Slack API error: {code}"
    if hint:
        message = f"{message} - {hint}"
    return {"error": message, "retryable": retryable}


# --------------------------------------------------------------------------- #
# GitHub client
# --------------------------------------------------------------------------- #


class GitHubClient:
    """Thin httpx wrapper that tracks the rate limit and normalizes errors."""

    def __init__(self, token: str) -> None:
        self._token = token
        self._client: httpx.AsyncClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.remaining: int | None = None
        self.limit: int | None = None
        self.reset_at: float | None = None

    def _http(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        # --selftest and the served session run in different loops; rebind if so.
        if self._client is None or self._loop is not loop:
            self._client = httpx.AsyncClient(
                base_url=GITHUB_API,
                timeout=GITHUB_TIMEOUT,
                follow_redirects=True,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "github-pr-slack-mcp/1.0",
                },
            )
            self._loop = loop
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- rate limiting ----------------------------------------------------- #

    def rate_limit_snapshot(self) -> dict[str, Any]:
        reset_iso = (
            datetime.fromtimestamp(self.reset_at, tz=timezone.utc).isoformat()
            if self.reset_at
            else None
        )
        resets_in = max(0, int(self.reset_at - time.time())) if self.reset_at else None
        return {
            "remaining": self.remaining,
            "limit": self.limit,
            "reset_at": reset_iso,
            "resets_in_seconds": resets_in,
        }

    def _guard(self) -> None:
        """Refuse to spend the last of the budget; warn the caller instead."""
        if self.remaining is None or self.remaining >= RATE_LIMIT_FLOOR:
            return
        if self.reset_at and time.time() >= self.reset_at:
            # The window rolled over; let the next response re-seed the counters.
            self.remaining = None
            return
        snapshot = self.rate_limit_snapshot()
        raise RateLimitLow(
            {
                "warning": "github_rate_limit_low",
                "message": (
                    f"GitHub rate limit nearly exhausted: {self.remaining} of "
                    f"{self.limit} requests left (floor is {RATE_LIMIT_FLOOR}). "
                    f"No request was made. Retry after {snapshot['reset_at']}."
                ),
                "rate_limit": snapshot,
                "retryable": True,
            }
        )

    def _absorb(self, resp: httpx.Response) -> None:
        try:
            if (remaining := resp.headers.get("x-ratelimit-remaining")) is not None:
                self.remaining = int(remaining)
            if (limit := resp.headers.get("x-ratelimit-limit")) is not None:
                self.limit = int(limit)
            if (reset := resp.headers.get("x-ratelimit-reset")) is not None:
                self.reset_at = float(reset)
        except ValueError:
            pass

    # -- requests ---------------------------------------------------------- #

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
        allow_404: bool = False,
    ) -> httpx.Response | None:
        self._guard()
        headers = {"Accept": accept} if accept else None
        try:
            resp = await self._http().request(method, path, params=params, headers=headers)
        except httpx.TimeoutException as exc:
            raise ToolError(f"GitHub request timed out: {method} {path}", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise ToolError(f"GitHub request failed: {exc}", retryable=True) from exc

        self._absorb(resp)
        if resp.status_code == 404 and allow_404:
            return None
        if resp.status_code >= 400:
            raise self._error_for(resp, method, path)
        return resp

    def _error_for(self, resp: httpx.Response, method: str, path: str) -> ToolError:
        try:
            payload = resp.json()
            body = payload.get("message", "") if isinstance(payload, dict) else ""
        except Exception:  # noqa: BLE001
            body = (resp.text or "")[:200]
        status = resp.status_code
        where = f"{method} {path}"

        if status == 401:
            return ToolError(
                f"GitHub rejected the token (401) on {where}. Check GITHUB_TOKEN.", False
            )
        if status == 403 and self.remaining == 0:
            snap = self.rate_limit_snapshot()
            return ToolError(
                f"GitHub rate limit exhausted on {where}; resets at {snap['reset_at']}.", True
            )
        if status == 403 and "secondary rate limit" in body.lower():
            return ToolError(f"GitHub secondary rate limit on {where}: {body}", True)
        if status == 403:
            detail = body or "check the PAT repository access and its repo/pull-request read permissions"
            return ToolError(f"GitHub denied access (403) on {where}: {detail}", False)
        if status == 404:
            return ToolError(
                f"Not found (404): {where}. It does not exist, or the PAT cannot see it.", False
            )
        if status == 422:
            return ToolError(f"GitHub rejected the request (422) on {where}: {body}", False)
        if status == 429:
            return ToolError(f"GitHub throttled the request (429) on {where}.", True)
        if status >= 500:
            return ToolError(f"GitHub server error ({status}) on {where}: {body}", True)
        return ToolError(f"GitHub error {status} on {where}: {body}", False)

    async def get_json(self, path: str, **kwargs: Any) -> Any:
        resp = await self.request("GET", path, **kwargs)
        return None if resp is None else resp.json()

    async def paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
        max_pages: int = MAX_LIST_PAGES,
    ) -> list[Any]:
        out: list[Any] = []
        for page in range(1, max_pages + 1):
            merged = dict(params or {})
            merged.update({"per_page": per_page, "page": page})
            batch = await self.get_json(path, params=merged)
            if not isinstance(batch, list) or not batch:
                break
            out.extend(batch)
            if len(batch) < per_page:
                break
        return out


# --------------------------------------------------------------------------- #
# Shared state
# --------------------------------------------------------------------------- #

github: GitHubClient | None = None
slack: AsyncWebClient | None = None

_list_cache: dict[tuple, tuple[float, Any]] = {}


def gh() -> GitHubClient:
    if github is None:
        raise ToolError("GitHub client is not configured (GITHUB_TOKEN missing).", False)
    return github


def sl() -> AsyncWebClient:
    if slack is None:
        raise ToolError("Slack client is not configured (SLACK_BOT_TOKEN missing).", False)
    return slack


def cache_get(key: tuple) -> Any | None:
    hit = _list_cache.get(key)
    if hit is None:
        return None
    stored_at, value = hit
    if time.time() - stored_at > LIST_CACHE_TTL:
        _list_cache.pop(key, None)
        return None
    return value


def cache_put(key: tuple, value: Any) -> None:
    now = time.time()
    for stale in [k for k, (t, _) in _list_cache.items() if now - t > LIST_CACHE_TTL]:
        _list_cache.pop(stale, None)
    _list_cache[key] = (now, value)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def split_repo(repo: str) -> tuple[str, str]:
    parts = (repo or "").strip().strip("/").split("/")
    if len(parts) != 2 or not all(parts):
        raise ToolError(f"repo must be in 'owner/name' form, got {repo!r}", False)
    return parts[0], parts[1]


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_lockfile(filename: str) -> bool:
    base = filename.rsplit("/", 1)[-1]
    return base in LOCKFILE_NAMES or base.endswith(LOCKFILE_SUFFIXES)


def logins(entries: Iterable[dict[str, Any]] | None) -> list[str]:
    return [e.get("login", "") for e in (entries or []) if isinstance(e, dict)]


def derive_review_state(detail: dict[str, Any], reviews: list[dict[str, Any]]) -> str:
    """approved | changes_requested | review_required | none."""
    latest: dict[str, str] = {}
    for review in reviews:  # the API returns these in submission order
        state = (review.get("state") or "").upper()
        if state not in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}:
            continue  # COMMENTED and PENDING do not change the verdict
        login = (review.get("user") or {}).get("login")
        if login:
            latest[login] = state
    verdicts = set(latest.values())
    if "CHANGES_REQUESTED" in verdicts:
        return "changes_requested"
    if "APPROVED" in verdicts:
        return "approved"
    if detail.get("requested_reviewers") or detail.get("requested_teams"):
        return "review_required"
    return "none"


def pr_summary(detail: dict[str, Any], review_state: str) -> dict[str, Any]:
    return {
        "number": detail.get("number"),
        "title": detail.get("title"),
        "author": (detail.get("user") or {}).get("login"),
        "url": detail.get("html_url"),
        "created_at": detail.get("created_at"),
        "updated_at": detail.get("updated_at"),
        "draft": bool(detail.get("draft")),
        "additions": detail.get("additions"),
        "deletions": detail.get("deletions"),
        "changed_files": detail.get("changed_files"),
        "review_state": review_state,
        "mergeable_state": detail.get("mergeable_state"),
        "labels": [lbl.get("name") for lbl in detail.get("labels") or []],
    }


async def gather_bounded(coros: list[Any], limit: int = DETAIL_CONCURRENCY) -> list[Any]:
    """Run coroutines with bounded concurrency, surfacing the first failure."""
    sem = asyncio.Semaphore(limit)

    async def runner(coro):
        async with sem:
            return await coro

    results = await asyncio.gather(*(runner(c) for c in coros), return_exceptions=True)
    for item in results:
        if isinstance(item, BaseException):
            raise item
    return results


async def fetch_reviews(owner: str, name: str, number: int) -> list[dict[str, Any]]:
    return await gh().paginate(
        f"/repos/{owner}/{name}/pulls/{number}/reviews", max_pages=2
    )


async def fetch_detail_and_reviews(
    owner: str, name: str, number: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    detail = await gh().get_json(f"/repos/{owner}/{name}/pulls/{number}")
    reviews = await fetch_reviews(owner, name, number)
    return detail, reviews


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #

mcp = McpServer("github-pr-slack")


@mcp.tool()
@tool_result
async def list_pull_requests(
    repo: str,
    state: str = "open",
    author: str | None = None,
    reviewer: str | None = None,
    max_results: int = 20,
) -> Any:
    """List pull requests for a repository with review and size metadata.

    Args:
        repo: Repository in "owner/name" form.
        state: One of "open", "closed", "all".
        author: Only PRs opened by this GitHub login.
        reviewer: Only PRs this login has been requested to review or has reviewed.
        max_results: Maximum number of PRs to return (1-100).

    Returns a list of PR objects, or a single object with "error"/"warning" on failure.
    Results are cached for 60 seconds keyed on the full argument set.
    """
    owner, name = split_repo(repo)
    state = (state or "open").lower()
    if state not in {"open", "closed", "all"}:
        raise ToolError("state must be one of: open, closed, all", False)
    max_results = max(1, min(int(max_results), 100))

    key = ("list_pull_requests", owner, name, state, author, reviewer, max_results)
    if (cached := cache_get(key)) is not None:
        log.info("list_pull_requests cache hit for %s", key)
        return cached

    raw = await gh().paginate(
        f"/repos/{owner}/{name}/pulls",
        params={"state": state, "sort": "updated", "direction": "desc"},
    )

    if author:
        wanted = author.lower()
        raw = [pr for pr in raw if ((pr.get("user") or {}).get("login", "").lower() == wanted)]

    if reviewer:
        # requested_reviewers covers pending requests; a completed review needs the
        # reviews endpoint, so widen the candidate pool before filtering.
        pool = raw[: max(max_results * 3, 30)]
        enriched = await gather_bounded(
            [fetch_detail_and_reviews(owner, name, pr["number"]) for pr in pool]
        )
        wanted = reviewer.lower()
        matched = []
        for detail, reviews in enriched:
            requested = {login.lower() for login in logins(detail.get("requested_reviewers"))}
            reviewed = {
                (r.get("user") or {}).get("login", "").lower() for r in reviews
            }
            if wanted in requested or wanted in reviewed:
                matched.append((detail, reviews))
        enriched = matched[:max_results]
    else:
        pool = raw[:max_results]
        enriched = await gather_bounded(
            [fetch_detail_and_reviews(owner, name, pr["number"]) for pr in pool]
        )

    result = [pr_summary(detail, derive_review_state(detail, reviews)) for detail, reviews in enriched]
    cache_put(key, result)
    return result


@mcp.tool()
@tool_result
async def get_pull_request(repo: str, number: int, include_diff: bool = False) -> Any:
    """Fetch one pull request with body, review comments, and CI status.

    Args:
        repo: Repository in "owner/name" form.
        number: Pull request number.
        include_diff: Also return the unified diff. Lockfiles and files with more
            than 1000 changed lines are listed under "skipped_files" instead of
            being inlined; the diff itself is truncated to 50k characters with a
            "diff_truncated" flag.
    """
    owner, name = split_repo(repo)
    number = int(number)

    detail, reviews = await fetch_detail_and_reviews(owner, name, number)
    head_sha = ((detail.get("head") or {}).get("sha")) or ""

    raw_comments = await gh().paginate(
        f"/repos/{owner}/{name}/pulls/{number}/comments", max_pages=2
    )

    result: dict[str, Any] = pr_summary(detail, derive_review_state(detail, reviews))
    result.update(
        {
            "repo": f"{owner}/{name}",
            "state": detail.get("state"),
            "merged": bool(detail.get("merged")),
            "mergeable": detail.get("mergeable"),
            "body": detail.get("body"),
            "base": (detail.get("base") or {}).get("ref"),
            "head": (detail.get("head") or {}).get("ref"),
            "head_sha": head_sha,
            "commits": detail.get("commits"),
            "requested_reviewers": logins(detail.get("requested_reviewers")),
            "reviews": [
                {
                    "author": (r.get("user") or {}).get("login"),
                    "state": r.get("state"),
                    "body": r.get("body"),
                    "submitted_at": r.get("submitted_at"),
                    "url": r.get("html_url"),
                }
                for r in reviews
            ],
            "review_comments": [
                {
                    "author": (c.get("user") or {}).get("login"),
                    "path": c.get("path"),
                    "line": c.get("line") or c.get("original_line"),
                    "body": c.get("body"),
                    "created_at": c.get("created_at"),
                    "in_reply_to": c.get("in_reply_to_id"),
                    "url": c.get("html_url"),
                }
                for c in raw_comments[:MAX_COMMENTS]
            ],
            "checks": await fetch_checks(owner, name, head_sha),
        }
    )

    if include_diff:
        result.update(await build_diff(owner, name, number))

    return result


async def fetch_checks(owner: str, name: str, sha: str) -> dict[str, Any]:
    """Combine check-runs and legacy commit statuses into one verdict."""
    if not sha:
        return {"state": "unknown", "total": 0, "runs": []}

    runs: list[dict[str, Any]] = []

    check_runs = await gh().get_json(
        f"/repos/{owner}/{name}/commits/{sha}/check-runs",
        params={"per_page": 100},
        allow_404=True,
    )
    for run in (check_runs or {}).get("check_runs", []):
        runs.append(
            {
                "name": run.get("name"),
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
                "url": run.get("html_url") or run.get("details_url"),
            }
        )

    combined = await gh().get_json(
        f"/repos/{owner}/{name}/commits/{sha}/status", allow_404=True
    )
    for status in (combined or {}).get("statuses", []):
        runs.append(
            {
                "name": status.get("context"),
                "status": "completed" if status.get("state") != "pending" else "in_progress",
                "conclusion": {"success": "success", "failure": "failure", "error": "failure"}.get(
                    status.get("state"), status.get("state")
                ),
                "url": status.get("target_url"),
            }
        )

    failing = [r for r in runs if r["conclusion"] in {"failure", "timed_out", "action_required"}]
    pending = [r for r in runs if r["status"] in {"queued", "in_progress", "pending"}]
    passing = [r for r in runs if r["conclusion"] in {"success", "neutral", "skipped"}]

    if not runs:
        state = "none"
    elif failing:
        state = "failing"
    elif pending:
        state = "pending"
    else:
        state = "passing"

    return {
        "state": state,
        "total": len(runs),
        "passing": len(passing),
        "failing": len(failing),
        "pending": len(pending),
        "failing_checks": [r["name"] for r in failing],
        "runs": runs,
    }


async def build_diff(owner: str, name: str, number: int) -> dict[str, Any]:
    """Assemble a unified diff, skipping lockfiles and very large files."""
    files = await gh().paginate(
        f"/repos/{owner}/{name}/pulls/{number}/files", max_pages=MAX_FILE_PAGES
    )

    skipped: list[dict[str, Any]] = []
    chunks: list[str] = []

    for entry in files:
        filename = entry.get("filename", "")
        additions = entry.get("additions", 0) or 0
        deletions = entry.get("deletions", 0) or 0
        patch = entry.get("patch")

        reason = None
        if is_lockfile(filename):
            reason = "lockfile"
        elif additions + deletions > LARGE_FILE_LINE_LIMIT:
            reason = "over_1000_lines"
        elif patch is None:
            reason = "binary_or_unavailable"
        elif patch.count("\n") + 1 > LARGE_FILE_LINE_LIMIT:
            reason = "over_1000_lines"

        if reason:
            skipped.append(
                {
                    "filename": filename,
                    "additions": additions,
                    "deletions": deletions,
                    "reason": reason,
                }
            )
            continue

        previous = entry.get("previous_filename") or filename
        chunks.append(
            f"diff --git a/{previous} b/{filename}\n"
            f"--- a/{previous}\n"
            f"+++ b/{filename}\n"
            f"{patch}\n"
        )

    diff = "".join(chunks)
    truncated = len(diff) > DIFF_CHAR_BUDGET
    if truncated:
        diff = diff[:DIFF_CHAR_BUDGET]

    return {
        "diff": diff,
        "diff_truncated": truncated,
        "skipped_files": skipped,
        "files_in_diff": len(chunks),
        "files_total": len(files),
    }


@mcp.tool()
@tool_result
async def find_stale_pull_requests(repo: str, days: int = 3) -> Any:
    """Open, non-draft PRs with no push or review activity in the last N days.

    Args:
        repo: Repository in "owner/name" form.
        days: Inactivity threshold in days.

    Returns the same shape as list_pull_requests plus days_stale,
    last_activity_type ("push", "review" or "created") and last_activity_at.
    """
    owner, name = split_repo(repo)
    days = max(0, int(days))
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    open_prs = await gh().paginate(
        f"/repos/{owner}/{name}/pulls",
        params={"state": "open", "sort": "updated", "direction": "desc"},
    )
    candidates = [pr for pr in open_prs if not pr.get("draft")]

    activities = await gather_bounded(
        [last_activity(owner, name, pr) for pr in candidates]
    )

    stale: list[tuple[dict[str, Any], datetime, str]] = []
    for pr, (when, kind) in zip(candidates, activities):
        if when < cutoff:
            stale.append((pr, when, kind))

    if not stale:
        return []

    enriched = await gather_bounded(
        [fetch_detail_and_reviews(owner, name, pr["number"]) for pr, _, _ in stale]
    )

    results = []
    for (_, when, kind), (detail, reviews) in zip(stale, enriched):
        row = pr_summary(detail, derive_review_state(detail, reviews))
        row["days_stale"] = (now - when).days
        row["last_activity_type"] = kind
        row["last_activity_at"] = when.isoformat()
        results.append(row)

    results.sort(key=lambda r: r["days_stale"], reverse=True)
    return results


async def last_activity(owner: str, name: str, pr: dict[str, Any]) -> tuple[datetime, str]:
    """Latest push or review on a PR, falling back to when it was created."""
    created = parse_ts(pr.get("created_at")) or datetime.now(timezone.utc)
    best, kind = created, "created"

    sha = (pr.get("head") or {}).get("sha")
    if sha:
        commit = await gh().get_json(f"/repos/{owner}/{name}/commits/{sha}", allow_404=True)
        committer = ((commit or {}).get("commit") or {}).get("committer") or {}
        pushed = parse_ts(committer.get("date"))
        if pushed and pushed > best:
            best, kind = pushed, "push"

    for review in await fetch_reviews(owner, name, pr["number"]):
        submitted = parse_ts(review.get("submitted_at"))
        if submitted and submitted > best:
            best, kind = submitted, "review"

    return best, kind


@mcp.tool()
@tool_result
async def post_to_slack(
    channel: str,
    text: str,
    blocks: list[dict[str, Any]] | None = None,
    thread_ts: str | None = None,
) -> Any:
    """Post a message to Slack exactly as given. No formatting is added.

    Args:
        channel: Channel ID (C...) or #name. The bot must be a member.
        text: Message text, also used as the notification/accessibility fallback.
        blocks: Optional Slack Block Kit blocks. If Slack rejects them, the
            message is retried as plain text.
        thread_ts: Reply in this thread instead of posting to the channel.

    Returns {ts, channel, permalink}.
    """
    if not channel:
        raise ToolError("channel is required", False)
    if not text and not blocks:
        raise ToolError("text is required (Slack uses it as the notification fallback)", False)

    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts

    fell_back = False
    try:
        response = await sl().chat_postMessage(**payload, blocks=blocks) if blocks else await sl().chat_postMessage(**payload)
    except SlackApiError as exc:
        code = exc.response.get("error") if exc.response else None
        if blocks and code in {"invalid_blocks", "invalid_blocks_format", "blocks_too_long", "msg_too_long"}:
            log.warning("Slack rejected blocks (%s); retrying as plain text", code)
            response = await sl().chat_postMessage(**payload)
            fell_back = True
        else:
            raise

    ts = response.get("ts")
    posted_channel = response.get("channel")

    permalink = None
    try:
        link = await sl().chat_getPermalink(channel=posted_channel, message_ts=ts)
        permalink = link.get("permalink")
    except SlackApiError as exc:  # the message is already posted; don't fail on this
        log.warning("could not fetch permalink: %s", exc)

    result = {"ts": ts, "channel": posted_channel, "permalink": permalink}
    if fell_back:
        result["fell_back_to_text"] = True
    return result


# --------------------------------------------------------------------------- #
# Startup checks and selftest
# --------------------------------------------------------------------------- #


async def verify_github() -> dict[str, Any]:
    limits = await gh().get_json("/rate_limit")
    core = ((limits or {}).get("resources") or {}).get("core") or {}
    if core:
        github.remaining = core.get("remaining", github.remaining)
        github.limit = core.get("limit", github.limit)
        github.reset_at = float(core.get("reset", github.reset_at or 0)) or github.reset_at
    user = await gh().get_json("/user")
    return {"login": (user or {}).get("login"), "rate_limit": gh().rate_limit_snapshot()}


async def verify_slack() -> dict[str, Any]:
    auth = await sl().auth_test()
    scopes = (auth.headers or {}).get("x-oauth-scopes", "")
    granted = {s.strip() for s in scopes.split(",") if s.strip()}
    missing = [s for s in ("chat:write", "channels:read") if granted and s not in granted]
    if missing:
        print(
            f"WARNING: Slack token is missing scope(s): {', '.join(missing)}",
            file=sys.stderr,
        )
    return {
        "team": auth.get("team"),
        "bot": auth.get("user"),
        "bot_id": auth.get("bot_id"),
        "scopes": sorted(granted),
        "missing_scopes": missing,
    }


async def startup_check() -> None:
    """Fail fast if either token is rejected."""
    try:
        info = await verify_github()
    except (ToolError, RateLimitLow) as exc:
        die(f"GitHub token check failed: {exc}")
    except Exception as exc:  # noqa: BLE001
        die(f"GitHub token check failed: {type(exc).__name__}: {exc}")
    else:
        log.info(
            "GitHub OK as %s (%s/%s calls left)",
            info["login"],
            info["rate_limit"]["remaining"],
            info["rate_limit"]["limit"],
        )

    try:
        slack_info = await verify_slack()
    except SlackApiError as exc:
        die(f"Slack token check failed: {slack_error(exc)['error']}")
    except Exception as exc:  # noqa: BLE001
        die(f"Slack token check failed: {type(exc).__name__}: {exc}")
    else:
        log.info("Slack OK as %s in team %s", slack_info["bot"], slack_info["team"])

    await gh().aclose()


async def selftest(repo: str) -> int:
    """Verify both tokens and read 3 PRs. Posts nothing."""
    ok = True

    try:
        info = await verify_github()
        print(f"[ok]   GitHub authenticated as {info['login']}", file=sys.stderr)
        rl = info["rate_limit"]
        print(f"       rate limit: {rl['remaining']}/{rl['limit']} until {rl['reset_at']}", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001
        print(f"[fail] GitHub: {exc}", file=sys.stderr)
        ok = False

    try:
        slack_info = await verify_slack()
        print(
            f"[ok]   Slack authenticated as {slack_info['bot']} in {slack_info['team']}",
            file=sys.stderr,
        )
        if slack_info["scopes"]:
            print(f"       scopes: {', '.join(slack_info['scopes'])}", file=sys.stderr)
    except SlackApiError as exc:
        print(f"[fail] Slack: {slack_error(exc)['error']}", file=sys.stderr)
        ok = False
    except Exception as exc:  # noqa: BLE001
        print(f"[fail] Slack: {exc}", file=sys.stderr)
        ok = False

    if ok:
        result = await list_pull_requests(repo=repo, max_results=3)
        if isinstance(result, dict):
            print(f"[fail] list_pull_requests({repo}): {result}", file=sys.stderr)
            ok = False
        else:
            print(f"[ok]   list_pull_requests({repo}) returned {len(result)} PR(s)", file=sys.stderr)
            for pr in result:
                print(
                    f"       #{pr['number']} {pr['title']} "
                    f"(by {pr['author']}, {pr['review_state']}, "
                    f"+{pr['additions']}/-{pr['deletions']})",
                    file=sys.stderr,
                )

    print(f"[{'ok' if ok else 'fail'}]   selftest {'passed' if ok else 'failed'} (nothing was posted to Slack)", file=sys.stderr)
    await gh().aclose()
    return 0 if ok else 1


def die(message: str) -> None:
    print(f"FATAL: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub pull requests -> Slack MCP server")
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="Verify both tokens, list 3 PRs from --repo, then exit without posting.",
    )
    parser.add_argument("--repo", help="owner/name, used by --selftest")
    args = parser.parse_args()

    github_token = os.environ.get("GITHUB_TOKEN", "").strip()
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()

    missing = []
    if not github_token:
        missing.append("GITHUB_TOKEN (fine-grained PAT with repo + pull requests read)")
    if not slack_token:
        missing.append("SLACK_BOT_TOKEN (bot token with chat:write + channels:read)")
    if missing:
        die("missing environment variable(s):\n  - " + "\n  - ".join(missing))

    if not slack_token.startswith("xoxb-"):
        print(
            "WARNING: SLACK_BOT_TOKEN does not look like a bot token (expected xoxb-...)",
            file=sys.stderr,
        )

    global github, slack
    github = GitHubClient(github_token)
    slack = AsyncWebClient(token=slack_token)

    if args.selftest:
        if not args.repo:
            die("--selftest requires --repo owner/name")
        sys.exit(asyncio.run(selftest(args.repo)))

    asyncio.run(startup_check())
    log.info("starting MCP server on stdio")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
