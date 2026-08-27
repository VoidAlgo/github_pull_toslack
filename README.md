# github-pr-slack MCP server

An MCP server (stdio transport) that reads pull requests from GitHub and posts
messages to Slack.

**The server does not write summaries.** It returns structured PR data and posts
whatever text it is handed. Summarizing is the model's job, so the digest format
lives in your prompt and can change without touching this code.

## Tools

| Tool | What it does |
| --- | --- |
| `list_pull_requests(repo, state="open", author=None, reviewer=None, max_results=20)` | PRs with author, size, labels, `review_state` (`approved` / `changes_requested` / `review_required` / `none`) and `mergeable_state`. Cached 60s on the full argument set. |
| `get_pull_request(repo, number, include_diff=False)` | Everything above plus body, reviews, inline review comments and CI check status. With `include_diff`, a unified diff truncated to 50k chars (`diff_truncated: true`); lockfiles and files over 1000 changed lines are listed under `skipped_files` as `{filename, additions, deletions, reason}`. |


| `find_stale_pull_requests(repo, days=3)` | Open, non-draft PRs with no **push or review** in the last N days. Same shape as `list_pull_requests` plus `days_stale`, `last_activity_type` (`push` / `review` / `created`) and `last_activity_at`. A PR that only got comments still counts as stale. |
| `post_to_slack(channel, text, blocks=None, thread_ts=None)` | Posts the message as given. Returns `{ts, channel, permalink}`. Accepts Block Kit; if Slack rejects the blocks it retries as plain text and sets `fell_back_to_text`. |

`repo` is always `owner/name`.

### Failure and rate-limit behavior

- Tools never raise. Failures come back as `{"error": "...", "retryable": true|false}`.
  Timeouts, 5xx, 429 and secondary rate limits are `retryable: true`; 401/403/404/422 are not.
- Every response's `x-ratelimit-remaining` is tracked. Below **100** remaining the
  server stops calling GitHub and returns a structured warning instead:

  ```json
  {
    "warning": "github_rate_limit_low",
    "message": "GitHub rate limit nearly exhausted: 87 of 5000 requests left ...",
    "rate_limit": {"remaining": 87, "limit": 5000, "reset_at": "...", "resets_in_seconds": 412},
    "retryable": true
  }
  ```

- All logging goes to **stderr**. stdout is the MCP transport.

## Install

```bash
python -m venv .venv
```

```bash
.venv/Scripts/python.exe -m pip install -r requirements.txt
```

On macOS/Linux the interpreter is `.venv/bin/python` instead.

## Creating the GitHub token

A fine-grained personal access token.

1. GitHub → your avatar → **Settings** → **Developer settings** →
   **Personal access tokens** → **Fine-grained tokens** → **Generate new token**.
2. **Resource owner**: pick the account or organization that owns the repos.
   If it is an org, an org owner may need to approve the token before it works.
3. **Repository access**: *Only select repositories* → pick the ones you want to
   read (or *All repositories*).
4. **Repository permissions** — set these two to **Read-only**:
   - **Contents** (`repo` read) — needed for commits and diffs
   - **Pull requests** — needed for PRs, reviews and review comments
   
   Everything else can stay *No access*. For CI check status on private repos,
   also grant **Checks: Read-only** — without it `checks` comes back empty
   rather than failing.
5. Generate, copy the `github_pat_...` value, and set it as `GITHUB_TOKEN`.

A classic PAT with the `repo` scope also works if you prefer.

## Creating the Slack app

1. Go to <https://api.slack.com/apps> → **Create New App** → **From scratch**.
   Name it (e.g. `PR Digest`) and pick your workspace.
2. In the sidebar: **OAuth & Permissions** → **Scopes** → **Bot Token Scopes**
   → **Add an OAuth Scope**, and add:
   - `chat:write` — post messages
   - `channels:read` — resolve public channel names to IDs
   
   Optional, depending on where you post: `groups:read` (private channels),
   `chat:write.public` (post to public channels without joining them first).
3. Scroll up → **Install to Workspace** → **Allow**.
4. Copy the **Bot User OAuth Token** (`xoxb-...`) and set it as `SLACK_BOT_TOKEN`.
5. **Invite the bot to every channel it posts in**, otherwise Slack returns
   `not_in_channel`:

   ```
   /invite @PR Digest
   ```

If you change scopes later you must reinstall the app for the new token to carry them.

## Self test

Verifies both tokens and reads 3 PRs. **It never posts to Slack.**

```bash
.venv/Scripts/python.exe server.py --selftest --repo owner/name
```

```
[ok]   GitHub authenticated as your-login
       rate limit: 4987/5000 until 2026-08-27T18:22:00+00:00
[ok]   Slack authenticated as pr_digest in Acme
       scopes: channels:read, chat:write
[ok]   list_pull_requests(owner/name) returned 3 PR(s)
       #412 Fix retry backoff (by alice, approved, +64/-12)
       ...
[ok]   selftest passed (nothing was posted to Slack)
```

Exit code is 0 on success, 1 on failure. All output is on stderr.

## Claude Desktop configuration

`claude_desktop_config.json` lives at:

- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "github-pr-slack": {
      "command": "D:\\GEN AI\\github_pull_toslack\\.venv\\Scripts\\python.exe",
      "args": ["D:\\GEN AI\\github_pull_toslack\\server.py"],
      "env": {
        "GITHUB_TOKEN": "github_pat_...",
        "SLACK_BOT_TOKEN": "xoxb-..."
      }
    }
  }
}
```

macOS/Linux equivalent:

```json
{
  "mcpServers": {
    "github-pr-slack": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["/absolute/path/to/server.py"],
      "env": {
        "GITHUB_TOKEN": "github_pat_...",
        "SLACK_BOT_TOKEN": "xoxb-..."
      }
    }
  }
}
```

Use absolute paths for both the interpreter and the script — Claude Desktop does
not launch the server from this directory. Restart Claude Desktop after editing.

If either token is missing or rejected, the server prints a `FATAL:` line to
stderr and exits 1 before serving anything.

## Example prompt

> Give me a digest of stale PRs in `acme/api` older than 4 days, group them by
> author, and post it to `#eng-standup`.

The model calls `find_stale_pull_requests`, writes the digest itself, then calls
`post_to_slack`.

## Optional environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `GITHUB_API_URL` | `https://api.github.com` | Point at GitHub Enterprise |
| `GITHUB_TIMEOUT_SECONDS` | `30` | Per-request timeout |
| `LOG_LEVEL` | `INFO` | `DEBUG` to see every HTTP call on stderr |

## Notes on cost

`list_pull_requests` needs per-PR detail (for `additions`/`deletions`/
`mergeable_state`) and per-PR reviews (for `review_state`), so it spends roughly
`1 + 2 × max_results` API calls. That is why results are cached for 60 seconds —
a multi-step digest that lists, then inspects a few PRs, does not refetch the list.
