# opencode-stats-plugin

`opencode-stats-plugin` is a local OpenCode plugin that captures session, response, and tool activity and writes analytics into Turso.

It keeps a durable local outbox so batches survive process exits or transient write failures, and it can backfill the full history from OpenCode's local SQLite database.

## What It Tracks

- Projects, sessions, and session lineage
- User turns, including synthetic and compaction markers
- Assistant responses, model/provider metadata, tokens, cost, finish state, and errors
- Response text and reasoning parts
- LLM step timing
- Tool calls plus tool input/output payload sizes
- Rollups by session, project, model, tool, and day

## How It Works

1. `UsageTracker` hydrates known session and message context from `opencode.db`.
2. Incoming OpenCode events are normalized into fact rows plus a set of touched rollup keys.
3. Facts are persisted to a durable JSON outbox under `~/.local/share/opencode/usage-outbox/`.
4. The queue writes fact tables to Turso, then recomputes only the affected rollups.
5. On startup, orphaned outbox files are replayed before normal ingestion resumes.

## Repository Layout

- `plugins/usage-tracker/`: OpenCode plugin entrypoint, history hydration, event normalization, and ingestion queue
- `src/analytics/`: schema, fact derivation, Turso writes, rollups, durable outbox, and reporting helpers
- `src/commands/`: maintenance commands exposed through plugin tools
- `test/`: smoke tests for report writing and tool registration

## Requirements

- Bun
- OpenCode
- A Turso/libSQL database
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- Optional: `OPENCODE_DB` if your OpenCode SQLite DB is not at the default path

## Installation

Install dependencies:

```bash
bun install
```

Set the required environment variables:

```bash
export TURSO_DATABASE_URL="libsql://your-database.turso.io"
export TURSO_AUTH_TOKEN="your-token"

# optional override for the OpenCode SQLite history database
export OPENCODE_DB="$HOME/.local/share/opencode/opencode.db"
```

If this repo lives inside `~/.config/opencode/plugins/opencode-stats-plugin`, add a top-level wrapper file next to it:

```js
// ~/.config/opencode/plugins/usage-tracker.js
export { UsageTracker } from "./opencode-stats-plugin/plugins/usage-tracker/index.js"
```

OpenCode only auto-loads plugin files directly under `~/.config/opencode/plugins/`, not nested directories.

## Plugin Tools

Once the plugin is loaded, OpenCode gets these maintenance tools:

| Tool | Purpose |
| --- | --- |
| `usage-tracker-flush` | Flush pending in-memory writes and replay this process outbox |
| `usage-tracker-replay-all` | Replay all durable outbox batches across processes |
| `usage-tracker-backfill` | Import historical data from `opencode.db` into Turso and rebuild rollups |
| `usage-tracker-backfill-fresh` | Drop analytics tables, then run a full backfill |
| `usage-tracker-rebuild-rollups` | Recompute rollups from existing fact tables |
| `usage-tracker-replay-outbox` | Replay durable outbox batches and rebuild rollups |
| `usage-tracker-verify-analytics` | Run coarse count checks and write a report |

`usage-tracker-backfill-fresh` is destructive: it drops all analytics tables before re-importing data.

## Data Model

Fact tables:

- `projects`
- `sessions`
- `turns`
- `responses`
- `response_parts`
- `llm_steps`
- `tool_calls`
- `tool_payloads`

Rollup tables:

- `session_rollups`
- `session_model_rollups`
- `project_rollups`
- `project_model_rollups`
- `tool_rollups`
- `daily_global_rollups`
- `daily_model_rollups`
- `daily_tool_rollups`
- `daily_project_rollups`

## Local State

- OpenCode history source: `${XDG_DATA_HOME:-~/.local/share}/opencode/opencode.db`
- Durable outbox: `~/.local/share/opencode/usage-outbox/`
- Command reports: `~/.local/share/opencode/usage-tracker/logs/`

Maintenance commands write both Markdown and JSONL reports and return the generated report path.

## Development

There is no build step.

```bash
bun test
bun run typecheck
```
