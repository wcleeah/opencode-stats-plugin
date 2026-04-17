import { tool } from "@opencode-ai/plugin"

import { backfillOpenCodeDb } from "../../src/commands/backfill-opencode-db.js"
import { rebuildAllRollups } from "../../src/commands/rebuild-rollups.js"
import { replayOutboxCommand } from "../../src/commands/replay-outbox.js"
import { verifyAnalytics } from "../../src/commands/verify-analytics.js"
import { createOpenCodeHydrator } from "./history.js"
import { createTrackerState } from "./normalize.js"
import { createIngestionQueue } from "./queue.js"

export const UsageTracker = async ({ project }) => {
  const state = createTrackerState(project)
  const history = createOpenCodeHydrator({ state })
  await history.hydrateSessions()
  const queue = createIngestionQueue({
    project,
    state,
    ensureEventContext: history.hydrateEventContext,
  })
  await queue.start()

  return {
    tool: {
      "usage-tracker-flush": tool({
        description: "Flush pending usage tracker writes and replay this process outbox.",
        args: {},
        async execute() {
          await queue.flush()
          return { ok: true, processID: queue.processID }
        },
      }),
      "usage-tracker-replay-all": tool({
        description: "Replay all durable usage tracker outbox batches.",
        args: {},
        async execute() {
          await queue.replayAllOutbox()
          return { ok: true }
        },
      }),
      "usage-tracker-backfill": tool({
        description: "Backfill OpenCode SQLite history into Turso analytics.",
        args: {},
        async execute() {
          const result = await backfillOpenCodeDb({ fresh: false })
          return { ok: result.ok, reportPath: result.reportPath }
        },
      }),
      "usage-tracker-backfill-fresh": tool({
        description: "Drop analytics tables, then backfill OpenCode SQLite history into Turso analytics.",
        args: {},
        async execute() {
          const result = await backfillOpenCodeDb({ fresh: true })
          return { ok: result.ok, reportPath: result.reportPath }
        },
      }),
      "usage-tracker-rebuild-rollups": tool({
        description: "Rebuild analytics rollups from Turso fact tables.",
        args: {},
        async execute() {
          const result = await rebuildAllRollups()
          return { ok: result.ok, reportPath: result.reportPath }
        },
      }),
      "usage-tracker-replay-outbox": tool({
        description: "Replay durable outbox batches into Turso and rebuild rollups.",
        args: {},
        async execute() {
          const result = await replayOutboxCommand()
          return { ok: result.ok, reportPath: result.reportPath }
        },
      }),
      "usage-tracker-verify-analytics": tool({
        description: "Run coarse analytics verification queries and write a report.",
        args: {},
        async execute() {
          const result = await verifyAnalytics()
          return { ok: result.ok, reportPath: result.reportPath }
        },
      }),
    },
    event: async ({ event }) => {
      try {
        await queue.enqueue(event)
      } catch (error) {
        console.error("[usage-tracker] enqueue failed", error instanceof Error ? error.message : String(error))
      }
    },
    "tool.execute.after": async (input, output) => {
      if (input.tool?.startsWith("usage-tracker-")) {
        output.title = "Usage tracker maintenance"
      }
    },
    "command.execute.before": async (input) => {
      if (input.command === "exit") {
        await queue.flush()
      }
    },
    "tool.execute.before": async (input) => {
      if (
        input.tool === "usage-tracker-flush" ||
        input.tool === "usage-tracker-replay-all" ||
        input.tool === "usage-tracker-rebuild-rollups" ||
        input.tool === "usage-tracker-replay-outbox" ||
        input.tool === "usage-tracker-verify-analytics"
      ) {
        await queue.flush()
      }
    },
  }
}
