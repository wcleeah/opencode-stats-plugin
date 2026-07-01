import { createOpenCodeHydrator } from "./history.js"
import { createTrackerState } from "./normalize.js"
import { createIngestionQueue } from "./queue.js"
import { createLocalSqlite } from "../../src/analytics/local-sqlite.js"

export const UsageTracker = async ({ project }) => {
  const state = createTrackerState(project)
  const history = createOpenCodeHydrator({ state })
  await history.hydrateSessions()
  const localSqlite = createLocalSqlite(console)
  const queue = createIngestionQueue({
    project,
    state,
    ensureEventContext: history.hydrateEventContext,
    localSqlite,
  })
  await queue.start()

  return {
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
