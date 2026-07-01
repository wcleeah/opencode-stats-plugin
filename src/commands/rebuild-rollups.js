import { createReportRun } from "../analytics/report.js"
import { rebuildRollups } from "../analytics/rollups.js"
import { ensureSchema, getTursoClient } from "../analytics/turso.js"
import { toErrorMessage } from "../analytics/utils.js"

export async function rebuildAllRollups() {
  const run = createReportRun("usage-tracker-rebuild-rollups")
  const client = getTursoClient()
  if (!client) return run.finish({ ok: false, error: "Turso not configured (missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN)" })
  try {
    run.log("Starting rollup rebuild command")
    await ensureSchema(client)
    // Full rebuild reads from Turso directly (INSERT...SELECT cannot split across databases)
    // The hot-path rollup recomputation in the queue uses local SQLite for reads
    await rebuildRollups(client, run.log)
    return run.finish({ ok: true, summary: { rebuilt: true } })
  } catch (error) {
    return run.finish({ ok: false, error: toErrorMessage(error) })
  } finally {
    client?.close()
  }
}
