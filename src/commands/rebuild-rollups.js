import { createReportRun } from "../analytics/report.js"
import { rebuildRollups } from "../analytics/rollups.js"
import { ensureSchema, getTursoClient } from "../analytics/turso.js"
import { toErrorMessage } from "../analytics/utils.js"

export async function rebuildAllRollups() {
  const run = createReportRun("usage-tracker-rebuild-rollups")
  const client = getTursoClient()
  try {
    run.log("Starting rollup rebuild command")
    await ensureSchema(client)
    await rebuildRollups(client, run.log)
    return run.finish({ ok: true, summary: { rebuilt: true } })
  } catch (error) {
    return run.finish({ ok: false, error: toErrorMessage(error) })
  } finally {
    client.close()
  }
}
