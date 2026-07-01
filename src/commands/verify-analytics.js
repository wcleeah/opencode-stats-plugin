import { createReportRun } from "../analytics/report.js"
import { getTursoClient } from "../analytics/turso.js"
import { toErrorMessage } from "../analytics/utils.js"

const checks = [
  ["projects", "SELECT COUNT(*) AS count FROM projects"],
  ["sessions", "SELECT COUNT(*) AS count FROM sessions"],
  ["turns", "SELECT COUNT(*) AS count FROM turns"],
  ["responses", "SELECT COUNT(*) AS count FROM responses"],
  ["llm_steps", "SELECT COUNT(*) AS count FROM llm_steps"],
  ["tool_calls", "SELECT COUNT(*) AS count FROM tool_calls"],
  ["session_rollups", "SELECT COUNT(*) AS count FROM session_rollups"],
  ["project_rollups", "SELECT COUNT(*) AS count FROM project_rollups"],
]

export async function verifyAnalytics() {
  const run = createReportRun("usage-tracker-verify-analytics")
  const client = getTursoClient()
  if (!client) return run.finish({ ok: false, error: "Turso not configured (missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN)" })
  try {
    const counts = {}
    run.log("Starting analytics verification", { check_count: checks.length })
    for (const [label, sql] of checks) {
      const result = await client.execute(sql)
      const count = Number(result.rows[0]?.count ?? 0)
      counts[label] = count
      run.log("Verification query complete", { label, count })
    }
    return run.finish({ ok: true, summary: { counts } })
  } catch (error) {
    return run.finish({ ok: false, error: toErrorMessage(error) })
  } finally {
    client?.close()
  }
}
