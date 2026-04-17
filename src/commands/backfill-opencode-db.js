import { deriveFacts } from "../analytics/derive.js"
import { loadOpenCodeData } from "../analytics/opencode-db.js"
import { createReportRun } from "../analytics/report.js"
import { rebuildRollups } from "../analytics/rollups.js"
import { dropAllAnalyticsTables, ensureSchema, getTursoClient } from "../analytics/turso.js"
import { upsertFacts } from "../analytics/upsert.js"
import { sumCounts, toErrorMessage } from "../analytics/utils.js"

export async function backfillOpenCodeDb(options = {}) {
  const run = createReportRun(options.fresh ? "usage-tracker-backfill-fresh" : "usage-tracker-backfill", { destructive: options.fresh ?? false })
  const client = getTursoClient()
  try {
    run.log("Starting OpenCode DB backfill", { fresh: options.fresh ?? false })
    if (options.fresh) {
      await dropAllAnalyticsTables(client)
      run.log("Dropped analytics tables")
    }
    await ensureSchema(client)
    run.log("Ensured schema")
    const source = loadOpenCodeData(undefined, run.log)
    run.log("Loaded source data summary", {
      projects: source.projects.length,
      sessions: source.sessions.length,
      messages: source.messages.length,
      part_groups: source.partsByMessage.size,
    })
    const facts = deriveFacts(source, run.log)
    run.log("Derived fact summary", {
      projects: facts.projects.length,
      sessions: facts.sessions.length,
      turns: facts.turns.length,
      responses: facts.responses.length,
      response_parts: facts.response_parts.length,
      llm_steps: facts.llm_steps.length,
      tool_calls: facts.tool_calls.length,
      tool_payloads: facts.tool_payloads.length,
    })
    const counts = await upsertFacts(client, facts, run.log)
    await rebuildRollups(client, run.log)
    return run.finish({ ok: true, summary: { total_rows: sumCounts(counts), counts } })
  } catch (error) {
    return run.finish({ ok: false, summary: {}, error: toErrorMessage(error) })
  } finally {
    client.close()
  }
}
