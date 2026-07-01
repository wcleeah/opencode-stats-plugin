import { listAllOutboxFiles, readOutboxFile, removeOutboxFile } from "../analytics/outbox.js"
import { createLocalSqlite } from "../analytics/local-sqlite.js"
import { createReportRun } from "../analytics/report.js"
import { rebuildRollups } from "../analytics/rollups.js"
import { ensureSchema, getTursoClient, writeFacts } from "../analytics/turso.js"
import { sumCounts, toErrorMessage } from "../analytics/utils.js"

function summarizeFacts(facts) {
  const totals = {}
  for (const [table, rows] of Object.entries(facts)) {
    totals[table] = (totals[table] ?? 0) + rows.length
  }
  return totals
}

export async function replayOutboxCommand() {
  const run = createReportRun("usage-tracker-replay-outbox")
  const client = getTursoClient()
  if (!client) return run.finish({ ok: false, error: "Turso not configured (missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN)" })
  const localSqlite = createLocalSqlite(run.log)
  try {
    run.log("Starting durable journal replay")
    await ensureSchema(client)
    await localSqlite.ensureSchema()
    const files = listAllOutboxFiles()
    run.log("Discovered outbox files", { file_count: files.length })
    const totals = {}
    const replayedFiles = []

    for (const file of files) {
      const payload = readOutboxFile(file)
      run.log("Parsed outbox payload", { file, table_count: Object.keys(payload.facts).length })
      await writeFacts(client, payload.facts)
      await localSqlite.writeFacts(payload.facts)
      const summary = summarizeFacts(payload.facts)
      for (const [table, count] of Object.entries(summary)) {
        totals[table] = (totals[table] ?? 0) + count
      }
      replayedFiles.push(file)
    }

    await rebuildRollups(client, run.log)
    for (const file of replayedFiles) {
      removeOutboxFile(file)
      run.log("Removed replayed journal file", { file })
    }
    return run.finish({ ok: true, summary: { files: files.length, total_rows: sumCounts(totals), counts: totals } })
  } catch (error) {
    return run.finish({ ok: false, error: toErrorMessage(error) })
  } finally {
    client?.close()
    localSqlite.close()
  }
}
