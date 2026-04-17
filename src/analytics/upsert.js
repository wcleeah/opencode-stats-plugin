import { FACT_TABLE_ORDER, makeUpsertStatement, rowArgs } from "./schema.js"
import { chunk } from "./utils.js"

const FACT_TABLES = new Map(FACT_TABLE_ORDER.map((table) => [table.name, table]))
const FACT_TABLE_NAMES = [
  "projects",
  "sessions",
  "turns",
  "responses",
  "response_parts",
  "llm_steps",
  "tool_calls",
  "tool_payloads",
]

async function upsertTableRows(client, tableName, rows, log = () => {}) {
  if (rows.length === 0) {
    log("Skipping empty fact table", { table: tableName })
    return 0
  }

  const table = FACT_TABLES.get(tableName)
  if (!table) throw new Error(`Unknown fact table: ${tableName}`)
  const sql = makeUpsertStatement(table)
  const batches = chunk(rows, 100)
  log("Upserting fact table", { table: tableName, rows: rows.length, batch_count: batches.length })
  for (const [index, batch] of batches.entries()) {
    log("Writing fact batch", { table: tableName, batch: index + 1, batch_count: batches.length, row_count: batch.length })
    await client.batch(
      batch.map((row) => ({ sql, args: rowArgs(table, row) })),
      "write",
    )
  }
  log("Finished fact table upsert", { table: tableName, rows: rows.length })
  return rows.length
}

export async function upsertFacts(client, facts, log = () => {}) {
  const counts = {}
  log("Starting fact upserts")
  for (const tableName of FACT_TABLE_NAMES) {
    counts[tableName] = await upsertTableRows(client, tableName, facts[tableName] ?? [], log)
  }
  log("Finished fact upserts", counts)
  return counts
}
