import { createClient } from "@libsql/client"

import {
  CREATE_STATEMENTS,
  FACT_TABLE_ORDER,
  INDEX_STATEMENTS,
  ROLLUP_TABLES,
  SCHEMA_VERSION,
  dropAnalyticsStatements,
  makeUpsertStatement,
  rowArgs,
} from "./schema.js"

const FACT_UPSERTS = new Map(FACT_TABLE_ORDER.map((table) => [table.name, makeUpsertStatement(table)]))
const ROLLUP_UPSERTS = new Map(Object.values(ROLLUP_TABLES).map((table) => [table.name, makeUpsertStatement(table)]))
const FACT_TABLES_BY_NAME = new Map(FACT_TABLE_ORDER.map((table) => [table.name, table]))
const ROLLUP_TABLES_BY_NAME = new Map(Object.values(ROLLUP_TABLES).map((table) => [table.name, table]))
const BATCH_SIZE = 100

async function runInChunks(client, statements) {
  for (let index = 0; index < statements.length; index += BATCH_SIZE) {
    await client.batch(statements.slice(index, index + BATCH_SIZE), "write")
  }
}

export function getTursoClient() {
  const url = process.env.TURSO_DATABASE_URL
  const authToken = process.env.TURSO_AUTH_TOKEN
  if (!url || !authToken) return null
  return createClient({ url, authToken })
}

export async function ensureSchema(client) {
  const statements = [
    ...CREATE_STATEMENTS.map((sql) => ({ sql })),
    ...INDEX_STATEMENTS.map((sql) => ({ sql })),
    {
      sql: "INSERT INTO schema_meta (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
      args: ["schema_version", String(SCHEMA_VERSION), Date.now()],
    },
  ]
  await runInChunks(client, statements)
}

export async function dropAllAnalyticsTables(client) {
  await runInChunks(client, dropAnalyticsStatements())
}

export async function writeFacts(client, facts) {
  const statements = []
  for (const [tableName, rows] of Object.entries(facts)) {
    if (!rows?.length) continue
    const table = FACT_TABLES_BY_NAME.get(tableName)
    if (!table) continue
    const sql = FACT_UPSERTS.get(tableName)
    for (const row of rows) {
      statements.push({ sql, args: rowArgs(table, row) })
    }
  }
  if (statements.length === 0) return
  await runInChunks(client, statements)
}

export async function replaceRollups(client, rollups) {
  const statements = []
  for (const [tableName, payload] of Object.entries(rollups)) {
    const table = ROLLUP_TABLES_BY_NAME.get(tableName)
    if (!table || !payload) continue
    for (const key of payload.deleteKeys || []) {
      const where = table.primaryKey.map((column) => `${column} = ?`).join(" AND ")
      statements.push({ sql: `DELETE FROM ${table.name} WHERE ${where}`, args: key })
    }
    for (const row of payload.rows || []) {
      statements.push({ sql: ROLLUP_UPSERTS.get(tableName), args: rowArgs(table, row) })
    }
  }
  if (statements.length === 0) return
  await runInChunks(client, statements)
}

export function createTurso() {
  const client = getTursoClient()
  if (!client) return null
  return {
    client,
    ensureSchema: () => ensureSchema(client),
    writeFacts: (facts) => writeFacts(client, facts),
    replaceRollups: (rollups) => replaceRollups(client, rollups),
    query(sql, args) {
      return client.execute(args ? { sql, args } : sql)
    },
    close() {
      client.close()
    },
  }
}
