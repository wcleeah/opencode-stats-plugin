import { createClient } from "@libsql/client"
import { join } from "node:path"
import { homedir } from "node:os"

import { listAllOutboxFiles, readOutboxFile } from "./outbox.js"
import {
  CREATE_STATEMENTS,
  FACT_TABLE_ORDER,
  INDEX_STATEMENTS,
  SCHEMA_VERSION,
  makeUpsertStatement,
  rowArgs,
} from "./schema.js"

function dataHome() {
  return join(homedir(), ".local", "share")
}

function localDbPath() {
  return join(dataHome(), "opencode", "analytics.db")
}

const FACT_UPSERTS = new Map(FACT_TABLE_ORDER.map((table) => [table.name, makeUpsertStatement(table)]))
const FACT_TABLES_BY_NAME = new Map(FACT_TABLE_ORDER.map((table) => [table.name, table]))
const BATCH_SIZE = 100

async function ensureLocalSchema(client, logger = console) {
  // WAL mode for concurrent reads
  await client.execute("PRAGMA journal_mode=WAL")
  await client.execute("PRAGMA busy_timeout=5000")

  const statements = [
    ...CREATE_STATEMENTS.map((sql) => ({ sql })),
    ...INDEX_STATEMENTS.map((sql) => ({ sql })),
    {
      sql: "INSERT INTO schema_meta (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
      args: ["schema_version", String(SCHEMA_VERSION), Date.now()],
    },
  ]

  for (let i = 0; i < statements.length; i += BATCH_SIZE) {
    await client.batch(statements.slice(i, i + BATCH_SIZE), "write")
  }
}

async function getLocalSchemaVersion(client) {
  try {
    const result = await client.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'")
    if (result.rows.length === 0) return null
    return Number(result.rows[0].value)
  } catch {
    return null
  }
}

async function writeFactsToLocal(client, facts) {
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
  for (let i = 0; i < statements.length; i += BATCH_SIZE) {
    await client.batch(statements.slice(i, i + BATCH_SIZE), "write")
  }
}

async function bootstrapFromOutbox(client, logger = console) {
  const files = listAllOutboxFiles()
  logger.log("[local-sqlite] bootstrapping from outbox", { fileCount: files.length })

  // Drop all fact tables and rebuild from scratch
  const dropStatements = FACT_TABLE_ORDER.map((table) => table.name)
    .reverse()
    .map((name) => `DROP TABLE IF EXISTS ${name}`)
  for (const sql of dropStatements) {
    await client.execute(sql)
  }
  await client.execute("DROP TABLE IF EXISTS schema_meta")

  // Re-create schema
  await ensureLocalSchema(client, logger)

  // Replay all outbox files
  const totals = {}
  for (const file of files) {
    try {
      const payload = readOutboxFile(file)
      await writeFactsToLocal(client, payload.facts)
      for (const [table, rows] of Object.entries(payload.facts)) {
        totals[table] = (totals[table] ?? 0) + (rows?.length ?? 0)
      }
    } catch (error) {
      logger.error("[local-sqlite] bootstrap replay failed", file, error instanceof Error ? error.message : String(error))
      throw error
    }
  }
  logger.log("[local-sqlite] bootstrap complete", { fileCount: files.length, totals })
}

function normalizeLogger(input) {
  if (typeof input?.log === "function" && typeof input?.error === "function") return input
  const fn = typeof input === "function" ? input : () => {}
  return {
    log(message, fields) {
      try {
        if (typeof fields === "object" && fields !== null) {
          fn(message, fields)
        } else {
          fn(message)
        }
      } catch {}
    },
    error(...args) {
      try {
        fn(...args)
      } catch {
        console.error(...args)
      }
    },
  }
}

export function createLocalSqlite(inputLogger = console) {
  const logger = normalizeLogger(inputLogger)
  const path = localDbPath()
  logger.log("[local-sqlite] opening database", { path })

  const client = createClient({ url: `file:${path}` })
  let initialized = false
  let closed = false

  async function init() {
    if (initialized || closed) return
    const version = await getLocalSchemaVersion(client)
    if (version === SCHEMA_VERSION) {
      logger.log("[local-sqlite] schema up to date", { version })
      initialized = true
      return
    }

    logger.log("[local-sqlite] schema mismatch or missing, bootstrapping", {
      existingVersion: version,
      requiredVersion: SCHEMA_VERSION,
    })
    await bootstrapFromOutbox(client, logger)
    initialized = true
  }

  return {
    client,
    async ensureSchema() {
      await init()
    },
    async writeFacts(facts) {
      await init()
      await writeFactsToLocal(client, facts)
    },
    async query(sql, args) {
      await init()
      return client.execute(args ? { sql, args } : sql)
    },
    close() {
      closed = true
      client.close()
    },
  }
}
