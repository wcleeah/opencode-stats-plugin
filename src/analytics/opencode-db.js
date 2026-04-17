import { Database } from "bun:sqlite"
import { homedir } from "node:os"
import { join } from "node:path"

export function defaultOpenCodeDbPath() {
  return process.env.OPENCODE_DB ?? join(process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"), "opencode", "opencode.db")
}

function parseJson(value) {
  return JSON.parse(value)
}

export function loadOpenCodeData(dbPath = defaultOpenCodeDbPath(), log = () => {}) {
  log("Opening OpenCode SQLite database", { db_path: dbPath })
  const db = new Database(dbPath, { readonly: true })
  try {
    const projects = db.query("SELECT * FROM project ORDER BY time_created").all()
    log("Loaded project rows", { count: projects.length })

    const sessions = db.query("SELECT * FROM session ORDER BY time_created").all()
    log("Loaded session rows", { count: sessions.length })

    const messages = db.query("SELECT id, session_id, time_created, data FROM message ORDER BY time_created, id").all()
    log("Loaded raw message rows", { count: messages.length })

    const mappedMessages = messages.map((row) => ({
      id: String(row.id),
      session_id: String(row.session_id),
      time_created: Number(row.time_created),
      data: parseJson(String(row.data)),
    }))
    log("Parsed message payloads", { count: mappedMessages.length })

    const parts = db.query("SELECT id, message_id, session_id, time_created, data FROM part ORDER BY time_created, id").all()
    log("Loaded raw part rows", { count: parts.length })

    const mappedParts = parts.map((row) => ({
      id: String(row.id),
      message_id: String(row.message_id),
      session_id: String(row.session_id),
      time_created: Number(row.time_created),
      data: parseJson(String(row.data)),
    }))
    log("Parsed part payloads", { count: mappedParts.length })

    const partsByMessage = new Map()
    for (const part of mappedParts) {
      const bucket = partsByMessage.get(part.message_id) ?? []
      bucket.push({ id: part.id, session_id: part.session_id, time_created: part.time_created, data: part.data })
      partsByMessage.set(part.message_id, bucket)
    }
    log("Grouped parts by message", { message_groups: partsByMessage.size, total_parts: mappedParts.length })

    return {
      projects,
      sessions,
      messages: mappedMessages,
      partsByMessage,
    }
  } finally {
    db.close()
    log("Closed OpenCode SQLite database")
  }
}
