import { appendFileSync, mkdirSync, writeFileSync } from "node:fs"
import { homedir } from "node:os"
import { join } from "node:path"
import { randomUUID } from "node:crypto"

function dataHome() {
  return join(homedir(), ".local", "share")
}

function sanitizeTimestamp(value) {
  return value.replaceAll(":", "-")
}

function logsRoot() {
  const path = join(dataHome(), "opencode", "usage-tracker", "logs")
  mkdirSync(path, { recursive: true })
  return path
}

export function createReportRun(command, options = {}) {
  const startedAt = new Date()
  const slug = `${command}-${sanitizeTimestamp(startedAt.toISOString())}-${randomUUID().slice(0, 8)}`
  const root = logsRoot()
  const jsonlPath = join(root, `${slug}.jsonl`)
  const markdownPath = join(root, `${slug}.md`)
  const entries = []

  function log(message, fields = {}) {
    const entry = {
      timestamp: new Date().toISOString(),
      command,
      message,
      fields,
    }
    entries.push(entry)
    appendFileSync(jsonlPath, `${JSON.stringify(entry)}\n`)
  }

  log("run started", { options })

  return {
    jsonlPath,
    markdownPath,
    startedAt,
    log,
    finish({ ok, summary = {}, error = null }) {
      const finishedAt = new Date()
      log(ok ? "run completed" : "run failed", { summary, error })
      const lines = [
        `# ${command}`,
        "",
        `- Status: ${ok ? "ok" : "failed"}`,
        `- Started: ${startedAt.toISOString()}`,
        `- Finished: ${finishedAt.toISOString()}`,
        `- JSONL Log: ${jsonlPath}`,
      ]
      if (options.destructive) lines.push(`- Destructive: true`)
      const summaryEntries = Object.entries(summary)
      if (summaryEntries.length > 0) {
        lines.push("", "## Summary", "")
        for (const [key, value] of summaryEntries) {
          lines.push(`- ${key}: ${typeof value === "object" ? JSON.stringify(value) : value}`)
        }
      }
      if (error) {
        lines.push("", "## Error", "", "```text", String(error), "```")
      }
      writeFileSync(markdownPath, `${lines.join("\n")}\n`)
      return { ok, reportPath: markdownPath, logPath: jsonlPath, summary }
    },
  }
}
