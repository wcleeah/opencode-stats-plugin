import { existsSync, readFileSync, rmSync } from "node:fs"

import { describe, expect, test } from "bun:test"

import { createReportRun } from "../src/analytics/report.js"

describe("report writer", () => {
  test("creates markdown and jsonl report files", () => {
    const run = createReportRun("smoke-test")
    const result = run.finish({ ok: true, summary: { rows: 1 } })
    expect(existsSync(result.reportPath)).toBe(true)
    expect(existsSync(result.logPath)).toBe(true)
    rmSync(result.reportPath, { force: true })
    rmSync(result.logPath, { force: true })
  })
})

describe("plugin tools", () => {
  test("declares maintenance tool names", () => {
    const source = readFileSync(new URL("../plugins/usage-tracker/index.js", import.meta.url), "utf8")
    expect(source).toContain("usage-tracker-flush")
    expect(source).toContain("usage-tracker-replay-all")
    expect(source).toContain("usage-tracker-backfill")
    expect(source).toContain("usage-tracker-backfill-fresh")
    expect(source).toContain("usage-tracker-rebuild-rollups")
    expect(source).toContain("usage-tracker-replay-outbox")
    expect(source).toContain("usage-tracker-verify-analytics")
  })
})
