import { mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { homedir } from "node:os"

function dataHome() {
  return join(homedir(), ".local", "share")
}

function ensureDir(path) {
  mkdirSync(path, { recursive: true })
  return path
}

function safeReadJSON(filePath) {
  return JSON.parse(readFileSync(filePath, "utf8"))
}

function sequenceFromPath(file) {
  const name = file.split("/").pop() ?? ""
  return Number.parseInt(name.split("-")[0] ?? "0", 10)
}

function listFiles(directory) {
  return readdirSync(directory)
    .filter((name) => name.endsWith(".json"))
    .map((name) => join(directory, name))
    .sort((left, right) => sequenceFromPath(left) - sequenceFromPath(right) || left.localeCompare(right))
}

export function outboxRoot() {
  return ensureDir(join(dataHome(), "opencode", "usage-outbox"))
}

export function listAllOutboxFiles() {
  const root = outboxRoot()
  return readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .flatMap((entry) => listFiles(join(root, entry.name)))
    .sort((left, right) => sequenceFromPath(left) - sequenceFromPath(right) || left.localeCompare(right))
}

export function readOutboxFile(file) {
  return safeReadJSON(file)
}

export function removeOutboxFile(file) {
  rmSync(file, { force: true })
}

export function createOutbox(processID) {
  const root = outboxRoot()
  const processDir = ensureDir(join(root, processID))
  let nextSequence = listFiles(processDir).reduce((max, file) => Math.max(max, sequenceFromPath(file)), 0) + 1

  function filePath(sequence, batchID) {
    return join(processDir, `${String(sequence).padStart(12, "0")}-${batchID}.json`)
  }

  return {
    root,
    processDir,
    persist(batch) {
      const sequence = batch.sequence ?? nextSequence++
      const payload = {
        ...batch,
        sequence,
        factsAppliedAt: batch.factsAppliedAt ?? null,
      }
      const path = filePath(sequence, batch.batchID)
      const tempPath = `${path}.tmp`
      writeFileSync(tempPath, JSON.stringify(payload) + "\n")
      renameSync(tempPath, path)
      return { file: path, sequence }
    },
    remove(batchID) {
      const match = this.list().find((file) => this.read(file).batchID === batchID)
      if (match) rmSync(match, { force: true })
    },
    removeFile(file) {
      removeOutboxFile(file)
    },
    list() {
      return listFiles(processDir)
    },
    read(file) {
      return readOutboxFile(file)
    },
    listAllOrphans() {
      return listAllOutboxFiles()
    },
  }
}
