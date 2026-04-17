import { randomUUID } from "node:crypto"

import { createOutbox } from "../../src/analytics/outbox.js"
import { recomputeTouchedRollups } from "../../src/analytics/rollups.js"
import { primaryKeyValue } from "../../src/analytics/schema.js"
import { createTurso } from "../../src/analytics/turso.js"
import { sleep, stableStringify, toErrorMessage } from "../../src/analytics/utils.js"
import { buildProject, mergeTurnRows, normalizeEvent, rememberSessionProject } from "./normalize.js"

function mergeRows(target, source) {
  for (const [tableName, rows] of Object.entries(source)) {
    const bucket = target[tableName] ?? new Map()
    for (const row of rows) {
      const key = primaryKeyValue(tableName, row)
      const existing = bucket.get(key)
      bucket.set(key, tableName === "turns" ? mergeTurnRows(existing, row) : row)
    }
    target[tableName] = bucket
  }
}

function mergeTouched(target, source) {
  for (const key of Object.keys(target)) {
    const merged = new Map()
    for (const item of [...(target[key] ?? []), ...(source[key] ?? [])]) {
      merged.set(stableStringify(item), item)
    }
    target[key] = Array.from(merged.values())
  }
}

function emptyFacts() {
  return {
    projects: new Map(),
    sessions: new Map(),
    turns: new Map(),
    responses: new Map(),
    response_parts: new Map(),
    llm_steps: new Map(),
    tool_calls: new Map(),
    tool_payloads: new Map(),
  }
}

function emptyTouched() {
  return {
    projectIDs: [],
    sessionIDs: [],
    rootSessionIDs: [],
    days: [],
    projectDayKeys: [],
    modelKeys: [],
    toolKeys: [],
  }
}

function serializeFacts(facts) {
  return Object.fromEntries(Object.entries(facts).map(([tableName, rows]) => [tableName, Array.from(rows.values())]))
}

function hasPendingFacts(facts) {
  return Object.values(facts).some((map) => map.size > 0)
}

function hasTouched(touched) {
  return Object.values(touched).some((items) => items.length > 0)
}

export function createIngestionQueue({
  project,
  state,
  logger = console,
  ensureEventContext = async () => {},
  turso: providedTurso,
  outbox: providedOutbox,
  setTimeoutFn = setTimeout,
  clearTimeoutFn = clearTimeout,
  sleepFn = sleep,
  flushDelayMs = 150,
  rollupDelayMs = 15000,
}) {
  const processID = `pid-${process.pid}-${Date.now()}`
  const turso = providedTurso ?? createTurso()
  const outbox = providedOutbox ?? createOutbox(processID)
  let pendingFacts = emptyFacts()
  let pendingTouched = emptyTouched()
  let pendingRollupTouched = emptyTouched()
  let pendingRollupFiles = new Map()
  let closed = false
  let initialized = false
  let flushTimer = null
  let rollupTimer = null
  let lastPersistedSequence = 0
  let factsAppliedThrough = 0
  let rollupsAppliedThrough = 0
  let nextProgressIndex = 1
  let journalQueue = []
  let journalProgress = new Map()
  let journalDraining = false
  let rollupRunning = false
  let rollupKickRequested = false
  let journalFailure = null
  let rollupFailure = null
  const factWaiters = new Map()
  const rollupWaiters = new Map()

  if (project) {
    mergeRows(pendingFacts, { projects: [buildProject(project)] })
  }

  async function init() {
    if (initialized) return
    await turso.ensureSchema()
    initialized = true
  }

  function resolveProgressWaiters(waiters, progress) {
    for (const [target, resolvers] of Array.from(waiters.entries())) {
      if (progress < target) continue
      waiters.delete(target)
      for (const entry of resolvers) entry.resolve()
    }
  }

  function rejectProgressWaiters(waiters, error) {
    for (const resolvers of waiters.values()) {
      for (const entry of resolvers) entry.reject(error)
    }
    waiters.clear()
  }

  function waitForProgressAtLeast(waiters, current, target, currentFailure) {
    if (target <= 0 || current() >= target) return Promise.resolve()
    const failure = currentFailure()
    if (failure) return Promise.reject(failure)
    return new Promise((resolve, reject) => {
      const bucket = waiters.get(target) ?? []
      bucket.push({ resolve, reject })
      waiters.set(target, bucket)
    })
  }

  function waitForFactsThrough(target) {
    return waitForProgressAtLeast(factWaiters, () => factsAppliedThrough, target, () => journalFailure)
  }

  function waitForRollupsThrough(target) {
    return waitForProgressAtLeast(rollupWaiters, () => rollupsAppliedThrough, target, () => rollupFailure)
  }

  function progressIndexForFile(file) {
    const existing = journalProgress.get(file)
    if (existing) return existing
    const created = nextProgressIndex++
    journalProgress.set(file, created)
    return created
  }

  function enqueueJournalEntry(entry) {
    const progress = progressIndexForFile(entry.file)
    if (journalQueue.some((queued) => queued.file === entry.file)) return progress
    journalQueue.push(entry)
    journalQueue.sort((left, right) => left.sequence - right.sequence || left.file.localeCompare(right.file))
    return progress
  }

  async function persistJournalBatch(batch) {
    const entry = outbox.persist(batch)
    lastPersistedSequence = entry.sequence
    return enqueueJournalEntry(entry)
  }

  async function persistPendingBatch() {
    if (!hasPendingFacts(pendingFacts)) return null
    const facts = serializeFacts(pendingFacts)
    const touched = pendingTouched
    pendingFacts = emptyFacts()
    pendingTouched = emptyTouched()
    try {
      return await persistJournalBatch({
        batchID: randomUUID(),
        createdAt: Date.now(),
        facts,
        touched,
      })
    } catch (error) {
      mergeRows(pendingFacts, facts)
      mergeTouched(pendingTouched, touched)
      throw error
    }
  }

  function scheduleRollupFlush() {
    if (closed || rollupTimer || !hasTouched(pendingRollupTouched)) return
    rollupTimer = setTimeoutFn(async () => {
      rollupTimer = null
      const target = Array.from(pendingRollupFiles.values()).reduce((max, sequence) => Math.max(max, sequence), rollupsAppliedThrough)
      kickRollupPass()
      try {
        await waitForRollupsThrough(target)
      } catch {}
    }, rollupDelayMs)
  }

  async function applyJournalFile(file) {
    const batch = outbox.read(file)
    const progress = progressIndexForFile(file)
    await init()
    await turso.writeFacts(batch.facts)

    if (!hasTouched(batch.touched)) {
      outbox.removeFile(file)
      journalProgress.delete(file)
      factsAppliedThrough = Math.max(factsAppliedThrough, progress)
      resolveProgressWaiters(factWaiters, factsAppliedThrough)
      rollupsAppliedThrough = Math.max(rollupsAppliedThrough, progress)
      resolveProgressWaiters(rollupWaiters, rollupsAppliedThrough)
      return
    }

    factsAppliedThrough = Math.max(factsAppliedThrough, progress)
    resolveProgressWaiters(factWaiters, factsAppliedThrough)
    mergeTouched(pendingRollupTouched, batch.touched)
    pendingRollupFiles.set(file, progress)
    scheduleRollupFlush()
  }

  async function drainJournalQueue() {
    try {
      while (!closed && journalQueue.length > 0) {
        const next = journalQueue.shift()
        if (!next) continue
        try {
          await applyJournalFile(next.file)
        } catch (error) {
          const failure = error instanceof Error ? error : new Error(toErrorMessage(error))
          journalFailure = failure
          logger.error("[usage-tracker] flush failed", toErrorMessage(error))
          journalQueue.unshift(next)
          rejectProgressWaiters(factWaiters, failure)
          return
        }
        await sleepFn(10)
      }
    } finally {
      journalDraining = false
      if (!closed && !journalFailure && journalQueue.length > 0) {
        kickJournalDrain()
      }
    }
  }

  function kickJournalDrain() {
    if (closed || journalDraining || journalQueue.length === 0) return
    journalFailure = null
    journalDraining = true
    void drainJournalQueue()
  }

  async function flushRollupsOnce() {
    if (!hasTouched(pendingRollupTouched)) {
      rollupsAppliedThrough = Math.max(rollupsAppliedThrough, factsAppliedThrough)
      resolveProgressWaiters(rollupWaiters, rollupsAppliedThrough)
      return
    }

    const touched = pendingRollupTouched
    const files = Array.from(pendingRollupFiles.entries())
    pendingRollupTouched = emptyTouched()
    pendingRollupFiles = new Map()

    try {
      await init()
      const rollups = await recomputeTouchedRollups(turso, touched)
      await turso.replaceRollups(rollups)
      for (const [file] of files) {
        outbox.removeFile(file)
        journalProgress.delete(file)
      }
      const maxCovered = files.reduce((max, [, progress]) => Math.max(max, progress), rollupsAppliedThrough)
      rollupsAppliedThrough = Math.max(rollupsAppliedThrough, maxCovered)
      resolveProgressWaiters(rollupWaiters, rollupsAppliedThrough)
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(toErrorMessage(error))
      rollupFailure = failure
      logger.error("[usage-tracker] rollup flush failed", toErrorMessage(error))
      mergeTouched(pendingRollupTouched, touched)
      for (const [file, sequence] of files) {
        pendingRollupFiles.set(file, sequence)
      }
      rejectProgressWaiters(rollupWaiters, failure)
      scheduleRollupFlush()
    }
  }

  async function runRollupPass() {
    try {
      await flushRollupsOnce()
    } finally {
      rollupRunning = false
      const rerunNow = !closed && rollupKickRequested && !rollupFailure && hasTouched(pendingRollupTouched)
      rollupKickRequested = false
      if (rerunNow) {
        kickRollupPass()
      }
    }
  }

  function kickRollupPass() {
    if (closed || !hasTouched(pendingRollupTouched)) return
    if (rollupRunning) {
      rollupKickRequested = true
      return
    }
    rollupFailure = null
    rollupRunning = true
    void runRollupPass()
  }

  function scheduleFlush() {
    if (flushTimer) return
    flushTimer = setTimeoutFn(async () => {
      flushTimer = null
      let persisted = null
      try {
        persisted = await persistPendingBatch()
      } catch (error) {
        logger.error("[usage-tracker] persist failed", toErrorMessage(error))
        if (!closed) scheduleFlush()
        return
      }
      if (persisted === null) return
      kickJournalDrain()
      try {
        await waitForFactsThrough(persisted)
      } catch {}
    }, flushDelayMs)
  }

  async function replayOutbox(files) {
    let replayTarget = rollupsAppliedThrough
    for (const file of files) {
      if (closed) return
      const batch = outbox.read(file)
      const sequence = batch.sequence ?? 0
      try {
        const progress = enqueueJournalEntry({ file, sequence })
        replayTarget = Math.max(replayTarget, progress)
      } catch (error) {
        logger.error("[usage-tracker] replay failed", toErrorMessage(error))
        return
      }
    }

    if (rollupTimer) {
      clearTimeoutFn(rollupTimer)
      rollupTimer = null
    }

    kickJournalDrain()
    await waitForFactsThrough(replayTarget)
    kickRollupPass()
    await waitForRollupsThrough(replayTarget)
  }

  async function recoverFromJournal() {
    const files = outbox.listAllOrphans()
    if (files.length === 0) return

    let recoveryTarget = rollupsAppliedThrough
    for (const file of files) {
      const batch = outbox.read(file)
      const progress = enqueueJournalEntry({ file, sequence: batch.sequence ?? 0 })
      recoveryTarget = Math.max(recoveryTarget, progress)
      lastPersistedSequence = Math.max(lastPersistedSequence, batch.sequence ?? 0)
    }

    kickJournalDrain()
    await waitForFactsThrough(recoveryTarget)
    kickRollupPass()
    try {
      await waitForRollupsThrough(recoveryTarget)
    } catch (error) {
      logger.error("[usage-tracker] startup recovery rollup failed", toErrorMessage(error))
    }
  }

  return {
    processID,
    async start() {
      await recoverFromJournal()
    },
    async enqueue(event) {
      if (closed) return
      if (event.type === "session.created" || event.type === "session.updated" || event.type === "session.deleted") {
        rememberSessionProject(state, event.properties.info)
      }
      await ensureEventContext(event)
      const normalized = normalizeEvent(event, state)
      mergeRows(pendingFacts, normalized.facts)
      mergeTouched(pendingTouched, normalized.touched)
      scheduleFlush()
    },
    async flush() {
      if (flushTimer) {
        clearTimeoutFn(flushTimer)
        flushTimer = null
      }

      let target = lastPersistedSequence
      const persisted = await persistPendingBatch()
      if (persisted !== null) {
        target = persisted
      }

      kickJournalDrain()
      await waitForFactsThrough(target)

      if (rollupTimer) {
        clearTimeoutFn(rollupTimer)
        rollupTimer = null
      }

      kickRollupPass()
      await waitForRollupsThrough(target)
    },
    async close() {
      await this.flush()
      closed = true
      await turso.close()
    },
    async replayAllOutbox() {
      await replayOutbox(outbox.listAllOrphans())
    },
  }
}
