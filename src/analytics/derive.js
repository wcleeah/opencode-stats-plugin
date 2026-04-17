import { byteLength, textValue } from "./utils.js"

function sessionSummary(summary) {
  const value = summary ?? {}
  return {
    additions: Number(value.additions ?? 0),
    deletions: Number(value.deletions ?? 0),
    files: Number(value.files ?? 0),
  }
}

function messageTokens(message) {
  const tokens = message.tokens ?? {}
  const cache = tokens.cache ?? {}
  return {
    input: Number(tokens.input ?? 0),
    output: Number(tokens.output ?? 0),
    reasoning: Number(tokens.reasoning ?? 0),
    cacheRead: Number(cache.read ?? 0),
    cacheWrite: Number(cache.write ?? 0),
  }
}

function mergeTurnRows(existing, next) {
  if (!existing) return next
  const existingHasContent = existing.content !== null && existing.content !== undefined
  const nextHasContent = next.content !== null && next.content !== undefined
  const existingSynthetic = existing.synthetic
  const nextSynthetic = next.synthetic
  const existingCompaction = existing.compaction
  const nextCompaction = next.compaction

  let content = existing.content ?? null
  if (nextHasContent) {
    if (nextSynthetic === 0) {
      content = next.content
    } else if (!existingHasContent || existingSynthetic === 1) {
      content = next.content
    }
  }

  let synthetic = existingSynthetic
  if (nextCompaction === 1 || existingCompaction === 1) {
    synthetic = 1
  } else if (nextHasContent) {
    if (nextSynthetic === 0) {
      synthetic = 0
    } else if (!existingHasContent || existingSynthetic === 1) {
      synthetic = 1
    }
  } else {
    synthetic = Math.max(existingSynthetic, nextSynthetic)
  }

  return {
    ...existing,
    ...next,
    content,
    synthetic,
    compaction: Math.max(existingCompaction, nextCompaction),
    undone_at: next.undone_at ?? existing.undone_at,
    time_created: Math.min(existing.time_created, next.time_created),
    time_updated: Math.max(existing.time_updated, next.time_updated),
    turn_duration_ms: next.turn_duration_ms ?? existing.turn_duration_ms,
  }
}

function updateTurnDuration(turnsById, turnCreated, input) {
  if (!input.turnID || input.completedAt == null) return
  const turnCreatedAt = turnCreated.get(input.turnID)
  if (turnCreatedAt == null || input.completedAt < turnCreatedAt) return
  turnsById.set(
    input.turnID,
    mergeTurnRows(turnsById.get(input.turnID), {
      id: input.turnID,
      session_id: input.sessionID,
      root_session_id: input.rootSessionID,
      project_id: input.projectID,
      content: null,
      synthetic: 0,
      compaction: 0,
      undone_at: null,
      time_created: turnCreatedAt,
      time_updated: input.completedAt,
      turn_duration_ms: input.completedAt - turnCreatedAt,
    }),
  )
}

export function deriveFacts(input, log = () => {}) {
  log("Starting fact derivation", {
    project_rows: input.projects.length,
    session_rows: input.sessions.length,
    message_rows: input.messages.length,
    part_groups: input.partsByMessage.size,
  })
  const rootBySession = new Map()
  const sessionProject = new Map()
  const turnsById = new Map()
  const facts = {
    projects: [],
    sessions: [],
    turns: [],
    responses: [],
    response_parts: [],
    llm_steps: [],
    tool_calls: [],
    tool_payloads: [],
  }

  function pushTurn(row) {
    turnsById.set(row.id, mergeTurnRows(turnsById.get(row.id), row))
  }

  for (const project of input.projects) {
    facts.projects.push({
      id: String(project.id),
      worktree: String(project.worktree),
      vcs: project.vcs ? String(project.vcs) : null,
      name: project.name ? String(project.name) : null,
      icon_url: project.icon_url ? String(project.icon_url) : null,
      icon_color: project.icon_color ? String(project.icon_color) : null,
      time_created: Number(project.time_created ?? 0),
      time_updated: Number(project.time_updated ?? project.time_created ?? 0),
      time_initialized: project.time_initialized == null ? null : Number(project.time_initialized),
    })
  }
  log("Derived project facts", { count: facts.projects.length })

  const sessionRows = input.sessions.slice().sort((left, right) => Number(left.time_created ?? 0) - Number(right.time_created ?? 0))
  for (const session of sessionRows) {
    const parentID = session.parent_id ? String(session.parent_id) : null
    const rootSessionID = parentID ? rootBySession.get(parentID) ?? parentID : String(session.id)
    rootBySession.set(String(session.id), rootSessionID)
    sessionProject.set(String(session.id), String(session.project_id))
    const summary = sessionSummary({
      additions: session.summary_additions,
      deletions: session.summary_deletions,
      files: session.summary_files,
    })
    facts.sessions.push({
      id: String(session.id),
      project_id: String(session.project_id),
      parent_session_id: parentID,
      root_session_id: rootSessionID,
      slug: String(session.slug),
      directory: String(session.directory),
      title: String(session.title),
      version: String(session.version),
      share_url: session.share_url ? String(session.share_url) : null,
      summary_additions: summary.additions,
      summary_deletions: summary.deletions,
      summary_files: summary.files,
      archived_at: session.time_archived == null ? null : Number(session.time_archived),
      deleted_at: null,
      time_created: Number(session.time_created ?? 0),
      time_updated: Number(session.time_updated ?? session.time_created ?? 0),
    })
  }
  log("Derived session facts", { count: facts.sessions.length })

  const turnCreated = new Map()
  const responseByMessage = new Map()
  const currentStepByMessage = new Map()
  const messages = input.messages.slice().sort((left, right) => left.time_created - right.time_created)

  for (const message of messages) {
    const info = message.data
    const sessionID = message.session_id
    const projectID = sessionProject.get(sessionID) ?? "_unknown"
    const rootSessionID = rootBySession.get(sessionID) ?? sessionID
    const role = String(info.role ?? "")

    if (role === "user") {
      const createdAt = Number(info.time?.created ?? message.time_created)
      turnCreated.set(message.id, createdAt)
      pushTurn({
        id: message.id,
        session_id: sessionID,
        root_session_id: rootSessionID,
        project_id: projectID,
        content: null,
        synthetic: 0,
        compaction: 0,
        undone_at: null,
        time_created: createdAt,
        time_updated: createdAt,
        turn_duration_ms: null,
      })
    }

    if (role === "assistant") {
      const tokens = messageTokens(info)
      const createdAt = Number(info.time?.created ?? message.time_created)
      const completedAt = info.time?.completed == null ? null : Number(info.time.completed)
      const response = {
        id: message.id,
        turn_id: String(info.parentID),
        session_id: sessionID,
        root_session_id: rootSessionID,
        project_id: projectID,
        agent: info.agent ? String(info.agent) : null,
        provider_id: info.providerID ? String(info.providerID) : "_unknown",
        model_id: info.modelID ? String(info.modelID) : "_unknown",
        summary: info.summary ? 1 : 0,
        finish: info.finish ? String(info.finish) : null,
        error_type: info.error?.name ? String(info.error.name) : null,
        error_message: info.error?.message ? String(info.error.message) : info.error?.data ? String(info.error.data.message ?? "") || null : null,
        cost: Number(info.cost ?? 0),
        tokens_in: tokens.input,
        tokens_out: tokens.output,
        tokens_reasoning: tokens.reasoning,
        tokens_cache_read: tokens.cacheRead,
        tokens_cache_write: tokens.cacheWrite,
        time_created: createdAt,
        time_completed: completedAt,
        response_time_ms: completedAt == null ? null : Math.max(0, completedAt - createdAt),
      }
      facts.responses.push(response)
      responseByMessage.set(message.id, {
        model_id: response.model_id,
        provider_id: response.provider_id,
        time_created: createdAt,
      })
      updateTurnDuration(turnsById, turnCreated, {
        turnID: info.parentID ? String(info.parentID) : null,
        sessionID,
        rootSessionID,
        projectID,
        completedAt,
      })
    }

    const parts = input.partsByMessage.get(message.id) ?? []
    for (const part of parts) {
      const data = part.data
      const type = String(data.type ?? "")
      if (type === "text") {
        const text = textValue(data.text)
        if (role === "user") {
          pushTurn({
            id: message.id,
            session_id: sessionID,
            root_session_id: rootSessionID,
            project_id: projectID,
            content: text,
            synthetic: data.synthetic ? 1 : 0,
            compaction: 0,
            undone_at: null,
            time_created: Number(info.time?.created ?? message.time_created),
            time_updated: part.time_created,
            turn_duration_ms: null,
          })
        } else if (text) {
          facts.response_parts.push({
            response_id: message.id,
            part_id: part.id,
            part_type: "text",
            sort_key: part.id,
            content: text,
            size_bytes: byteLength(text),
          })
        }
      }

      if (type === "reasoning") {
        const text = textValue(data.text)
        if (text) {
          facts.response_parts.push({
            response_id: message.id,
            part_id: part.id,
            part_type: "reasoning",
            sort_key: part.id,
            content: text,
            size_bytes: byteLength(text),
          })
        }
      }

      if (type === "compaction") {
        pushTurn({
          id: message.id,
          session_id: sessionID,
          root_session_id: rootSessionID,
          project_id: projectID,
          content: null,
          synthetic: 1,
          compaction: 1,
          undone_at: null,
          time_created: part.time_created,
          time_updated: part.time_created,
          turn_duration_ms: null,
        })
      }

      if (type === "step-start") {
        currentStepByMessage.set(message.id, { id: part.id, startedAt: part.time_created })
      }

      if (type === "step-finish") {
        const response = responseByMessage.get(message.id)
        if (!response) continue
        const step = currentStepByMessage.get(message.id) ?? { id: part.id, startedAt: null }
        currentStepByMessage.set(message.id, step)
        const tokens = data.tokens ?? {}
        const cache = tokens.cache ?? {}
        facts.llm_steps.push({
          id: step.id,
          response_id: message.id,
          session_id: sessionID,
          root_session_id: rootSessionID,
          project_id: projectID,
          provider_id: response.provider_id,
          model_id: response.model_id,
          finish_reason: data.reason ? String(data.reason) : null,
          cost: Number(data.cost ?? 0),
          tokens_in: Number(tokens.input ?? 0),
          tokens_out: Number(tokens.output ?? 0),
          tokens_reasoning: Number(tokens.reasoning ?? 0),
          tokens_cache_read: Number(cache.read ?? 0),
          tokens_cache_write: Number(cache.write ?? 0),
          time_created: step.startedAt ?? response.time_created,
          time_updated: part.time_created,
        })
      }

      if (type === "tool") {
        const state = data.state ?? {}
        const inputContent = textValue(state.input)
        const outputContent = textValue(state.output)
        const time = state.time ?? {}
        facts.tool_calls.push({
          id: part.id,
          response_id: message.id,
          session_id: sessionID,
          root_session_id: rootSessionID,
          project_id: projectID,
          step_id: currentStepByMessage.get(message.id)?.id ?? null,
          call_id: String(data.callID ?? ""),
          tool: String(data.tool ?? ""),
          status: String(state.status ?? "pending"),
          title: state.title ? String(state.title) : null,
          error: state.error ? String(state.error) : null,
          input_bytes: byteLength(inputContent),
          output_bytes: byteLength(outputContent),
          compacted_at: time.compacted == null ? null : Number(time.compacted),
          started_at: time.start == null ? null : Number(time.start),
          completed_at: time.end == null ? null : Number(time.end),
          duration_ms: time.start == null || time.end == null ? null : Math.max(0, Number(time.end) - Number(time.start)),
          time_updated: Number(time.end ?? time.compacted ?? time.start ?? part.time_created),
        })
        if (inputContent) {
          facts.tool_payloads.push({
            tool_call_id: part.id,
            payload_type: "input",
            content: inputContent,
            size_bytes: byteLength(inputContent),
          })
        }
        if (outputContent) {
          facts.tool_payloads.push({
            tool_call_id: part.id,
            payload_type: "output",
            content: outputContent,
            size_bytes: byteLength(outputContent),
          })
        }
      }
    }
  }

  facts.turns = Array.from(turnsById.values())
  log("Completed fact derivation", {
    projects: facts.projects.length,
    sessions: facts.sessions.length,
    turns: facts.turns.length,
    responses: facts.responses.length,
    response_parts: facts.response_parts.length,
    llm_steps: facts.llm_steps.length,
    tool_calls: facts.tool_calls.length,
    tool_payloads: facts.tool_payloads.length,
  })
  return facts
}
