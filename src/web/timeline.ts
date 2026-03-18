import { eq } from "@durable-streams/state"
import { coalesce } from "@tanstack/db"
import type { TextDelta } from "../lib/schema"
export type { TextDelta }

export interface TimelineRow {
  seq: number
  kind: "message" | "run" | "step" | "text" | "tool_call" | "error"
  key: string
  messageFrom: string | null
  messageText: string | null
  messageTimestamp: number | null
  runStatus: string | null
  stepStatus: string | null
  stepDurationMs: number | null
  textKey: string | null
  toolCallId: string | null
  toolName: string | null
  toolArgs: unknown | null
  toolResult: unknown | null
  toolStatus: string | null
  errorMessage: string | null
}

export type TimelineContentItem =
  | { kind: "text"; text: string }
  | {
      kind: "tool_call"
      toolCallId: string
      toolName: string
      args: Record<string, unknown>
      status?: string | null
      result?: string
      isError?: boolean
    }

export type TimelineSection =
  | {
      kind: "user_message"
      text: string
      timestamp: number
    }
  | {
      kind: "agent_response"
      items: TimelineContentItem[]
      done?: true
      error?: string
    }

export function accumulateTextDeltas(
  deltas: Array<TextDelta & { _seq?: number }>
): Map<string, string> {
  const textMap = new Map<string, string>()
  const sorted = [...deltas].sort((a, b) => (a._seq ?? 0) - (b._seq ?? 0))
  for (const delta of sorted) {
    textMap.set(delta.text_id, `${textMap.get(delta.text_id) ?? ""}${delta.delta}`)
  }
  return textMap
}

export function buildTimeline(
  rows: TimelineRow[],
  textDeltas: Array<TextDelta & { _seq?: number }>
): TimelineSection[] {
  const sections: TimelineSection[] = []
  const textMap = accumulateTextDeltas(textDeltas)
  type AgentSection = Extract<TimelineSection, { kind: "agent_response" }>
  let currentAgent: AgentSection | null = null

  const ensureAgentSection = (): AgentSection => {
    if (currentAgent) return currentAgent
    currentAgent = { kind: "agent_response", items: [] }
    sections.push(currentAgent)
    return currentAgent
  }

  for (const row of rows) {
    switch (row.kind) {
      case "message": {
        currentAgent = null
        sections.push({
          kind: "user_message",
          text: row.messageText ?? "",
          timestamp: row.messageTimestamp ?? Date.now(),
        })
        break
      }
      case "run": {
        if (row.runStatus === "started") {
          currentAgent = null
          ensureAgentSection()
        } else if (row.runStatus === "completed") {
          ensureAgentSection().done = true
        }
        break
      }
      case "text": {
        const text = row.textKey ? (textMap.get(row.textKey) ?? "") : ""
        if (!text) break
        ensureAgentSection().items.push({ kind: "text", text })
        break
      }
      case "tool_call": {
        const args =
          row.toolArgs && typeof row.toolArgs === "object" && !Array.isArray(row.toolArgs)
            ? (row.toolArgs as Record<string, unknown>)
            : {}
        const item: Extract<TimelineContentItem, { kind: "tool_call" }> = {
          kind: "tool_call",
          toolCallId: row.toolCallId ?? row.key,
          toolName: row.toolName ?? "tool",
          args,
          status: row.toolStatus,
          isError: row.toolStatus === "failed",
        }
        if (row.toolResult != null) {
          item.result =
            typeof row.toolResult === "string"
              ? row.toolResult
              : JSON.stringify(row.toolResult)
        }
        ensureAgentSection().items.push(item)
        break
      }
      case "error": {
        ensureAgentSection().error = row.errorMessage ?? "Unknown error"
        break
      }
    }
  }

  return sections
}

interface TimelineDBLike {
  collections: Record<string, unknown>
}

export interface TimelineQueries {
  rows: (q: any) => unknown
  textDeltas: (q: any) => unknown
}

export function createTimelineQuery(db: TimelineDBLike | null): TimelineQueries {
  const nullFields = {
    messageFrom: null,
    messageText: null,
    messageTimestamp: null,
    runStatus: null,
    stepStatus: null,
    stepDurationMs: null,
    textKey: null,
    toolCallId: null,
    toolName: null,
    toolArgs: null,
    toolResult: null,
    toolStatus: null,
    errorMessage: null,
  }

  return {
    rows: (q: any) => {
      if (!db) return null

      const inboxRows = q
        .from({ inbox: db.collections.inbox as any })
        .select(({ inbox }: any) => ({
          seq: inbox._seq,
          kind: "message",
          key: inbox.key,
          ...nullFields,
          messageFrom: inbox.from,
          messageText: inbox.text,
          messageTimestamp: inbox.timestamp,
        }))

      const runRows = q
        .from({ runs: db.collections.runs as any })
        .select(({ runs }: any) => ({
          seq: runs._seq,
          kind: "run",
          key: runs.key,
          ...nullFields,
          runStatus: runs.status,
        }))

      const stepRows = q
        .from({ steps: db.collections.steps as any })
        .select(({ steps }: any) => ({
          seq: steps._seq,
          kind: "step",
          key: steps.key,
          ...nullFields,
          stepStatus: steps.status,
          stepDurationMs: steps.duration_ms,
        }))

      const textRows = q
        .from({ texts: db.collections.texts as any })
        .select(({ texts }: any) => ({
          seq: texts._seq,
          kind: "text",
          key: texts.key,
          ...nullFields,
          textKey: texts.key,
        }))

      const toolRows = q
        .from({ toolCalls: db.collections.toolCalls as any })
        .select(({ toolCalls }: any) => ({
          seq: toolCalls._seq,
          kind: "tool_call",
          key: toolCalls.key,
          ...nullFields,
          toolCallId: toolCalls.key,
          toolName: toolCalls.tool_name,
          toolArgs: toolCalls.args,
          toolResult: toolCalls.result,
          toolStatus: toolCalls.status,
        }))

      const errorRows = q
        .from({ errors: db.collections.errors as any })
        .select(({ errors }: any) => ({
          seq: errors._seq,
          kind: "error",
          key: errors.key,
          ...nullFields,
          errorMessage: errors.message,
        }))

      const coalesceRow = (leftAlias: string, rightAlias: string) =>
        ({ [leftAlias]: left, [rightAlias]: right }: any) => ({
          seq: coalesce(left.seq, right.seq),
          kind: coalesce(left.kind, right.kind),
          key: coalesce(left.key, right.key),
          messageFrom: coalesce(left.messageFrom, right.messageFrom),
          messageText: coalesce(left.messageText, right.messageText),
          messageTimestamp: coalesce(left.messageTimestamp, right.messageTimestamp),
          runStatus: coalesce(left.runStatus, right.runStatus),
          stepStatus: coalesce(left.stepStatus, right.stepStatus),
          stepDurationMs: coalesce(left.stepDurationMs, right.stepDurationMs),
          textKey: coalesce(left.textKey, right.textKey),
          toolCallId: coalesce(left.toolCallId, right.toolCallId),
          toolName: coalesce(left.toolName, right.toolName),
          toolArgs: coalesce(left.toolArgs, right.toolArgs),
          toolResult: coalesce(left.toolResult, right.toolResult),
          toolStatus: coalesce(left.toolStatus, right.toolStatus),
          errorMessage: coalesce(left.errorMessage, right.errorMessage),
        })

      const withInbox = q
        .from({ left: inboxRows })
        .fullJoin({ right: runRows }, ({ left, right }: any) => eq(left.seq, right.seq))
        .select(coalesceRow("left", "right"))

      const withSteps = q
        .from({ left: withInbox })
        .fullJoin({ right: stepRows }, ({ left, right }: any) => eq(left.seq, right.seq))
        .select(coalesceRow("left", "right"))

      const withTexts = q
        .from({ left: withSteps })
        .fullJoin({ right: textRows }, ({ left, right }: any) => eq(left.seq, right.seq))
        .select(coalesceRow("left", "right"))

      const withTools = q
        .from({ left: withTexts })
        .fullJoin({ right: toolRows }, ({ left, right }: any) => eq(left.seq, right.seq))
        .select(coalesceRow("left", "right"))

      const withErrors = q
        .from({ left: withTools })
        .fullJoin({ right: errorRows }, ({ left, right }: any) => eq(left.seq, right.seq))
        .select(coalesceRow("left", "right"))

      return q
        .from({ rows: withErrors })
        .orderBy(({ rows }: any) => rows.seq, "asc")
    },

    textDeltas: (q: any) =>
      db
        ? q
            .from({ textDeltas: db.collections.textDeltas as any })
            .orderBy(({ textDeltas }: any) => textDeltas._seq, "asc")
        : null,
  }
}
