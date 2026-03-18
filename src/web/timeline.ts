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

