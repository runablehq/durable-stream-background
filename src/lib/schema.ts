import { createStateSchema } from "@durable-streams/state"
import type { ChangeEvent } from "@durable-streams/state"
import type { StandardSchemaV1 } from "@standard-schema/spec"

function passthrough<T>(): StandardSchemaV1<T> {
  return {
    "~standard": {
      version: 1 as const,
      vendor: "durable-stream-demo",
      validate: (value: unknown): StandardSchemaV1.Result<T> => ({
        value: value as T,
      }),
    },
  }
}

export interface Run {
  key: string
  _seq: number
  status: "started" | "completed"
  finish_reason?: string
}

export interface Step {
  key: string
  _seq: number
  step_number: number
  status: "started" | "completed"
  finish_reason?: string
  duration_ms?: number
}

export interface Text {
  key: string
  _seq: number
  status: "streaming" | "completed"
}

export interface TextDelta {
  key: string
  _seq: number
  text_id: string
  delta: string
}

export interface ToolCall {
  key: string
  _seq: number
  tool_name: string
  status: "started" | "completed" | "failed"
  args?: unknown
  result?: unknown
}

export interface ErrorEvent {
  key: string
  _seq: number
  message: string
}

export interface Message {
  key: string
  _seq: number
  from: string
  text: string
  timestamp: number
}

export const schema = createStateSchema({
  inbox: { schema: passthrough<Message>(), type: "message", primaryKey: "key" },
  runs: { schema: passthrough<Run>(), type: "run", primaryKey: "key" },
  steps: { schema: passthrough<Step>(), type: "step", primaryKey: "key" },
  texts: { schema: passthrough<Text>(), type: "text", primaryKey: "key" },
  textDeltas: { schema: passthrough<TextDelta>(), type: "text_delta", primaryKey: "key" },
  toolCalls: { schema: passthrough<ToolCall>(), type: "tool_call", primaryKey: "key" },
  errors: { schema: passthrough<ErrorEvent>(), type: "error", primaryKey: "key" },
})

export type { ChangeEvent }
