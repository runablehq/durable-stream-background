import type { ChangeEvent } from "@durable-streams/state"
import { schema } from "./schema.ts"

export interface OutboundBridge {
  onRunStart(): void
  onRunEnd(opts?: { finishReason?: string }): void
  onStepStart(): void
  onStepEnd(opts?: { finishReason?: string; durationMs?: number }): void
  onTextStart(id: string): void
  onTextDelta(id: string, delta: string): void
  onTextEnd(id: string): void
  onToolCallStart(toolCallId: string, name: string, args: unknown): void
  onToolCallEnd(toolCallId: string, name: string, result: unknown, isError: boolean): void
  onError(message: string): void
}

export function createOutboundBridge(
  writeEvent: (event: ChangeEvent) => void,
  opts?: { startSeq?: number; startRunCounter?: number; startStepCounter?: number }
): OutboundBridge {
  let seq = opts?.startSeq ?? 0
  let runCounter = opts?.startRunCounter ?? 0
  let stepCounter = opts?.startStepCounter ?? 0
  let stepNumber = 0
  let errorCounter = 0
  const deltaSeqs = new Map<string, number>()
  const toolCallArgs = new Map<string, unknown>()

  let currentRunKey: string | null = null
  let currentStepKey: string | null = null

  const nextSeq = () => seq++

  return {
    onRunStart() {
      currentRunKey = `run-${runCounter++}`
      writeEvent(schema.runs.insert({
        value: { key: currentRunKey, _seq: nextSeq(), status: "started" },
      }))
    },

    onRunEnd(opts) {
      if (!currentRunKey) return
      writeEvent(schema.runs.update({
        value: {
          key: currentRunKey,
          _seq: nextSeq(),
          status: "completed",
          finish_reason: opts?.finishReason ?? "stop",
        },
      }))
    },

    onStepStart() {
      currentStepKey = `step-${stepCounter++}`
      stepNumber++
      writeEvent(schema.steps.insert({
        value: {
          key: currentStepKey,
          _seq: nextSeq(),
          step_number: stepNumber,
          status: "started",
        },
      }))
    },

    onStepEnd(opts) {
      if (!currentStepKey) return
      writeEvent(schema.steps.update({
        value: {
          key: currentStepKey,
          _seq: nextSeq(),
          step_number: stepNumber,
          status: "completed",
          finish_reason: opts?.finishReason ?? "stop",
          ...(opts?.durationMs !== undefined && { duration_ms: opts.durationMs }),
        },
      }))
    },

    onTextStart(id: string) {
      const textKey = `${currentStepKey}-text-${id}`
      deltaSeqs.set(textKey, 0)
      writeEvent(schema.texts.insert({
        value: { key: textKey, _seq: nextSeq(), status: "streaming" },
      }))
    },

    onTextDelta(id: string, delta: string) {
      const textKey = `${currentStepKey}-text-${id}`
      const dseq = deltaSeqs.get(textKey) ?? 0
      deltaSeqs.set(textKey, dseq + 1)
      writeEvent(schema.textDeltas.insert({
        value: { key: `${textKey}:${dseq}`, _seq: nextSeq(), text_id: textKey, delta },
      }))
    },

    onTextEnd(id: string) {
      const textKey = `${currentStepKey}-text-${id}`
      writeEvent(schema.texts.update({
        value: { key: textKey, _seq: nextSeq(), status: "completed" },
      }))
    },

    onToolCallStart(toolCallId: string, name: string, args: unknown) {
      toolCallArgs.set(toolCallId, args)
      writeEvent(schema.toolCalls.insert({
        value: { key: toolCallId, _seq: nextSeq(), tool_name: name, status: "started", args },
      }))
    },

    onToolCallEnd(toolCallId: string, name: string, result: unknown, isError: boolean) {
      const args = toolCallArgs.get(toolCallId)
      writeEvent(schema.toolCalls.update({
        value: {
          key: toolCallId,
          _seq: nextSeq(),
          tool_name: name,
          status: isError ? "failed" : "completed",
          args,
          result: typeof result === "string" ? result : JSON.stringify(result),
        },
      }))
    },

    onError(message: string) {
      writeEvent(schema.errors.insert({
        value: { key: `err-${errorCounter++}`, _seq: nextSeq(), message },
      }))
    },
  }
}
