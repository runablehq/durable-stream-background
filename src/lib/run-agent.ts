import { DurableStream, stream } from "@durable-streams/client"
import { agent } from "./agent.ts"
import { createOutboundBridge } from "./outbound-bridge.ts"
import { STREAM_SERVER_URL } from "./durable.ts"
import type { ChangeEvent } from "@durable-streams/state"

interface RunOptions {
  runId: string
  prompt: string
}

/**
 * Reconstruct conversation history from stream events.
 * Messages come from inbox events, assistant text from text_delta events.
 */
function buildConversationFromEvents(
  events: ChangeEvent[]
): Array<{ role: "user" | "assistant"; content: string }> {
  // Collect messages and text deltas in stream order
  const messages: Array<{ role: "user" | "assistant"; content: string }> = []
  const textDeltasByTextId = new Map<string, string>()
  const textIdOrder: string[] = []
  for (const ev of events) {
    if (ev.type === "message") {
      const val = ev.value as Record<string, unknown> | undefined
      if (typeof val?.text === "string") {
        // Flush any accumulated assistant text before this user message
        const assistantText = flushAssistantText(textDeltasByTextId, textIdOrder)
        if (assistantText) {
          messages.push({ role: "assistant", content: assistantText })
        }
        textDeltasByTextId.clear()
        textIdOrder.length = 0
        messages.push({ role: "user", content: val.text as string })
      }
    }
    if (ev.type === "text_delta") {
      const val = ev.value as { text_id?: string; delta?: string } | undefined
      if (val?.text_id && val?.delta) {
        textDeltasByTextId.set(
          val.text_id,
          (textDeltasByTextId.get(val.text_id) ?? "") + val.delta
        )
        if (!textIdOrder.includes(val.text_id)) {
          textIdOrder.push(val.text_id)
        }
      }
    }
  }

  // Flush remaining assistant text
  const assistantText = flushAssistantText(textDeltasByTextId, textIdOrder)
  if (assistantText) {
    messages.push({ role: "assistant", content: assistantText })
  }

  return messages
}

function flushAssistantText(
  deltaMap: Map<string, string>,
  order: string[]
): string {
  let text = ""
  for (const id of order) {
    text += deltaMap.get(id) ?? ""
  }
  return text
}

export async function runAgentToStream({ runId, prompt }: RunOptions) {
  const streamUrl = `${STREAM_SERVER_URL}/v1/stream/${runId}`

  // Read existing events to determine startSeq, counters, and conversation history
  const existing = await stream({ url: streamUrl, offset: "-1", live: false })
  const existingEvents = await existing.json() as ChangeEvent[]

  let maxSeq = 0
  let maxRun = 0
  let maxStep = 0
  for (const ev of existingEvents) {
    const val = ev.value as Record<string, unknown> | undefined
    if (val && typeof val._seq === "number" && val._seq > maxSeq) {
      maxSeq = val._seq
    }
    if (ev.type === "run" && ev.key) {
      const m = ev.key.match(/^run-(\d+)/)
      if (m) maxRun = Math.max(maxRun, parseInt(m[1]!, 10) + 1)
    }
    if (ev.type === "step" && ev.key) {
      const m = ev.key.match(/^step-(\d+)/)
      if (m) maxStep = Math.max(maxStep, parseInt(m[1]!, 10) + 1)
    }
  }

  // Build conversation history from stream
  const conversation = buildConversationFromEvents(existingEvents)

  const handle = await DurableStream.connect({
    url: streamUrl,
    contentType: "application/json",
  })

  const bridge = createOutboundBridge(
    (event) => { handle.append(JSON.stringify(event)) },
    { startSeq: maxSeq + 1, startRunCounter: maxRun, startStepCounter: maxStep }
  )

  try {
    // If there's conversation history, pass messages; otherwise just the prompt
    const streamArgs = conversation.length > 0
      ? { messages: conversation }
      : { prompt }

    const result = await agent.stream(streamArgs)

    for await (const part of result.fullStream) {
      switch (part.type) {
        case "start":
          bridge.onRunStart()
          break
        case "start-step":
          bridge.onStepStart()
          break
        case "text-start":
          bridge.onTextStart(part.id)
          break
        case "text-delta":
          bridge.onTextDelta(part.id, part.text)
          break
        case "text-end":
          bridge.onTextEnd(part.id)
          break
        case "tool-call":
          bridge.onToolCallStart(part.toolCallId, part.toolName, part.input)
          break
        case "tool-result":
          bridge.onToolCallEnd(part.toolCallId, part.toolName, part.output, false)
          break
        case "tool-error":
          bridge.onToolCallEnd(part.toolCallId, part.toolName, part.error, true)
          break
        case "finish-step":
          bridge.onStepEnd({ finishReason: part.finishReason })
          break
        case "finish":
          bridge.onRunEnd({ finishReason: part.finishReason })
          break
        case "abort":
          bridge.onError("aborted")
          bridge.onRunEnd({ finishReason: "abort" })
          break
        case "error":
          bridge.onError(String(part.error))
          break
      }
    }

  } catch (err) {
    bridge.onError(String(err))
    bridge.onRunEnd({ finishReason: "error" })
    throw err
  }
}
