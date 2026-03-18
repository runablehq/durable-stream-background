import { test, expect, describe } from "bun:test"
import type { ChangeEvent } from "@durable-streams/state"
import { createOutboundBridge } from "./outbound-bridge"
import { buildTimeline, accumulateTextDeltas, type TimelineRow } from "../web/timeline"

describe("OutboundBridge", () => {
  test("emits run lifecycle events with correct _seq", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge((e) => events.push(e))

    bridge.onRunStart()
    bridge.onRunEnd({ finishReason: "stop" })

    expect(events).toHaveLength(2)
    expect(events[0]!.type).toBe("run")
    expect(events[0]!.key).toBe("run-0")
    expect((events[0]!.value as any)._seq).toBe(0)
    expect((events[0]!.value as any).status).toBe("started")
    expect(events[0]!.headers.operation).toBe("insert")

    expect(events[1]!.type).toBe("run")
    expect(events[1]!.key).toBe("run-0")
    expect((events[1]!.value as any)._seq).toBe(1)
    expect((events[1]!.value as any).status).toBe("completed")
    expect(events[1]!.headers.operation).toBe("update")
  })

  test("text events scope keys by step to avoid collisions", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge((e) => events.push(e))

    bridge.onRunStart()
    bridge.onStepStart()
    bridge.onTextStart("0")
    bridge.onTextDelta("0", "hello ")
    bridge.onTextEnd("0")
    bridge.onStepEnd()

    // Second step — same text part.id "0" but different step
    bridge.onStepStart()
    bridge.onTextStart("0")
    bridge.onTextDelta("0", "world")
    bridge.onTextEnd("0")
    bridge.onStepEnd()
    bridge.onRunEnd()

    const textInserts = events.filter(
      (e) => e.type === "text" && e.headers.operation === "insert"
    )
    expect(textInserts).toHaveLength(2)
    expect(textInserts[0]!.key).toBe("step-0-text-0")
    expect(textInserts[1]!.key).toBe("step-1-text-0")
  })

  test("text deltas get sequential keys", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge((e) => events.push(e))

    bridge.onRunStart()
    bridge.onStepStart()
    bridge.onTextStart("0")
    bridge.onTextDelta("0", "hello ")
    bridge.onTextDelta("0", "world")
    bridge.onTextEnd("0")

    const deltas = events.filter((e) => e.type === "text_delta")
    expect(deltas).toHaveLength(2)
    expect(deltas[0]!.key).toBe("step-0-text-0:0")
    expect(deltas[1]!.key).toBe("step-0-text-0:1")
    expect((deltas[0]!.value as any).text_id).toBe("step-0-text-0")
    expect((deltas[0]!.value as any).delta).toBe("hello ")
    expect((deltas[1]!.value as any).delta).toBe("world")
  })

  test("tool call preserves args on completion", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge((e) => events.push(e))

    bridge.onRunStart()
    bridge.onStepStart()
    bridge.onToolCallStart("tc-1", "execute_javascript", { code: "4 * 5" })
    bridge.onToolCallEnd("tc-1", "execute_javascript", { ok: true, result: "20" }, false)

    const toolUpdate = events.find(
      (e) => e.type === "tool_call" && e.headers.operation === "update"
    )
    expect(toolUpdate).toBeDefined()
    expect((toolUpdate!.value as any).args).toEqual({ code: "4 * 5" })
    expect((toolUpdate!.value as any).status).toBe("completed")
  })

  test("startSeq continues numbering for follow-up runs", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge(
      (e) => events.push(e),
      { startSeq: 10, startRunCounter: 1, startStepCounter: 2 }
    )

    bridge.onRunStart()
    bridge.onStepStart()

    expect(events[0]!.key).toBe("run-1")
    expect((events[0]!.value as any)._seq).toBe(10)
    expect(events[1]!.key).toBe("step-2")
    expect((events[1]!.value as any)._seq).toBe(11)
  })

  test("error events get unique keys", () => {
    const events: ChangeEvent[] = []
    const bridge = createOutboundBridge((e) => events.push(e))

    bridge.onError("first error")
    bridge.onError("second error")

    const errors = events.filter((e) => e.type === "error")
    expect(errors).toHaveLength(2)
    expect(errors[0]!.key).toBe("err-0")
    expect(errors[1]!.key).toBe("err-1")
  })
})

describe("buildTimeline", () => {
  function makeRow(overrides: Partial<TimelineRow> & { seq: number; kind: TimelineRow["kind"]; key: string }): TimelineRow {
    return {
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
      ...overrides,
    }
  }

  test("builds timeline from a simple run with text", () => {
    const rows: TimelineRow[] = [
      makeRow({ seq: 0, kind: "message", key: "msg-0", messageFrom: "user", messageText: "hello", messageTimestamp: 1000 }),
      makeRow({ seq: 5, kind: "text", key: "step-0-text-0", textKey: "step-0-text-0" }),
      makeRow({ seq: 7, kind: "run", key: "run-0", runStatus: "completed" }),
    ]
    const deltas = [
      { key: "step-0-text-0:0", _seq: 3, text_id: "step-0-text-0", delta: "Hi there!" },
    ]

    const timeline = buildTimeline(rows, deltas)

    expect(timeline).toHaveLength(2)
    expect(timeline[0]!.kind).toBe("user_message")
    if (timeline[0]!.kind === "user_message") {
      expect(timeline[0]!.text).toBe("hello")
    }
    expect(timeline[1]!.kind).toBe("agent_response")
    if (timeline[1]!.kind === "agent_response") {
      expect(timeline[1]!.items).toHaveLength(1)
      expect(timeline[1]!.items[0]!.kind).toBe("text")
      expect(timeline[1]!.done).toBe(true)
    }
  })

  test("builds timeline with follow-up messages", () => {
    const rows: TimelineRow[] = [
      makeRow({ seq: 0, kind: "message", key: "msg-0", messageFrom: "user", messageText: "what is 2+2?", messageTimestamp: 1000 }),
      makeRow({ seq: 10, kind: "text", key: "step-1-text-0", textKey: "step-1-text-0" }),
      makeRow({ seq: 13, kind: "run", key: "run-0", runStatus: "completed" }),
      makeRow({ seq: 14, kind: "message", key: "msg-1", messageFrom: "user", messageText: "how about 3+3?", messageTimestamp: 2000 }),
      makeRow({ seq: 25, kind: "text", key: "step-3-text-0", textKey: "step-3-text-0" }),
      makeRow({ seq: 27, kind: "run", key: "run-1", runStatus: "completed" }),
    ]
    const deltas = [
      { key: "step-1-text-0:0", _seq: 8, text_id: "step-1-text-0", delta: "4" },
      { key: "step-3-text-0:0", _seq: 22, text_id: "step-3-text-0", delta: "6" },
    ]

    const timeline = buildTimeline(rows, deltas)

    expect(timeline).toHaveLength(4)
    expect(timeline[0]!.kind).toBe("user_message")
    expect(timeline[1]!.kind).toBe("agent_response")
    expect(timeline[2]!.kind).toBe("user_message")
    expect(timeline[3]!.kind).toBe("agent_response")

    if (timeline[1]!.kind === "agent_response") {
      expect(timeline[1]!.items[0]).toEqual({ kind: "text", text: "4" })
      expect(timeline[1]!.done).toBe(true)
    }
    if (timeline[2]!.kind === "user_message") {
      expect(timeline[2]!.text).toBe("how about 3+3?")
    }
    if (timeline[3]!.kind === "agent_response") {
      expect(timeline[3]!.items[0]).toEqual({ kind: "text", text: "6" })
      expect(timeline[3]!.done).toBe(true)
    }
  })

  test("accumulates text from deltas", () => {
    const rows: TimelineRow[] = [
      makeRow({ seq: 5, kind: "text", key: "step-0-text-0", textKey: "step-0-text-0" }),
    ]
    const deltas = [
      { key: "step-0-text-0:0", _seq: 1, text_id: "step-0-text-0", delta: "hello " },
      { key: "step-0-text-0:1", _seq: 2, text_id: "step-0-text-0", delta: "world" },
    ]

    const timeline = buildTimeline(rows, deltas)
    if (timeline[0]!.kind === "agent_response") {
      expect(timeline[0]!.items[0]).toEqual({ kind: "text", text: "hello world" })
    }
  })

  test("handles tool calls with args and result", () => {
    const rows: TimelineRow[] = [
      makeRow({
        seq: 3,
        kind: "tool_call",
        key: "tc-1",
        toolCallId: "tc-1",
        toolName: "execute_javascript",
        toolArgs: { code: "4 * 5" },
        toolResult: '{"ok":true,"result":"20"}',
        toolStatus: "completed",
      }),
    ]

    const timeline = buildTimeline(rows, [])
    if (timeline[0]!.kind === "agent_response") {
      const item = timeline[0]!.items[0]!
      expect(item.kind).toBe("tool_call")
      if (item.kind === "tool_call") {
        expect(item.toolName).toBe("execute_javascript")
        expect(item.args).toEqual({ code: "4 * 5" })
        expect(item.result).toBe('{"ok":true,"result":"20"}')
        expect(item.isError).toBe(false)
      }
    }
  })

  test("catch-up: completed run without seeing started", () => {
    const rows: TimelineRow[] = [
      makeRow({ seq: 10, kind: "text", key: "step-0-text-0", textKey: "step-0-text-0" }),
      makeRow({ seq: 13, kind: "run", key: "run-0", runStatus: "completed" }),
    ]
    const deltas = [
      { key: "step-0-text-0:0", _seq: 5, text_id: "step-0-text-0", delta: "answer" },
    ]

    const timeline = buildTimeline(rows, deltas)
    expect(timeline).toHaveLength(1)
    expect(timeline[0]!.kind).toBe("agent_response")
    if (timeline[0]!.kind === "agent_response") {
      expect(timeline[0]!.items).toHaveLength(1)
      expect(timeline[0]!.done).toBe(true)
    }
  })
})
