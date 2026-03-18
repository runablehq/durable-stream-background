# Durable Stream Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire a Vercel AI SDK agent to run as a BullMQ background job, capturing all events into a durable state stream, with a React client that renders the timeline via StreamDB.

**Architecture:** API endpoint creates a durable stream + enqueues a BullMQ job. Worker picks up the job, runs the agent, writes lifecycle events to the stream via an OutboundBridge adapter. React client creates a StreamDB, subscribes to collections, and renders a timeline with text + tool calls.

**Tech Stack:** Bun, Hono, BullMQ, Vercel AI SDK v6, @durable-streams/client, @durable-streams/state, @durable-streams/server, @tanstack/react-db, React

**Spec:** `docs/superpowers/specs/2026-03-18-durable-stream-agent-design.md`

**Key reference files (darix):**
- `~/programs/darix/packages/ts-darix-runtime/src/entity-schema.ts` — entity types + passthrough
- `~/programs/darix/packages/ts-darix-runtime/src/outbound-bridge.ts` — bridge implementation
- `~/programs/darix/packages/ts-darix-runtime/src/entity-timeline.ts` — timeline query + builder
- `~/programs/darix/examples/webhook-agents-ui/src/components/TaskCard.tsx` — rendering pattern
- `~/programs/darix/examples/webhook-agents-ui/src/lib/use-task-db.ts` — StreamDB hook

---

## Task 1: Install dependencies

**Files:**
- Modify: `package.json`

- [ ] **Step 1: Install server-side dependencies**

```bash
bun add @durable-streams/server @standard-schema/spec
```

- [ ] **Step 2: Install client-side dependencies**

```bash
bun add @tanstack/react-db
```

- [ ] **Step 3: Verify all packages installed**

Run: `bun run --bun -e "import '@durable-streams/server'; import '@tanstack/react-db'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add package.json bun.lock
git commit -m "feat: add durable streams server and react-db dependencies"
```

---

## Task 2: State schema + passthrough validator

**Files:**
- Create: `src/lib/schema.ts`

This is the shared schema used by both the server-side bridge (to create typed ChangeEvents) and the client-side StreamDB (to materialize collections). Read `~/programs/darix/packages/ts-darix-runtime/src/entity-schema.ts` for the reference pattern.

- [ ] **Step 1: Create schema file with entity types and passthrough validator**

```typescript
// src/lib/schema.ts
import { createStateSchema } from "@durable-streams/state"
import type { ChangeEvent } from "@durable-streams/state"
import type { StandardSchemaV1 } from "@standard-schema/spec"

// Passthrough Standard Schema validator — shape enforced by write side
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

// Entity types — all include _seq for cross-collection ordering

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

// Schema definition
export const schema = createStateSchema({
  runs: { schema: passthrough<Run>(), type: "run", primaryKey: "key" },
  steps: { schema: passthrough<Step>(), type: "step", primaryKey: "key" },
  texts: { schema: passthrough<Text>(), type: "text", primaryKey: "key" },
  textDeltas: { schema: passthrough<TextDelta>(), type: "text_delta", primaryKey: "key" },
  toolCalls: { schema: passthrough<ToolCall>(), type: "tool_call", primaryKey: "key" },
  errors: { schema: passthrough<ErrorEvent>(), type: "error", primaryKey: "key" },
})

export type { ChangeEvent }
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { schema } from './src/lib/schema.ts'; console.log(Object.keys(schema))"`
Expected: prints the collection names

- [ ] **Step 3: Commit**

```bash
git add src/lib/schema.ts
git commit -m "feat: add state schema with entity types for agent events"
```

---

## Task 3: OutboundBridge

**Files:**
- Create: `src/lib/outbound-bridge.ts`

The bridge maps agent lifecycle callbacks to `ChangeEvent` objects. Read `~/programs/darix/packages/ts-darix-runtime/src/outbound-bridge.ts` as the reference. Key differences from darix:
- Uses AI SDK stable IDs for text (`part.id`) and tool call (`part.toolCallId`) keys
- Auto-increments only `run-N` and `step-N` keys
- Stamps `_seq` on every event value (installed @durable-streams/state doesn't inject it)
- No retry/resume — `attempts: 1`, so counters start at 0

- [ ] **Step 1: Create the outbound bridge**

```typescript
// src/lib/outbound-bridge.ts
import type { ChangeEvent } from "@durable-streams/state"

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
  writeEvent: (event: ChangeEvent) => void
): OutboundBridge {
  let seq = 0
  let runCounter = 0
  let stepCounter = 0
  let stepNumber = 0
  let errorCounter = 0
  const deltaSeqs = new Map<string, number>()

  let currentRunKey: string | null = null
  let currentStepKey: string | null = null
  const toolCallArgs = new Map<string, unknown>()

  const nextSeq = () => seq++

  return {
    onRunStart() {
      currentRunKey = `run-${runCounter++}`
      writeEvent({
        type: "run",
        key: currentRunKey,
        value: { _seq: nextSeq(), status: "started" },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },

    onRunEnd(opts) {
      if (!currentRunKey) return
      writeEvent({
        type: "run",
        key: currentRunKey,
        value: {
          _seq: nextSeq(),
          status: "completed",
          finish_reason: opts?.finishReason ?? "stop",
        },
        headers: { operation: "update" } as ChangeEvent["headers"],
      })
    },

    onStepStart() {
      currentStepKey = `step-${stepCounter++}`
      stepNumber++
      writeEvent({
        type: "step",
        key: currentStepKey,
        value: { _seq: nextSeq(), step_number: stepNumber, status: "started" },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },

    onStepEnd(opts) {
      if (!currentStepKey) return
      writeEvent({
        type: "step",
        key: currentStepKey,
        value: {
          _seq: nextSeq(),
          step_number: stepNumber,
          status: "completed",
          finish_reason: opts?.finishReason ?? "stop",
          ...(opts?.durationMs !== undefined && { duration_ms: opts.durationMs }),
        },
        headers: { operation: "update" } as ChangeEvent["headers"],
      })
    },

    onTextStart(id: string) {
      deltaSeqs.set(id, 0)
      writeEvent({
        type: "text",
        key: id,
        value: { _seq: nextSeq(), status: "streaming" },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },

    onTextDelta(id: string, delta: string) {
      const dseq = deltaSeqs.get(id) ?? 0
      deltaSeqs.set(id, dseq + 1)
      writeEvent({
        type: "text_delta",
        key: `${id}:${dseq}`,
        value: { _seq: nextSeq(), text_id: id, delta },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },

    onTextEnd(id: string) {
      writeEvent({
        type: "text",
        key: id,
        value: { _seq: nextSeq(), status: "completed" },
        headers: { operation: "update" } as ChangeEvent["headers"],
      })
    },

    onToolCallStart(toolCallId: string, name: string, args: unknown) {
      toolCallArgs.set(toolCallId, args)
      writeEvent({
        type: "tool_call",
        key: toolCallId,
        value: { _seq: nextSeq(), tool_name: name, status: "started", args },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },

    onToolCallEnd(toolCallId: string, name: string, result: unknown, isError: boolean) {
      const args = toolCallArgs.get(toolCallId)
      writeEvent({
        type: "tool_call",
        key: toolCallId,
        value: {
          _seq: nextSeq(),
          tool_name: name,
          status: isError ? "failed" : "completed",
          args,
          result: typeof result === "string" ? result : JSON.stringify(result),
        },
        headers: { operation: "update" } as ChangeEvent["headers"],
      })
    },

    onError(message: string) {
      writeEvent({
        type: "error",
        key: `err-${errorCounter++}`,
        value: { _seq: nextSeq(), message },
        headers: { operation: "insert" } as ChangeEvent["headers"],
      })
    },
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { createOutboundBridge } from './src/lib/outbound-bridge.ts'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/lib/outbound-bridge.ts
git commit -m "feat: add OutboundBridge adapter for agent lifecycle events"
```

---

## Task 4: Durable stream dev server setup

**Files:**
- Create: `src/lib/durable.ts`

- [ ] **Step 1: Create durable stream server config**

```typescript
// src/lib/durable.ts
import { DurableStreamTestServer } from "@durable-streams/server"

export const STREAM_SERVER_URL = "http://127.0.0.1:4437"

export async function startStreamServer() {
  const server = new DurableStreamTestServer({
    port: 4437,
    host: "127.0.0.1",
  })
  await server.start()
  console.log(`Durable stream dev server running on ${STREAM_SERVER_URL}`)
  return server
}
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { STREAM_SERVER_URL } from './src/lib/durable.ts'; console.log(STREAM_SERVER_URL)"`
Expected: `http://127.0.0.1:4437`

- [ ] **Step 3: Commit**

```bash
git add src/lib/durable.ts
git commit -m "feat: add durable stream dev server setup"
```

---

## Task 5: Agent-to-stream runner

**Files:**
- Create: `src/lib/run-agent.ts`

This wires the AI SDK `agent.stream()` → OutboundBridge → IdempotentProducer → durable stream. Read the spec section "AI SDK Integration" and "Writing Events" for the full flow.

- [ ] **Step 1: Create the run-agent module**

```typescript
// src/lib/run-agent.ts
import { DurableStream, IdempotentProducer } from "@durable-streams/client"
import { agent } from "./agent.ts"
import { createOutboundBridge } from "./outbound-bridge.ts"
import { STREAM_SERVER_URL } from "./durable.ts"

export async function runAgentToStream(prompt: string, runId: string) {
  const handle = await DurableStream.connect({
    url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
    contentType: "application/json",
  })

  const producer = new IdempotentProducer(handle, "agent-worker", {
    autoClaim: true,
    onError: (err) => console.error("[run-agent] Write error:", err),
  })

  const bridge = createOutboundBridge((event) => {
    producer.append(JSON.stringify(event))
  })

  try {
    const result = await agent.stream({ prompt })

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

    await producer.flush()
    await producer.close()
  } catch (err) {
    bridge.onError(String(err))
    bridge.onRunEnd({ finishReason: "error" })
    await producer.flush()
    await producer.close()
    throw err
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { runAgentToStream } from './src/lib/run-agent.ts'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/lib/run-agent.ts
git commit -m "feat: add agent-to-stream runner wiring AI SDK to OutboundBridge"
```

---

## Task 6: Modify worker to handle agent-run jobs

**Files:**
- Modify: `src/worker.ts`

- [ ] **Step 1: Add agent-run job handler**

Replace the contents of `src/worker.ts` with:

```typescript
import { Worker } from "bullmq";
import { connection } from "./lib/redis";
import { runAgentToStream } from "./lib/run-agent";

const worker = new Worker(
  "jobs",
  async (job) => {
    console.log(`Processing job ${job.id}:`, job.name, job.data);

    switch (job.name) {
      case "agent-run": {
        const { prompt, runId } = job.data;
        try {
          await runAgentToStream(prompt, runId);
        } catch (err) {
          throw err;
        }
        break;
      }
      default:
        console.log(`Unknown job: ${job.name}`);
    }
  },
  { connection }
);

worker.on("completed", (job) => console.log(`Job ${job.id} completed`));
worker.on("failed", (job, err) =>
  console.error(`Job ${job?.id} failed:`, err.message)
);

console.log("Worker started");
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import './src/worker.ts'" 2>&1 | head -5`
Expected: should print "Worker started" (then fail connecting to Redis, which is fine)

- [ ] **Step 3: Commit**

```bash
git add src/worker.ts
git commit -m "feat: add agent-run job handler to BullMQ worker"
```

---

## Task 7: Modify API to add /api/agent endpoint + start stream server

**Files:**
- Modify: `src/api.ts`

- [ ] **Step 1: Add stream server startup + /api/agent endpoint**

Replace the contents of `src/api.ts` with:

```typescript
import { Hono } from "hono";
import { serveStatic } from "hono/bun";
import { createAgentUIStreamResponse } from "ai";
import { DurableStream } from "@durable-streams/client";
import { agent } from "./lib/agent";
import { jobQueue } from "./lib/queue";
import { startStreamServer, STREAM_SERVER_URL } from "./lib/durable";

const app = new Hono();

// Chat endpoint (existing)
app.post("/api/chat", async (c) => {
  const { messages } = await c.req.json();

  return createAgentUIStreamResponse({
    agent,
    uiMessages: messages,
  });
});

// Background agent endpoint (new)
app.post("/api/agent", async (c) => {
  const { prompt } = await c.req.json();
  const runId = crypto.randomUUID();

  await DurableStream.create({
    url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
    contentType: "application/json",
  });

  await jobQueue.add("agent-run", { prompt, runId }, { attempts: 1 });
  return c.json({ runId });
});

// Example: enqueue a job
app.post("/api/jobs", async (c) => {
  const body = await c.req.json();
  const job = await jobQueue.add(body.name ?? "default", body.data ?? {});
  return c.json({ id: job.id });
});

// Serve built frontend
app.use("/*", serveStatic({ root: "./dist" }));

// Start durable stream dev server before the app
await startStreamServer();

export default {
  port: 4000,
  fetch: app.fetch,
};
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { STREAM_SERVER_URL } from './src/lib/durable.ts'; console.log('api imports ok')"`
Expected: `api imports ok`

- [ ] **Step 3: Commit**

```bash
git add src/api.ts
git commit -m "feat: add /api/agent endpoint with stream creation + dev server startup"
```

---

## Task 8: Timeline query builder + buildTimeline

**Files:**
- Create: `src/web/timeline.ts`

Read `~/programs/darix/packages/ts-darix-runtime/src/entity-timeline.ts` for the full reference. Our version is simpler — no inbox/reasoning/childStatus collections, and we use our write-side `_seq` instead of dispatcher-injected `_seq`.

- [ ] **Step 1: Create the timeline module**

```typescript
// src/web/timeline.ts
import { eq } from "@durable-streams/state"
import { coalesce } from "@tanstack/db"
import type { TextDelta } from "../lib/schema"
export type { TextDelta }

// Row shape projected from each collection
export interface TimelineRow {
  seq: number
  kind: "run" | "step" | "text" | "tool_call" | "error"
  key: string
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

// Rendered sections
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

export type TimelineSection = {
  kind: "agent_response"
  items: TimelineContentItem[]
  done?: true
  error?: string
}

// Accumulate text deltas by text_id
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

// Build timeline sections from materialized rows
export function buildTimeline(
  rows: TimelineRow[],
  textDeltas: Array<TextDelta & { _seq?: number }>
): TimelineSection[] {
  const sections: TimelineSection[] = []
  const textMap = accumulateTextDeltas(textDeltas)
  let currentAgent: TimelineSection | null = null

  const ensureAgentSection = () => {
    if (currentAgent) return currentAgent
    currentAgent = { kind: "agent_response", items: [] }
    sections.push(currentAgent)
    return currentAgent
  }

  for (const row of rows) {
    switch (row.kind) {
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

// Build timeline query — projects each collection to TimelineRow, full joins on _seq
interface TimelineDBLike {
  collections: Record<string, unknown>
}

export interface TimelineQueries {
  rows: (q: any) => unknown
  textDeltas: (q: any) => unknown
}

export function createTimelineQuery(db: TimelineDBLike | null): TimelineQueries {
  const nullFields = {
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

      // Chain full joins
      const coalesceRow = (leftAlias: string, rightAlias: string) =>
        ({ [leftAlias]: left, [rightAlias]: right }: any) => ({
          seq: coalesce(left.seq, right.seq),
          kind: coalesce(left.kind, right.kind),
          key: coalesce(left.key, right.key),
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

      const withSteps = q
        .from({ left: runRows })
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
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { createTimelineQuery, buildTimeline } from './src/web/timeline.ts'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/web/timeline.ts
git commit -m "feat: add timeline query builder and buildTimeline for agent events"
```

---

## Task 9: useAgentDB hook

**Files:**
- Create: `src/web/use-agent-db.ts`

Read `~/programs/darix/examples/webhook-agents-ui/src/lib/use-task-db.ts` for the reference pattern.

- [ ] **Step 1: Create the hook**

```typescript
// src/web/use-agent-db.ts
import { useEffect, useState } from "react"
import { createStreamDB } from "@durable-streams/state"
import { schema } from "../lib/schema"

const STREAM_SERVER_URL = "http://localhost:4437"

export function useAgentDB(runId: string | null) {
  const [db, setDb] = useState<ReturnType<typeof createStreamDB> | null>(null)

  useEffect(() => {
    if (!runId) return

    const streamDb = createStreamDB({
      streamOptions: {
        url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
        contentType: "application/json",
      },
      state: schema,
    })

    void streamDb.preload().then(() => {
      setDb(streamDb)
    })

    return () => {
      streamDb.close()
      setDb(null)
    }
  }, [runId])

  return db
}
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import { useAgentDB } from './src/web/use-agent-db.ts'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/web/use-agent-db.ts
git commit -m "feat: add useAgentDB hook for StreamDB lifecycle"
```

---

## Task 10: AgentChat React component

**Files:**
- Create: `src/web/AgentChat.tsx`

Read `~/programs/darix/examples/webhook-agents-ui/src/components/TaskCard.tsx` for the rendering reference. Our version is simpler — one run at a time, no follow-up input, minimal styling.

- [ ] **Step 1: Create the AgentChat component**

```tsx
// src/web/AgentChat.tsx
import { useState, useMemo, useRef, useEffect } from "react"
import { useLiveQuery } from "@tanstack/react-db"
import { useAgentDB } from "./use-agent-db"
import {
  createTimelineQuery,
  buildTimeline,
  type TimelineRow,
  type TimelineSection,
  type TimelineContentItem,
  type TextDelta,
} from "./timeline"

export default function AgentChat() {
  const [runId, setRunId] = useState<string | null>(null)
  const [input, setInput] = useState("")
  const [submitting, setSubmitting] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)

  const db = useAgentDB(runId)
  const timelineQuery = useMemo(() => createTimelineQuery(db), [db])

  const { data: rows = [] } = useLiveQuery(timelineQuery.rows as any, [timelineQuery])
  const { data: textDeltas = [] } = useLiveQuery(
    timelineQuery.textDeltas as any,
    [timelineQuery]
  )

  const timeline = useMemo(
    () => buildTimeline(rows as TimelineRow[], textDeltas as (TextDelta & { _seq?: number })[]),
    [rows, textDeltas]
  )

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [timeline])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || submitting) return
    setSubmitting(true)
    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt: input.trim() }),
      })
      const { runId: newRunId } = await res.json()
      setRunId(newRunId)
      setInput("")
    } finally {
      setSubmitting(false)
    }
  }

  const isWorking = timeline.some(
    (s) => s.kind === "agent_response" && !s.done && !s.error
  )

  return (
    <>
      <div className="messages">
        {runId && !db && <div className="message system">Connecting to stream...</div>}

        {timeline.map((section, i) => (
          <AgentResponseView key={i} section={section} isLast={i === timeline.length - 1} working={isWorking} />
        ))}

        <div ref={bottomRef} />
      </div>

      <div className="input-area">
        <form onSubmit={handleSubmit}>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Send a prompt to the background agent…"
            disabled={submitting || isWorking}
          />
          <button type="submit" disabled={submitting || isWorking || !input.trim()}>
            Run
          </button>
        </form>
      </div>
    </>
  )
}

function AgentResponseView({
  section,
  isLast,
  working,
}: {
  section: TimelineSection
  isLast: boolean
  working: boolean
}) {
  return (
    <div className="message assistant">
      {section.items.map((item, i) => {
        if (item.kind === "text") {
          return (
            <div key={i} className="text-content">
              {item.text}
            </div>
          )
        }
        return <ToolCallCard key={item.toolCallId} toolCall={item} />
      })}

      {isLast && working && !section.done && (
        <div className="status working">Working...</div>
      )}

      {section.done && (
        <div className="status done">Complete</div>
      )}

      {section.error && (
        <div className="status error">Error: {section.error}</div>
      )}
    </div>
  )
}

function ToolCallCard({
  toolCall,
}: {
  toolCall: Extract<TimelineContentItem, { kind: "tool_call" }>
}) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div
      className="tool-call"
      onClick={() => setExpanded(!expanded)}
      style={{ cursor: "pointer" }}
    >
      <div className="tool-call-header">
        <span className="tool-call-chevron">{expanded ? "▾" : "▸"}</span>
        <strong>{toolCall.toolName}</strong>
        {toolCall.status === "started" && <span className="tool-call-spinner">⏳</span>}
        {toolCall.status === "completed" && <span className="tool-call-check">✓</span>}
        {toolCall.isError && <span className="tool-call-error">✗</span>}
      </div>
      {expanded && (
        <div className="tool-call-details">
          <div className="tool-call-section">
            <div className="tool-call-label">Arguments:</div>
            <pre>{JSON.stringify(toolCall.args, null, 2)}</pre>
          </div>
          {toolCall.result != null && (
            <div className="tool-call-section">
              <div className="tool-call-label">Result:</div>
              <pre style={{ maxHeight: 200, overflow: "auto" }}>{toolCall.result}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
```

- [ ] **Step 2: Verify it compiles**

Run: `bun run --bun -e "import AgentChat from './src/web/AgentChat.tsx'; console.log('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/web/AgentChat.tsx
git commit -m "feat: add AgentChat component with StreamDB timeline rendering"
```

---

## Task 11: Wire up AgentChat in the frontend

**Files:**
- Modify: `src/web/index.tsx`
- Modify: `src/web/index.html`

- [ ] **Step 1: Update index.tsx to render AgentChat**

Replace `src/web/index.tsx` with:

```tsx
import { createRoot } from "react-dom/client";
import AgentChat from "./AgentChat";

createRoot(document.getElementById("root")!).render(<AgentChat />);
```

- [ ] **Step 2: Update index.html title**

Replace the `<title>` in `src/web/index.html`:

Change: `<title>Chat</title>`
To: `<title>Durable Stream Agent</title>`

- [ ] **Step 3: Build and verify**

Run: `bun run build`
Expected: `Built to dist/`

- [ ] **Step 4: Commit**

```bash
git add src/web/index.tsx src/web/index.html
git commit -m "feat: wire up AgentChat as the default frontend"
```

---

## Task 12: End-to-end smoke test

This task verifies the full flow works. You need Redis running locally.

- [ ] **Step 1: Start the API server (in one terminal)**

Run: `bun run dev`

This should:
- Build the frontend
- Start the durable stream dev server on port 4437
- Start the Hono API server on port 4000

- [ ] **Step 2: Start the worker (in another terminal)**

Run: `bun run worker`

This should print: `Worker started`

- [ ] **Step 3: Test the API endpoint**

Run: `curl -s -X POST http://localhost:4000/api/agent -H 'content-type: application/json' -d '{"prompt": "What is 2 + 2?"}' | jq .`

Expected: `{ "runId": "<uuid>" }`

- [ ] **Step 4: Verify stream was created**

Using the runId from step 3:
Run: `curl -s http://localhost:4437/v1/stream/<runId>?offset=-1 | head -20`

Expected: JSON events (run started, step started, text deltas, etc.)

- [ ] **Step 5: Open the UI**

Open `http://localhost:4000` in a browser. Type a prompt and click "Run". Verify:
- The agent runs in the background
- Text appears as it streams
- Tool calls show with expandable args/result
- Run completes with "Complete" status

- [ ] **Step 6: Commit (if any fixes were needed)**

```bash
git add -A
git commit -m "fix: smoke test fixes for end-to-end flow"
```
