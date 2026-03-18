# Durable Stream Agent — Design Spec

## Overview

Wire a Vercel AI SDK `ToolLoopAgent` to run as a BullMQ background job, capturing all agent events into a durable state stream via an OutboundBridge adapter. A React client subscribes to the stream via `StreamDB` and renders a live timeline using full-join queries across typed collections.

This demo targets the exact architecture Runable needs: agent execution decoupled from the HTTP request, events persisted to a durable stream (not an in-memory buffer), and any number of clients tailing the stream with full catch-up + live updates.

## Architecture

```
┌─────────┐   POST /api/agent   ┌──────────┐   BullMQ job   ┌─────────────┐
│  Client  │ ──────────────────► │  Hono    │ ─────────────► │   Worker    │
│  (React) │   ◄── { runId }     │  API     │                │  (agent.ts) │
└────┬─────┘                     └──────────┘                └──────┬──────┘
     │                                                              │
     │  StreamDB(url)                              OutboundBridge   │
     │  preload() + live tail                      + IdempotentProducer
     │                                                              │
     │         ┌────────────────────────────────────┐               │
     └────────►│     Durable Stream Server          │◄──────────────┘
               │  /v1/stream/{runId}                │
               │  (DurableStreamTestServer for dev) │
               └────────────────────────────────────┘
```

## State Schema

Multi-collection schema following the darix entity stream pattern. Each entity type gets its own collection with insert/update lifecycle semantics. All collections use `key` as primary key and passthrough validators (shape enforced by write side).

### Sequence Numbering

The installed `@durable-streams/state@0.2.2` does not inject a `_seq` field on collection rows (the darix fork does). To enable cross-collection chronological ordering, the OutboundBridge stamps a monotonically increasing `_seq` on every event value it writes. This is a write-side concern — the bridge maintains a single counter and includes `_seq` in every `event.value` object. The timeline query then sorts and joins on this field.

### Entity Types

All entity types include `_seq: number` for cross-collection ordering.

```typescript
/** Agent run lifecycle */
interface Run {
  key: string                           // "run-0", "run-1", ...
  _seq: number                          // global insertion order
  status: "started" | "completed"
  finish_reason?: string
}

/** LLM call step lifecycle */
interface Step {
  key: string                           // "step-0", "step-1", ...
  _seq: number
  step_number: number
  status: "started" | "completed"
  finish_reason?: string
  duration_ms?: number
}

/** Text message lifecycle */
interface Text {
  key: string                           // AI SDK part.id
  _seq: number
  status: "streaming" | "completed"
}

/** Incremental text content delta */
interface TextDelta {
  key: string                           // "{text_id}:{seq}" e.g. "abc123:0"
  _seq: number
  text_id: string                       // back-reference to Text key (AI SDK part.id)
  delta: string                         // the token
}

/** Tool call lifecycle */
interface ToolCall {
  key: string                           // AI SDK part.toolCallId
  _seq: number
  tool_name: string
  status: "started" | "completed" | "failed"
  args?: unknown
  result?: unknown
}

/** Diagnostic error */
interface ErrorEvent {
  key: string
  _seq: number
  message: string
}
```

### Collection Definitions

```typescript
import { createStateSchema } from "@durable-streams/state"

const schema = createStateSchema({
  runs:      { schema: passthrough<Run>(),        type: "run",        primaryKey: "key" },
  steps:     { schema: passthrough<Step>(),       type: "step",       primaryKey: "key" },
  texts:     { schema: passthrough<Text>(),       type: "text",       primaryKey: "key" },
  textDeltas:{ schema: passthrough<TextDelta>(),  type: "text_delta", primaryKey: "key" },
  toolCalls: { schema: passthrough<ToolCall>(),   type: "tool_call",  primaryKey: "key" },
  errors:    { schema: passthrough<ErrorEvent>(), type: "error",      primaryKey: "key" },
})
```

Passthrough validator (same as darix):
```typescript
function passthrough<T>(): StandardSchemaV1<T> {
  return {
    "~standard": {
      version: 1 as const,
      vendor: "durable-stream-demo",
      validate: (value: unknown) => ({ value: value as T }),
    },
  }
}
```

## OutboundBridge

Stateful adapter that maps AI SDK agent lifecycle events to typed `ChangeEvent` writes. Tracks auto-incrementing counters for `run-N` and `step-N` keys, uses AI SDK stable IDs for text and tool call keys, maintains per-text delta sequence numbers, and stamps a global `_seq` counter on every event value. Same pattern as `darix/packages/ts-darix-runtime/src/outbound-bridge.ts`.

**Retry safety:** Each agent run writes to a unique stream (one `runId` = one stream). BullMQ jobs are configured with `attempts: 1` so a failed job is never retried on the same stream. If retry-on-same-stream is needed later, the bridge must scan existing events to reconstruct counters (as darix does via `scanCounters`), but that is out of scope for this demo.

### Interface

```typescript
interface OutboundBridge {
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
```

### Event Flow

```
agent.stream()
  ├── "start"          → bridge.onRunStart()                    → insert run{status: started}
  ├── "start-step"     → bridge.onStepStart()                   → insert step{status: started}
  ├── "text-start"     → bridge.onTextStart(id)                 → insert text{status: streaming}
  ├── "text-delta"     → bridge.onTextDelta(id, text)            → insert text_delta{text_id, delta}
  ├── "text-end"       → bridge.onTextEnd(id)                   → update text{status: completed}
  ├── "tool-call"      → bridge.onToolCallStart(tcId, name, args) → insert tool_call{status: started}
  ├── "tool-result"    → bridge.onToolCallEnd(tcId, name, out)  → update tool_call{status: completed}
  ├── "finish-step"    → bridge.onStepEnd()                     → update step{status: completed}
  └── "finish"         → bridge.onRunEnd()                      → update run{status: completed}
```

The bridge uses the AI SDK's stable identifiers (`part.id` for text, `part.toolCallId` for tools) as entity keys instead of auto-incrementing counters, making it resilient to parallel tool calls and multi-text responses within a single step.

Each `ChangeEvent` has `headers.operation: "insert" | "update"` to distinguish creation from mutation. StreamDB's dispatcher routes these to the correct collection based on the `type` field.

### Writing Events

The bridge's `writeEvent` callback serializes each `ChangeEvent` as JSON and passes it to an `IdempotentProducer` for batched, exactly-once delivery:

```typescript
// Stream already created by the API endpoint — connect to it
const handle = await DurableStream.connect({
  url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
  contentType: "application/json",
})

const producer = new IdempotentProducer(handle, "agent-worker", {
  autoClaim: true,
  onError: (err) => console.error("Write error:", err),
})

const bridge = createOutboundBridge([], (event) => {
  producer.append(JSON.stringify(event))
})
```

## AI SDK Integration

### Wiring `fullStream` to the Bridge

The agent's `stream()` method returns an async iterable `fullStream`. We iterate it and dispatch each part to the bridge:

```typescript
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
```

**Error handling:** `runAgentToStream` wraps the entire stream iteration in a try/catch. On error, it writes an error entity via `bridge.onError()`, writes a `run_done` with `finish_reason: "error"` via `bridge.onRunEnd()`, flushes the producer, and closes the stream before re-throwing. This ensures clients always see a terminal state even if the agent crashes.

**Abort handling:** The `abort` stream part (if the agent run is cancelled via `AbortSignal`) is treated as an error — `bridge.onError("aborted")` followed by `bridge.onRunEnd({ finishReason: "abort" })`.

**Field name mapping note:** The AI SDK uses `part.text` for text deltas and `part.output` for tool results. The schema stores these as `TextDelta.delta` and `ToolCall.result` respectively. The `args` and `result` fields on `ToolCall` are stored as their original types (objects); the bridge stringifies non-string results (matching darix's pattern).

## BullMQ Worker

The existing worker (`src/worker.ts`) gets an `"agent-run"` job handler:

```typescript
case "agent-run": {
  const { prompt, runId } = job.data
  try {
    await runAgentToStream(prompt, runId)
  } catch (err) {
    // runAgentToStream handles writing error events + closing stream internally.
    // Re-throw so BullMQ marks the job as failed.
    throw err
  }
  break
}
```

The API endpoint creates the durable stream first (so clients can immediately connect), then enqueues the job:

```typescript
app.post("/api/agent", async (c) => {
  const { prompt } = await c.req.json()
  const runId = crypto.randomUUID()

  // Create the stream BEFORE returning runId to the client.
  // This eliminates the race where the client calls preload()
  // before the worker has created the stream.
  await DurableStream.create({
    url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
    contentType: "application/json",
  })

  await jobQueue.add("agent-run", { prompt, runId })
  return c.json({ runId })
})
```

## Durable Stream Dev Server

`DurableStreamTestServer` from `@durable-streams/server` runs on port 4437 alongside the Hono app. Started in `api.ts`:

```typescript
import { DurableStreamTestServer } from "@durable-streams/server"

const streamServer = new DurableStreamTestServer({ port: 4437 })
await streamServer.start()
```

The API endpoint creates streams at `http://127.0.0.1:4437/v1/stream/{runId}` before returning the `runId`. The worker connects to the already-created stream via `DurableStream.connect()`.

## Client

### StreamDB Hook

```typescript
function useAgentDB(runId: string | null) {
  const [db, setDb] = useState<StreamDB | null>(null)

  useEffect(() => {
    if (!runId) return
    const streamDb = createStreamDB({
      streamOptions: {
        url: `http://localhost:4437/v1/stream/${runId}`,
        contentType: "application/json",
      },
      state: schema,
    })
    streamDb.preload().then(() => setDb(streamDb))
    return () => streamDb.close()
  }, [runId])

  return db
}
```

### Timeline Query

Following the darix `createEntityTimelineQuery` pattern — query each collection with a `select` that projects to a common `TimelineRow` shape, full join all on `_seq`, order ascending.

```typescript
interface TimelineRow {
  seq: number
  kind: "run" | "step" | "text" | "tool_call" | "error"
  key: string
  runStatus: string | null
  textKey: string | null
  toolCallId: string | null
  toolName: string | null
  toolArgs: unknown | null
  toolResult: unknown | null
  toolStatus: string | null
  errorMessage: string | null
}
```

Each collection is queried and projected:

```typescript
const runRows = q
  .from({ runs: db.collections.runs })
  .select(({ runs }) => ({
    seq: runs._seq,
    kind: "run",
    key: runs.key,
    runStatus: runs.status,
    // ... null for other fields
  }))

const textRows = q
  .from({ texts: db.collections.texts })
  .select(({ texts }) => ({
    seq: texts._seq,
    kind: "text",
    key: texts.key,
    textKey: texts.key,
    // ... null for other fields
  }))

// ... same for toolCalls, errors
```

Then chained full joins:

```typescript
const withTexts = q
  .from({ left: runRows })
  .fullJoin({ right: textRows }, ({ left, right }) =>
    eq(left.seq, right.seq)
  )
  .select(({ left, right }) => ({
    seq: coalesce(left.seq, right.seq),
    kind: coalesce(left.kind, right.kind),
    // ... coalesce all fields
  }))

// ... join toolCalls, errors

return q
  .from({ rows: final })
  .orderBy(({ rows }) => rows.seq, "asc")
```

### Timeline Builder

**Materialized state, not event log.** StreamDB materializes state: when a run is updated from `started` to `completed`, the collection holds ONE row with the latest value and `_seq`. After catch-up of a finished run, you see one run row (status=completed), one text row (status=completed), one tool_call row (status=completed) — not the full insert/update history. The timeline builder must handle this.

`buildTimeline(rows, textDeltas)` uses an `ensureAgentSection()` pattern (same as darix `buildEntityTimeline`): any row that needs an agent section creates one on demand if none exists. This makes it work for both live tailing (where you see `started` first) and catch-up (where you may only see `completed`).

Rules (rows ordered by `_seq` ascending):

- **`run`** → if status is `started`, start a new agent section. If status is `completed`, ensure a section exists and mark it done. After catch-up, a completed run creates + marks done in one step.
- **`text`** → `ensureAgentSection()`, look up accumulated deltas, add text item. The text's status (`streaming` vs `completed`) indicates whether more deltas may arrive.
- **`tool_call`** → `ensureAgentSection()`, add tool call item. Status tells the UI whether to show a spinner (`started`) or result (`completed`/`failed`).
- **`error`** → `ensureAgentSection()`, attach error message.
- **`step`** rows are not rendered directly but can be used for metadata (token counts, duration).

Text deltas are accumulated separately (they are insert-only, never updated, so they survive materialization intact):
```typescript
const { data: textDeltas } = useLiveQuery((q) =>
  q.from({ textDeltas: db.collections.textDeltas })
   .orderBy(({ textDeltas }) => textDeltas._seq, "asc")
)
```

Then grouped by `text_id` and concatenated in sequence order to produce full text strings.

### Rendering

The `AgentChat` component:
1. Shows an input form
2. On submit, POSTs to `/api/agent`, gets `runId`, creates StreamDB
3. Renders timeline sections:
   - Text items: rendered as message content
   - Tool calls: collapsible cards showing tool name, args (expandable JSON), status indicator (spinner while executing, checkmark/x when done), result (expandable)
   - Run status: "Working..." spinner while running, "Complete" when done
   - Errors: red badge with message

## Dependencies to Add

- `@durable-streams/server` — dev server
- `@tanstack/react-db` — `useLiveQuery` for reactive queries
- `@standard-schema/spec` — StandardSchemaV1 type (for passthrough validator)

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `src/lib/schema.ts` | New | State schema + entity types + passthrough validator (shared server/client) |
| `src/lib/outbound-bridge.ts` | New | OutboundBridge: maps agent lifecycle to ChangeEvent writes |
| `src/lib/run-agent.ts` | New | Wires AI SDK fullStream → OutboundBridge → IdempotentProducer |
| `src/lib/durable.ts` | New | DurableStreamTestServer setup + STREAM_SERVER_URL constant |
| `src/worker.ts` | Modify | Add `"agent-run"` job handler |
| `src/api.ts` | Modify | Add `POST /api/agent` endpoint, start durable stream dev server |
| `src/web/AgentChat.tsx` | New | Chat UI: input, StreamDB hook, timeline query, section rendering |
| `src/web/use-agent-db.ts` | New | `useAgentDB(runId)` hook — creates/manages StreamDB lifecycle |
| `src/web/timeline.ts` | New | Timeline query builder + `buildTimeline()` (adapted from darix entity-timeline) |
| `src/web/index.html` | Modify | Wire up AgentChat |
| `src/web/index.tsx` | Modify | Render AgentChat |

Existing files (`agent.ts`, `queue.ts`, `redis.ts`, `stream.ts`, `App.tsx`) remain untouched.

## Key Reference Files (darix)

- `packages/ts-darix-runtime/src/entity-schema.ts` — entity type definitions + builtInCollections
- `packages/ts-darix-runtime/src/outbound-bridge.ts` — OutboundBridge lifecycle adapter
- `packages/ts-darix-runtime/src/entity-timeline.ts` — timeline query + buildEntityTimeline
- `packages/ts-darix-runtime/src/entity-stream-db.ts` — createEntityStreamDB setup
- `examples/webhook-agents-ui/src/components/TaskCard.tsx` — React rendering of timeline
- `examples/webhook-agents-ui/src/lib/use-task-db.ts` — useTaskDB hook pattern
