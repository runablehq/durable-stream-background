import { test, expect, describe, beforeAll, afterAll } from "bun:test"
import { DurableStreamTestServer } from "@durable-streams/server"
import { DurableStream, IdempotentProducer } from "@durable-streams/client"
import { createStreamDB } from "@durable-streams/state"
import { schema } from "./schema"
import { createOutboundBridge } from "./outbound-bridge"

let server: InstanceType<typeof DurableStreamTestServer>
let serverUrl: string

beforeAll(async () => {
  server = new DurableStreamTestServer({ port: 0 })
  await server.start()
  serverUrl = server.url!
})

afterAll(async () => {
  await server.stop()
})

async function createTestStream(name: string) {
  const url = `${serverUrl}/v1/stream/${name}`
  await DurableStream.create({ url, contentType: "application/json" })
  return url
}

// Wait until a condition is true or timeout
async function waitFor(fn: () => boolean, timeoutMs = 3000, intervalMs = 50) {
  const start = Date.now()
  while (!fn()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    }
    await new Promise((r) => setTimeout(r, intervalMs))
  }
}

describe("StreamDB incremental updates", () => {
  test("handle.append() triggers subscribeChanges for every write", async () => {
    const streamUrl = await createTestStream("append-test")

    const db = createStreamDB({
      streamOptions: { url: streamUrl, contentType: "application/json" },
      state: schema,
    })
    await db.preload()

    let changeCount = 0
    db.collections.textDeltas.subscribeChanges(() => { changeCount++ })

    const handle = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })

    // Write first delta
    await handle.append(JSON.stringify(
      schema.textDeltas.insert({ value: { key: "td-0", _seq: 0, text_id: "t-0", delta: "a" } })
    ))
    await waitFor(() => changeCount >= 1)
    expect(changeCount).toBe(1)

    // Write second delta
    await handle.append(JSON.stringify(
      schema.textDeltas.insert({ value: { key: "td-1", _seq: 1, text_id: "t-0", delta: "b" } })
    ))
    await waitFor(() => changeCount >= 2)
    expect(changeCount).toBe(2)

    // Write third delta after a gap
    await new Promise((r) => setTimeout(r, 200))
    await handle.append(JSON.stringify(
      schema.textDeltas.insert({ value: { key: "td-2", _seq: 2, text_id: "t-0", delta: "c" } })
    ))
    await waitFor(() => changeCount >= 3)
    expect(changeCount).toBe(3)

    expect(Array.from(db.collections.textDeltas.entries()).length).toBe(3)
    db.close()
  })

  test("IdempotentProducer triggers subscribeChanges for single run", async () => {
    const streamUrl = await createTestStream("producer-single-test")

    const db = createStreamDB({
      streamOptions: { url: streamUrl, contentType: "application/json" },
      state: schema,
    })
    await db.preload()

    let deltaCount = 0
    db.collections.textDeltas.subscribeChanges(() => { deltaCount++ })

    const handle = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })
    const producer = new IdempotentProducer(handle, "worker", { autoClaim: true })
    const bridge = createOutboundBridge((e) => producer.append(JSON.stringify(e)))

    bridge.onRunStart()
    bridge.onStepStart()
    bridge.onTextStart("0")
    bridge.onTextDelta("0", "hello ")
    bridge.onTextDelta("0", "world")
    await producer.flush()

    await waitFor(() => deltaCount >= 1)
    expect(Array.from(db.collections.textDeltas.entries()).length).toBe(2)

    bridge.onTextEnd("0")
    bridge.onStepEnd()
    bridge.onRunEnd()
    await producer.flush()

    await waitFor(() => Array.from(db.collections.runs.entries()).some(
      ([, v]: any) => v.status === "completed"
    ))

    db.close()
  })

  // Known bug: second IdempotentProducer's autoClaim epoch transition
  // breaks the consumer's live connection. Events never arrive at StreamDB.
  // Workaround: use handle.append() for subsequent runs.
  test.skip("two sequential IdempotentProducers both trigger updates", async () => {
    const streamUrl = await createTestStream("producer-sequential-test")

    const db = createStreamDB({
      streamOptions: { url: streamUrl, contentType: "application/json" },
      state: schema,
    })
    await db.preload()

    let deltaChangeCount = 0
    db.collections.textDeltas.subscribeChanges(() => { deltaChangeCount++ })

    // === First producer/run ===
    const handle1 = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })
    const producer1 = new IdempotentProducer(handle1, "worker", { autoClaim: true })
    const bridge1 = createOutboundBridge((e) => producer1.append(JSON.stringify(e)))

    bridge1.onRunStart()
    bridge1.onStepStart()
    bridge1.onTextStart("0")
    bridge1.onTextDelta("0", "first")
    bridge1.onTextEnd("0")
    bridge1.onStepEnd()
    bridge1.onRunEnd()
    await producer1.flush()

    // Wait for first run to fully arrive
    await waitFor(() => Array.from(db.collections.textDeltas.entries()).length >= 1)
    await waitFor(() => Array.from(db.collections.runs.entries()).some(
      ([, v]: any) => v.status === "completed"
    ))

    const deltasAfterFirst = Array.from(db.collections.textDeltas.entries()).length
    const changesAfterFirst = deltaChangeCount
    console.log("after first run:", { deltasAfterFirst, changesAfterFirst })

    // Let everything settle
    await new Promise((r) => setTimeout(r, 500))

    // === Second producer/run (new epoch via autoClaim) ===
    const handle2 = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })
    const producer2 = new IdempotentProducer(handle2, "worker", { autoClaim: true })
    const bridge2 = createOutboundBridge((e) => producer2.append(JSON.stringify(e)), {
      startSeq: 50, startRunCounter: 1, startStepCounter: 2,
    })

    bridge2.onRunStart()
    bridge2.onStepStart()
    bridge2.onTextStart("0")
    bridge2.onTextDelta("0", "second")
    bridge2.onTextEnd("0")
    bridge2.onStepEnd()
    bridge2.onRunEnd()
    await producer2.flush()

    // Wait for second run's delta to arrive
    try {
      await waitFor(
        () => Array.from(db.collections.textDeltas.entries()).length >= 2,
        3000
      )
    } catch {
      // If it times out, that's the bug
    }

    const deltasAfterSecond = Array.from(db.collections.textDeltas.entries()).length
    const changesAfterSecond = deltaChangeCount
    console.log("after second run:", { deltasAfterSecond, changesAfterSecond })

    // This is the critical assertion — does the second producer's data arrive?
    expect(deltasAfterSecond).toBe(2)
    expect(changesAfterSecond).toBeGreaterThan(changesAfterFirst)

    db.close()
  })

  test("handle.append works for second run (workaround)", async () => {
    const streamUrl = await createTestStream("append-workaround-test")

    const db = createStreamDB({
      streamOptions: { url: streamUrl, contentType: "application/json" },
      state: schema,
    })
    await db.preload()

    let deltaChangeCount = 0
    db.collections.textDeltas.subscribeChanges(() => { deltaChangeCount++ })

    // First run with producer
    const handle1 = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })
    const producer1 = new IdempotentProducer(handle1, "worker", { autoClaim: true })
    const bridge1 = createOutboundBridge((e) => producer1.append(JSON.stringify(e)))

    bridge1.onRunStart()
    bridge1.onStepStart()
    bridge1.onTextStart("0")
    bridge1.onTextDelta("0", "first")
    bridge1.onTextEnd("0")
    bridge1.onStepEnd()
    bridge1.onRunEnd()
    await producer1.flush()

    await waitFor(() => Array.from(db.collections.textDeltas.entries()).length >= 1)
    await new Promise((r) => setTimeout(r, 300))

    const changesAfterFirst = deltaChangeCount
    console.log("after first run (producer):", { changes: changesAfterFirst })

    // Second run with handle.append (no producer)
    const handle2 = await DurableStream.connect({ url: streamUrl, contentType: "application/json" })
    const bridge2 = createOutboundBridge(
      (e) => { handle2.append(JSON.stringify(e)) },
      { startSeq: 50, startRunCounter: 1, startStepCounter: 2 },
    )

    bridge2.onRunStart()
    bridge2.onStepStart()
    bridge2.onTextStart("0")
    bridge2.onTextDelta("0", "second")
    bridge2.onTextEnd("0")
    bridge2.onStepEnd()
    bridge2.onRunEnd()

    // handle.append is fire-and-forget, wait for delivery
    await waitFor(
      () => Array.from(db.collections.textDeltas.entries()).length >= 2,
      3000
    )

    const changesAfterSecond = deltaChangeCount
    console.log("after second run (append):", { changes: changesAfterSecond })

    expect(changesAfterSecond).toBeGreaterThan(changesAfterFirst)
    expect(Array.from(db.collections.textDeltas.entries()).length).toBe(2)

    db.close()
  })
})
