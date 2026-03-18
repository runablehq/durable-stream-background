import { Hono } from "hono";
import { serveStatic } from "hono/bun";
import { createAgentUIStreamResponse } from "ai";
import { DurableStream, stream as readStream } from "@durable-streams/client";
import { agent } from "./lib/agent";
import { jobQueue } from "./lib/queue";
import { startStreamServer, STREAM_SERVER_URL } from "./lib/durable";
import { schema } from "./lib/schema";

const app = new Hono();

// Chat endpoint (existing)
app.post("/api/chat", async (c) => {
  const { messages } = await c.req.json();

  return createAgentUIStreamResponse({
    agent,
    uiMessages: messages,
  });
});

// Background agent endpoint — start new conversation
app.post("/api/agent", async (c) => {
  const { prompt } = await c.req.json();
  if (typeof prompt !== "string" || !prompt.trim()) {
    return c.json({ error: "prompt is required" }, 400);
  }
  const runId = crypto.randomUUID();

  await DurableStream.create({
    url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
    contentType: "application/json",
  });

  // Write the initial user message to the stream
  const handle = await DurableStream.connect({
    url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
    contentType: "application/json",
  });
  await handle.append(JSON.stringify(
    schema.inbox.insert({
      value: { key: "msg-0", _seq: 0, from: "user", text: prompt, timestamp: Date.now() },
    })
  ));

  await jobQueue.add("agent-run", { prompt, runId }, { attempts: 1 });
  return c.json({ runId });
});

// Follow-up message on existing conversation
app.post("/api/agent/:runId/message", async (c) => {
  const { runId } = c.req.param();
  const { text } = await c.req.json();
  if (typeof text !== "string" || !text.trim()) {
    return c.json({ error: "text is required" }, 400);
  }

  const streamUrl = `${STREAM_SERVER_URL}/v1/stream/${runId}`;

  // Read existing events to find next msg counter and max _seq
  const existing = await readStream({ url: streamUrl, offset: "-1", live: false });
  const events = await existing.json() as any[];

  let msgCounter = 0;
  let maxSeq = 0;
  for (const ev of events) {
    const val = ev.value as Record<string, unknown> | undefined;
    if (val && typeof val._seq === "number" && val._seq > maxSeq) {
      maxSeq = val._seq;
    }
    if (ev.type === "message" && ev.key) {
      const m = ev.key.match(/^msg-(\d+)/);
      if (m) msgCounter = Math.max(msgCounter, parseInt(m[1]!, 10) + 1);
    }
  }

  // Write the follow-up user message to the stream
  const handle = await DurableStream.connect({
    url: streamUrl,
    contentType: "application/json",
  });
  await handle.append(JSON.stringify(
    schema.inbox.insert({
      value: { key: `msg-${msgCounter}`, _seq: maxSeq + 1, from: "user", text, timestamp: Date.now() },
    })
  ));

  // Enqueue agent job — worker reads conversation from stream
  await jobQueue.add("agent-run", { prompt: text, runId }, { attempts: 1 });

  return c.json({ ok: true });
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
