import { Hono } from "hono";
import { serveStatic } from "hono/bun";
import { createAgentUIStreamResponse } from "ai";
import { agent } from "./lib/agent";
import { jobQueue } from "./lib/queue";

const app = new Hono();

// Chat endpoint
app.post("/api/chat", async (c) => {
  const { messages } = await c.req.json();

  return createAgentUIStreamResponse({
    agent,
    uiMessages: messages,
  });
});

// Example: enqueue a job
app.post("/api/jobs", async (c) => {
  const body = await c.req.json();
  const job = await jobQueue.add(body.name ?? "default", body.data ?? {});
  return c.json({ id: job.id });
});

// Serve built frontend
app.use("/*", serveStatic({ root: "./dist" }));

export default {
  port: 4000,
  fetch: app.fetch,
};
