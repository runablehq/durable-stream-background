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
          await runAgentToStream({ prompt, runId });
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
