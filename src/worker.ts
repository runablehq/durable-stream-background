import { Worker } from "bullmq";
import { connection } from "./lib/redis";

const worker = new Worker(
  "jobs",
  async (job) => {
    console.log(`Processing job ${job.id}:`, job.name, job.data);

    // Add job handlers here
    switch (job.name) {
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
