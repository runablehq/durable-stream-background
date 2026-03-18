import { Queue } from "bullmq";
import { connection } from "./redis";

export const jobQueue = new Queue("jobs", { connection });
