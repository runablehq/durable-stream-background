import { ToolLoopAgent, tool, stepCountIs } from "ai";
import { z } from "zod";

export const agent = new ToolLoopAgent({
  model: "anthropic/claude-haiku-4.5" ,
  instructions:
    "You are a helpful assistant that can execute JavaScript code. Use the runJS tool when the user asks you to compute something or run code.",
  tools: {
    execute_javascript: tool({
      description: "Execute JavaScript code and return the result",
      inputSchema: z.object({
        code: z.string().describe("JavaScript code to execute"),
      }),
      execute: async ({ code }) => {
        try {
          const result = await eval(`(async () => { ${code} })()`);
          return { ok: true, result: String(result ?? "undefined") };
        } catch (e) {
          return { ok: false, error: String(e) };
        }
      },
    }),
  },
  stopWhen: stepCountIs(5),
});
