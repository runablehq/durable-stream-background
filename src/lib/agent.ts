import { ToolLoopAgent, tool, stepCountIs } from "ai";
import { anthropic } from "@ai-sdk/anthropic";
import { z } from "zod";

export const agent = new ToolLoopAgent({
  model: anthropic("claude-haiku-4-5-20251001"),
  instructions:
    "You are a helpful assistant that can execute JavaScript code. Use the runJS tool when the user asks you to compute something or run code.",
  tools: {
    execute_javascript: tool({
      description:
        "Execute JavaScript code and return the result. For expressions like '4 * 5', just provide the expression. For multi-statement code, use 'return' to return the final value, e.g. 'const x = 4 * 5; return x;'",
      inputSchema: z.object({
        code: z.string().describe("JavaScript code to execute. Use 'return' for multi-statement code."),
      }),
      execute: async ({ code }) => {
        try {
          // Try as expression first (e.g. "4 * 5"), fall back to statements
          let result;
          try {
            result = await eval(`(async () => (${code}))()`);
          } catch {
            result = await eval(`(async () => { ${code} })()`);
          }
          return { ok: true, result: String(result ?? "undefined") };
        } catch (e) {
          return { ok: false, error: String(e) };
        }
      },
    }),
  },
  stopWhen: stepCountIs(5),
});
