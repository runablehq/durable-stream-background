import type IORedis from "ioredis";

const TTL = 86400; // 24h

function key(id: string) {
  return `stream:${id}`;
}
function stateKey(id: string) {
  return `stream:${id}:state`;
}

export interface RedisStreamWriter {
  write(data: string): Promise<void>;
  close(): Promise<void>;
  error(message: string): Promise<void>;
}

export interface RedisStream {
  writer(streamId: string): RedisStreamWriter;
  consumer(
    streamId: string,
    options?: { signal?: AbortSignal }
  ): Promise<ReadableStream<string>>;
}

export function createRedisStream(options: {
  publisher: IORedis;
  subscriber: IORedis;
}) {
  const { publisher, subscriber } = options;

  // Multiple handlers per channel for concurrent consumers on the same streamId
  const listeners = new Map<string, Set<(msg: string) => void>>();

  subscriber.on("message", (channel: string, msg: string) => {
    const set = listeners.get(channel);
    if (!set) return;
    for (const fn of set) fn(msg);
  });

  function addListener(ch: string, fn: (msg: string) => void) {
    let set = listeners.get(ch);
    if (!set) {
      set = new Set();
      listeners.set(ch, set);
      subscriber.subscribe(ch);
    }
    set.add(fn);
  }

  function removeListener(ch: string, fn: (msg: string) => void) {
    const set = listeners.get(ch);
    if (!set) return;
    set.delete(fn);
    if (set.size > 0) return;
    listeners.delete(ch);
    subscriber.unsubscribe(ch);
  }

  function writer(streamId: string) {
    const k = key(streamId);
    const ch = k;

    return {
      async write(data: string) {
        const p = publisher.pipeline();
        p.xadd(k, "*", "d", data);
        p.publish(ch, "c");
        await p.exec();
      },

      async close() {
        const p = publisher.pipeline();
        p.set(stateKey(streamId), "done", "EX", TTL);
        p.expire(k, TTL);
        p.publish(ch, "d");
        await p.exec();
      },

      async error(message: string) {
        await this.write(
          JSON.stringify({ type: "error", errorText: message })
        );
        await this.close();
      },
    };
  }

  async function consumer(
    streamId: string,
    opts?: { signal?: AbortSignal }
  ) {
    const k = key(streamId);
    const ch = k;
    const group = `g-${crypto.randomUUID()}`;
    const con = "c";
    let closed = false;
    let handler: ((msg: string) => void) | undefined;
    let onAbort: (() => void) | undefined;

    try {
      await publisher.xgroup("CREATE", k, group, "0", "MKSTREAM");
    } catch (e: any) {
      if (!e.message?.includes("BUSYGROUP")) throw e;
    }

    const cleanup = async () => {
      if (closed) return;
      closed = true;
      if (handler) removeListener(ch, handler);
      if (onAbort && opts?.signal) {
        opts.signal.removeEventListener("abort", onAbort);
      }
      try {
        await publisher.xgroup("DESTROY", k, group);
      } catch {}
    };

    return new ReadableStream<string>({
      async start(controller) {
        // Serialize drain calls via promise chain to prevent interleaving
        let pending = Promise.resolve();

        const drain = () => {
          pending = pending.then(async () => {
            if (closed) return;
            while (!closed) {
              const results = (await publisher.xreadgroup(
                "GROUP",
                group,
                con,
                "COUNT",
                "100",
                "STREAMS",
                k,
                ">"
              )) as [string, [string, string[]][]][] | null;

              if (!results) break;
              let count = 0;
              for (const [, messages] of results) {
                for (const [, fields] of messages) {
                  if (fields[1] != null) {
                    try {
                      controller.enqueue(fields[1]);
                    } catch {}
                    count++;
                  }
                }
              }
              if (count < 100) break; // caught up
            }
          });
          return pending;
        };

        // 1. Register handler + subscribe FIRST so we never miss a notification
        handler = (msg) => {
          if (closed) return;
          drain()
            .then(() => {
              if (msg !== "d" || closed) return;
              try {
                controller.close();
              } catch {}
              cleanup();
            })
            .catch(() => {});
        };
        addListener(ch, handler);

        // 2. Backfill everything written before subscribe
        await drain();

        // 3. If already done, close after backfill
        const state = await publisher.get(stateKey(streamId));
        if (state === "done") {
          await drain();
          try {
            controller.close();
          } catch {}
          await cleanup();
          return;
        }

        // 4. Handle client disconnect
        onAbort = () => {
          try {
            controller.close();
          } catch {}
          cleanup();
        };
        opts?.signal?.addEventListener("abort", onAbort);
      },

      cancel() {
        return cleanup();
      },
    });
  }

  return { writer, consumer };
}
