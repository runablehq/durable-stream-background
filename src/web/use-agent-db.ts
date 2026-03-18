import { useEffect, useState } from "react"
import { createStreamDB } from "@durable-streams/state"
import { schema } from "../lib/schema"

const STREAM_SERVER_URL = "http://localhost:4437"

export function useAgentDB(runId: string | null) {
  const [db, setDb] = useState<ReturnType<typeof createStreamDB<any>> | null>(null)

  useEffect(() => {
    if (!runId) return

    const streamDb = createStreamDB({
      streamOptions: {
        url: `${STREAM_SERVER_URL}/v1/stream/${runId}`,
        contentType: "application/json",
      },
      state: schema,
    })

    void streamDb.preload().then(() => {
      setDb(streamDb)
    })

    return () => {
      streamDb.close()
      setDb(null)
    }
  }, [runId])

  return db
}
