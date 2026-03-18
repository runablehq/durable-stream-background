import { DurableStreamTestServer } from "@durable-streams/server"

export const STREAM_SERVER_URL = "http://127.0.0.1:4437"

export async function startStreamServer() {
  const server = new DurableStreamTestServer({
    port: 4437,
    host: "127.0.0.1",
  })
  await server.start()
  console.log(`Durable stream dev server running on ${STREAM_SERVER_URL}`)
  return server
}
