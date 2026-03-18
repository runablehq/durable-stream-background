import { useState, useMemo, useRef, useEffect } from "react"
import { useLiveQuery } from "@tanstack/react-db"
import { useAgentDB } from "./use-agent-db"
import {
  buildTimeline,
  type TimelineRow,
  type TimelineSection,
  type TimelineContentItem,
  type TextDelta,
} from "./timeline"

interface RunEntry {
  id: string
  prompt: string
  createdAt: number
}

export default function AgentChat() {
  const [runs, setRuns] = useState<RunEntry[]>([])
  const [activeRunId, setActiveRunId] = useState<string | null>(null)
  const [input, setInput] = useState("")
  const [submitting, setSubmitting] = useState(false)

  const handleNewRun = async (prompt: string) => {
    setSubmitting(true)
    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt }),
      })
      const { runId } = await res.json()
      const entry: RunEntry = { id: runId, prompt, createdAt: Date.now() }
      setRuns((prev) => [entry, ...prev])
      setActiveRunId(runId)
      setInput("")
    } finally {
      setSubmitting(false)
    }
  }

  const handleFollowUp = async (text: string) => {
    if (!activeRunId) return
    setSubmitting(true)
    try {
      await fetch(`/api/agent/${activeRunId}/message`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text }),
      })
      setInput("")
    } finally {
      setSubmitting(false)
    }
  }

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!input.trim() || submitting) return
    const text = input.trim()

    if (activeRunId) {
      await handleFollowUp(text)
    } else {
      await handleNewRun(text)
    }
  }

  const handleNewConversation = () => {
    setActiveRunId(null)
    setInput("")
  }

  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="sidebar-header">
          <span>Runs</span>
          <button className="sidebar-new-btn" onClick={handleNewConversation} title="New conversation">+</button>
        </div>
        <div className="sidebar-runs">
          {runs.length === 0 && (
            <div className="sidebar-empty">No runs yet</div>
          )}
          {runs.map((run) => (
            <button
              key={run.id}
              className={`sidebar-run ${run.id === activeRunId ? "active" : ""}`}
              onClick={() => setActiveRunId(run.id)}
            >
              <div className="sidebar-run-prompt">{run.prompt}</div>
              <div className="sidebar-run-time">
                {new Date(run.createdAt).toLocaleTimeString()}
              </div>
            </button>
          ))}
        </div>
      </aside>

      <main className="main-panel">
        {activeRunId ? (
          <RunView runId={activeRunId} />
        ) : (
          <div className="empty-state">
            Submit a prompt to start an agent run
          </div>
        )}

        <div className="input-area">
          <form onSubmit={handleSubmit}>
            <input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={activeRunId ? "Send a follow-up message…" : "Start a new agent run…"}
              disabled={submitting}
            />
            <button type="submit" disabled={submitting || !input.trim()}>
              {activeRunId ? "Send" : "Run"}
            </button>
          </form>
        </div>
      </main>
    </div>
  )
}

function RunView({ runId }: { runId: string }) {
  const bottomRef = useRef<HTMLDivElement>(null)
  const db = useAgentDB(runId)

  // Pass collections directly to useLiveQuery — uses subscribeChanges for reactivity
  const { data: inboxData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.inbox : null, [db]
  )
  const { data: runsData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.runs : null, [db]
  )
  const { data: textsData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.texts : null, [db]
  )
  const { data: textDeltasData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.textDeltas : null, [db]
  )
  const { data: toolCallsData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.toolCalls : null, [db]
  )
  const { data: errorsData = [] } = useLiveQuery(
    (_q: any) => db ? db.collections.errors : null, [db]
  )

  // Merge collections into timeline rows sorted by _seq, then build timeline
  const timeline = useMemo(() => {
    const rows: TimelineRow[] = []

    for (const m of inboxData as any[]) {
      rows.push({
        seq: m._seq ?? 0, kind: "message", key: m.key,
        messageFrom: m.from, messageText: m.text, messageTimestamp: m.timestamp,
        runStatus: null, stepStatus: null, stepDurationMs: null,
        textKey: null, toolCallId: null, toolName: null, toolArgs: null,
        toolResult: null, toolStatus: null, errorMessage: null,
      })
    }
    for (const r of runsData as any[]) {
      rows.push({
        seq: r._seq ?? 0, kind: "run", key: r.key,
        messageFrom: null, messageText: null, messageTimestamp: null,
        runStatus: r.status, stepStatus: null, stepDurationMs: null,
        textKey: null, toolCallId: null, toolName: null, toolArgs: null,
        toolResult: null, toolStatus: null, errorMessage: null,
      })
    }
    for (const t of textsData as any[]) {
      rows.push({
        seq: t._seq ?? 0, kind: "text", key: t.key,
        messageFrom: null, messageText: null, messageTimestamp: null,
        runStatus: null, stepStatus: null, stepDurationMs: null,
        textKey: t.key, toolCallId: null, toolName: null, toolArgs: null,
        toolResult: null, toolStatus: null, errorMessage: null,
      })
    }
    for (const tc of toolCallsData as any[]) {
      rows.push({
        seq: tc._seq ?? 0, kind: "tool_call", key: tc.key,
        messageFrom: null, messageText: null, messageTimestamp: null,
        runStatus: null, stepStatus: null, stepDurationMs: null,
        textKey: null, toolCallId: tc.key, toolName: tc.tool_name,
        toolArgs: tc.args, toolResult: tc.result, toolStatus: tc.status,
        errorMessage: null,
      })
    }
    for (const e of errorsData as any[]) {
      rows.push({
        seq: e._seq ?? 0, kind: "error", key: e.key,
        messageFrom: null, messageText: null, messageTimestamp: null,
        runStatus: null, stepStatus: null, stepDurationMs: null,
        textKey: null, toolCallId: null, toolName: null, toolArgs: null,
        toolResult: null, toolStatus: null, errorMessage: e.message,
      })
    }

    rows.sort((a, b) => a.seq - b.seq)

    return buildTimeline(rows, textDeltasData as (TextDelta & { _seq?: number })[])
  }, [inboxData, runsData, textsData, textDeltasData, toolCallsData, errorsData])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [timeline])

  const isWorking = timeline.some(
    (s) => s.kind === "agent_response" && !s.done && !s.error
  )

  return (
    <div className="messages">
      {!db && <div className="message system">Connecting to stream...</div>}

      {timeline.map((section, i) => {
        if (section.kind === "user_message") {
          return <UserMessageView key={i} section={section} />
        }
        return (
          <AgentResponseView
            key={i}
            section={section}
            isLast={i === timeline.length - 1}
            working={isWorking}
          />
        )
      })}

      <div ref={bottomRef} />
    </div>
  )
}

function UserMessageView({
  section,
}: {
  section: Extract<TimelineSection, { kind: "user_message" }>
}) {
  return (
    <div className="message user">
      {section.text}
    </div>
  )
}

function AgentResponseView({
  section,
  isLast,
  working,
}: {
  section: Extract<TimelineSection, { kind: "agent_response" }>
  isLast: boolean
  working: boolean
}) {
  return (
    <div className="message assistant">
      {section.items.map((item, i) => {
        if (item.kind === "text") {
          return (
            <div key={i} className="text-content">
              {item.text}
            </div>
          )
        }
        return <ToolCallCard key={item.toolCallId} toolCall={item} />
      })}

      {isLast && working && !section.done && (
        <div className="status working">Working...</div>
      )}

      {section.done && (
        <div className="status done">Complete</div>
      )}

      {section.error && (
        <div className="status error">Error: {section.error}</div>
      )}
    </div>
  )
}

function ToolCallCard({
  toolCall,
}: {
  toolCall: Extract<TimelineContentItem, { kind: "tool_call" }>
}) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div
      className="tool-call"
      onClick={() => setExpanded(!expanded)}
    >
      <div className="tool-call-header">
        <span className="tool-call-chevron">{expanded ? "▾" : "▸"}</span>
        <strong>{toolCall.toolName}</strong>
        {toolCall.status === "started" && <span className="tool-call-spinner">⏳</span>}
        {toolCall.status === "completed" && <span className="tool-call-check">✓</span>}
        {toolCall.isError && <span className="tool-call-error">✗</span>}
        {!expanded && <span style={{ color: "#666", fontSize: "0.75rem", marginLeft: "auto" }}>click to expand</span>}
      </div>
      {expanded && (
        <div className="tool-call-details">
          <div className="tool-call-section">
            <div className="tool-call-label">Arguments:</div>
            <pre>{JSON.stringify(toolCall.args, null, 2)}</pre>
          </div>
          {toolCall.result != null && (
            <div className="tool-call-section">
              <div className="tool-call-label">Result:</div>
              <pre style={{ maxHeight: 200, overflow: "auto" }}>{toolCall.result}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
