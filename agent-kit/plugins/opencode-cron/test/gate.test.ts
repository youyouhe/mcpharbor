import { mkdtemp, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import * as path from "node:path"
import { describe, expect, it } from "vitest"
import { conditionGate, parseCondition } from "../src/index.js"

function sse(data: unknown, sessionId = "sid-1") {
  return new Response(`event: message\ndata: ${JSON.stringify(data)}\n\n`, {
    status: 200,
    headers: { "mcp-session-id": sessionId },
  })
}

function harborFetch(calls: { agentId: string; token: string; count: number }) {
  const seen: Array<{ method: string; headers: Record<string, string>; body: { method?: string } | undefined }> = []
  const impl = async (url: string, init?: RequestInit) => {
    const headers = Object.fromEntries(Object.entries(init?.headers ?? {})) as Record<string, string>
    const body = init?.body ? (JSON.parse(String(init.body)) as { method?: string }) : undefined
    seen.push({ method: String(init?.method ?? "GET"), headers, body })
    if (body?.method === "initialize") return sse({ ok: true })
    if (body?.method === "tools/call") {
      const params = (body as { params?: { name?: string; arguments?: unknown } }).params
      expect(params?.name).toBe("get_messages")
      expect(params?.arguments).toEqual({
        agent_id: calls.agentId,
        token: calls.token,
        unread_only: true,
        limit: 20,
      })
      return sse({
        result: { content: [{ type: "text", text: JSON.stringify({ messages: [], count: calls.count }) }] },
      })
    }
    return new Response(null, { status: 202 })
  }
  return { impl, seen }
}

async function tokenFile(token: string) {
  const dir = await mkdtemp(path.join(tmpdir(), "cron-gate-"))
  const file = path.join(dir, "token")
  await writeFile(file, `${token}\n`, "utf8")
  return file
}

describe("parseCondition", () => {
  it("splits the three segments", () => {
    expect(parseCondition("__TOKEN__ # agent-1 # http://x:8931/mcp")).toEqual({
      tokenPlaceholder: "__TOKEN__",
      agentId: "agent-1",
      endpoint: "http://x:8931/mcp",
    })
  })
  it("rejects malformed shapes", () => {
    expect(() => parseCondition("__TOKEN__ # agent-1")).toThrow()
    expect(() => parseCondition("__TOKEN__ # a # b # c")).toThrow()
    expect(() => parseCondition("__TOKEN__ #  # http://x")).toThrow()
  })
})

describe("conditionGate", () => {
  it("passes through when no condition is set", async () => {
    const result = await conditionGate({}, async () => "", async () => new Response(null))
    expect(result).toEqual({ go: true })
  })

  it("goes when the inbox has unread messages", async () => {
    const file = await tokenFile("secret-token")
    const { impl, seen } = harborFetch({ agentId: "order-agent", token: "secret-token", count: 3 })
    const result = await conditionGate(
      { condition: "__TOKEN__ # order-agent # http://127.0.0.1:8931/mcp", tokenFile: file },
      undefined,
      impl,
    )
    expect(result).toEqual({ go: true, detail: "unread=3" })
    const [initCall, , getCall] = seen
    expect(initCall?.body?.method).toBe("initialize")
    expect(getCall?.headers["mcp-session-id"]).toBe("sid-1")
  })

  it("skips when the inbox is empty (count=0)", async () => {
    const file = await tokenFile("secret-token")
    const { impl } = harborFetch({ agentId: "a", token: "secret-token", count: 0 })
    const result = await conditionGate({ condition: "__TOKEN__ # a # http://x/mcp", tokenFile: file }, undefined, impl)
    expect(result).toEqual({ go: false, detail: "unread=0" })
  })

  it("fails closed on transport errors", async () => {
    const file = await tokenFile("secret-token")
    const result = await conditionGate(
      { condition: "__TOKEN__ # a # http://x/mcp", tokenFile: file },
      undefined,
      async () => {
        throw new Error("connection refused")
      },
    )
    expect(result.go).toBe(false)
    expect(result.detail).toContain("connection refused")
  })

  it("fails closed on unreadable token files", async () => {
    const result = await conditionGate(
      { condition: "__TOKEN__ # a # http://x/mcp", tokenFile: "/nonexistent/token" },
      async () => {
        throw new Error("ENOENT")
      },
      async () => new Response(null),
    )
    expect(result.go).toBe(false)
    expect(result.detail).toContain("ENOENT")
  })

  it("fails closed on a malformed condition", async () => {
    const result = await conditionGate({ condition: "no-separators", tokenFile: "/x" }, undefined, async () => new Response(null))
    expect(result.go).toBe(false)
  })

  it("fails closed when the endpoint answers without a data line", async () => {
    const file = await tokenFile("secret-token")
    const result = await conditionGate(
      { condition: "__TOKEN__ # a # http://x/mcp", tokenFile: file },
      undefined,
      async () => new Response("plain json", { status: 200, headers: { "mcp-session-id": "sid" } }),
    )
    expect(result.go).toBe(false)
    expect(result.detail).toContain("no data line")
  })
})
