import type { PluginInput } from "@opencode-ai/plugin"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { createCron, StoreSchema, type StoreIO } from "../src/index.js"

type Client = PluginInput["client"]
type Stored = ReturnType<typeof StoreSchema.parse>

const BASE = new Date(2026, 8, 11, 10, 0, 0) // Friday 2026-09-11 10:00 local

function response(data: unknown, status = 200) {
  return { data, response: new Response(null, { status }) }
}

function missing() {
  return { error: { name: "NotFound" }, response: new Response(null, { status: 404 }) }
}

function memoryIO(): StoreIO & { store: () => Stored | undefined } {
  let saved: Stored | undefined
  return {
    async load() {
      return saved
    },
    async save(store) {
      saved = store
    },
    now() {
      return new Date()
    },
    store() {
      return saved
    },
  }
}

function setup(options?: { prompt?: () => Promise<unknown>; promptAsync?: () => Promise<unknown> }) {
  const client = {
    session: {
      create: vi.fn(async () => response({ id: `child-${client.session.create.mock.calls.length}`, parentID: undefined })),
      prompt: vi.fn(options?.prompt ?? (async () => response({ parts: [{ type: "text", text: "done" }] }))),
      promptAsync: vi.fn(options?.promptAsync ?? (async () => response({}))),
      status: vi.fn(async () => response({})),
    },
    app: {
      agents: vi.fn(async () =>
        response([
          { name: "build", mode: "primary" },
          { name: "general", mode: "subagent" },
        ]),
      ),
    },
    config: {
      get: vi.fn(async () => response({ experimental: { primary_tools: ["apply_patch"] } })),
    },
    provider: {
      list: vi.fn(async () =>
        response({
          all: [
            {
              id: "openai",
              name: "OpenAI",
              models: { main: { id: "main", name: "Main", variants: { high: {} } } },
            },
            {
              id: "offline",
              name: "Offline",
              models: { model: { id: "model", name: "Model", variants: {} } },
            },
          ],
          default: { openai: "main" },
          connected: ["openai"],
        }),
      ),
    },
  }
  const io = memoryIO()
  return { client: client as unknown as Client, io }
}

type Tool = NonNullable<Awaited<ReturnType<typeof createCron>>["tool"]["cron"]>

async function call(tool: Tool, input: Record<string, unknown>) {
  const result = await tool.execute(input as never, {
    sessionID: "session",
    messageID: "message",
    agent: "build",
    directory: "/project",
    worktree: "/project",
    abort: new AbortController().signal,
    metadata: () => undefined,
    ask: async () => undefined,
  } as never)
  return JSON.parse(typeof result === "string" ? result : JSON.stringify(result))
}

async function advanceTo(date: Date) {
  await vi.advanceTimersByTimeAsync(date.getTime() - Date.now())
}

function taskTarget(input: Record<string, unknown>) {
  return { target: "task", ...input }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(BASE)
})

afterEach(() => {
  vi.useRealTimers()
})

describe("cron tool", () => {
  it("creates a task, persists it, and lists it with the next run time", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    const result = await call(tool.cron, {
      action: "create",
      name: "nightly",
      schedule: "0 9 * * *",
      prompt: "Check the overnight failures",
    })
    expect(result.created.enabled).toBe(true)
    expect(result.created.target).toBe("session")
    expect(result.created.sessionID).toBe("session")
    expect(result.created.nextRun).toBe(new Date(2026, 8, 12, 9, 0).toISOString())
    expect(io.store()?.jobs).toHaveLength(1)

    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs).toHaveLength(1)
    expect(list.jobs[0].name).toBe("nightly")
    await dispose()
  })

  it("rejects duplicate names, invalid schedules, unknown agents, and bad models", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    const base = { action: "create", name: "job", schedule: "* * * * *", prompt: "hi" }
    await call(tool.cron, base)
    await expect(call(tool.cron, base)).rejects.toThrow("already exists")
    await expect(call(tool.cron, { ...base, name: "bad-schedule", schedule: "* * * *" })).rejects.toThrow("5 fields")
    await expect(call(tool.cron, { ...base, name: "two", schedule: "* * * * *", every_seconds: 5 })).rejects.toThrow(
      "exactly one",
    )
    await expect(call(tool.cron, { action: "create", name: "none", prompt: "hi" })).rejects.toThrow("exactly one")
    await expect(
      call(tool.cron, { action: "create", name: "short", prompt: "hi", every_seconds: 3 }),
    ).rejects.toThrow("at least 5")
    await expect(
      call(tool.cron, { action: "create", name: "daily", prompt: "hi", daily_at: "25:00" }),
    ).rejects.toThrow("HH:MM")
    await expect(
      call(tool.cron, { ...base, name: "agent", agent: "nope" }),
    ).rejects.toThrow("Unknown agent")
    await expect(
      call(tool.cron, { ...base, name: "model", model: "openai/missing" }),
    ).rejects.toThrow("Unknown model")
    await expect(
      call(tool.cron, { ...base, name: "conn", model: "offline/model" }),
    ).rejects.toThrow("not connected")
    await expect(
      call(tool.cron, { ...base, name: "variant", model: "openai/main", variant: "no" }),
    ).rejects.toThrow("Unknown variant")
    expect(io.store()?.jobs).toHaveLength(1)
    await dispose()
  })

  it("enforces the omp-aligned limits", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    const result = await call(tool.cron, {
      action: "create",
      name: "x".repeat(100),
      prompt: "p".repeat(5000),
      every_seconds: 5,
    })
    expect(result.created.name).toHaveLength(80)
    expect(result.created.prompt).toHaveLength(4000)
    for (let i = 0; i < 31; i++) {
      await call(tool.cron, { action: "create", name: `job-${i}`, every_seconds: 5, prompt: "hi" })
    }
    await expect(call(tool.cron, { action: "create", name: "one-too-many", every_seconds: 5, prompt: "hi" })).rejects.toThrow(
      "limit",
    )
    await dispose()
  })

  it("fires headless tasks on schedule with denied permissions and records a completed run", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, {
      action: "create",
      name: "nightly",
      target: "task",
      schedule: "0 9 * * *",
      prompt: "Check the overnight failures",
    })
    await advanceTo(new Date(2026, 8, 12, 9, 0))
    expect(client.session.create).toHaveBeenCalledTimes(1)
    expect(client.session.create).toHaveBeenCalledWith({
      body: {
        title: "cron: nightly",
        metadata: { "opencode-cron": { name: "nightly", kind: "cron" } },
        permission: [
          { permission: "task", pattern: "*", action: "deny" },
          { permission: "todowrite", pattern: "*", action: "deny" },
          { permission: "apply_patch", pattern: "*", action: "deny" },
        ],
      },
    })
    expect(client.session.prompt).toHaveBeenCalledWith({
      path: { id: "child-1" },
      body: { parts: [{ type: "text", text: "Check the overnight failures" }] },
    })
    expect(client.session.promptAsync).not.toHaveBeenCalled()
    const store = StoreSchema.parse(io.store())
    expect(store.jobs[0]?.lastRun).toMatchObject({ status: "completed", sessionID: "child-1" })
    await dispose()
  })

  it("passes model, variant, and agent fields to the server for headless tasks", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(
      tool.cron,
      taskTarget({
        action: "create",
        name: "job",
        schedule: "0 9 * * *",
        prompt: "Work",
        agent: "general",
        model: "openai/main",
        variant: "high",
      }),
    )
    await advanceTo(new Date(2026, 8, 12, 9, 0))
    expect(client.session.create).toHaveBeenCalledWith({
      body: expect.objectContaining({
        agent: "general",
        model: { id: "main", providerID: "openai", variant: "high" },
      }),
    })
    expect(client.session.prompt).toHaveBeenCalledWith({
      path: { id: "child-1" },
      body: {
        parts: [{ type: "text", text: "Work" }],
        model: { providerID: "openai", modelID: "main" },
        variant: "high",
        agent: "general",
      },
    })
    await dispose()
  })

  it("injects into the creating session by default (omp-style)", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, { action: "create", name: "tick", every_seconds: 5, prompt: "Check the deploy" })
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    expect(client.session.promptAsync).toHaveBeenCalledTimes(1)
    expect(client.session.promptAsync).toHaveBeenCalledWith({
      path: { id: "session" },
      body: { parts: [{ type: "text", text: "⏰ 定时任务 [tick] 触发，请执行：\nCheck the deploy" }] },
    })
    expect(client.session.create).not.toHaveBeenCalled()
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs[0].lastRun).toMatchObject({ status: "completed", sessionID: "session" })
    await dispose()
  })

  it("queues injections into busy sessions by default and cancels them on demand", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    ;(client.session.status as unknown as { mockResolvedValue: (value: unknown) => void }).mockResolvedValue(
      response({ session: { type: "busy" } }),
    )
    await call(tool.cron, { action: "create", name: "queued", every_seconds: 5, prompt: "Work" })
    await call(tool.cron, {
      action: "create",
      name: "cancelled",
      every_seconds: 5,
      prompt: "Work",
      on_busy: "cancel",
    })
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    // queue (default): the server holds the message until the session idles.
    expect(client.session.promptAsync).toHaveBeenCalledTimes(1)
    expect(client.session.promptAsync).toHaveBeenCalledWith(
      expect.objectContaining({ path: { id: "session" } }),
    )
    // cancel: this fire is dropped; the task remains scheduled.
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs.find((job: { name: string }) => job.name === "cancelled").lastRun).toBeUndefined()
    await dispose()
  })

  it("removes a session task when its session is gone", async () => {
    const { client, io } = setup({ promptAsync: async () => missing() })
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, { action: "create", name: "ghost", every_seconds: 5, prompt: "Work" })
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    expect(client.session.promptAsync).toHaveBeenCalledTimes(1)
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs).toHaveLength(0)
    expect(io.store()?.jobs).toHaveLength(0)
    await dispose()
  })

  it("supports every_seconds, daily_at, and one-shot once_in_seconds schedules", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, { action: "create", name: "every", every_seconds: 5, prompt: "Work" })
    await call(tool.cron, { action: "create", name: "daily", daily_at: "10:30", prompt: "Work" })
    await call(tool.cron, { action: "create", name: "once", once_in_seconds: 5, prompt: "Work" })

    const list = await call(tool.cron, { action: "list" })
    const byName = Object.fromEntries(list.jobs.map((job: { name: string; nextRun: string }) => [job.name, job.nextRun]))
    expect(byName["every"]).toBe(new Date(2026, 8, 11, 10, 0, 5).toISOString())
    expect(byName["daily"]).toBe(new Date(2026, 8, 11, 10, 30).toISOString())
    expect(byName["once"]).toBe(new Date(2026, 8, 11, 10, 0, 5).toISOString())

    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    // The one-shot task fired and was removed; the interval task re-armed.
    expect(client.session.promptAsync).toHaveBeenCalledTimes(2)
    const after = await call(tool.cron, { action: "list" })
    expect(after.jobs.find((job: { name: string }) => job.name === "once")).toBeUndefined()
    expect(after.jobs.find((job: { name: string }) => job.name === "every").nextRun).toBe(
      new Date(2026, 8, 11, 10, 0, 10).toISOString(),
    )

    await advanceTo(new Date(2026, 8, 11, 10, 0, 10))
    expect(client.session.promptAsync).toHaveBeenCalledTimes(3)
    // With the interval task gone, the daily task fires at its fixed point.
    await call(tool.cron, { action: "remove", name: "every" })
    await advanceTo(new Date(2026, 8, 11, 10, 30))
    expect(client.session.promptAsync).toHaveBeenCalledTimes(4)
    await dispose()
  })

  it("removes a fired one-shot headless task", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "once", once_in_seconds: 5, prompt: "Work" }))
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    expect(client.session.create).toHaveBeenCalledTimes(1)
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs).toHaveLength(0)
    await dispose()
  })

  it("records a failed run when the prompt fails", async () => {
    const { client, io } = setup({
      prompt: async () => {
        throw new Error("provider exploded")
      },
    })
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work" }))
    await advanceTo(new Date(2026, 8, 12, 9, 0))
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs[0].lastRun).toMatchObject({
      status: "failed",
      sessionID: "child-1",
      error: expect.stringContaining("provider exploded"),
    })
    await dispose()
  })

  it("cancels headless fires while a run is in flight and queues them by default", async () => {
    let release: (value: unknown) => void = () => undefined
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const { client, io } = setup({
      prompt: () => gate.then(() => response({ parts: [{ type: "text", text: "done" }] })),
    })
    const { tool, dispose } = await createCron(client, io)
    await call(
      tool.cron,
      taskTarget({ action: "create", name: "cancel-mode", every_seconds: 5, prompt: "Work", on_busy: "cancel" }),
    )
    await call(tool.cron, taskTarget({ action: "create", name: "queue-mode", every_seconds: 5, prompt: "Work" }))
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    expect(client.session.create).toHaveBeenCalledTimes(2)
    // While both runs are pending there are no armed timers, so later
    // occurrences are not queued by the scheduler itself.
    await advanceTo(new Date(2026, 8, 11, 10, 0, 20))
    expect(client.session.create).toHaveBeenCalledTimes(2)
    release(undefined)
    await vi.advanceTimersByTimeAsync(0)
    // Both runs complete; scheduling resumes from completion time.
    await advanceTo(new Date(2026, 8, 11, 10, 0, 25))
    expect(client.session.create).toHaveBeenCalledTimes(4)
    await dispose()
  })

  it("queues one headless occurrence after the in-flight run completes", async () => {
    let release: (value: unknown) => void = () => undefined
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const { client, io } = setup({
      prompt: () => gate.then(() => response({ parts: [{ type: "text", text: "done" }] })),
    })
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "job", every_seconds: 5, prompt: "Work" }))
    // First fire starts and blocks on the prompt; the manual run arrives
    // while it is in flight and is queued (on_busy defaults to queue).
    await advanceTo(new Date(2026, 8, 11, 10, 0, 5))
    expect(client.session.create).toHaveBeenCalledTimes(1)
    const result = await call(tool.cron, { action: "run", name: "job" })
    expect(result.status).toBe("queued")
    release(undefined)
    await vi.advanceTimersByTimeAsync(0)
    expect(client.session.create).toHaveBeenCalledTimes(2)
    await dispose()
  })

  it("runs a task immediately with the run action", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work" }))
    const result = await call(tool.cron, { action: "run", name: "job" })
    expect(result.status).toBe("started")
    await vi.advanceTimersByTimeAsync(0)
    expect(client.session.create).toHaveBeenCalledTimes(1)
    const list = await call(tool.cron, { action: "list" })
    expect(list.jobs[0].lastRun).toMatchObject({ status: "completed" })
    await dispose()
  })

  it("does not fire disabled or removed tasks", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "a", every_seconds: 5, prompt: "Work" }))
    await call(tool.cron, taskTarget({ action: "create", name: "b", every_seconds: 5, prompt: "Work" }))
    await call(tool.cron, { action: "disable", name: "a" })
    await call(tool.cron, { action: "remove", name: "b" })
    await advanceTo(new Date(2026, 8, 11, 10, 1))
    expect(client.session.create).not.toHaveBeenCalled()
    expect(io.store()?.jobs).toHaveLength(1)
    await dispose()
  })

  it("updates a task and reschedules it", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work" }))
    const result = await call(tool.cron, { action: "update", name: "job", schedule: "30 11 * * *", prompt: "Updated" })
    expect(result.updated.nextRun).toBe(new Date(2026, 8, 11, 11, 30).toISOString())
    await advanceTo(new Date(2026, 8, 11, 11, 31))
    expect(client.session.create).toHaveBeenCalledTimes(1)
    expect(client.session.prompt).toHaveBeenCalledWith({
      path: { id: "child-1" },
      body: { parts: [{ type: "text", text: "Updated" }] },
    })
    expect(io.store()?.jobs[0]?.schedule).toBe("30 11 * * *")
    await dispose()
  })

  it("replaces the whole schedule configuration on update", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, { action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work" })
    const result = await call(tool.cron, { action: "update", name: "job", every_seconds: 10 })
    expect(result.updated.schedule).toBeUndefined()
    expect(result.updated.everySeconds).toBe(10)
    expect(result.updated.nextRun).toBe(new Date(2026, 8, 11, 10, 0, 10).toISOString())
    await dispose()
  })

  it("reloads tasks from the store on startup and skips missed runs by default", async () => {
    const { client, io } = setup()
    const first = await createCron(client, io)
    await call(first.tool.cron, taskTarget({ action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work" }))
    await first.dispose()

    // Restart two days later: the missed runs are skipped, the next occurrence is scheduled.
    vi.setSystemTime(new Date(2026, 8, 13, 10, 0, 0))
    const second = await createCron(client, io)
    const list = await call(second.tool.cron, { action: "list" })
    expect(list.jobs[0].nextRun).toBe(new Date(2026, 8, 14, 9, 0).toISOString())
    await advanceTo(new Date(2026, 8, 14, 9, 0))
    expect(client.session.create).toHaveBeenCalledTimes(1)
    await second.dispose()
  })

  it("catches up once on missed runs when missed is run_once", async () => {
    const { client, io } = setup()
    const first = await createCron(client, io)
    await call(
      first.tool.cron,
      taskTarget({ action: "create", name: "job", schedule: "0 9 * * *", prompt: "Work", missed: "run_once" }),
    )
    await first.dispose()

    vi.setSystemTime(new Date(2026, 8, 13, 10, 0, 0))
    const second = await createCron(client, io)
    await vi.advanceTimersByTimeAsync(0)
    // Exactly one catch-up run, then the schedule continues from now.
    expect(client.session.create).toHaveBeenCalledTimes(1)
    const list = await call(second.tool.cron, { action: "list" })
    expect(list.jobs[0].lastRun).toMatchObject({ status: "completed" })
    expect(list.jobs[0].nextRun).toBe(new Date(2026, 8, 14, 9, 0).toISOString())
    await second.dispose()
  })

  it("stops scheduling after dispose", async () => {
    const { client, io } = setup()
    const { tool, dispose } = await createCron(client, io)
    await call(tool.cron, taskTarget({ action: "create", name: "job", every_seconds: 5, prompt: "Work" }))
    await dispose()
    await advanceTo(new Date(2026, 8, 11, 10, 1))
    expect(client.session.create).not.toHaveBeenCalled()
  })
})
