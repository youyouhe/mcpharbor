import { mkdir, readFile, rename, writeFile } from "node:fs/promises"
import * as path from "node:path"
import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin"
import { tool } from "@opencode-ai/plugin"
import { z } from "zod"
import { nextDailyAt, nextRun, parseCron } from "./cron.js"

const MetadataKey = "opencode-cron"
const MAX_TIMEOUT_MS = 2 ** 31 - 1
// Limits aligned with omp-cron-extension.
const MIN_SECONDS = 5
const MAX_JOBS = 32
const MAX_PROMPT = 4_000
const MAX_NAME = 80

const PermissionRuleSchema = z.object({
  permission: z.string(),
  pattern: z.string(),
  action: z.enum(["allow", "ask", "deny"]),
})
const TaskSelectionSchema = z.object({
  model: z.object({ providerID: z.string(), modelID: z.string() }),
  variant: z.string().optional(),
})
const LastRunSchema = z.object({
  at: z.string(),
  sessionID: z.string().optional(),
  status: z.enum(["completed", "failed"]),
  error: z.string().optional(),
})
export const JobSchema = z.object({
  name: z.string().min(1),
  prompt: z.string().min(1),
  // Exactly one schedule field is present (enforced at create/update).
  schedule: z.string().optional(), // 5-field cron
  everySeconds: z.number().optional(),
  dailyAt: z.string().optional(), // "HH:MM" local time
  onceInSeconds: z.number().optional(),
  // "session" injects the prompt into the creating session (default);
  // "task" runs the prompt in a fresh standalone session.
  target: z.enum(["session", "task"]).optional(),
  sessionID: z.string().optional(), // target=session: the session the job was created in
  agent: z.string().optional(),
  model: z.string().optional(),
  variant: z.string().optional(),
  onBusy: z.enum(["queue", "cancel"]).optional(), // default queue
  missed: z.enum(["skip", "run_once"]).optional(), // default skip
  enabled: z.boolean(),
  createdAt: z.string(),
  nextAt: z.number().optional(), // epoch ms of the armed occurrence (missed-run detection)
  lastRun: LastRunSchema.optional(),
})
export const StoreSchema = z.object({
  version: z.literal(1),
  jobs: z.array(JobSchema),
})
const SessionSchema = z
  .object({
    id: z.string(),
    parentID: z.string().optional(),
    metadata: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough()
const AgentSchema = z
  .object({
    name: z.string(),
    mode: z.enum(["subagent", "primary", "all"]),
  })
  .passthrough()
const ConfigSchema = z
  .object({
    experimental: z
      .object({
        primary_tools: z.array(z.string()).optional(),
      })
      .optional(),
  })
  .passthrough()
const ModelSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    variants: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough()
const ProviderSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    models: z.record(z.string(), ModelSchema),
  })
  .passthrough()
const ProviderListSchema = z.object({
  all: z.array(ProviderSchema),
  default: z.record(z.string(), z.string()),
  connected: z.array(z.string()),
})
const PromptResultSchema = z.object({
  parts: z.array(
    z
      .object({
        type: z.string(),
        text: z.string().optional(),
      })
      .passthrough(),
  ),
})
const SessionStatusMapSchema = z.record(
  z.string(),
  z
    .object({
      type: z.string(),
    })
    .passthrough(),
)

type Client = PluginInput["client"]
type ApiResult = { data?: unknown; error?: unknown; response?: Response }
type Store = z.infer<typeof StoreSchema>
export type Job = z.infer<typeof JobSchema>
type TaskSelection = z.infer<typeof TaskSelectionSchema>
type ProviderList = z.infer<typeof ProviderListSchema>

export type StoreIO = {
  load(): Promise<Store | undefined>
  save(store: Store): Promise<void>
  now(): Date
}

function parseResult<T>(result: unknown, schema: z.ZodType<T>, operation: string): T {
  const response = result as ApiResult
  if (response.data === undefined) {
    const detail = response.error === undefined ? `HTTP ${response.response?.status ?? "error"}` : JSON.stringify(response.error)
    throw new Error(`${operation} failed: ${detail}`)
  }
  return schema.parse(response.data)
}

function parseModel(value: string) {
  const slash = value.indexOf("/")
  if (slash <= 0 || slash === value.length - 1) {
    throw new Error(`Invalid model "${value}"; expected provider/model-id`)
  }
  return { providerID: value.slice(0, slash), modelID: value.slice(slash + 1) }
}

function normalizeVariant(variant?: string) {
  return variant === "default" ? undefined : variant
}

function validateModel(catalog: ProviderList, model: { providerID: string; modelID: string }, variant?: string) {
  const provider = catalog.all.find((item) => item.id === model.providerID)
  if (!provider) throw new Error(`Unknown provider: ${model.providerID}`)
  if (!catalog.connected.includes(model.providerID)) throw new Error(`Provider is not connected: ${model.providerID}`)
  const selected = provider.models[model.modelID]
  if (!selected) throw new Error(`Unknown model: ${model.providerID}/${model.modelID}`)
  if (variant !== undefined && !(variant in (selected.variants ?? {}))) {
    throw new Error(`Unknown variant for ${model.providerID}/${model.modelID}: ${variant}`)
  }
}

/**
 * File-backed store at <worktree>/.opencode/cron.json, written atomically.
 * A corrupt store is reported and treated as empty rather than failing plugin load.
 */
export function fileStoreIO(worktree: string): StoreIO {
  const file = path.join(worktree, ".opencode", "cron.json")
  return {
    async load() {
      let raw: string
      try {
        raw = await readFile(file, "utf8")
      } catch {
        return undefined
      }
      try {
        return StoreSchema.parse(JSON.parse(raw))
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`[opencode-cron] ignoring corrupt store ${file}: ${message}`)
        return undefined
      }
    },
    async save(store: Store) {
      await mkdir(path.dirname(file), { recursive: true })
      const tmp = `${file}.${process.pid}.tmp`
      await writeFile(tmp, JSON.stringify(store, null, 2))
      await rename(tmp, file)
    },
    now() {
      return new Date()
    },
  }
}

type ScheduleInput = { schedule?: string; every_seconds?: number; daily_at?: string; once_in_seconds?: number }

function parseScheduleInput(input: ScheduleInput): Partial<Job> {
  const given = [input.schedule, input.every_seconds, input.daily_at, input.once_in_seconds].filter(
    (value) => value !== undefined,
  )
  if (given.length !== 1) {
    throw new Error("schedule / every_seconds / daily_at / once_in_seconds: provide exactly one")
  }
  if (input.schedule !== undefined) {
    parseCron(input.schedule)
    return { schedule: input.schedule }
  }
  if (input.every_seconds !== undefined) {
    if (input.every_seconds < MIN_SECONDS) throw new Error(`every_seconds must be at least ${MIN_SECONDS}`)
    return { everySeconds: Math.round(input.every_seconds) }
  }
  if (input.daily_at !== undefined) {
    nextDailyAt(input.daily_at, new Date())
    return { dailyAt: input.daily_at }
  }
  if (input.once_in_seconds === undefined || input.once_in_seconds < MIN_SECONDS) {
    throw new Error(`once_in_seconds must be at least ${MIN_SECONDS}`)
  }
  return { onceInSeconds: Math.round(input.once_in_seconds) }
}

type CronTool = ReturnType<typeof tool>

export async function createCron(
  client: Client,
  io: StoreIO,
): Promise<{ tool: { cron: CronTool }; dispose: () => Promise<void> }> {
  const jobs = new Map<string, Job>()
  const timers = new Map<string, ReturnType<typeof setTimeout>>()
  const inFlight = new Set<string>()
  const pendingRun = new Set<string>()
  let disposed = false

  function clearTimer(name: string) {
    const existing = timers.get(name)
    if (existing) {
      clearTimeout(existing)
      timers.delete(name)
    }
  }

  function targetOf(job: Job): "session" | "task" {
    // Jobs stored before targets existed ran headless; keep that behavior.
    return job.target ?? "task"
  }

  function scheduleKindOf(job: Job): "cron" | "every" | "daily" | "once" {
    if (job.everySeconds !== undefined) return "every"
    if (job.dailyAt !== undefined) return "daily"
    if (job.onceInSeconds !== undefined) return "once"
    return "cron"
  }

  function nextFireAt(job: Job, from: Date): Date {
    switch (scheduleKindOf(job)) {
      case "cron":
        return nextRun(job.schedule ?? "* * * * *", from)
      case "every":
        return new Date(from.getTime() + (job.everySeconds ?? MIN_SECONDS) * 1000)
      case "daily":
        return nextDailyAt(job.dailyAt ?? "00:00", from)
      case "once":
        return new Date(from.getTime() + (job.onceInSeconds ?? MIN_SECONDS) * 1000)
    }
  }

  function scheduleJob(job: Job, options?: { persist?: boolean }) {
    clearTimer(job.name)
    if (!job.enabled || disposed) return
    const target = nextFireAt(job, io.now())
    job.nextAt = target.getTime()
    if (options?.persist !== false) void persist()
    const delay = Math.max(0, target.getTime() - io.now().getTime())
    if (delay > MAX_TIMEOUT_MS) {
      // setTimeout cannot hold delays past ~24.8 days (monthly schedules); re-arm later.
      timers.set(job.name, setTimeout(() => scheduleJob(job, options), MAX_TIMEOUT_MS))
    } else {
      timers.set(job.name, setTimeout(() => void onFired(job), delay))
    }
  }

  async function persist() {
    await io.save({ version: 1, jobs: [...jobs.values()].map((job) => ({ ...job })) })
  }

  async function updateJob(name: string, patch: Partial<Job>) {
    const job = jobs.get(name)
    if (!job) return
    Object.assign(job, patch)
    await persist()
  }

  async function resolveSelection(input: { model?: string; variant?: string }): Promise<TaskSelection | undefined> {
    if (!input.model) return undefined
    const catalog = parseResult(await client.provider.list(), ProviderListSchema, "List providers")
    const model = parseModel(input.model)
    const variant = normalizeVariant(input.variant)
    validateModel(catalog, model, variant)
    return { model, variant }
  }

  async function deniedPermissions() {
    const config = parseResult(await client.config.get(), ConfigSchema, "Get config")
    const denied = ["task", "todowrite", ...(config.experimental?.primary_tools ?? [])]
    return denied.map((permission) => ({
      permission,
      pattern: "*",
      action: "deny" as const,
    }))
  }

  async function validateJobFields(input: {
    prompt?: string
    agent?: string
    model?: string
    variant?: string
  } & ScheduleInput) {
    if (input.prompt !== undefined && input.prompt.trim().length === 0) {
      throw new Error("prompt must not be empty")
    }
    if (input.agent !== undefined) {
      const agents = parseResult(await client.app.agents(), z.array(AgentSchema), "List agents")
      if (!agents.some((agent) => agent.name === input.agent)) {
        throw new Error(`Unknown agent: ${input.agent}`)
      }
    }
    if (input.model !== undefined) {
      await resolveSelection(input)
    }
  }

  /** Run the prompt in a fresh standalone session with unattended-safe permissions. */
  async function executeTaskJob(job: Job) {
    const startedAt = io.now()
    let sessionID: string | undefined
    try {
      const selection = await resolveSelection(job)
      const agent = job.agent ? { agent: job.agent } : {}
      const model = selection
        ? {
            model: {
              id: selection.model.modelID,
              providerID: selection.model.providerID,
              variant: selection.variant,
            },
          }
        : {}
      const createBody = {
        title: `cron: ${job.name}`,
        metadata: { [MetadataKey]: { name: job.name, kind: scheduleKindOf(job) } },
        permission: await deniedPermissions(),
        ...model,
        ...agent,
      }
      // Generated SDK types lag behind the server HttpApi, which accepts these session fields here.
      const created = parseResult(
        await client.session.create({ body: createBody } as never),
        SessionSchema,
        "Create scheduled session",
      )
      sessionID = created.id
      const promptBody = {
        parts: [{ type: "text" as const, text: job.prompt }],
        ...(selection ? { model: selection.model, variant: selection.variant } : {}),
        ...agent,
      }
      // Generated SDK types lag behind the server HttpApi, which accepts variant here.
      const result = parseResult(
        await client.session.prompt({ path: { id: created.id }, body: promptBody } as never),
        PromptResultSchema,
        "Run scheduled prompt",
      )
      await updateJob(job.name, {
        lastRun: { at: startedAt.toISOString(), sessionID: created.id, status: "completed" },
      })
      return result.parts.findLast((part) => part.type === "text")?.text ?? ""
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      await updateJob(job.name, {
        lastRun: {
          at: startedAt.toISOString(),
          ...(sessionID ? { sessionID } : {}),
          status: "failed",
          error: message,
        },
      })
      return undefined
    }
  }

  async function isSessionBusy(sessionID: string): Promise<boolean> {
    try {
      const map = parseResult(await client.session.status(), SessionStatusMapSchema, "Get session status")
      const type = map[sessionID]?.type
      return type === "busy" || type === "retry"
    } catch {
      return false
    }
  }

  /** Inject the prompt into the session the job was created in (omp-style). */
  async function dispatchSession(job: Job): Promise<string> {
    const sessionID = job.sessionID
    if (!sessionID) {
      await updateJob(job.name, {
        lastRun: { at: io.now().toISOString(), status: "failed", error: "session task has no sessionID" },
      })
      return "failed"
    }
    const busy = await isSessionBusy(sessionID)
    if (busy && (job.onBusy ?? "queue") === "cancel") return "skipped"
    try {
      const selection = await resolveSelection(job)
      const agent = job.agent ? { agent: job.agent } : {}
      const model = selection
        ? { model: selection.model, ...(selection.variant ? { variant: selection.variant } : {}) }
        : {}
      const body = {
        parts: [{ type: "text" as const, text: `⏰ 定时任务 [${job.name}] 触发，请执行：\n${job.prompt}` }],
        ...model,
        ...agent,
      }
      // Generated SDK types lag behind the server HttpApi, which accepts variant here.
      const result = (await client.session.promptAsync({ path: { id: sessionID }, body } as never)) as ApiResult
      if (result.data === undefined) {
        if (result.response?.status === 404) {
          // A session-scoped task dies with its session.
          jobs.delete(job.name)
          clearTimer(job.name)
          await persist()
          console.warn(`[opencode-cron] session ${sessionID} no longer exists; removed task "${job.name}"`)
          return "skipped"
        }
        const detail = result.error === undefined ? `HTTP ${result.response?.status ?? "error"}` : JSON.stringify(result.error)
        throw new Error(`Inject scheduled prompt failed: ${detail}`)
      }
      await updateJob(job.name, {
        lastRun: { at: io.now().toISOString(), sessionID, status: "completed" },
      })
      return "completed"
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      await updateJob(job.name, {
        lastRun: { at: io.now().toISOString(), sessionID, status: "failed", error: message },
      })
      return "failed"
    }
  }

  async function dispatchTask(job: Job): Promise<string> {
    if (inFlight.has(job.name)) {
      if ((job.onBusy ?? "queue") === "cancel") return "skipped"
      pendingRun.add(job.name)
      return "queued"
    }
    inFlight.add(job.name)
    try {
      await executeTaskJob(job)
      if (pendingRun.delete(job.name)) await executeTaskJob(job)
    } finally {
      inFlight.delete(job.name)
    }
    return "completed"
  }

  function dispatch(job: Job): Promise<string> {
    return targetOf(job) === "session" ? dispatchSession(job) : dispatchTask(job)
  }

  async function onFired(job: Job) {
    if (disposed) return
    const current = jobs.get(job.name)
    if (!current || !current.enabled) return
    if (scheduleKindOf(current) === "once") {
      jobs.delete(current.name)
      clearTimer(current.name)
      await persist()
    }
    await dispatch(current)
    const again = jobs.get(current.name)
    if (again?.enabled && !disposed) {
      scheduleJob(again, { persist: false })
      await persist()
    }
  }

  function jobSummary(job: Job) {
    let next: string | undefined
    if (job.enabled) {
      try {
        next = nextFireAt(job, io.now()).toISOString()
      } catch {
        next = undefined
      }
    }
    return { ...job, nextRun: next }
  }

  const cronTool = tool({
    description:
      "Manage scheduled tasks. A task runs a prompt either by injecting it into the session it was created in (target session, default; idle sessions start a new turn, busy sessions follow on_busy) or in a fresh standalone session (target task). Schedule is exactly one of: 5-field cron expression (schedule, server local time), every_seconds, daily_at (\"HH:MM\" local), once_in_seconds. OpenCode must be running at the trigger time. Use the list action to see existing tasks, their next run time, and last run status.",
    args: {
      action: tool.schema.enum(["list", "create", "update", "remove", "enable", "disable", "run"]),
      name: tool.schema.string().min(1).optional(),
      schedule: tool.schema.string().min(1).optional(),
      every_seconds: tool.schema.number().optional(),
      daily_at: tool.schema.string().optional(),
      once_in_seconds: tool.schema.number().optional(),
      target: tool.schema.enum(["session", "task"]).optional(),
      prompt: tool.schema.string().min(1).optional(),
      agent: tool.schema.string().min(1).optional(),
      model: tool.schema.string().min(1).optional(),
      variant: tool.schema.string().min(1).optional(),
      on_busy: tool.schema.enum(["queue", "cancel"]).optional(),
      missed: tool.schema.enum(["skip", "run_once"]).optional(),
    },
    async execute(input, context) {
      switch (input.action) {
        case "list": {
          const list = [...jobs.values()].map((job) => jobSummary(job))
          return JSON.stringify({ jobs: list }, null, 2)
        }
        case "create": {
          if (!input.name) throw new Error("name is required to create a task")
          if (!input.prompt) throw new Error("prompt is required to create a task")
          if (jobs.size >= MAX_JOBS) throw new Error(`Task limit reached (${MAX_JOBS}); remove a task first`)
          const name = input.name.trim().slice(0, MAX_NAME)
          const prompt = input.prompt.trim().slice(0, MAX_PROMPT)
          if (!name) throw new Error("name must not be empty")
          if (!prompt) throw new Error("prompt must not be empty")
          if (jobs.has(name)) throw new Error(`A task named "${name}" already exists`)
          const schedule = parseScheduleInput(input)
          await validateJobFields({ ...input, prompt })
          const job: Job = {
            name,
            prompt,
            ...schedule,
            target: input.target ?? "session",
            sessionID: context.sessionID,
            agent: input.agent,
            model: input.model,
            variant: normalizeVariant(input.variant),
            onBusy: input.on_busy ?? "queue",
            missed: input.missed ?? "skip",
            enabled: true,
            createdAt: io.now().toISOString(),
          }
          jobs.set(job.name, job)
          scheduleJob(job, { persist: false })
          await persist()
          return JSON.stringify({ created: jobSummary(job) }, null, 2)
        }
        case "update": {
          if (!input.name) throw new Error("name is required to update a task")
          const job = jobs.get(input.name)
          if (!job) throw new Error(`Unknown task: ${input.name}`)
          // A provided schedule field replaces the whole schedule configuration.
          if (input.schedule !== undefined || input.every_seconds !== undefined || input.daily_at !== undefined || input.once_in_seconds !== undefined) {
            const schedule = parseScheduleInput(input)
            delete job.schedule
            delete job.everySeconds
            delete job.dailyAt
            delete job.onceInSeconds
            Object.assign(job, schedule)
          }
          if (input.prompt !== undefined) {
            const prompt = input.prompt.trim().slice(0, MAX_PROMPT)
            if (!prompt) throw new Error("prompt must not be empty")
            job.prompt = prompt
          }
          await validateJobFields(input)
          if (input.agent !== undefined) job.agent = input.agent
          if (input.model !== undefined) job.model = input.model
          if (input.variant !== undefined) job.variant = normalizeVariant(input.variant)
          if (input.on_busy !== undefined) job.onBusy = input.on_busy
          if (input.missed !== undefined) job.missed = input.missed
          if (input.target !== undefined) job.target = input.target
          scheduleJob(job, { persist: false })
          await persist()
          return JSON.stringify({ updated: jobSummary(job) }, null, 2)
        }
        case "remove": {
          if (!input.name) throw new Error("name is required to remove a task")
          if (!jobs.delete(input.name)) throw new Error(`Unknown task: ${input.name}`)
          clearTimer(input.name)
          await persist()
          return JSON.stringify({ removed: input.name }, null, 2)
        }
        case "enable": {
          if (!input.name) throw new Error("name is required to enable a task")
          const job = jobs.get(input.name)
          if (!job) throw new Error(`Unknown task: ${input.name}`)
          job.enabled = true
          scheduleJob(job, { persist: false })
          await persist()
          return JSON.stringify({ enabled: jobSummary(job) }, null, 2)
        }
        case "disable": {
          if (!input.name) throw new Error("name is required to disable a task")
          const job = jobs.get(input.name)
          if (!job) throw new Error(`Unknown task: ${input.name}`)
          job.enabled = false
          clearTimer(input.name)
          await persist()
          return JSON.stringify({ disabled: jobSummary(job) }, null, 2)
        }
        case "run": {
          if (!input.name) throw new Error("name is required to run a task")
          const job = jobs.get(input.name)
          if (!job) throw new Error(`Unknown task: ${input.name}`)
          if (targetOf(job) === "task" && inFlight.has(job.name)) {
            const queued = (job.onBusy ?? "queue") === "queue"
            // The dispatch registers the queued occurrence or drops it per on_busy.
            void dispatch(job)
            return JSON.stringify(
              { status: queued ? "queued" : "skipped", name: job.name, reason: `task "${job.name}" is already running` },
              null,
              2,
            )
          }
          // The dispatch records the run in lastRun; the tool returns immediately.
          void dispatch(job)
          return JSON.stringify({ status: "started", name: job.name }, null, 2)
        }
      }
    },
  })

  const store = await io.load()
  const now = io.now().getTime()
  const missed: Job[] = []
  for (const job of store?.jobs ?? []) {
    if (job.enabled && job.nextAt !== undefined && job.nextAt <= now) missed.push(job)
    jobs.set(job.name, job)
    scheduleJob(job, { persist: false })
  }
  // Missed runs: "run_once" fires once (per job, never accumulated); the
  // default "skip" keeps the reschedule scheduleJob already applied.
  for (const job of missed) {
    if ((job.missed ?? "skip") === "run_once") void onFired(job)
  }

  return {
    tool: { cron: cronTool },
    async dispose() {
      disposed = true
      for (const timer of timers.values()) clearTimeout(timer)
      timers.clear()
    },
  }
}

const plugin: Plugin = async ({ client, worktree }) => {
  const { tool, dispose } = await createCron(client, fileStoreIO(worktree))
  return { tool, dispose }
}

export default plugin
