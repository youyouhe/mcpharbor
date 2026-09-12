# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An OpenCode plugin (`opencode-cron`) that adds scheduled tasks. Each task runs a prompt either by injecting it into the session it was created in (`target: "session"`, default, aligned with omp-cron-extension) or in a fresh standalone session (`target: "task"`, unattended-safe). Schedule is exactly one of: 5-field cron (`schedule`), `every_seconds`, `daily_at`, `once_in_seconds`. The implementation is `src/index.ts` (plugin entry, cron tool, scheduling engine) and `src/cron.ts` (dependency-free cron parser + `nextDailyAt`). Tests live in `test/cron.test.ts` and `test/plugin.test.ts` against a mock of OpenCode's injected client.

## Commands

```bash
pnpm install --frozen-lockfile
pnpm typecheck          # tsc --noEmit over src + test
pnpm test               # vitest run (single file: pnpm vitest run test/cron.test.ts)
pnpm build              # tsc -p tsconfig.build.json → dist/
```

Package manager is pnpm 10 (`packageManager` pin); Node >= 22 required.

## Architecture

`src/index.ts` exports `createCron(client, io)` (tools + dispose, `io` is an injectable `StoreIO` for tests) and a default `Plugin` that wires `fileStoreIO(worktree)`. Zod schemas at the top of the file parse OpenCode server responses at adaptation boundaries.

- **Scheduling**: per-job `setTimeout` armed to the next fire (`nextFireAt` dispatches on the schedule kind). Delays longer than the `setTimeout` cap (~24.8 days) re-arm in a chain. `dispose` clears all timers. `job.nextAt` (epoch ms) is persisted so restarts can detect missed runs.
- **Session injection**: fire → `client.session.promptAsync` into the creating session. `client.session.status()` reports `idle/busy/retry`; `on_busy: queue` sends anyway (server queues), `cancel` drops the fire. A 404 from the inject removes the task — session tasks die with their session.
- **Headless execution**: fire → `client.session.create` + `client.session.prompt`, sessions deny `task`/`todowrite`/`experimental.primary_tools`. `inFlight` guards concurrency: `queue` chains one extra run after the current one, `cancel` skips.
- **Missed runs**: at load, jobs whose persisted `nextAt` is in the past either reschedule from now (`missed: "skip"`, default) or fire once and continue (`missed: "run_once"`). Never accumulated.
- **Storage**: `<worktree>/.opencode/cron.json`, written atomically (tmp + rename). A corrupt store is logged and treated as empty. `StoreIO` abstracts load/save/now for tests.
- **Validation**: exactly one schedule field, minimum 5 seconds for second-based fields, max 32 tasks, name ≤80, prompt ≤4000 (aligned with omp-cron-extension); agent checked against `client.app.agents()`; model/variant checked against the provider catalog and connected list (model split on the first `/`).

## SDK-type lag convention

The generated `@opencode-ai/plugin` client types lag behind the server's HttpApi. Places where the code sends fields the SDK types don't declare (`session.create` with `metadata`/`permission`, `session.prompt`/`promptAsync` with `variant`) carry an English comment noting this and pass the body as `as never`. The SDK does not throw on HTTP errors — results come back as `{ data?, error?, response? }`, so check `data === undefined` and `response.status` (see `parseResult` and `dispatchSession`).

## Test conventions

Tests use `vi.useFakeTimers()` (which also mocks `new Date()`, so `io.now()` follows the fake clock) plus an in-memory `StoreIO`. `advanceTo(date)` computes the real delta and calls `vi.advanceTimersByTimeAsync`. While a run is in flight there is no armed timer, so in-flight tests assert on skipped occurrences accordingly. Session-injection tests mock `session.promptAsync` and `session.status`.
