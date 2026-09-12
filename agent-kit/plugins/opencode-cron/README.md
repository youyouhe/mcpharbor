# opencode-cron

An [OpenCode](https://opencode.ai/) plugin that runs prompts on a schedule. Task definitions persist across restarts, and each fire either injects the prompt into the session the task was created in (aligned with [omp-cron-extension](https://github.com/youyouhe/omp-cron-extension)) or runs it in a fresh standalone session with a restricted permission set for unattended execution.

## Requirements

- OpenCode 1.18.2 or later
- A connected provider with at least one model (only if the task pins a model)

## Install From GitHub Release

Add the release tarball spec to your project or global `opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": [
    "opencode-cron@https://github.com/TTTPOB/opencode-cron-plugin/releases/download/v0.1.0/opencode-cron-0.1.0.tgz"
  ]
}
```

Quit and restart OpenCode after changing the configuration. OpenCode installs the plugin on startup.

## Install Locally

```bash
git clone https://github.com/TTTPOB/opencode-cron-plugin.git
cd opencode-cron-plugin
pnpm install
pnpm build
```

Reference the cloned directory with an absolute file URL:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["file:///absolute/path/to/opencode-cron-plugin"]
}
```

Restart OpenCode after building or changing the plugin.

## The `cron` tool

Manage scheduled tasks through one tool:

```ts
cron({ action: "list" })
cron({ action: "create", name: "nightly", daily_at: "09:00", prompt: "Check the overnight failures" })
cron({ action: "create", name: "watch", every_seconds: 300, prompt: "Check git status and remind me about uncommitted changes" })
cron({ action: "create", name: "deploy", once_in_seconds: 600, prompt: "Remind me to redeploy" })
cron({ action: "create", name: "headless", schedule: "0 9 * * *", prompt: "...", target: "task" })
cron({ action: "update", name: "nightly", schedule: "30 9 * * *" })
cron({ action: "run", name: "nightly" })
cron({ action: "disable", name: "nightly" })
cron({ action: "remove", name: "nightly" })
```

Arguments:

- `action`: `list`, `create`, `update`, `remove`, `enable`, `disable`, or `run`
- `name`: unique task name, up to 80 characters (required for every action except `list`)
- `prompt`: the prompt executed at each fire, up to 4000 characters
- schedule — exactly one of:
  - `schedule`: 5-field cron expression, evaluated in the server's local time
  - `every_seconds`: fixed interval, minimum 5 seconds
  - `daily_at`: every day at `"HH:MM"` (24-hour, local time)
  - `once_in_seconds`: one-shot delay, minimum 5 seconds; the task is removed after firing
- `target`: `session` (default) injects the prompt into the session the task was created in; `task` runs it in a fresh standalone session
- `agent`: optional agent name; defaults to the session's or server's default agent
- `model`: optional `provider/model-id`; the plugin splits on the first `/`, so model IDs may contain `/`
- `variant`: optional model variant, or `default` to use the model's base configuration
- `on_busy`: what to do when the target is not free — `queue` (default) or `cancel`
- `missed`: what to do about runs missed while OpenCode was stopped — `skip` (default) or `run_once`
- `condition`: optional Harbor condition gate — `"__TOKEN__ # <agent_id> # <mcp endpoint>"`. At each fire the plugin calls `get_messages` on the endpoint first and **skips the dispatch entirely (no LLM call) while the unread count is 0**; any failure (bad shape, unreadable token, transport/parse error) also skips the fire (fail-closed). Requires `token_file` when set.
- `token_file`: plaintext token file for the condition gate; the first line fills the `__TOKEN__` placeholder (e.g. a token saved from MCP Harbor's `register_agent`).

`create` validates the schedule, agent, provider connection, model, variant, and condition shape immediately, so mistakes surface before the first fire. A worktree can hold at most 32 tasks.

### Harbor scheduled inbox check (condition gate example)

```json
cron({
  action: "create", name: "harbor-inbox", every_seconds: 60,
  prompt: "检查 Harbor 收件箱：get_conversations 看未读，有就 get_messages 读取 → 处理 → 回复 → mark_messages_read + ack_messages",
  condition: "__TOKEN__ # order-agent # http://192.168.8.107:8931/mcp",
  token_file: "~/.harbor/order-agent.token",
})
```

The gate means "wake me only when there is mail": empty inbox → silent skip, no tokens burned.

## Behavior

- **Session target (default)**: at each fire the plugin injects `⏰ 定时任务 [name] 触发，请执行：<prompt>` into the creating session via the server's async prompt API. An idle session starts a new turn right away. A busy session follows `on_busy`: `queue` hands the message to the server's queue so it runs when the session idles; `cancel` drops this fire (one-shot tasks are consumed either way). When the target session has been deleted, the task removes itself — a session task's lifecycle follows its session.
- **Task target**: each fire creates a standalone session (no parent), runs the prompt to completion, and records the run. Those sessions deny `task`, `todowrite`, and any `experimental.primary_tools` tools, so an automated run cannot spawn subagents. While a run is in flight, `on_busy: queue` runs one queued occurrence after it completes and `on_busy: cancel` skips the fire.
- **Storage**: task definitions persist in `<worktree>/.opencode/cron.json` and survive restarts. Consider adding that file to `.gitignore`.
- **Missed runs**: OpenCode must be running at the trigger time. On restart, missed fires are skipped by default; a task with `missed: "run_once"` fires exactly once per missed period, then continues on schedule. Missed fires are never accumulated.
- **Run records**: `list` shows each task's next run time and its `lastRun` (`completed`/`failed`, session id, and error text for failures).
- **`run` action**: starts a task immediately and returns; the result lands in `lastRun`.

### `/cron` command

OpenCode plugins cannot register slash commands, but a config-defined command gets the same effect. Add to `opencode.json`:

```json
{
  "command": {
    "cron": {
      "template": "Use the cron tool with action list, then show me the scheduled tasks in a compact table.",
      "description": "查看定时任务"
    }
  }
}
```

## Development

```bash
pnpm install
pnpm typecheck
pnpm test
pnpm build
```

The test suite uses a mock of OpenCode's injected client with fake timers and verifies cron parsing, all four schedule kinds, session injection and busy strategies, headless permissions, model validation, run recording, missed-run semantics, persistence reload, and dispose behavior.
