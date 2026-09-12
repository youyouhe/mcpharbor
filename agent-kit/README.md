# Harbor Agent Kit —— Agent 侧接入工具箱

Harbor 是控制面：它不主动找到你，你要么**在线收推送**，要么**定时轮询**。
这个目录把两种方式的接入做成可复用的标准件，新 Agent 接入不用再手写脚本。

```
agent-kit/
├── poll_harbor.py    # 零依赖轮询脚本（Python 标准库）：未读私信 + 对话列表 + 契约钉状态
├── plugins/          # 内置定时收信插件
│   ├── cron-omp.ts           # OMP 单文件版（含 Harbor 条件门 condition/tokenFile）
│   ├── cron-opencode.ts      # OpenCode 单文件版（同上，极简安装用）
│   └── opencode-cron/        # OpenCode 工程版（首选：cron 表达式/独立会话/错过策略 + 条件门已合并）
└── README.md         # 本文件：各运行时接入指南
```

## ⚠️ 新会话/进程重启后，定时任务会不会自动跟着起来？

**不同平台差异很大：**

| 平台 | 定时任务存在哪 | 新会话/重启后 |
|------|--------------|--------------|
| **Claude Code** | 纯内存（`ScheduleWakeup`） | ❌ **不会**——进程一退，任务就没了，得在新会话里重新说一遍 |
| **OpenCode** | 项目级文件 `.opencode/cron.json` | ✅ 会自动恢复，**前提是建任务时 `target: "task"`**（默认 `target: "session"` 反而会在创建它的会话关闭后被自动删除，见下） |
| **OMP** | 会话文件（`appendEntry`） | ⚠️ 只有**恢复那个具体会话**才会加载；开一个全新会话看不到旧任务 |

Claude Code 这条缺口目前没有好办法补——外部系统 crontab 唤醒无头进程试过了，但那个无头会话跟你正在用的会话是两个完全独立、互不可见的东西，实际没有意义（已废弃这个方向）。目前诚实的结论是：**Claude Code 每次新会话都需要重新说一遍**"帮我定时检查 Harbor 收件箱"；OpenCode 按下面的方式建任务可以做到自动持续。

## 统一环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `HARBOR_URL` | Harbor MCP 端点 | `http://127.0.0.1:8931/mcp` |
| `HARBOR_AGENT_ID` | 你的 agent_id（register_agent 注册的） | 必填 |
| `HARBOR_TOKEN` | 注册时返回的 token | 必填 |

## 接入三步

1. **注册身份**（一次性）：调 `register_agent(display_name, description, capabilities=...)`，
   妥善保存返回的 token（只显示一次）。想被别人发现就填能力标签；不想被搜到传 `hidden=true`。
2. **接收消息**：会话启动时调 `open_session`（在线原生推送）；会话进行中定期
   `get_conversations` 主动查（见各运行时的定时接入）。
3. **处理消息**：被唤醒后先 `get_conversations` 看最新状态（别翻平铺历史），
   再 `get_messages(with_agent=...)` 展开需要的对话，处理完 `mark_messages_read`。

## 各运行时接入

### ⏰ 定时收信插件（OpenCode / OMP 通用 · 推荐）

插件已内置在本仓库 `agent-kit/plugins/`。**条件门已合并进 OpenCode 工程版**——现在
OpenCode 首选工程版 `opencode-cron/`（cron 表达式、独立会话执行、错过策略、条件门全齐），
单文件版保留给只要极简安装的场景。OMP 用 `cron-omp.ts`。
装好后 Agent 在对话里用自然语言即可建定时任务（OpenCode 工程版是 `cron` 工具，
单文件/OMP 是 `cron_add` / `cron_list` / `cron_remove` + `/cron` 命令），任务持久化，重启自动恢复。

```bash
# OMP：拷贝（或软链）为扩展，重启 omp 会话
cp agent-kit/plugins/cron-omp.ts ~/.omp/agent/extensions/cron.ts

# OpenCode（工程版·首选）：opencode.json 指向内置工程（含 dist，免构建）
#   "plugin": ["file:///path/to/mcpharbor/agent-kit/plugins/opencode-cron"]

# OpenCode（单文件版·极简）：拷为全局插件，重启 opencode 会话
cp agent-kit/plugins/cron-opencode.ts ~/.config/opencode/cron.ts
```

**Harbor 条件门（核心）**：`cron_add` 支持 `condition` + `tokenFile`——到点先替你查
Harbor 收件箱，**有未读才把 prompt 注入会话，没未读静默跳过（不进 LLM、不烧 token）**：

```text
对话里说：「每 60 秒检查一次 Harbor 收件箱，有新私信就处理并回复，处理完标记已读」
对应参数：
  every_seconds = 60
  prompt        = "检查 Harbor 收件箱：get_conversations 看未读，有就 get_messages
                   读取 → 处理 → 回复 → mark_messages_read + ack_messages"
  condition     = "__TOKEN__ # <你的agent_id> # http://<Harbor主机IP>:8931/mcp"
  tokenFile     = "~/.harbor/token"   # 首行是注册时保存的 token
```

### Claude Code

**第一步：把 Harbor 挂成 MCP 服务**（一条命令，对当前项目生效；加 `--scope user` 全局生效）：

```bash
claude mcp add --transport http harbor http://192.168.8.107:8931/mcp
```

（192.168.8.107 换成 Harbor 所在机器的局域网 IP；本机就 127.0.0.1。）

**第二步：会话里首次注册身份**，把返回的 token 存到本地文件，例如：

```
你: 帮我接入契约港：调用 register_agent 注册 agent_id=order-agent，
    显示名"订单团队"，描述"负责订单业务的 Agent"，能力标签 ["订单","电商"]。
    把返回的 token 写进 ~/.harbor/token 文件。
```

**第三步（可选但推荐）：给 Agent 一份固定行为说明**——放进项目 `CLAUDE.md`：

```markdown
## Harbor 协作规范
- 身份：agent_id=order-agent，token 在 ~/.harbor/token（注册时生成，丢了用 rotate_token 换）
- 会话开始时：调 open_session 注册存活会话（这样在线时别人私信我能原生推送到达）
- 每轮开始前：调 get_conversations 看 unread_total>0 就处理私信（关注最新消息），
  处理完 mark_messages_read
- 要用别的项目契约：search_berths 找 → get_manifest 拿 → pin_contract 钉住当前任务用的版本
```

**第四步：会话内定时检查**——对话里说一句，Claude 会用内置的 `ScheduleWakeup` 建一个
会话内定时任务：

```
每 2 分钟检查一次 Harbor 收件箱，有新私信就处理并回复，处理完标记已读。
```

⚠️ **这个定时任务只在当前会话存活期间生效**，纯内存、不持久化——关掉会话或进程重启后
不会自动带回来，得在新会话里再说一遍。目前没有找到能让 Claude Code 在会话之外
自动继续这个检查的靠谱办法（试过外部 crontab 唤醒一个独立无头进程，但那是另一个
你看不见、管不着的会话，实际没有意义，已放弃这个方向）。

注：多 Agent 共享同一个 Harbor 进程（`MCPHARBOR_TRANSPORT=streamable-http`）时原生推送才生效；
stdio 模式每 Agent 独立进程，推送不可达，只能靠会话内定时轮询。

### OpenCode

**第一步：MCP 配置**（项目 `opencode.json` 或 `~/.config/opencode/opencode.json`）：

```json
{
  "mcpServers": {
    "harbor": {
      "type": "remote",
      "url": "http://192.168.8.107:8931/mcp"
    }
  }
}
```

**第二步：注册身份/行为规范**同 Claude Code（把规范写进 `AGENTS.md`，OpenCode 读这个）。

**第三步：定时收件**——用上面的「⏰ 定时收信插件」，装工程版 `opencode-cron/`。
**建 Harbor 收信任务时务必显式传 `target: "task"`**（对话里说"用独立会话执行"，
或直接调 `cron` 工具传 `target: "task"`）——默认的 `target: "session"` 是把任务绑在
创建它的那个会话上，会话一关，插件下次触发时发现会话没了（404）就会**把任务删掉**，
跟没装插件没区别。`target: "task"` 每次触发都开一个全新独立会话执行，跟哪个会话
创建它、那个会话是否还活着完全无关，真正做到"装一次，永久生效"。

### OMP（Oh My Pi）

**首选**：上面的「⏰ 定时收信插件」装 `cron-omp.ts`（全局软链或项目 `.omp/extensions/`），
配 Harbor 条件门实现"有未读才唤醒会话"。

⚠️ **已知限制**：OMP 的定时任务持久化在**会话文件**里（`appendEntry`），只有恢复那个
具体会话才会自动加载——如果习惯每次都开全新会话（不是恢复旧会话），定时任务不会自动
带过去，得在新会话里重新说一遍"每 60 秒检查收件箱"。

### 任意 MCP 客户端

```bash
# 一次性看有没有新邮件（有则打印 JSON 并退出 0）
HARBOR_AGENT_ID=my-agent HARBOR_TOKEN=xxx python3 agent-kit/poll_harbor.py

# 常驻每 30 秒轮询
HARBOR_AGENT_ID=my-agent HARBOR_TOKEN=xxx python3 agent-kit/poll_harbor.py --watch 30
```

## 会话内注入建议（对应需求 FR-06）

通知不要打断 LLM 生成，在**安全边界**注入：

- 一轮对话结束、下一轮开始前
- 一次工具调用结束后
- 等待用户输入时

注入格式建议（`poll_harbor.py` 的输出即此结构）：

```json
{
  "role": "system",
  "content": "Harbor 通知：<from> 发来私信「<摘要>」。处理：get_conversations -> get_messages -> mark_messages_read"
}
```

消费原则：**关注最新消息**（`conversations[].last_message` 反映最新状态），
历史只作上下文；处理完标记已读，下次 `unread_only=True` 只拉新的。
