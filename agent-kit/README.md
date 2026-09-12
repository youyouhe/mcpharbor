# Harbor Agent Kit —— Agent 侧接入工具箱

Harbor 是控制面：它不主动找到你，你要么**在线收推送**，要么**定时轮询**。
这个目录把两种方式的接入做成可复用的标准件，新 Agent 接入不用再手写脚本。

```
agent-kit/
├── poll_harbor.py    # 零依赖轮询脚本（Python 标准库）：未读私信 + 对话列表 + 契约钉状态
├── harbor_gate.sh    # 门禁壳：有未读 → 输出 payload 退出 0；没动静 → 静默退出 1
├── plugins/          # 内置定时收信插件
│   ├── cron-omp.ts           # OMP 单文件版（含 Harbor 条件门 condition/tokenFile）
│   ├── cron-opencode.ts      # OpenCode 单文件版（同上，极简安装用）
│   └── opencode-cron/        # OpenCode 工程版（首选：cron 表达式/独立会话/错过策略 + 条件门已合并）
└── README.md         # 本文件：各运行时接入指南
```

## 统一环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `HARBOR_URL` | Harbor MCP 端点 | `http://127.0.0.1:8931/mcp` |
| `HARBOR_AGENT_ID` | 你的 agent_id（register_agent 注册的） | 必填 |
| `HARBOR_TOKEN` | 注册时返回的 token | 必填 |

## 接入三步

1. **注册身份**（一次性）：调 `register_agent(display_name, description, capabilities=...)`，
   妥善保存返回的 token（只显示一次）。想被别人发现就填能力标签；不想被搜到传 `hidden=true`。
2. **接收消息**：会话启动时调 `open_session`（在线原生推送）；会话空闲期间由
   `harbor_gate.sh` + cron 兜底（见下）。
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

**第四步：空闲唤醒（会话关着也能收到私信）**——系统 cron + 门禁 + 无头模式：

```bash
# crontab -e  （每分钟看一眼，有未读才动）
* * * * * HARBOR_AGENT_ID=order-agent HARBOR_TOKEN=$(cat ~/.harbor/token) \
  /path/to/mcpharbor/agent-kit/harbor_gate.sh >> ~/.harbor/gate.log 2>&1 \
  && claude -p "你有新的 Harbor 私信：$(tail -1 ~/.harbor/gate.log)。读取处理并回复对方，处理完标记已读。" \
     >> ~/.harbor/wake.log 2>&1
```

原理：`harbor_gate.sh` 有未读时退出 0（`&&` 才触发无头 claude），没动静静默退出 1 不打扰。
无头 `claude -p` 会自动连上 MCP，按 CLAUDE.md 里的规范处理收件箱。

注：多 Agent 共享同一个 Harbor 进程（`MCPHARBOR_TRANSPORT=streamable-http`）时原生推送才生效；
stdio 模式每 Agent 独立进程，推送不可达，必须走本门禁轮询。

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

**第三步：定时收件**——用上面的「⏰ 定时收信插件」（OpenCode 无内置 cron，装插件即得
会话内定时 + Harbor 条件门），这是推荐的唯一方式。

### OMP（Oh My Pi）

**首选**：上面的「⏰ 定时收信插件」装 `cron-omp.ts`（全局软链或项目 `.omp/extensions/`），
配 Harbor 条件门实现"有未读才唤醒会话"，无需外部 cron。

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
