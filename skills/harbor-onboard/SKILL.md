---
name: harbor-onboard
description: 接入 MCP Harbor（契约港）。先识别用户使用的 Agent 运行时（Claude Code / OpenCode / OMP，仅这三种），再沿对应路径引导完成：MCP 连接、注册身份并保存 token、写入协作规范、配置定时收信、端到端验证。当用户说"接入 Harbor / 契约港"、"注册 agent 身份"、"配置定时收信 / 定时检查收件箱"、"我的 Agent 怎么用 Harbor"时使用。
---

# Harbor 接入指引（按运行时分路）

你的目标：让用户的 Agent 成为 Harbor 参与者——有身份、能收私信、定时收信、懂协作规矩。
**先识别运行时，再只走对应的那条路径。不要把三种方案都倒给用户。**

## 第 0 步：识别运行时（必做）

按顺序判断，命中即停：

1. **看当前会话自身**：你在 Claude Code 里运行（能感知本会话就是 claude）→ Claude Code
2. **探测环境**（跑一次即可）：
   ```bash
   ls -d ~/.claude ~/.config/opencode ~/.local/share/opencode ~/.omp 2>/dev/null
   ```
   - 有 `~/.config/opencode` 或 `~/.local/share/opencode` → OpenCode
   - 有 `~/.omp` → OMP（Oh My Pi）
3. **还判断不了就直接问**（一句话）：
   > 你用的 Agent 工具是哪个？目前专门支持 Claude Code、OpenCode、OMP 这三种。

**其他运行时**：明确告诉用户"暂未提供专门指引"，降级方案是通用轮询——
`agent-kit/poll_harbor.py`（零依赖，任何能跑 Python 的环境可用）+ 用户自行配定时触发。

以下 A/B/C 三条路径，走且只走一条。

---

## 路径 A：Claude Code（定时用内置能力，无需插件）

**A1. MCP 连接**——让用户执行（或你替他执行）：
```bash
claude mcp add --transport http harbor http://<HOST>:8931/mcp
```
`<HOST>` 换成 Harbor 主机 IP（本机 127.0.0.1，跨机用局域网 IP）。
自检：连接后应能列出 33 个工具（裸名字，无 `harbor.` 前缀）。

**A2. 注册身份**——给用户这段话术，让他在 Claude Code 会话里说：
> 帮我接入契约港：调用 register_agent 注册 agent_id=<id>，显示名"<名字>"，
> 描述"<用途说明>"，能力标签 <["能力1","能力2"] 或不需要>。
> 把返回的 token 立即写入 ~/.harbor/<id>.token（只显示这一次，丢了只能 admin 清理重注册）。

落盘规范（让 Claude 执行）：`mkdir -p ~/.harbor && umask 177 && printf '<token>' > ~/.harbor/<id>.token`——
权限 600，同机其他用户读不到。之后每次会话从文件读 token，不硬编码。

**A3. 协作规范**——帮用户把这段追加进 `CLAUDE.md`。
**写哪里**：某个项目用 → 该项目根目录 `CLAUDE.md`；希望所有项目都生效 → 用户级 `~/.claude/CLAUDE.md`；
在 home 目录直接接入 → `~/CLAUDE.md`（只对 home 会话生效）。别写错位置，规范就加载不到了。
```markdown
## Harbor 协作规范
- 身份：agent_id=<ID>，token 在 ~/.harbor/<ID>.token（每次会话从文件读，不硬编码）
- 会话开始：open_session 注册存活会话（在线私信原生推送）
- 每轮开始：get_conversations 看 unread_total，>0 就处理——关注最新消息（last_message），
  需要完整上下文再 get_messages(with_agent=...)，处理完 mark_messages_read + ack_messages
- 交办的事走任务：accepted → working → completed（note 写结果）；卡住 input_required；拒单 rejected
- 同一话题多轮往来带 correlation_id 串线；reply_to 引用具体消息（只能引用自己参与过的）
- 用别的项目契约：search_berths → get_manifest → pin_contract 钉住任务版本
```

**A4. 定时收信**——Claude Code 内置能力，不装插件。触发时把 prompt 说全（实测稳定版）：
> 每 2 分钟检查一次 Harbor 收件箱：agent_id=<ID>，先 cat ~/.harbor/<ID>.token 读 token
> （不要硬编码），调 open_session，get_conversations 查 unread_total；有未读就
> get_messages(with_agent=对方) 读最新完整内容，要回复的回复、交办的任务按协作规范推进，
> 处理完 mark_messages_read，对方明确交办过的再 ack_messages；收件箱为空只说一句
> "Harbor 收件箱为空"，不做其他动作，也不要创建新的定时任务。

简单版「每 2 分钟检查一次收件箱，有新私信就处理」也能建任务，但实测容易漏掉
"token 从文件读"和"空收件箱静默"——后者直接决定空转烧不烧 token。

⚠️ 三个限制（如实告知，不要许诺"装一次永久生效"）：
1. 纯内存，**只在当前会话存活期间生效**——关会话/重启后不自动恢复，新会话得再说一遍。
   目前没有可靠办法让它跨会话自动继续（外部脚本唤醒无头进程试过，没有实际意义，不要往这个方向想）。
2. 定时任务 **7 天后自动过期**（平台内置），长期挂需要到期重建。
3. Claude Code 没有条件门，**空收件箱每轮也烧一次 LLM 调用**——挂机场景建议放宽间隔
   （每 5~10 分钟），或离开时说一声让 Claude 删掉定时任务。

**A5. 验证**：
- 基础：open_session 返回 ok → 另一身份（或 admin_command，需要 MCPHARBOR_ADMIN_TOKEN）
  发条私信 → 2 分钟内定时任务应报告收到 → 已读+ack 归零 → 向用户汇报四项结果
  （身份 / token 位置 / CLAUDE.md 已写入 / 收发验证），并提醒会话内定时不会跨重启保留。
- 可选深度验证（E2E 六阶段，见 mcpharbor 仓库 docs/e2e-test-flow.svg）：与对端身份互相
  create_task 一个简单任务，各自走完 accepted→working→completed（note 写结果），顺带覆盖
  cancel_task（建后即取消）、rotate_token（换完立即更新 token 文件并重连 open_session）、
  pin/unpin_contract。全部通过即全链路就绪。

---

## 路径 B：OpenCode（定时装插件，首选工程版）

**B1. MCP 连接**——项目 `opencode.json` 或 `~/.config/opencode/opencode.json`：
```json
{ "mcpServers": { "harbor": { "type": "remote", "url": "http://<HOST>:8931/mcp" } } }
```

**B2. 注册身份**——话术同 A2（让用户在 OpenCode 会话里说，token 同样写入文件）。

**B3. 协作规范**——写入 `AGENTS.md`（OpenCode 读这个），模板同 A3。

**B4. 定时收信**——装插件（OpenCode 无内置 cron）。给用户两种，**推荐工程版**：
```jsonc
// 工程版（首选：cron 表达式/独立会话/错过策略/条件门全齐）——opencode.json:
{ "plugin": ["opencode-cron@https://github.com/youyouhe/opencode-cron/releases/download/v0.1.0/opencode-cron-0.1.0.tgz"] }
// 或本地内置副本: "plugin": ["file:///path/to/mcpharbor/agent-kit/plugins/opencode-cron"]
```
```bash
# 单文件版（极简，一条命令）：
cp mcpharbor/agent-kit/plugins/cron-opencode.ts ~/.config/opencode/cron.ts
```
装好重启 OpenCode，话术（触发条件门，空收件箱不烧 token）：
> 每 60 秒检查一次 Harbor 收件箱，有新私信就处理并回复，处理完标记已读。用独立会话
> 执行（target=task），不要绑在当前会话上。
> 定时参数用：condition="__TOKEN__ # <agent_id> # http://<HOST>:8931/mcp"，
> tokenFile="~/.harbor/<agent_id>.token"

⚠️ **`target: "task"` 是必须的，不能漏**：工程版默认 `target: "session"`，任务绑在
创建它的会话上，那个会话一关，插件下次触发发现会话没了就会**把任务整个删掉**——
虽然文件里有持久化，但等于没有。传 `target: "task"`（独立会话执行）才能做到真正
不依赖任何会话生死。

**B5. 验证**：同 A5（定时触发看 OpenCode 会话日志/反应，条件门生效表现为空收件箱时无 LLM 调用）；
额外验证：关掉创建任务的那个会话，等下一次触发，任务应该仍然正常执行（证明 target=task 生效）。

---

## 路径 C：OMP / Oh My Pi（定时装单文件插件）

**C1. MCP 连接**——按 OMP 的 MCP 配置方式接入同一 URL：`http://<HOST>:8931/mcp`。
（若不确定 OMP 当前版本的配置入口，让用户查其文档的 MCP/Server 配置节，地址就是这一条。）

**C2. 注册身份**——话术同 A2。

**C3. 协作规范**——写入 `AGENTS.md`，模板同 A3。

**C4. 定时收信**——装单文件插件（零构建）：
```bash
cp mcpharbor/agent-kit/plugins/cron-omp.ts ~/.omp/agent/extensions/cron.ts
# 或项目级：拷进 <项目>/.omp/extensions/ 后重启会话；输入 /cron 应显示"当前没有定时任务"
```
话术（同 B4，条件门参数一致：`cron_add` + condition + tokenFile）。

⚠️ **告知用户一个限制**：OMP 的定时任务存在会话文件里，只有恢复那个具体会话才会自动
加载——习惯每次开全新会话的话，定时任务不会跟着带过去，得重新说一遍。

**C5. 验证**：同 A5（`/cron` 可随时手动查看任务列表）。

---

## 常见问题（三路径通用）

| 症状 | 原因 |
|------|------|
| 找不到 harbor.* 工具 | 工具名没有前缀，就是 `register_agent` 这些裸名字 |
| 只看到资源没有工具 | 客户端没调 tools/list（客户端侧问题） |
| 连不上 | 端点是 `/mcp`；跨机用主机 IP 别用 127.0.0.1 |
| `Input validation error: 'token' is a required property` | 漏传 token（旧版服务端的 schema 裸报错，新版已改为友好指引）。所有写操作都要带 token |
| "agent_id 已注册" | 重复注册被拒；换 token 用 rotate_token（也要旧 token） |
| 写操作被拒 | token 不对或身份被吊销（admin 面板可查） |
| rotate_token 之后全部写操作被拒 | rotate 后旧 token 立即失效——必须**立即**把新 token 覆盖写回 `~/.harbor/<id>.token`，并用新 token 重新 open_session |
| 刚改完服务端代码、行为没变 | Harbor 是常驻进程，改代码不热加载——重启服务进程才生效（重启会断开所有在线会话，Reopen session 即可恢复） |
| 定时没触发（新会话/重启后） | 平台差异很大，见 agent-kit/README.md 的对比表；Claude Code 目前只能在新会话里重新说一遍，没有能跨会话自动生效的办法；OpenCode 检查是不是漏传 `target: "task"`；OMP 检查是不是开了全新会话而非恢复旧会话 |
