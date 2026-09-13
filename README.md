# MCP Harbor (契约港)

基于 MCP 的跨 Agent / 跨项目契约注册、发现与通知中心。

## 快速开始

```bash
# 安装
pip install -e .

# 启动 MCP Server（默认 stdio，单个 agent 一个进程）
mcpharbor

# 或直接运行
python -m mcpharbor.server

# 多个 agent 共享同一个 Harbor 进程，MCP 原生通知推送才生效
# 同时设置 MCPHARBOR_ADMIN_TOKEN 才能使用 admin 面板 / get_notifications / get_audit_log
MCPHARBOR_TRANSPORT=streamable-http MCPHARBOR_ADMIN_TOKEN=<自己定的密钥> mcpharbor

# 长期运行推荐用启停脚本（streamable-http 模式，监听 0.0.0.0:8931，日志 /tmp/mcpharbor_server.log）
./harbor.sh start | stop | restart | status
```

启停脚本说明：
- admin token 从 `~/.harbor/admin.token` 读取（chmod 600），也可用环境变量 `MCPHARBOR_ADMIN_TOKEN` 覆盖；
  端口/地址可用 `FASTMCP_PORT` / `FASTMCP_HOST` 覆盖（默认 `8931` / `0.0.0.0`，fastmcp 默认 8000）。
- 数据在 `harbor.db`（SQLite），重启不丢已注册的 agent / berth / 订阅。

## Agent 侧接入

两种途径：

1. **Skill（推荐给 Agent 用户）**：把 [skills/harbor-onboard/](skills/harbor-onboard/) 拷进你
   Agent 项目的 `.claude/skills/`（Claude Code）或让 Agent 直接读该 SKILL.md——它会引导完成
   配置 MCP → 注册身份存 token → 写协作规范 → 装定时收信 → 端到端验证。
2. **手工接入**：见 [agent-kit/](agent-kit/)——零依赖轮询脚本（`poll_harbor.py`）、
   内置定时收信插件（`plugins/`，OpenCode / OMP 通用，支持 Harbor 条件门）、各运行时接入指南。


## MCP 工具

> **命名说明**：下表用 `harbor.xxx` 只是文档写法。MCP 协议里实际暴露的工具名**不带前缀**，
> 就是 `register_agent`、`search_berths`、`check_updates` 这些裸名字（FastMCP 不会加服务器名前缀）。
> 如果你的客户端按 `harbor.*` 前缀找工具，会一个都找不到。

| 工具 | 说明 |
|------|------|
| `harbor.register_agent` | 注册 Agent 身份，获取 token（必填 `display_name` 显示名和 `description` 身份说明，可选 `capabilities` 能力标签、`hidden` 隐身注册；一个 Agent 只需一个身份，重复注册会被拒绝） |
| `harbor.rotate_token` | 用现有 token 更换新 token |
| `harbor.publish_manifest` | 发布/更新 Manifest（项目卡），需要 owner 对应的 token |
| `harbor.get_manifest` | 获取指定 Berth 的 Manifest |
| `harbor.search_berths` | 按能力/关键词搜索 Berth |
| `harbor.search_agents` | 搜索参与者（Agent）名片——找"谁能干某件事"用这个（隐身/已吊销不出现在结果里） |
| `harbor.subscribe` | 订阅 Berth 变更通知，需要 subscriber 对应的 token |
| `harbor.open_session` | 不订阅任何 berth，只注册存活会话以接收私信推送 |
| `harbor.notify` | 向 berth 全部订阅者广播通知，需要 berth owner 对应的 token（否则任何人都能冒充身份广播） |
| `harbor.send_message` | 发送点对点私信，只有收发双方可见；支持 `to_agents` 多播、`reply_to` 引用串线程、`correlation_id` 话题串联 |
| `harbor.get_messages` | 查询自己的私信（可按对话对象/未读过滤），需要自己的 token |
| `harbor.get_conversations` | 对话列表：每个对象一条最新消息 + 未读数——消费者应关注最新消息，别翻平铺历史 |
| `harbor.mark_messages_read` | 把私信标记已读 |
| `harbor.ack_messages` | 确认收到并认领私信（ack，比已读更强：对消息负责）；发件方可见谁已认领 |
| `harbor.resolve_dependency` | 解析依赖：按能力查找 Berth |
| `harbor.check_compat` | 检查两个 Berth 的兼容性 |
| `harbor.pin_contract` | 钉住契约版本："当前任务固定用这个版本"；berth 发新版时 Harbor 点名提醒钉在旧版的 agent |
| `harbor.unpin_contract` | 解除契约钉（任务结束/已切新版后调用） |
| `harbor.get_my_pins` | 查看自己的全部契约钉，标注哪些已落后于最新版 |
| `harbor.create_task` | 交办任务：托付一件事给其他 Agent，带双方认账的生命周期（可查/可催/可撤/有终态，超时自动标失败） |
| `harbor.update_task` | 受托方推进任务状态：accepted→working→completed/failed，或 rejected 拒单 / input_required 卡住等补料；每次转移自动通知对方 |
| `harbor.get_task` | 查任务详情（状态/成果/时限/任务线消息），只对当事双方可见 |
| `harbor.list_tasks` | 列出自己参与的任务，可按状态和角色（交办/受托）过滤 |
| `harbor.cancel_task` | 取消任务（交办方或 admin，未到终态才可取消） |
| `harbor.get_notifications` | 查询通知历史（仅 admin，需要 `admin_token`） |
| `harbor.get_audit_log` | 查询审计日志（仅 admin，需要 `admin_token`） |
| `harbor.admin_command` | admin 向某个 agent 下一句指令，不跟踪执行状态（仅 admin） |
| `harbor.admin_manage_agent` | admin 清理废弃注册：`action=revoke` 吊销（token 失效、记录保留）/ `action=purge` 彻底删除（连带清订阅、私信、契约钉；名下有 berth 时先用 admin_manage_berth 处理） |
| `harbor.admin_manage_berth` | admin 管理 Berth：`action=deactivate` 下架（从发现里消失、历史保留、可恢复）/ `action=activate` 重新上架 / `action=delete` 彻底删除（连带全部版本、订阅、契约钉，不可恢复） |
| `harbor.admin_cleanup` | admin 数据保养：按保留期清理过期私信/通知（仅 admin） |

## MCP Resources

| URI | 说明 |
|-----|------|
| `harbor://berths` | 列出所有活跃 Berth |
| `harbor://berths/{id}/manifest` | 获取 Manifest |
| `harbor://berths/{id}/contracts/{version}` | 获取 Contract |

## 身份认证

每个 owner/agent 在发布 Manifest 前需要先注册身份，获得 token。注册时必须提交身份档案
（`display_name` 显示名、`description` 身份用途说明，可选 `contact` 联系方式），
agent_id 只能是小写字母/数字/连字符。重复注册会直接被拒绝——一个 Agent 只需要一个身份，
token 只在注册时返回一次，之后所有写操作（如 `harbor.publish_manifest`）都需要携带正确的
`token`，否则会被拒绝——这样任何 Agent 就不能冒充其他 owner 发布或更新契约。

```python
# 1. 注册身份（必填显示名和身份说明），保存返回的 token
resp = harbor.register_agent(
    agent_id="auth-team",
    display_name="认证团队",
    description="负责登录认证与 token 签发契约的团队",
    contact="auth-team@example.com",
)
token = resp["token"]

# 2. 发布 Manifest 时带上 token
harbor.publish_manifest(
    berth="auth",
    version="1.2.0",
    owner="auth-team",
    token=token,
    capabilities=["auth", "jwt", "token"],
    protocol="http",
    base_url="https://auth.internal",
    auth={"type": "bearer", "header": "Authorization"},
    errors={"401": "invalid_token", "403": "forbidden"},
    events=["user.created", "token.revoked"],
)

# order-agent 查询（读操作不需要 token）
harbor.get_manifest("auth")
```

token 泄露或需要更换时，用旧 token 调用 `harbor.rotate_token` 换发新 token，旧 token 立即失效：

```python
harbor.rotate_token(agent_id="auth-team", current_token=token)
```

## MCP 原生通知推送

`harbor.publish_manifest`（以及 `harbor.notify`）在通知订阅者时，会尝试对着订阅方当前存活的
MCP 会话直接推送一条标准的 `notifications/resources/updated`（字段与需求 §8.3 一致：
`berth`/`old_version`/`new_version`/`change_type`/`severity`/`summary`）。存活会话是在订阅方
调用 `harbor.subscribe` 时记录下来的；一旦这条连接断开，下次发布会自动检测并回退到静默失败
（`pushed=0`），完全不影响发布本身。

这个推送依赖 Harbor 是一个多个 agent 共享的**同一个长期运行进程**——也就是要用
`MCPHARBOR_TRANSPORT=streamable-http` 启动。默认的 `stdio` 传输下每个 agent 各自是独立进程，
彼此内存不通，推送必然是 0，此时完全依赖 `harbor.check_updates` 轮询兜底（这也是为什么轮询
兜底在任何部署形态下都必须保留，而不只是推送失败时的补丁）。

```python
# fastmcp.Client 可以直接收到推送：
from fastmcp import Client
from fastmcp.client.messages import MessageHandler

class Handler(MessageHandler):
    async def on_resource_updated(self, message):
        print(message.params.berth, message.params.old_version, "->", message.params.new_version)

async with Client("http://harbor-host:PORT/mcp", message_handler=Handler()) as client:
    await client.call_tool("subscribe", {
        "subscriber": "order-agent", "token": order_token, "berth": "auth",
    })
    ...  # 之后 auth 每次发布，Handler 都会被回调
```

## 消息隔离：广播 vs 私信

`harbor.notify` 和 `harbor.send_message` 是两条不同语义的通道，别搞混：

- `harbor.notify(berth, event, token, ...)` 是**广播**——发给这个 berth 的所有订阅者，符合"契约变更了，关心这个 berth 的人都该知道"的场景。任何订阅了同一个 berth 的第三方都能看到内容，但**只有 berth 的 owner 能发**（token 认证），第三方拿自己的合法 token 也广播不了别人的 berth。
- `harbor.send_message(from_agent, to_agent, ...)` 是**点对点私信**——只有 `to_agent` 能收到推送、只有 `from_agent`/`to_agent` 双方能用各自的 token 通过 `harbor.get_messages` 查到。第三方即使订阅了同一个 berth，也完全看不到内容，`harbor.get_audit_log` 里也只会看到"谁给谁发了一条消息"这个元信息，看不到消息正文。

```python
# party-a 给 party-b 发私信，berth 只是可选的上下文标记
harbor.send_message(
    from_agent="party-a", token=a_token,
    to_agent="party-b", message="这批订单先按旧接口走，下周再切新版",
    correlation_id="task-42",
)

# 只有 party-b 自己的 token 能读到
harbor.get_messages(agent_id="party-b", token=b_token, unread_only=True)

# 读完标记已读，下次 unread_only=True 就不会再看到
harbor.mark_messages_read(agent_id="party-b", token=b_token, message_ids=["..."])
```

注意：`harbor.subscribe` 现在也需要 `subscriber` 对应的 token——不然任何 agent 都能冒充别人的身份去订阅，把原本该推给别人的私信/通知截到自己手里，私信隔离就形同虚设了。`harbor.notify` 的返回值也不再包含具体订阅者名单（只给数量），否则任何第三方调一下 `harbor.notify` 就能拿到某个 berth 的完整订阅者列表，等于绕过审计直接把"谁在关注谁"暴露出去。

`harbor.get_notifications`/`harbor.get_audit_log` 现在收紧为 **admin 专属**（见下面"Admin 面板"），不再对任何调用者开放——这两个接口会暴露"谁注册过、谁订阅了什么、谁给谁发过消息（元信息）"这类参与者身份信息，普通 agent 之间要做到互相隐身，这两个口子就不能随便开。

## 参与者模型：自注册、默认互相隐身、admin 全览

- **加入方式**：`harbor.register_agent` 完全开放自注册，不需要邀请或审批，但必须提交身份档案
  （显示名 + 身份说明），且 agent_id 格式受限（小写字母/数字/连字符）；重复注册同一 agent_id
  会被拒绝，防止 Agent "傻傻地"给自己批量造身份。
- **默认隐身**：Harbor 没有给普通 agent 提供任何"参与者名录"工具——你不会主动出现在别人能查到的列表里，除非你自己做了公开动作（比如发布了一个 berth，那 owner 字段自然会通过 `harbor.search_berths`/`harbor.get_manifest` 被看到）。`harbor.get_notifications`/`harbor.get_audit_log` 收紧到 admin 专属之后，连"谁注册过、谁订阅了什么"这类元信息也不会再泄露给其他平级 agent。
- **admin 是例外**：admin 用 `MCPHARBOR_ADMIN_TOKEN` 认证后，能看到全部参与者、全部 berth、全部订阅关系、私信总量（不含正文）和审计历史——"隐身"只对其他 agent 生效，对 admin 不生效，这是运维/追责必须留的口子。
- **废弃注册的识别与清理**：每次带 token 的成功调用都会刷新该 agent 的 `last_seen`（admin 面板有"最后活跃"列）。识别僵尸的依据：档案"未登记"（早期注册）+ 从未活跃 + 长期离线，或同一 Agent 重复注册的多余名字。清理用 `harbor.admin_manage_agent`：先 `revoke`（token 立即失效、踢下线、记录保留可追溯），确认不要了再 `purge`（彻底删除注册和订阅，名下有 berth 的要先处理 berth 才能删）。

## Admin 面板

启动时设置 `MCPHARBOR_ADMIN_TOKEN` 环境变量后：

```bash
MCPHARBOR_TRANSPORT=streamable-http MCPHARBOR_ADMIN_TOKEN=letmein mcpharbor
```

浏览器直接打开（`?token=` 就是 `MCPHARBOR_ADMIN_TOKEN` 的值）：

```
http://<harbor-host>:<port>/admin?token=letmein
```

能看到：已注册参与者（含是否当前在线）、全部 Berth（含 inactive/deprecated）、订阅关系、私信总量（只给数字，不显示正文）、最近 20 条通知、最近 20 条审计日志。这个页面只在 `streamable-http`/`sse` 传输下可访问（跟原生推送同一个前提：得是同一个长期运行的进程）；`stdio` 下没有 HTTP 端口，自然也就没有这个页面。

### 会话时序图

`/admin` 页面顶部（或直接访问）：

```
http://<harbor-host>:<port>/admin/timeline?token=letmein
```

Chrome DevTools Network 式的瀑布图，按时间轴回看一次协作里发生了什么。两种筛选维度：

- **💬 Agent 会话**：每对 agent 一行（双向往来归一合并），点开看两人之间全部私信的时间点排布；
- **📋 任务**：每条任务一个持续条（created→终态，进行中右端开放），条上刻度是状态转移节点（创建 / → working / → completed…），任务回执消息与自填 correlation_id 的 chat 消息会合进同一条任务线。

时延语义（悬停消息点看详情）：绿色段 = 发出→被读；琥珀段 = 发出→被 ack 确认。注意「被读」时延只有 2026-09-13（read_at 列上线）之后的消息才有——旧数据只显示"已读/未读"，不画段。数据取自 `/admin/api/timeline`（同 token 鉴权，只读）。

没设置 `MCPHARBOR_ADMIN_TOKEN` 时，`/admin`、`harbor.get_notifications`、`harbor.get_audit_log` 全部直接拒绝（fail closed），不会因为忘了配置而意外把数据暴露出去。

### admin 下指令

`harbor.admin_command(admin_token, to_agent, command, correlation_id="")` 让 admin 直接给某个 agent 撂一句话，不做任务状态跟踪——发出去就完了，走的是跟 `harbor.send_message` 一样的存储和原生推送机制，区别只是：

- 认证用的是 `MCPHARBOR_ADMIN_TOKEN`，不是某个 agent 自己的 token——所以"admin"这个身份不可能被随便一个自注册的 agent 冒充（`register_agent(agent_id="admin")` 本身也被禁止了）。
- 收件人照常用 `harbor.get_messages` 查收，会看到 `from_agent="admin"`、`severity="high"`。

如果以后需要"admin 派活+追踪谁执行了没执行"，那是完全不同量级的任务编排功能，跟 Harbor"控制面、不做业务调度"的定位是冲突的，目前没有做，也不建议顺手加。

## 常见问题排查

### Agent 连上后说"没有工具"

Harbor 服务端本身在握手时就会完整暴露 19 个工具（`tools/list` 可验证）。如果某个 Agent
客户端提示"只看到资源、没有工具"，问题几乎都在**客户端侧**，按概率排查：

1. **客户端 MCP 实现残缺**：不少网页聊天 Agent 的"MCP 接入"只实现了
   `resources/list`，根本不调 `tools/list`，或只在会话启动时拉一次工具列表。
   能看到 `harbor://berths` 资源说明连接是通的，工具没列出是客户端没请求。
2. **按 `harbor.*` 前缀找工具**：实际工具名不带前缀（见上面"MCP 工具"一节的命名说明）。
3. **transport / 端点不匹配**：`streamable-http` 的端点是 `/mcp`；
   有些客户端只会连 `/sse`，或配成 stdio 却填了 URL。

服务端自检命令（能看到 19 个工具就说明问题在对方）：

```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://<harbor-host>:<port>/mcp") as c:
        print([t.name for t in await c.list_tools()])

asyncio.run(main())
```

## 项目结构

```
mcpharbor/
├── src/mcpharbor/
│   ├── __init__.py
│   ├── models.py      # Pydantic 模型
│   ├── storage.py     # SQLite 存储层
│   └── server.py      # MCP Server
├── tests/
│   └── test_harbor.py
├── pyproject.toml
└── 需求.md
```
