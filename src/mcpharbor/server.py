"""MCP Harbor Server - 契约注册、发现与通知中心。"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import mcp.types as mcp_types
from fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from .models import (
    AuditEntry, Berth, BerthStatus, Contract, ContractPin, DirectMessage, Manifest,
    Notification, NotifyPriority, Subscription, Task, TaskStatus,
)
from .storage import HarborStorage

# ── 任务状态机 ──
# 终态之后冻结；worker 态（执行类转移）只能由 assignee 推动，canceled 只能由 creator/admin 推动。
_TASK_TERMINAL = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED, TaskStatus.REJECTED}
_TASK_WORKER_MOVES = {TaskStatus.ACCEPTED, TaskStatus.WORKING, TaskStatus.INPUT_REQUIRED,
                      TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.REJECTED}
_TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.CREATED: {TaskStatus.ACCEPTED, TaskStatus.REJECTED, TaskStatus.CANCELED},
    TaskStatus.ACCEPTED: {TaskStatus.WORKING, TaskStatus.CANCELED},
    TaskStatus.WORKING: {TaskStatus.INPUT_REQUIRED, TaskStatus.COMPLETED,
                         TaskStatus.FAILED, TaskStatus.CANCELED},
    TaskStatus.INPUT_REQUIRED: {TaskStatus.WORKING, TaskStatus.CANCELED},
}

# 会话健康状态：agent_id -> {"alive": bool, "checked_at": iso}
# 心跳只探测、只记录、只展示，绝不把会话从 _live_sessions 踢掉。
_session_health: dict[str, dict[str, Any]] = {}
_HEARTBEAT_INTERVAL_SECONDS = 30
_HEARTBEAT_TIMEOUT_SECONDS = 5


async def _heartbeat_loop() -> None:
    """定时对登记在册的活跃会话发 MCP ping，记录存活状态；顺带做任务超时扫尾。"""
    while True:
        for agent_id, session in list(_live_sessions.items()):
            alive = True
            try:
                await asyncio.wait_for(session.send_ping(), timeout=_HEARTBEAT_TIMEOUT_SECONDS)
            except Exception:
                alive = False
            _session_health[agent_id] = {
                "alive": alive,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
        try:
            await _sweep_stale_tasks()
        except Exception:
            pass  # 扫尾失败不影响心跳主职责，下一轮再试
        await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)


@asynccontextmanager
async def _harbor_lifespan(server):
    """服务启动时拉起心跳循环，关闭时停掉。"""
    task = asyncio.create_task(_heartbeat_loop())
    yield
    task.cancel()


mcp = FastMCP(
    "MCP Harbor",
    instructions="契约注册、发现与通知中心 - 各 Agent 项目的契约港",
    lifespan=_harbor_lifespan,
)

_store: HarborStorage | None = None

# Agent 订阅时（harbor.subscribe）留下的活跃会话，仅用于原生通知推送。
# 只在多客户端共享的传输（如 streamable-http）下才跨 agent 有效；
# stdio 下每个 agent 是独立进程，推送不到彼此，只能靠 harbor.check_updates 轮询兜底。
_live_sessions: dict[str, ServerSession] = {}

# "admin" 是 harbor.admin_command 用来标记发件人的保留身份，不能被普通 agent 注册，
# 否则谁都能抢注这个名字，冒充真正的（用 MCPHARBOR_ADMIN_TOKEN 认证的）admin。
_RESERVED_AGENT_IDS = {"admin"}


def _get_store() -> HarborStorage:
    global _store
    if _store is None:
        _store = HarborStorage()
    return _store


def _audit(action: str, actor: str, target: str, detail: dict[str, Any] | None = None) -> None:
    entry = AuditEntry(action=action, actor=actor, target=target, detail=detail or {})
    _get_store().log_audit(entry)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _generate_token() -> str:
    return secrets.token_urlsafe(32)


async def _push_notification(recipient: str, resource_uri: str, **extra: Any) -> bool:
    """尝试向 recipient 当前存活的会话推送 notifications/resources/updated。

    找不到活跃会话，或推送失败（连接已断开），返回 False，交给轮询/收件箱兜底。
    """
    session = _live_sessions.get(recipient)
    if session is None:
        return False
    try:
        await session.send_notification(
            mcp_types.ServerNotification(
                mcp_types.ResourceUpdatedNotification(
                    params=mcp_types.ResourceUpdatedNotificationParams(
                        uri=resource_uri, **extra,
                    ),
                )
            )
        )
        return True
    except Exception:
        _live_sessions.pop(recipient, None)
        return False


def _check_recipient(store: HarborStorage, agent_id: str) -> str | None:
    """校验目标 agent_id 能否作为消息/任务的接收方。未注册或已吊销都返回错误——
    已吊销的身份永远登录不了（_check_auth 会拒绝），发给它等于消息进了死信箱。"""
    token = store.get_agent_token(agent_id)
    if token is None:
        return f"agent_id={agent_id} 未注册"
    if token.revoked:
        return f"agent_id={agent_id} 已被吊销，无法收发（对方已无法登录读取）"
    return None


def _check_auth(agent_id: str, token: str) -> str | None:
    """校验 agent_id 与 token 是否匹配。返回错误信息，通过则返回 None。"""
    store = _get_store()
    existing = store.get_agent_token(agent_id)
    if existing is None:
        return f"agent_id={agent_id} 未注册，请先调用 harbor.register_agent"
    if not store.verify_agent_token(agent_id, _hash_token(token)):
        return f"agent_id={agent_id} 的 token 无效"
    store.touch_agent_last_seen(agent_id)
    return None


def _check_admin(admin_token: str) -> str | None:
    """校验 admin_token。未配置 MCPHARBOR_ADMIN_TOKEN 时，管理功能整体不可用（fail closed）。"""
    expected = os.environ.get("MCPHARBOR_ADMIN_TOKEN", "")
    if not expected:
        return "admin 功能未启用：请在启动 Harbor 前设置环境变量 MCPHARBOR_ADMIN_TOKEN"
    if not admin_token or not secrets.compare_digest(admin_token, expected):
        return "admin_token 无效"
    return None


def _collect_admin_overview() -> dict[str, Any]:
    """汇总 Harbor 全貌，仅供 admin 使用。私信只给计数，不带正文。"""
    store = _get_store()
    agents = store.list_agents()
    berths = store.list_all_berths()
    subs = store.list_all_subscriptions()
    return {
        "agents": [
            {
                "agent_id": a.agent_id,
                "display_name": a.display_name or "（未登记）",
                "description": a.description or "（未登记）",
                "contact": a.contact or "",
                "capabilities": "、".join(a.capabilities) if a.capabilities else "",
                "created_at": a.created_at.isoformat(),
                "last_seen": a.last_seen.isoformat() if a.last_seen else "从未活跃",
                "heartbeat": (
                    ("🟢 存活" if h["alive"] else "🔴 无响应")
                    + f"（{h['checked_at'][11:19]} UTC）"
                ) if (h := _session_health.get(a.agent_id)) else "⏳ 未登记会话",
                "revoked": a.revoked,
                "online": a.agent_id in _live_sessions,
            }
            for a in agents
        ],
        "berths": [
            {
                "id": b.id, "owner": b.owner, "version": b.version,
                "status": b.status.value, "capabilities": b.capabilities,
                "contact": b.contact,
            }
            for b in berths
        ],
        "subscriptions": [
            {
                "subscriber": s.subscriber, "berth": s.berth,
                "events": s.events, "version_range": s.version_range,
            }
            for s in subs
        ],
        "message_count": store.count_messages(),
        "notification_count": store.count_notifications(),
        "recent_tasks": [
            {
                "id": t.id, "title": t.title, "creator": t.creator, "assignee": t.assignee,
                "status": t.status.value, "deadline": t.deadline or "—",
                "updated_at": t.updated_at.isoformat(),
            }
            for t in store.list_all_tasks(limit=20)
        ],
        "recent_notifications": [n.model_dump(mode="json") for n in store.get_notification_history(limit=20)],
        "recent_audit": [e.model_dump(mode="json") for e in store.get_audit_log(limit=20)],
        "live_agents": sorted(_live_sessions.keys()),
    }


def _render_admin_html(data: dict[str, Any], mcp_url: str = "") -> str:
    def _rows(items: list[dict[str, Any]], cols: list[str]) -> str:
        if not items:
            return "<tr><td colspan='99' class='empty'>暂无数据</td></tr>"
        out = []
        for it in items:
            cells = "".join(f"<td>{html.escape(str(it.get(c, '')))}</td>" for c in cols)
            out.append(f"<tr>{cells}</tr>")
        return "".join(out)

    agents_rows = _rows(
        [{"agent_id": a["agent_id"], "display_name": a["display_name"],
          "description": a["description"], "contact": a["contact"],
          "capabilities": a.get("capabilities", ""),
          "created_at": a["created_at"], "last_seen": a["last_seen"],
          "heartbeat": a["heartbeat"],
          "online": "🟢 在线" if a["online"] else "⚪ 离线",
          "revoked": "已吊销" if a["revoked"] else "正常"}
         for a in data["agents"]],
        ["agent_id", "display_name", "description", "capabilities", "contact", "created_at", "last_seen", "heartbeat", "online", "revoked"],
    )
    berths_rows = _rows(data["berths"], ["id", "owner", "version", "status", "capabilities", "contact"])
    subs_rows = _rows(data["subscriptions"], ["subscriber", "berth", "events", "version_range"])
    task_status_badge = {"created": "🆕", "accepted": "🙋", "working": "🔧",
                         "input_required": "⏸", "completed": "✅", "failed": "❌",
                         "canceled": "🚫", "rejected": "🙅"}
    tasks_rows = _rows(
        [{"id": t["id"], "title": t["title"], "creator": t["creator"], "assignee": t["assignee"],
          "status": f"{task_status_badge.get(t['status'], '')} {t['status']}",
          "deadline": t["deadline"], "updated_at": t["updated_at"]}
         for t in data.get("recent_tasks", [])],
        ["id", "title", "creator", "assignee", "status", "deadline", "updated_at"],
    )
    notif_rows = _rows(
        [{"created_at": n["created_at"], "berth": n["berth"], "old_version": n["old_version"],
          "new_version": n["new_version"], "severity": n["severity"], "summary": n["summary"]}
         for n in data["recent_notifications"]],
        ["created_at", "berth", "old_version", "new_version", "severity", "summary"],
    )
    audit_rows = _rows(
        [{"timestamp": e["timestamp"], "action": e["action"], "actor": e["actor"], "target": e["target"]}
         for e in data["recent_audit"]],
        ["timestamp", "action", "actor", "target"],
    )

    mcp_url_safe = html.escape(mcp_url) if mcp_url else ""
    if mcp_url_safe:
        connection_card = f"""
<div class="card connect">
  <h2>🔌 MCP 连接信息</h2>
  <div class="connect-row">
    <span class="label">Server URL</span>
    <code class="url" id="mcpUrl">{mcp_url_safe}</code>
    <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('mcpUrl').textContent)">复制</button>
  </div>
  <div class="connect-row"><span class="label">Transport</span><code>streamable-http</code></div>
  <div class="connect-row"><span class="label">Claude Code 接入</span>
    <code>claude mcp add --transport http harbor "{mcp_url_safe}"</code>
  </div>
  <p class="hint">当前局域网内使用：把上面的地址换成同一网段能访问到的 IP（不要用 127.0.0.1）即可从其他机器连接。写操作需要先 <code>harbor.register_agent</code> 拿 token。</p>
</div>"""
    else:
        connection_card = ""

    tools_card = """
<div class="card connect" style="border-left-color:#16a34a;">
  <h2>🧰 工具清单与"没有工具"排查</h2>
  <p class="hint" style="margin:0 0 0.6rem;">Harbor 共 <b>33 个工具</b>，实际暴露的工具名<b>不带 <code>harbor.</code> 前缀</b>（README 里的 <code>harbor.xxx</code> 只是文档写法）：<code>register_agent</code>、<code>rotate_token</code>、<code>publish_manifest</code>、<code>get_manifest</code>、<code>search_berths</code>、<code>subscribe</code>、<code>open_session</code>、<code>send_message</code>、<code>get_messages</code>、<code>mark_messages_read</code>、<code>get_conversations</code>、<code>search_agents</code>、<code>admin_command</code>、<code>admin_manage_agent</code>、<code>admin_manage_berth</code>、<code>admin_cleanup</code>、<code>notify</code>、<code>resolve_dependency</code>、<code>check_compat</code>、<code>pin_contract</code>、<code>unpin_contract</code>、<code>get_my_pins</code>、<code>create_task</code>、<code>update_task</code>、<code>get_task</code>、<code>list_tasks</code>、<code>cancel_task</code>、<code>ack_messages</code>、<code>check_updates</code>、<code>sync</code>、<code>diff_versions</code>、<code>get_notifications</code>、<code>get_audit_log</code>。</p>
  <p class="hint" style="margin:0;">如果某个 Agent 连上后说"只看到资源、没有工具"，问题几乎都在客户端侧，按概率排查：
    ① 客户端 MCP 实现残缺——不少网页聊天 Agent 只调 <code>resources/list</code> 不调 <code>tools/list</code>，能看到 <code>harbor://berths</code> 说明连接是通的；
    ② 按 <code>harbor.*</code> 前缀找工具——实际是裸名字；
    ③ transport/端点不匹配——streamable-http 端点是 <code>/mcp</code>，有的客户端只连 <code>/sse</code>。
    服务端自检：用 fastmcp Client 连上来跑 <code>list_tools()</code>，能看到 33 个工具就说明问题在对方。</p>
  <p class="hint" style="margin:0.4rem 0 0;">📌 注册新规：<code>register_agent</code> 必须提交 <code>display_name</code>（显示名）和 <code>description</code>（身份用途），agent_id 仅限小写字母/数字/连字符；同一 agent_id 重复注册会被拒绝——一个 Agent 只需要一个身份。</p>
</div>"""

    # 定时收信插件（OpenCode / OMP 通用）：https://github.com/youyouhe/cron-extension
    cron_plugin_card = """
<div class="card connect" style="border-left-color:#0ea5e9;">
  <h2>⏰ 定时收信插件（OpenCode / OMP 通用 · 推荐）</h2>
  <p class="hint" style="margin:0 0 0.6rem;">一个仓库两个文件，按运行时选装：<b>OpenCode 装 <code>cron-opencode.ts</code>，OMP 装 <code>cron-omp.ts</code></b>。装好后 Agent 获得 <code>cron_add</code>（新建）/ <code>cron_list</code>（查看）/ <code>cron_remove</code>（删除）三个工具和 <code>/cron</code> 命令，在对话里用自然语言即可建定时任务；任务写入会话文件持久化，重启/切分支自动恢复，错过的补跑一次不堆积。</p>
  <p class="hint" style="margin:0 0 0.6rem;">
    <b>三种调度</b>（三选一）：<code>every_seconds</code> 固定间隔（≥5s）/ <code>daily_at</code> 每天定点（HH:MM，本机时区）/ <code>once_in_seconds</code> 一次性延时（≥5s）。<br>
    <b>忙碌策略</b> <code>on_busy</code>：queue（默认，排队不打断当前工作）/ cancel（跳过本次触发）。<br>
    <b>安全约束</b>：最小间隔 5 秒、单会话最多 32 个任务、定时器走托管通道（回调抛错只记日志）。
  </p>
  <p class="hint" style="margin:0 0 0.6rem;">🔑 <b>Harbor 条件门</b>：<code>cron_add</code> 支持 <code>condition</code> + <code>tokenFile</code> 参数——到点先替你查 Harbor 收件箱（<code>get_messages</code>），<b>有未读才把 prompt 注入会话；没未读静默跳过本次，不进 LLM、不烧 token</b>。这是"定时收信"的标准姿势，比盲目定时触发省得多。</p>
  <pre class="codeblock"># 安装（插件已内置在 mcpharbor 仓库 agent-kit/plugins/）

# OMP：拷贝为扩展，重启 omp 会话
cp mcpharbor/agent-kit/plugins/cron-omp.ts ~/.omp/agent/extensions/cron.ts

# OpenCode 工程版（首选：cron 表达式/独立会话/错过策略 + 条件门全齐）：
#   "plugin": ["file:///path/to/mcpharbor/agent-kit/plugins/opencode-cron"]

# OpenCode 单文件版（极简安装，只要定时收信）：
cp mcpharbor/agent-kit/plugins/cron-opencode.ts ~/.config/opencode/cron.ts

# Harbor 定时收信——对话里直接说：
#   「每 60 秒检查一次 Harbor 收件箱，有新私信就处理并回复，处理完标记已读」
# 对应 cron_add 关键参数：
#   every_seconds = 60
#   prompt        = "检查 Harbor 收件箱：get_conversations 看未读，有就
#                    get_messages 读取 → 处理 → 回复 → mark_messages_read + ack_messages"
#   condition     = "__TOKEN__ # <你的agent_id> # http://<Harbor主机IP>:8931/mcp"
#   tokenFile     = "~/.harbor/token"    # 文件首行是注册时保存的 token
# 效果：收件箱 count&gt;0 才注入会话；count=0 静默跳过本次</pre>
</div>"""

    # Claude Code 内置会话定时任务（无需插件）
    claude_cron_card = """
<div class="card connect" style="border-left-color:#f59e0b;">
  <h2>⏰ Claude Code 定时任务（内置 · 无需插件）</h2>
  <p class="hint" style="margin:0 0 0.6rem;">Claude Code 会话本身就支持定时任务，<b>不用装任何插件</b>——在对话里用自然语言说一句，Claude 就会自己创建定时任务，到点在<b>会话空闲时</b>自动触发执行（不打断正在进行的对话）。</p>
  <p class="hint" style="margin:0 0 0.6rem;">
    <b>特点</b>：任务随会话存亡（会话关闭即停，重开会话需重建）；触发时机是空闲边界，与 Harbor 的"安全边界注入"天然契合；配合 <code>open_session</code> 在线推送，会话开着时收信零延迟。<br>
    <b>会话关闭后的兜底</b>：系统 crontab + <code>agent-kit/harbor_gate.sh</code> 门禁 + 无头 <code>claude -p</code>（详见 agent-kit/README.md 的 Claude Code 章节）。
  </p>
  <pre class="codeblock"># 会话里直接说（Claude 自动创建定时任务）：
#   「每 2 分钟检查一次 Harbor 收件箱，有新私信就处理并回复，处理完标记已读」
#
# 建议把身份规范写进项目 CLAUDE.md，定时任务触发时 Claude 会按规范处理：
#   - 身份：agent_id=xxx，token 在 ~/.harbor/xxx.token
#   - 收信流程：get_conversations 看未读 → get_messages 读取 → 处理/回复
#     → mark_messages_read + ack_messages
#
# 会话关闭后的系统级兜底（crontab -e，有未读才唤醒）：
* * * * * HARBOR_AGENT_ID=my-agent HARBOR_TOKEN=$(cat ~/.harbor/my-agent.token) \\
  /path/to/mcpharbor/agent-kit/harbor_gate.sh >> ~/.harbor/gate.log 2>&1 \\
  && claude -p "你有新的 Harbor 私信：$(tail -1 ~/.harbor/gate.log)。读取处理并回复，处理完标记已读。"</pre>
</div>"""


    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>MCP Harbor Admin</title>
<style>
* {{ box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  margin: 0; padding: 2rem; color: #1e293b; background: #f1f5f9;
  line-height: 1.5;
}}
.wrap {{ max-width: 1100px; margin: 0 auto; }}
h1 {{ margin: 0 0 0.3rem; font-size: 1.6rem; }}
.subtitle {{ color: #64748b; margin: 0 0 1.5rem; font-size: 0.9rem; }}
h2 {{ margin: 0 0 0.8rem; font-size: 1.05rem; color: #0f172a; }}
.card {{
  background: white; border-radius: 10px; padding: 1.2rem 1.4rem;
  margin-bottom: 1.2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #e2e8f0;
}}
.stats {{ display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1.2rem; }}
.stat {{
  flex: 1; min-width: 140px; background: white; border-radius: 10px; padding: 1rem 1.2rem;
  box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #e2e8f0;
}}
.stat b {{ font-size: 1.8rem; display: block; color: #2563eb; }}
.stat span {{ font-size: 0.82rem; color: #64748b; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 0.3rem; }}
th, td {{ padding: 7px 12px; text-align: left; font-size: 0.86rem; border-bottom: 1px solid #eef1f5; }}
th {{ color: #64748b; font-weight: 600; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; }}
tr:hover td {{ background: #f8fafc; }}
.empty {{ text-align: center; color: #94a3b8; padding: 1rem; }}
.connect {{ border-left: 4px solid #2563eb; }}
.connect-row {{ display: flex; align-items: center; gap: 0.6rem; margin: 0.5rem 0; flex-wrap: wrap; }}
.connect-row .label {{ min-width: 130px; color: #64748b; font-size: 0.85rem; }}
code {{
  background: #f1f5f9; padding: 3px 8px; border-radius: 5px; font-size: 0.85rem;
  font-family: ui-monospace, "SF Mono", Consolas, monospace; color: #0f172a;
}}
.copy-btn {{
  border: 1px solid #cbd5e1; background: white; border-radius: 5px; padding: 3px 10px;
  font-size: 0.78rem; cursor: pointer; color: #334155;
}}
.copy-btn:hover {{ background: #f1f5f9; }}
.hint {{ font-size: 0.8rem; color: #64748b; margin: 0.6rem 0 0; }}
.codeblock {{
  background: #0f172a; color: #e2e8f0; padding: 0.9rem 1rem; border-radius: 8px;
  font-size: 0.78rem; line-height: 1.5; overflow-x: auto; white-space: pre;
  font-family: ui-monospace, "SF Mono", Consolas, monospace; margin: 0.4rem 0 0;
}}
</style></head>
<body>
<div class="wrap">
<h1>🏠 MCP Harbor</h1>
<p class="subtitle">契约注册、发现与通知中心 — 管理全貌（仅 admin 可见）</p>

{connection_card}

{tools_card}

{cron_plugin_card}

{claude_cron_card}

<div class="stats">
  <div class="stat"><b>{len(data['agents'])}</b><span>已注册参与者</span></div>
  <div class="stat"><b>{len(data['live_agents'])}</b><span>当前在线</span></div>
  <div class="stat"><b>{len(data['berths'])}</b><span>Berth</span></div>
  <div class="stat"><b>{len(data['subscriptions'])}</b><span>活跃订阅</span></div>
  <div class="stat"><b>{data['message_count']}</b><span>私信（仅计数）</span></div>
  <div class="stat"><b>{data['notification_count']}</b><span>通知（累计）</span></div>
</div>

<div class="card">
<h2>参与者（{len(data['agents'])}）</h2>
<table><tr><th>agent_id</th><th>显示名</th><th>身份说明</th><th>联系方式</th><th>注册时间</th><th>最后活跃</th><th>心跳（每30s）</th><th>在线</th><th>状态</th></tr>{agents_rows}</table>
<p class="hint">🧹 「最后活跃」仅在 last_seen 机制上线（2026-09-11）之后才开始记录：此前的注册即使真的活跃过也显示"从未活跃"，不能作为废弃依据。识别僵尸请以最后活跃时间（机制上线后）+ 档案登记情况 + 你自己的实际使用记忆为准；清理用 MCP 工具 <code>admin_manage_agent</code>（action=revoke 吊销 / purge 彻底删除，purge 不可恢复，动手前确认）。</p>
</div>

<div class="card">
<h2>Berth（{len(data['berths'])}）</h2>
<table><tr><th>id</th><th>owner</th><th>version</th><th>status</th><th>capabilities</th><th>contact</th></tr>{berths_rows}</table>
</div>

<div class="card">
<h2>订阅关系（{len(data['subscriptions'])}）</h2>
<table><tr><th>subscriber</th><th>berth</th><th>events</th><th>version_range</th></tr>{subs_rows}</table>
</div>

<div class="card">
<h2>任务（最多20条）</h2>
<table><tr><th>id</th><th>标题</th><th>交办方</th><th>受托方</th><th>状态</th><th>截止</th><th>更新时间</th></tr>{tasks_rows}</table>
</div>

<div class="card">
<h2>最近通知（最多20条）</h2>
<table><tr><th>时间</th><th>berth</th><th>旧版本</th><th>新版本</th><th>优先级</th><th>摘要</th></tr>{notif_rows}</table>
</div>

<div class="card">
<h2>最近审计（最多20条）</h2>
<table><tr><th>时间</th><th>action</th><th>actor</th><th>target</th></tr>{audit_rows}</table>
</div>

</div>
</body></html>"""


def _generate_change_summary(old: Manifest | None, new: Manifest) -> str:
    """生成变更摘要。"""
    if old is None:
        return f"首次发布 v{new.version}"

    parts = []
    if old.base_url != new.base_url:
        parts.append(f"base_url: {old.base_url} -> {new.base_url}")
    if old.auth != new.auth:
        parts.append("认证方式已变更")
    if set(old.capabilities) != set(new.capabilities):
        added = set(new.capabilities) - set(old.capabilities)
        removed = set(old.capabilities) - set(new.capabilities)
        if added:
            parts.append(f"新增能力: {', '.join(added)}")
        if removed:
            parts.append(f"移除能力: {', '.join(removed)}")
    if old.errors != new.errors:
        parts.append("错误码已变更")
    if set(old.events) != set(new.events):
        added = set(new.events) - set(old.events)
        removed = set(old.events) - set(new.events)
        if added:
            parts.append(f"新增事件: {', '.join(added)}")
        if removed:
            parts.append(f"移除事件: {', '.join(removed)}")
    if old.protocol != new.protocol:
        parts.append(f"协议: {old.protocol} -> {new.protocol}")

    return "；".join(parts) if parts else f"版本 {old.version} -> {new.version}"


def _determine_priority(old: Manifest | None, new: Manifest) -> NotifyPriority:
    """根据变更内容判断通知优先级。"""
    if old is None:
        return NotifyPriority.NORMAL

    if old.auth != new.auth:
        return NotifyPriority.HIGH
    if old.protocol != new.protocol:
        return NotifyPriority.HIGH

    if old.errors != new.errors or set(old.events) != set(new.events):
        return NotifyPriority.NORMAL

    return NotifyPriority.LOW


async def _notify_subscribers(
    berth: str,
    old_version: str,
    new_version: str,
    summary: str,
    priority: NotifyPriority,
    actor: str = "system",
) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    """通知订阅者，支持5秒合并，并尝试原生推送给当前存活会话。

    返回 (订阅者列表, 推送成功数, 钉在旧版本上的 agent 列表)。
    """
    store = _get_store()
    resource_uri = f"harbor://berths/{berth}/manifest"

    recent = store.get_recent_notifications(berth, within_seconds=5)
    if recent:
        merged_ids = [n.id for n in recent] + [recent[0].id]
        store.mark_notification_merged(merged_ids, recent[0].id)
        notif = Notification(
            berth=berth, old_version=old_version, new_version=new_version,
            change_type="contract_changed", severity=priority,
            summary=summary, resource_uri=resource_uri,
            merged=True, merged_ids=merged_ids,
        )
        store.add_notification(notif)
        audit_action = "notify.merged"
        audit_detail = {
            "old_version": old_version, "new_version": new_version,
            "merged_count": len(merged_ids),
        }
    else:
        notif = Notification(
            berth=berth, old_version=old_version, new_version=new_version,
            change_type="contract_changed", severity=priority,
            summary=summary, resource_uri=resource_uri,
        )
        store.add_notification(notif)
        audit_action = "notify.send"
        audit_detail = {
            "old_version": old_version, "new_version": new_version,
            "severity": priority.value,
        }

    subs = store.get_subscriptions(berth)
    recipients = []
    pushed = 0
    # 还钉在旧版本上的 agent：变更通知里点名提醒，避免任务中途一半旧一半新
    stale_pins = [
        {"agent": p.agent_id, "pinned_version": p.version, "task_id": p.task_id}
        for p in store.get_pins_for_berth(berth, exclude_version=new_version)
    ]
    pinned_agents = {p["agent"] for p in stale_pins}
    for sub in subs:
        if not sub.events or any(e in sub.events for e in ["*", "contract_changed", "manifest.updated"]):
            recipients.append({
                "subscriber": sub.subscriber,
                "callback": sub.callback,
                "resource_uri": sub.resource_uri,
            })
            ok = await _push_notification(
                sub.subscriber, sub.resource_uri or resource_uri,
                berth=berth, old_version=old_version, new_version=new_version,
                change_type="contract_changed", severity=priority.value, summary=summary,
                stale_pins=stale_pins if sub.subscriber in pinned_agents else [],
            )
            if ok:
                pushed += 1

    audit_detail["pushed"] = pushed
    if stale_pins:
        audit_detail["stale_pins"] = stale_pins
    _audit(audit_action, actor, f"berth:{berth}", audit_detail)

    return recipients, pushed, stale_pins


# ═══════════════════════════════════════════════════════════════════
#  Tools - 写和交互
# ═══════════════════════════════════════════════════════════════════


@mcp.tool()
def register_agent(
    agent_id: str, display_name: str, description: str,
    contact: str = "", capabilities: list[str] | None = None, hidden: bool = False,
) -> str:
    """注册 Agent 身份，获取用于写操作的 token。

    注册时必须提交身份档案：
    - agent_id：小写字母/数字/连字符组成的唯一标识（如 wangxiaoya）
    - display_name：显示名（如"王小丫"）
    - description：这个身份是干什么的、为什么需要它（一个 Agent 只需要注册一个身份）
    - contact：联系方式（可选）
    - capabilities：能力标签（可选，如 ["前端", "部署"]），供其他 Agent 通过 search_agents 找到你
    - hidden：隐身注册（可选，默认 false）。隐身 agent 不出现在 search_agents 结果里，
      但仍可收私信（对方需要已知道你的 agent_id）和公开通知

    每个 agent_id 只能注册一次；token 只在本次调用中返回一次，请妥善保存。
    若已注册会直接拒绝——不要换名字重复注册，需要更换 token 请用 rotate_token。
    """
    store = _get_store()
    if agent_id in _RESERVED_AGENT_IDS:
        return json.dumps({
            "error": f"agent_id={agent_id} 是保留身份，不能自行注册",
        }, ensure_ascii=False)
    if not re.fullmatch(r"[a-z0-9]([a-z0-9\-]*[a-z0-9])?", agent_id) or len(agent_id) > 128:
        return json.dumps({
            "error": "agent_id 只能是小写字母、数字、连字符组成，且以字母或数字开头结尾（如 wangxiaoya）",
        }, ensure_ascii=False)
    display_name = display_name.strip()
    description = description.strip()
    if not display_name:
        return json.dumps({"error": "display_name 必填：请提供这个身份的显示名"}, ensure_ascii=False)
    if len(description) < 5:
        return json.dumps({
            "error": "description 必填：请说明这个身份的用途/职责（至少 5 个字），方便其他 Agent 知道你是谁",
        }, ensure_ascii=False)

    if store.get_agent_token(agent_id) is not None:
        return json.dumps({
            "error": f"agent_id={agent_id} 已注册。一个 Agent 只需要一个身份，不要重复注册；"
                     f"如需更换 token 请调用 harbor.rotate_token",
        }, ensure_ascii=False)

    token = _generate_token()
    created = store.create_agent_token(
        agent_id, _hash_token(token),
        display_name=display_name, description=description,
        contact=contact.strip(), capabilities=capabilities or [], hidden=hidden,
    )
    if not created:
        # 竞态：两个并发请求都通过了上面的"未注册"检查，数据库层唯一约束拦住了后来者。
        # 不能假装成功——那样对方会拿着一个从未持久化的 token，之后所有写操作都会失败。
        return json.dumps({
            "error": f"agent_id={agent_id} 刚被其他请求抢先注册，请换个 agent_id 或稍后用 rotate_token（需要对方的 token）",
        }, ensure_ascii=False)
    _audit("agent.register", agent_id, f"agent:{agent_id}", {
        "display_name": display_name, "description": description,
        "contact": contact.strip(), "capabilities": capabilities or [],
        "hidden": hidden,
    })

    return json.dumps({
        "status": "ok",
        "agent_id": agent_id,
        "display_name": display_name,
        "token": token,
        "message": ("⚠️ 请立即把 token 写入文件保存（如 ~/.harbor/token 或项目内的私密配置），"
                    "不要只留在对话里——本次返回之后不会再显示，下次会话/新进程拿不到对话记忆。"
                    "所有写操作都要带它。若彻底丢失：rotate_token 也需要旧 token，无法自助找回，"
                    "只能请 admin purge 掉这个身份（连带清掉全部私信）后重新注册。"),
    }, ensure_ascii=False)


@mcp.tool()
def rotate_token(agent_id: str, current_token: str) -> str:
    """使用现有 token 为 agent_id 更换新 token，旧 token 立即失效。"""
    store = _get_store()
    err = _check_auth(agent_id, current_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    new_token = _generate_token()
    store.rotate_agent_token(agent_id, _hash_token(new_token))
    _audit("agent.rotate_token", agent_id, f"agent:{agent_id}")

    return json.dumps({
        "status": "ok",
        "agent_id": agent_id,
        "token": new_token,
        "message": "token 已更换，旧 token 已失效。⚠️ 请立即把新 token 写入文件（如 ~/.harbor/token）覆盖旧值。",
    }, ensure_ascii=False)


@mcp.tool()
async def publish_manifest(
    berth: str,
    version: str,
    owner: str,
    token: str,
    capabilities: list[str] | None = None,
    protocol: str = "http",
    base_url: str = "",
    auth: dict[str, Any] | None = None,
    requirements: list[str] | None = None,
    errors: dict[str, str] | None = None,
    events: list[str] | None = None,
    contact: str = "",
) -> str:
    """发布或更新 Manifest（项目卡）。

    如果 berth 不存在则自动注册。已存在则更新版本。
    需要先用 owner 对应的 agent_id 调用 harbor.register_agent 获取 token。
    发布后自动通知订阅者，包含变更摘要和优先级。
    """
    store = _get_store()

    auth_err = _check_auth(owner, token)
    if auth_err:
        _audit("auth.denied", owner, f"berth:{berth}", {"reason": auth_err})
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    # 先做完全部校验再落库：errors 的 key 必须是数字错误码，不然半途抛异常会
    # 留下"berth 已经指向新版本号、但 manifest 数据从未写入"的脏状态。
    try:
        int_errors = {int(k): v for k, v in (errors or {}).items()}
    except (TypeError, ValueError):
        return json.dumps({
            "error": f"errors 的 key 必须是数字错误码（如 \"401\"），收到：{list((errors or {}).keys())}",
        }, ensure_ascii=False)

    old_manifest = store.get_manifest(berth)
    berth_obj = store.get_berth(berth)

    if berth_obj is None:
        berth_obj = Berth(id=berth, owner=owner, version=version,
                          capabilities=capabilities or [], contact=contact)
        store.upsert_berth(berth_obj)
        _audit("berth.register", owner, f"berth:{berth}", {"version": version})
    else:
        if berth_obj.owner != owner:
            _audit("auth.denied", owner, f"berth:{berth}", {"reason": "owner mismatch"})
            return json.dumps({"error": f"只有 owner({berth_obj.owner}) 可以更新 berth={berth}"}, ensure_ascii=False)
        berth_obj.version = version
        berth_obj.capabilities = capabilities or berth_obj.capabilities
        berth_obj.contact = contact or berth_obj.contact
        store.upsert_berth(berth_obj)

    manifest = Manifest(
        berth=berth, version=version, owner=owner,
        capabilities=capabilities or [], protocol=protocol,
        base_url=base_url, auth=auth or {}, requirements=requirements or [],
        errors=int_errors, events=events or [], contact=contact,
    )
    store.publish_manifest(manifest)
    _audit("manifest.publish", owner, f"berth:{berth}", {"version": version})

    summary = _generate_change_summary(old_manifest, manifest)
    priority = _determine_priority(old_manifest, manifest)
    old_version = old_manifest.version if old_manifest else ""
    recipients, pushed, stale_pins = await _notify_subscribers(
        berth, old_version, version, summary, priority, owner,
    )

    return json.dumps({
        "status": "ok",
        "berth": berth,
        "version": version,
        "summary": summary,
        "priority": priority.value,
        "notified": len(recipients),
        "pushed": pushed,
        "stale_pins": stale_pins,
        "message": (f"Manifest v{version} 已发布到 berth={berth}"
                    + (f"；注意：仍有 agent 钉在旧版本上：{[p['agent'] for p in stale_pins]}" if stale_pins else "")),
    }, ensure_ascii=False)


@mcp.tool()
def get_manifest(berth: str, version: str = "") -> str:
    """获取指定 Berth 的最新（或指定版本）Manifest。"""
    store = _get_store()
    manifest = store.get_manifest(berth, version or None)
    if manifest is None:
        return json.dumps({"error": f"berth={berth} 的 manifest 未找到"}, ensure_ascii=False)
    _audit("manifest.get", "mcp-client", f"berth:{berth}", {"version": manifest.version})
    return manifest.model_dump_json(indent=2)


@mcp.tool()
def search_berths(
    capability: str = "",
    keyword: str = "",
) -> str:
    """按能力标签或关键词搜索 Berth。"""
    store = _get_store()
    berths = store.list_berths(capability=capability or None)

    if keyword:
        keyword_lower = keyword.lower()
        berths = [
            b for b in berths
            if keyword_lower in b.id.lower()
            or keyword_lower in b.owner.lower()
            or keyword_lower in b.contact.lower()
            or any(keyword_lower in c.lower() for c in b.capabilities)
        ]

    _audit("berth.search", "mcp-client", "all", {
        "capability": capability, "keyword": keyword, "count": len(berths)
    })

    results = [
        {
            "berth": b.id, "owner": b.owner, "version": b.version,
            "capabilities": b.capabilities, "status": b.status.value,
            "contact": b.contact,
        }
        for b in berths
    ]
    return json.dumps({"berths": results, "count": len(results)}, ensure_ascii=False)


@mcp.tool()
def subscribe(
    subscriber: str,
    token: str,
    berth: str,
    events: list[str] | None = None,
    version_range: str = "*",
    callback: str = "",
    resource_uri: str = "",
    ctx: Context | None = None,
) -> str:
    """订阅某个 Berth 的变更通知。

    需要先用 subscriber 对应的 agent_id 调用 harbor.register_agent 获取 token——
    否则任何人都能冒充别人的身份订阅，把本该发给别人的推送和私信截到自己手里。
    events: 感兴趣的事件列表，如 ["contract_changed", "user.created"]，空列表=所有事件
    version_range: 版本范围，如 ">=1.2.0", "^1.0.0", "*"
    callback: 通知回调地址
    resource_uri: MCP 资源 URI，用于原生通知
    """
    store = _get_store()

    auth_err = _check_auth(subscriber, token)
    if auth_err:
        _audit("auth.denied", subscriber, f"berth:{berth}", {"reason": auth_err})
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    berth_obj = store.get_berth(berth)
    if berth_obj is None:
        return json.dumps({"error": f"berth={berth} 不存在"}, ensure_ascii=False)

    sub = Subscription(
        subscriber=subscriber, berth=berth,
        events=events or [], version_range=version_range,
        callback=callback, resource_uri=resource_uri,
    )
    store.add_subscription(sub)
    _audit("subscribe", subscriber, f"berth:{berth}", {
        "subscription_id": sub.id, "events": events or [],
        "version_range": version_range,
    })

    if ctx is not None:
        _live_sessions[subscriber] = ctx.session

    return json.dumps({
        "status": "ok",
        "subscription_id": sub.id,
        "message": f"{subscriber} 已订阅 berth={berth}",
    }, ensure_ascii=False)


@mcp.tool()
def open_session(agent_id: str, token: str, ctx: Context | None = None) -> str:
    """在不订阅任何 berth 的情况下，把当前连接注册为 agent_id 的存活会话。

    仅用于希望接收 harbor.send_message 私信原生推送、但不关心 berth 契约变更的场景。
    """
    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    if ctx is not None:
        _live_sessions[agent_id] = ctx.session

    return json.dumps({
        "status": "ok",
        "agent_id": agent_id,
        "message": f"{agent_id} 的会话已注册，可接收原生推送。",
    }, ensure_ascii=False)


@mcp.tool()
async def send_message(
    from_agent: str,
    token: str,
    message: str,
    to_agent: str = "",
    to_agents: list[str] | None = None,
    berth: str = "",
    correlation_id: str = "",
    reply_to: str = "",
    severity: str = "normal",
) -> str:
    """向指定 agent 发送点对点私信，只有收发双方可见（多播时每个收件人各自只见你和他）。

    与 harbor.notify（向 berth 全部订阅者广播）不同：这是定向消息，第三方即使订阅了
    同一个 berth 也看不到内容，也无法通过 harbor.get_messages 查到（需要各自的 token）。

    - to_agent：单个收件人（老用法，保持不变）
    - to_agents：多播收件人列表（如三方协作场景）；与 to_agent 二选一，可同时只填一个
    - correlation_id：话题/事务串联号，同一话题多轮往来请沿用同一个
    - reply_to：引用某条历史消息的 message_id，把多轮对话串成线程；
      只能引用你自己参与（收过或发过）的消息
    """
    store = _get_store()

    auth_err = _check_auth(from_agent, token)
    if auth_err:
        _audit("auth.denied", from_agent, "agent:unknown", {"reason": auth_err})
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    recipients = list(to_agents or [])
    if to_agent:
        recipients.append(to_agent)
    # 去重（保序）。注意：允许发给自己——"给自己发条消息触发条件门/cron 唤醒"是实测在用的模式
    recipients = [r for i, r in enumerate(recipients) if r not in recipients[:i]]
    if not recipients:
        return json.dumps({"error": "请提供收件人：to_agent 或 to_agents 至少一个有效 agent_id"}, ensure_ascii=False)

    recipient_errors = [e for r in recipients if (e := _check_recipient(store, r)) is not None]
    if recipient_errors:
        return json.dumps({"error": "；".join(recipient_errors)}, ensure_ascii=False)

    if reply_to:
        parent = store.get_message(reply_to)
        if parent is None:
            return json.dumps({"error": f"reply_to 引用的消息 message_id={reply_to} 不存在"}, ensure_ascii=False)
        if from_agent not in (parent.from_agent, parent.to_agent):
            # 引用他人私信等于把内容泄露给第三方，拒绝
            _audit("auth.denied", from_agent, f"message:{reply_to}", {"reason": "reply_to 非本人参与的消息"})
            return json.dumps({"error": "reply_to 只能引用你自己参与（收过或发过）的消息"}, ensure_ascii=False)

    pri = NotifyPriority.NORMAL
    try:
        pri = NotifyPriority(severity)
    except ValueError:
        pass

    results = []
    for to in recipients:
        msg = DirectMessage(
            from_agent=from_agent, to_agent=to, berth=berth,
            message=message, correlation_id=correlation_id,
            reply_to=reply_to, severity=pri,
        )
        store.add_message(msg)
        # 审计里不记录消息正文：audit_log 目前对任何调用者开放可查，写进去等于白做隔离。
        _audit("message.send", from_agent, f"agent:{to}", {
            "message_id": msg.id, "correlation_id": correlation_id,
            "reply_to": reply_to, "severity": severity,
        })
        pushed = await _push_notification(
            to, f"harbor://messages/{to}",
            change_type="direct_message", from_agent=from_agent, to_agent=to,
            berth=berth, correlation_id=correlation_id, severity=severity, summary=message,
        )
        results.append({"to": to, "message_id": msg.id, "pushed": pushed})

    pushed_count = sum(1 for r in results if r["pushed"])
    return json.dumps({
        "status": "ok",
        "sent": len(results),
        "pushed": pushed_count,
        "results": results,
        "message": (f"已发送给 {len(results)} 个 agent"
                    + (f"（{pushed_count} 个在线已原生推送）" if pushed_count else "（对方不在线，等待其轮询收件箱）")),
    }, ensure_ascii=False)


@mcp.tool()
def get_messages(
    agent_id: str,
    token: str,
    with_agent: str = "",
    unread_only: bool = False,
    limit: int = 50,
) -> str:
    """查询自己的私信（作为收件人或发件人），需要自己的 token——第三方无法查看。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    msgs = store.get_messages(agent_id, with_agent=with_agent or None,
                              unread_only=unread_only, limit=limit)
    return json.dumps({
        "messages": [m.model_dump(mode="json") for m in msgs],
        "count": len(msgs),
    }, ensure_ascii=False, default=str)


@mcp.tool()
def mark_messages_read(agent_id: str, token: str, message_ids: list[str]) -> str:
    """把发给自己的私信标记为已读，方便下次只拉取新消息（unread_only=True）。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    count = store.mark_messages_read(agent_id, message_ids)
    _audit("message.read", agent_id, "self", {"marked": count})

    return json.dumps({"status": "ok", "marked": count}, ensure_ascii=False)


@mcp.tool()
def get_conversations(agent_id: str, token: str) -> str:
    """查看自己的对话列表：每个对话对象一条最新消息 + 未读数。

    多轮往来后不要翻平铺的历史消息——消费者应关注最新消息（它反映最新状态和结果）。
    典型用法：先 get_conversations 看谁发了什么、几条没读，
    再用 get_messages(with_agent=...) 展开需要的对话，mark_messages_read 标记已读。
    有未读的对话排在最前面。
    """
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    convs = store.get_conversations(agent_id)
    total_unread = sum(c["unread"] for c in convs)
    return json.dumps({
        "conversations": convs,
        "count": len(convs),
        "unread_total": total_unread,
    }, ensure_ascii=False, default=str)


@mcp.tool()
def search_agents(keyword: str = "", capability: str = "") -> str:
    """搜索 Harbor 里的参与者（Agent），返回公开身份名片。

    结果不含 token、不含隐身（hidden）注册、不含已吊销身份。
    keyword 匹配 agent_id/显示名/描述/联系方式/能力标签；capability 精确匹配能力标签。
    要找"谁能干某件事"而不是"哪个项目提供某契约"时用这个（找项目用 search_berths）。
    """
    store = _get_store()
    agents = store.search_agents(keyword=keyword or "", capability=capability or "")

    _audit("agent.search", "mcp-client", "all", {
        "keyword": keyword, "capability": capability, "count": len(agents),
    })

    results = [
        {
            "agent_id": a.agent_id,
            "display_name": a.display_name or "（未登记）",
            "description": a.description or "（未登记）",
            "capabilities": a.capabilities,
            "contact": a.contact,
            "online": a.agent_id in _live_sessions,
            "last_seen": a.last_seen.isoformat() if a.last_seen else "",
        }
        for a in agents
    ]
    return json.dumps({"agents": results, "count": len(results)}, ensure_ascii=False)


@mcp.tool()
async def admin_command(admin_token: str, to_agent: str, command: str, correlation_id: str = "") -> str:
    """admin 直接向某个 agent 下一句指令，不做任务状态跟踪——发出去就完了。

    需要真正的 MCPHARBOR_ADMIN_TOKEN（不是随便一个 agent 自己的 token），这样收件人
    才能确定这是 Harbor 运维方下的命令，而不是某个平级 agent 冒充的。收件人用
    harbor.get_messages 查收（from_agent="admin"，severity="high"），跟私信走同一套
    存储和推送机制。
    """
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    store = _get_store()
    recipient_err = _check_recipient(store, to_agent)
    if recipient_err:
        return json.dumps({"error": recipient_err}, ensure_ascii=False)

    msg = DirectMessage(
        from_agent="admin", to_agent=to_agent, message=command,
        correlation_id=correlation_id, severity=NotifyPriority.HIGH,
    )
    store.add_message(msg)
    _audit("admin.command", "admin", f"agent:{to_agent}", {
        "message_id": msg.id, "correlation_id": correlation_id,
    })

    pushed = await _push_notification(
        to_agent, f"harbor://messages/{to_agent}",
        change_type="admin_command", from_agent="admin", to_agent=to_agent,
        correlation_id=correlation_id, severity="high", summary=command,
    )

    return json.dumps({
        "status": "ok",
        "message_id": msg.id,
        "to": to_agent,
        "pushed": pushed,
        "message": f"已向 {to_agent} 下发指令" + ("（已原生推送）" if pushed else "（对方不在线，等待其查收件箱）"),
    }, ensure_ascii=False)


@mcp.tool()
def admin_manage_agent(admin_token: str, agent_id: str, action: str) -> str:
    """admin 清理废弃/异常的 agent 注册。action 二选一：

    - "revoke"：吊销——token 立即失效、踢下线，但注册记录保留可追溯（推荐先用这个）。
    - "purge"：彻底删除——连注册记录、它的全部订阅、收发的私信、契约钉一起删掉，不可恢复。
      若该 agent 还拥有 berth（发布了项目卡），会被拒绝，需先用 admin_manage_berth 处理。

    识别僵尸的依据（admin 面板可看）：display_name/description 为"未登记"（新规前注册）、
    last_seen 为空或很久以前、长期离线。重复注册的垃圾身份（如同一 Agent 注册了多个名字）
    保留一个在用的，其余 purge 掉即可。
    """
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)
    if action not in ("revoke", "purge"):
        return json.dumps({"error": "action 只能是 revoke（吊销）或 purge（彻底删除）"}, ensure_ascii=False)

    store = _get_store()
    if store.get_agent_token(agent_id) is None:
        return json.dumps({"error": f"agent_id={agent_id} 未注册"}, ensure_ascii=False)

    if action == "revoke":
        store.revoke_agent(agent_id)
        _live_sessions.pop(agent_id, None)
        _audit("admin.revoke_agent", "admin", f"agent:{agent_id}", {})
        return json.dumps({
            "status": "ok", "action": "revoke", "agent_id": agent_id,
            "message": f"已吊销 {agent_id}：token 立即失效，注册记录保留。若确定不再需要，可再用 action=purge 彻底删除。",
        }, ensure_ascii=False)

    # purge：先检查名下是否有 berth
    owned = [b.id for b in store.list_all_berths() if b.owner == agent_id]
    if owned:
        return json.dumps({
            "error": f"agent_id={agent_id} 名下还有 berth：{owned}。"
                     f"请先用 admin_manage_berth 处理（deactivate 下架保留历史 / delete 彻底删除），"
                     f"再 purge，否则这些项目卡会变成无主状态。",
        }, ensure_ascii=False)

    removed_subs, removed_msgs, removed_tasks = store.purge_agent(agent_id)
    _live_sessions.pop(agent_id, None)
    _audit("admin.purge_agent", "admin", f"agent:{agent_id}", {
        "removed_subscriptions": removed_subs, "removed_messages": removed_msgs,
        "removed_tasks": removed_tasks,
    })
    return json.dumps({
        "status": "ok", "action": "purge", "agent_id": agent_id,
        "removed_subscriptions": removed_subs,
        "removed_messages": removed_msgs,
        "removed_tasks": removed_tasks,
        "message": (f"已彻底删除 {agent_id} 的注册记录"
                    f"（连带清理 {removed_subs} 条订阅、{removed_msgs} 条私信、{removed_tasks} 个任务）。"),
    }, ensure_ascii=False)


@mcp.tool()
def admin_manage_berth(admin_token: str, berth: str, action: str) -> str:
    """admin 管理某个 Berth（项目卡）。action 三选一：

    - "deactivate"：下架——berth 从搜索/发现里消失（inactive），全部版本历史保留，可恢复。
      适用于项目停运但还要留契约追溯的场景。
    - "activate"：重新上架——deactivate 的逆操作。
    - "delete"：彻底删除——连 berth 本体、全部 Manifest/Contract 版本、订阅、契约钉一起删，
      不可恢复。只想临时下架用 deactivate。删完 owner 名下无 berth，admin_manage_agent
      的 purge 也就不再被拦。
    """
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)
    if action not in ("deactivate", "activate", "delete"):
        return json.dumps({
            "error": "action 只能是 deactivate（下架）/ activate（重新上架）/ delete（彻底删除）",
        }, ensure_ascii=False)

    store = _get_store()
    if store.get_berth(berth) is None:
        return json.dumps({"error": f"berth={berth} 不存在"}, ensure_ascii=False)

    if action == "deactivate":
        store.deactivate_berth(berth)
        _audit("admin.berth_deactivate", "admin", f"berth:{berth}", {})
        return json.dumps({
            "status": "ok", "action": action, "berth": berth,
            "message": f"已下架 berth={berth}（版本历史保留，action=activate 可恢复）。",
        }, ensure_ascii=False)

    if action == "activate":
        store.activate_berth(berth)
        _audit("admin.berth_activate", "admin", f"berth:{berth}", {})
        return json.dumps({
            "status": "ok", "action": action, "berth": berth,
            "message": f"已重新上架 berth={berth}。",
        }, ensure_ascii=False)

    counts = store.delete_berth(berth)
    _audit("admin.berth_delete", "admin", f"berth:{berth}", counts)
    return json.dumps({
        "status": "ok", "action": "delete", "berth": berth,
        "removed": counts,
        "message": (f"已彻底删除 berth={berth}：连带 {counts['manifests']} 个 Manifest 版本、"
                    f"{counts['contracts']} 个 Contract、{counts['subscriptions']} 条订阅、"
                    f"{counts['pins']} 个契约钉。"),
    }, ensure_ascii=False)


@mcp.tool()
def admin_cleanup(
    admin_token: str,
    message_retention_days: int = 90,
    notification_retention_days: int = 90,
) -> str:
    """admin 数据保养：按保留期清理过期私信和通知（仅 admin）。

    - message_retention_days：私信保留天数，默认 90，最小 7（太短的保留期等于丢业务凭据）
    - notification_retention_days：通知保留天数，默认 90
    - 审计日志不在此清理：需求要求审计保留至少 90 天，到期由存储层 cleanup_old_audit 兜底

    私信是点对点凭据不是日志，长期堆着既占库又留敏感内容，建议定期跑。
    """
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)
    if message_retention_days < 7:
        return json.dumps({"error": "message_retention_days 最小为 7 天"}, ensure_ascii=False)
    if notification_retention_days < 30:
        return json.dumps({"error": "notification_retention_days 最小为 30 天"}, ensure_ascii=False)

    store = _get_store()
    removed_msgs = store.cleanup_old_messages(message_retention_days)
    removed_notifs = store.cleanup_old_notifications(notification_retention_days)
    _audit("admin.cleanup", "admin", "harbor", {
        "removed_messages": removed_msgs,
        "removed_notifications": removed_notifs,
        "message_retention_days": message_retention_days,
    })
    return json.dumps({
        "status": "ok",
        "removed_messages": removed_msgs,
        "removed_notifications": removed_notifs,
        "message": f"已清理 {removed_msgs} 条过期私信、{removed_notifs} 条过期通知。",
    }, ensure_ascii=False)


@mcp.tool()
async def notify(
    berth: str,
    event: str,
    token: str,
    message: str = "",
    correlation_id: str = "",
    actor: str = "",
    severity: str = "normal",
) -> str:
    """向 berth 的订阅者发送通知。只有 berth 的 owner（用其 token 认证）可以广播——

    否则任何人都能冒充任意 actor 向该 berth 的全部订阅者广播、拿着别人的名字发通知，
    这跟 publish_manifest/subscribe 要求 token 认证是同一个道理。
    actor 参数已废弃为兼容占位，实际广播身份以 token 认证到的 owner 为准。
    """
    store = _get_store()

    berth_obj = store.get_berth(berth)
    if berth_obj is None:
        return json.dumps({"error": f"berth={berth} 不存在"}, ensure_ascii=False)

    auth_err = _check_auth(berth_obj.owner, token)
    if auth_err:
        _audit("auth.denied", actor or "unknown", f"berth:{berth}", {"reason": auth_err})
        return json.dumps({"error": f"只有 berth={berth} 的 owner（{berth_obj.owner}）可以广播通知：{auth_err}"}, ensure_ascii=False)
    real_actor = berth_obj.owner

    pri = NotifyPriority.NORMAL
    try:
        pri = NotifyPriority(severity)
    except ValueError:
        pass

    notif = Notification(
        berth=berth, event=event, summary=message,
        severity=pri, change_type=event,
    )
    store.add_notification(notif)

    subs = store.get_subscriptions(berth)
    matched = 0
    pushed = 0
    for sub in subs:
        if not sub.events or event in sub.events or "*" in sub.events:
            matched += 1
            ok = await _push_notification(
                sub.subscriber, sub.resource_uri or f"harbor://berths/{berth}/manifest",
                berth=berth, event=event, change_type=event,
                severity=severity, summary=message, correlation_id=correlation_id,
            )
            if ok:
                pushed += 1

    _audit("notify", real_actor, f"berth:{berth}", {
        "event": event, "recipients": matched, "severity": severity, "pushed": pushed,
    })

    return json.dumps({
        "status": "ok",
        "event": event,
        "severity": severity,
        "notified": matched,
        "pushed": pushed,
    }, ensure_ascii=False)


@mcp.tool()
def resolve_dependency(
    needs: list[str],
    requester: str = "unknown",
) -> str:
    """解析依赖：给定所需能力列表，返回匹配的 Berth。"""
    store = _get_store()
    all_berths = store.list_berths()

    matched: dict[str, list[str]] = {}
    for need in needs:
        hits = [b.id for b in all_berths if need in b.capabilities]
        matched[need] = hits

    _audit("dependency.resolve", requester, "all", {"needs": needs, "matched": matched})

    return json.dumps({
        "needs": needs,
        "matched": matched,
        "unresolved": [n for n, h in matched.items() if not h],
    }, ensure_ascii=False)


@mcp.tool()
def check_compat(
    berth_a: str,
    berth_b: str,
) -> str:
    """检查两个 Berth 的契约兼容性。"""
    store = _get_store()
    m_a = store.get_manifest(berth_a)
    m_b = store.get_manifest(berth_b)

    if m_a is None:
        return json.dumps({"error": f"berth={berth_a} 的 manifest 未找到"}, ensure_ascii=False)
    if m_b is None:
        return json.dumps({"error": f"berth={berth_b} 的 manifest 未找到"}, ensure_ascii=False)

    issues = []
    if m_a.protocol != m_b.protocol:
        issues.append(f"协议不匹配: {m_a.protocol} vs {m_b.protocol}")
    if m_a.auth.get("type") != m_b.auth.get("type"):
        issues.append(f"认证方式不匹配: {m_a.auth.get('type')} vs {m_b.auth.get('type')}")

    shared_events = set(m_a.events) & set(m_b.events)
    a_only = set(m_a.events) - set(m_b.events)
    b_only = set(m_b.events) - set(m_a.events)

    _audit("compat.check", "mcp-client", f"{berth_a}<->{berth_b}", {
        "compatible": len(issues) == 0, "issues": issues,
    })

    return json.dumps({
        "berth_a": berth_a, "berth_b": berth_b,
        "compatible": len(issues) == 0,
        "issues": issues,
        "shared_events": list(shared_events),
        "a_only_events": list(a_only),
        "b_only_events": list(b_only),
    }, ensure_ascii=False)


@mcp.tool()
def pin_contract(agent_id: str, token: str, berth: str, version: str, task_id: str = "") -> str:
    """钉住某 berth 的契约版本："当前任务固定用这个版本"（contract pin）。

    任务进行到一半契约变了是协作的大敌——同一任务一半用旧契约、一半用新契约会出错。
    钉住后：该 berth 发布新版本时，Harbor 会在通知里点名提醒还钉在旧版本上的你
    （"你钉的 1.2.0 已不是最新，本次变更为 X，用 diff_versions 评估后再切换"）。
    同一 agent+berth+task_id 重复调用 = 更新钉的版本；任务结束请调 unpin_contract。
    """
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    if store.get_berth(berth) is None:
        return json.dumps({"error": f"berth={berth} 不存在"}, ensure_ascii=False)
    if store.get_manifest(berth, version) is None:
        available = store.list_manifest_versions(berth)
        return json.dumps({
            "error": f"berth={berth} 没有 version={version}，可选版本：{available}",
        }, ensure_ascii=False)

    pin = store.pin_contract(agent_id, berth, version, task_id)
    _audit("contract.pin", agent_id, f"berth:{berth}", {
        "version": version, "task_id": task_id,
    })
    return json.dumps({
        "status": "ok", "berth": berth, "version": version, "task_id": task_id,
        "message": f"已钉住 {berth} v{version}" + (f"（任务 {task_id}）" if task_id else ""),
    }, ensure_ascii=False)


@mcp.tool()
def unpin_contract(agent_id: str, token: str, berth: str, task_id: str = "") -> str:
    """解除契约钉（任务结束或已切换到新版本后调用）。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    if store.unpin_contract(agent_id, berth, task_id):
        _audit("contract.unpin", agent_id, f"berth:{berth}", {"task_id": task_id})
        return json.dumps({"status": "ok", "message": f"已解除 {berth} 的钉（task_id={task_id or '默认'}）"}, ensure_ascii=False)
    return json.dumps({"error": f"没有找到 {berth} 上 task_id={task_id or '默认'} 的钉"}, ensure_ascii=False)


@mcp.tool()
def get_my_pins(agent_id: str, token: str) -> str:
    """查看自己钉住的全部契约版本，并标注哪些已落后于最新版。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    pins = store.get_pins(agent_id)
    result = []
    for p in pins:
        latest = store.get_manifest(p.berth)
        latest_version = latest.version if latest else ""
        result.append({
            "berth": p.berth, "version": p.version, "task_id": p.task_id,
            "pinned_at": p.created_at.isoformat(),
            "latest_version": latest_version,
            "stale": bool(latest_version and latest_version != p.version),
        })
    return json.dumps({"pins": result, "count": len(result)}, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════
#  Task（任务状态机）— 把"说一句话"升级成"托付一件事"
# ═══════════════════════════════════════════════════════════════════

_TASK_EVENT_LABEL = {
    TaskStatus.ACCEPTED: "已接单", TaskStatus.WORKING: "进行中",
    TaskStatus.INPUT_REQUIRED: "卡住等补料", TaskStatus.COMPLETED: "已完成",
    TaskStatus.FAILED: "失败", TaskStatus.REJECTED: "已拒单",
    TaskStatus.CANCELED: "已取消", TaskStatus.CREATED: "新交办",
}


async def _notify_task_event(from_agent: str, to_agent: str, task: Task,
                             event: str, note: str = "") -> bool:
    """任务事件写进双方对话流（收件箱可见、correlation_id=task.id 串线程）+ 尝试原生推送。"""
    store = _get_store()
    label = _TASK_EVENT_LABEL.get(task.status, event)
    text = f"【任务·{label}】#{task.id} {task.title}" + (f"——{note}" if note else "")
    msg = DirectMessage(from_agent=from_agent, to_agent=to_agent, berth=task.berth,
                        message=text, correlation_id=task.id)
    store.add_message(msg)
    _audit("task.notify", from_agent, f"agent:{to_agent}", {
        "task_id": task.id, "status": task.status.value,
    })
    return await _push_notification(
        to_agent, f"harbor://messages/{to_agent}",
        change_type="task_event", task_id=task.id, status=task.status.value, summary=text,
    )


async def _sweep_stale_tasks() -> None:
    """过了 deadline 仍未到终态的任务自动标 failed，并通知交办方（防"永远 working"）。"""
    for task in _get_store().sweep_stale_tasks():
        _audit("task.sweep", "system", f"task:{task.id}", {"deadline": task.deadline})
        await _notify_task_event("system", task.creator, task, "超时",
                                 note=f"截止 {task.deadline} 未完成，已自动标记失败")


@mcp.tool()
async def create_task(
    creator: str, token: str, assignee: str, title: str,
    detail: str = "", berth: str = "", correlation_id: str = "", deadline: str = "",
) -> str:
    """交办任务：把一件事托付给另一个 Agent，带双方认账的生命周期（与私信的区别）。

    私信是"说了句话"，任务是"托付了件事"：可查（get_task）、可催、可撤（cancel_task）、
    有终态（completed/failed/canceled/rejected），超时自动标失败——不用再靠读聊天记录猜进度。
    受托方用 update_task 推进：accepted → working → completed/failed（或 rejected 拒单、
    input_required 卡住等补料）。每次状态变化双方都会收到通知（走私信收件箱+推送）。

    - title：一句话说清要什么（必填）
    - detail：补充说明/验收标准
    - deadline：截止时间（ISO 格式如 2026-09-13T18:00 或 2026-09-13 18:00），按 UTC 解释
      （不带时区信息的裸时间会被当成 UTC，不是本机时区——换算好了再填，否则超时判定会偏移）；
      空=不限时
    - correlation_id：要与哪条消息线串起来（可选；任务自身的事件消息会以 task_id 串线）
    """
    store = _get_store()

    auth_err = _check_auth(creator, token)
    if auth_err:
        _audit("auth.denied", creator, f"agent:{assignee}", {"reason": auth_err})
        return json.dumps({"error": auth_err}, ensure_ascii=False)
    if not title.strip():
        return json.dumps({"error": "title 必填：一句话说清要托付什么"}, ensure_ascii=False)
    assignee_err = _check_recipient(store, assignee)
    if assignee_err:
        return json.dumps({"error": f"受托方：{assignee_err}"}, ensure_ascii=False)

    deadline_iso = ""
    if deadline:
        try:
            parsed = datetime.fromisoformat(deadline.replace(" ", "T"))
        except ValueError:
            return json.dumps({"error": "deadline 格式不对，要 ISO 格式如 2026-09-13T18:00"}, ensure_ascii=False)
        # 裸时间（不带时区）统一按 UTC 解释，否则存进库里的字符串和 sweep_stale_tasks
        # 拿 UTC now 做字符串比较时会产生时区偏移的误判（该超时的没超时，或反之）。
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        deadline_iso = parsed.isoformat()

    task = Task(creator=creator, assignee=assignee, title=title.strip(),
                detail=detail, berth=berth, correlation_id=correlation_id,
                deadline=deadline_iso)
    store.create_task(task)
    _audit("task.create", creator, f"task:{task.id}", {
        "assignee": assignee, "title": title.strip(), "deadline": deadline_iso,
    })

    pushed = await _notify_task_event(creator, assignee, task, "新交办",
                                      note=detail if detail else "请 update_task 接单或拒单")

    return json.dumps({
        "status": "ok", "task_id": task.id, "task_status": task.status.value,
        "assignee": assignee, "deadline": deadline_iso,
        "pushed": pushed,
        "message": (f"任务 #{task.id} 已交办给 {assignee}"
                    + ("（对方在线已推送）" if pushed else "（对方不在线，已落收件箱等其轮询）")),
    }, ensure_ascii=False)


def _load_task_for(agent_id: str, task_id: str) -> tuple[Task | None, str | None]:
    """加载任务并校验参与方身份。返回 (task, error)。"""
    store = _get_store()
    task = store.get_task(task_id)
    if task is None:
        return None, f"任务 {task_id} 不存在"
    if agent_id not in (task.creator, task.assignee):
        return None, f"任务 {task_id} 只对当事方（{task.creator}/{task.assignee}）可见"
    return task, None


@mcp.tool()
async def update_task(
    task_id: str, agent_id: str, token: str, status: str, note: str = "",
) -> str:
    """受托方推进任务状态（执行类转移只能由 assignee 做）。

    合法转移：created→accepted/rejected；accepted→working；working→input_required/
    completed/failed；input_required→working。终态（completed/failed/canceled/rejected）
    之后冻结。completed/failed 时把 note 存为 result（任务成果）。取消任务用 cancel_task。
    每次转移自动通知对方（收件箱+推送）。
    """
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    task, err = _load_task_for(agent_id, task_id)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        target = TaskStatus(status)
    except ValueError:
        return json.dumps({"error": f"未知状态 {status}，可选：{[s.value for s in TaskStatus]}"}, ensure_ascii=False)

    if task.status in _TASK_TERMINAL:
        return json.dumps({"error": f"任务已到终态 {task.status.value}，不能再改"}, ensure_ascii=False)
    if target == TaskStatus.CANCELED:
        return json.dumps({"error": "取消任务请用 cancel_task"}, ensure_ascii=False)
    if target in _TASK_WORKER_MOVES and agent_id != task.assignee:
        return json.dumps({"error": f"执行类状态转移只能由受托方 {task.assignee} 操作"}, ensure_ascii=False)
    if target not in _TASK_TRANSITIONS[task.status]:
        allowed = ", ".join(s.value for s in _TASK_TRANSITIONS[task.status])
        return json.dumps({"error": f"不允许 {task.status.value} → {target.value}；当前可转：{allowed}"}, ensure_ascii=False)

    updated = store.update_task_status(task_id, target, result=note if target in _TASK_TERMINAL else "")
    _audit("task.update", agent_id, f"task:{task_id}", {"to": target.value, "note": note[:200]})

    pushed = await _notify_task_event(agent_id, task.creator if agent_id == task.assignee else task.assignee,
                                      updated, target.value, note=note)

    return json.dumps({
        "status": "ok", "task_id": task_id, "task_status": updated.status.value,
        "result": updated.result, "pushed": pushed,
        "message": f"任务 #{task_id}: {task.status.value} → {target.value}",
    }, ensure_ascii=False)


@mcp.tool()
async def cancel_task(
    task_id: str, agent_id: str = "", token: str = "", admin_token: str = "", reason: str = "",
) -> str:
    """取消任务（交办方或 admin；未到终态才可取消）。取消后受托方收到通知。"""
    store = _get_store()

    is_admin = False
    if admin_token:
        if _check_admin(admin_token):
            return json.dumps({"error": _check_admin(admin_token)}, ensure_ascii=False)
        is_admin = True
    else:
        auth_err = _check_auth(agent_id, token)
        if auth_err:
            return json.dumps({"error": auth_err}, ensure_ascii=False)

    task, err = _load_task_for(agent_id or "admin", task_id) if not is_admin else (store.get_task(task_id), None)
    if err or task is None:
        return json.dumps({"error": err or f"任务 {task_id} 不存在"}, ensure_ascii=False)
    if not is_admin and agent_id != task.creator:
        return json.dumps({"error": f"只有交办方 {task.creator}（或 admin）可以取消"}, ensure_ascii=False)
    if task.status in _TASK_TERMINAL:
        return json.dumps({"error": f"任务已到终态 {task.status.value}，无需取消"}, ensure_ascii=False)

    updated = store.update_task_status(task_id, TaskStatus.CANCELED,
                                       result=reason or f"由{'admin' if is_admin else agent_id}取消")
    _audit("task.cancel", "admin" if is_admin else agent_id, f"task:{task_id}", {"reason": reason[:200]})

    pushed = await _notify_task_event("admin" if is_admin else agent_id, task.assignee,
                                      updated, "canceled", note=reason)

    return json.dumps({
        "status": "ok", "task_id": task_id, "task_status": updated.status.value,
        "pushed": pushed, "message": f"任务 #{task_id} 已取消",
    }, ensure_ascii=False)


@mcp.tool()
def get_task(task_id: str, agent_id: str, token: str) -> str:
    """查任务详情（含状态、成果、时限），只对当事双方可见。
    附带该任务线上的最近 5 条消息（correlation_id=task_id 的双方往来）。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    task, err = _load_task_for(agent_id, task_id)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    related = store.get_messages(task.creator, with_agent=task.assignee, limit=50)
    related = [m for m in related if m.correlation_id == task_id][:5]

    return json.dumps({
        "task": task.model_dump(mode="json"),
        "related_messages": [m.model_dump(mode="json") for m in related],
    }, ensure_ascii=False, default=str)


@mcp.tool()
def list_tasks(agent_id: str, token: str, status: str = "", role: str = "") -> str:
    """列出自己参与的任务（交办给我的 / 我交办的）。status 过滤状态，role=creator/assignee 过滤角色。"""
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    if status:
        try:
            TaskStatus(status)
        except ValueError:
            return json.dumps({"error": f"未知状态 {status}"}, ensure_ascii=False)
    if role not in ("", "creator", "assignee"):
        return json.dumps({"error": "role 只能是 creator / assignee / 留空"}, ensure_ascii=False)

    tasks = store.list_tasks(agent_id, status=status or "", role=role)
    return json.dumps({
        "tasks": [t.model_dump(mode="json") for t in tasks],
        "count": len(tasks),
    }, ensure_ascii=False, default=str)


@mcp.tool()
async def ack_messages(agent_id: str, token: str, message_ids: list[str]) -> str:
    """确认收到并认领私信（ack）。比已读更强：已读=看到了，ack=对这条消息负责（会去处理/执行）。

    发件方查自己的已发消息可以看到 acked 状态——"谁还没认领"一目了然，
    适合"交办了要等认领"的场景（配合任务状态机：create_task 后等 assignee ack 再动手）。
    ack 时若发件方在线，会收到原生推送提醒。
    """
    store = _get_store()

    auth_err = _check_auth(agent_id, token)
    if auth_err:
        return json.dumps({"error": auth_err}, ensure_ascii=False)

    acked = store.mark_messages_acked(agent_id, message_ids)
    _audit("message.ack", agent_id, "self", {"acked": len(acked)})

    # 在线的原发件人立刻知道"对方认领了"
    push_targets = {m.from_agent for m in acked if m.from_agent != agent_id}
    for target in push_targets:
        await _push_notification(
            target, f"harbor://messages/{target}",
            change_type="message_acked", acked_by=agent_id,
            summary=f"{agent_id} 已认领你的 {len([m for m in acked if m.from_agent == target])} 条消息",
        )

    return json.dumps({
        "status": "ok", "acked": len(acked),
        "message": f"已确认认领 {len(acked)} 条消息" + ("（未找到或不属于自己的已跳过）" if len(acked) != len(message_ids) else ""),
    }, ensure_ascii=False)


@mcp.tool()
def check_updates(
    known_versions: dict[str, str],
) -> str:
    """轮询兜底：检查已知版本是否有更新。

    known_versions: {"berth_id": "known_version", ...}
    返回有更新的 berth 列表。
    """
    store = _get_store()
    updates = store.check_updates(known_versions)
    _audit("updates.check", "mcp-client", "all", {
        "checked": len(known_versions), "updates": len(updates),
    })
    return json.dumps({
        "updates": updates,
        "count": len(updates),
        "message": f"检查了 {len(known_versions)} 个 berth，{len(updates)} 个有更新",
    }, ensure_ascii=False)


@mcp.tool()
def sync(
    berths: list[str],
    requester: str = "unknown",
) -> str:
    """手动同步：拉取指定 Berth 的最新 Manifest。

    返回各 berth 的最新 Manifest 摘要。
    """
    store = _get_store()
    results = []
    for berth_id in berths:
        manifest = store.get_manifest(berth_id)
        if manifest:
            results.append({
                "berth": manifest.berth,
                "version": manifest.version,
                "base_url": manifest.base_url,
                "protocol": manifest.protocol,
                "capabilities": manifest.capabilities,
            })
        else:
            results.append({"berth": berth_id, "error": "not found"})

    _audit("sync", requester, "all", {"berths": berths, "synced": len([r for r in results if "error" not in r])})

    return json.dumps({"synced": results, "count": len(results)}, ensure_ascii=False)


@mcp.tool()
def diff_versions(
    berth: str,
    version_a: str,
    version_b: str,
) -> str:
    """对比两个版本的 Manifest 差异。"""
    store = _get_store()
    m_a = store.get_manifest(berth, version_a)
    m_b = store.get_manifest(berth, version_b)

    if m_a is None:
        return json.dumps({"error": f"berth={berth} v{version_a} 未找到"}, ensure_ascii=False)
    if m_b is None:
        return json.dumps({"error": f"berth={berth} v{version_b} 未找到"}, ensure_ascii=False)

    diffs: list[dict[str, Any]] = []
    fields = ["base_url", "protocol", "capabilities", "auth", "requirements", "errors", "events", "contact"]
    for field in fields:
        val_a = getattr(m_a, field)
        val_b = getattr(m_b, field)
        if val_a != val_b:
            diffs.append({"field": field, "old": val_a, "new": val_b})

    _audit("diff.versions", "mcp-client", f"berth:{berth}", {
        "version_a": version_a, "version_b": version_b, "diff_count": len(diffs),
    })

    return json.dumps({
        "berth": berth,
        "version_a": version_a,
        "version_b": version_b,
        "diffs": diffs,
        "diff_count": len(diffs),
        "summary": _generate_change_summary(m_a, m_b),
    }, ensure_ascii=False, default=str)


@mcp.tool()
def get_notifications(
    admin_token: str,
    berth: str = "",
    limit: int = 50,
) -> str:
    """查询通知历史（仅 admin）。需要 MCPHARBOR_ADMIN_TOKEN。"""
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    store = _get_store()
    notifs = store.get_notification_history(berth_id=berth or None, limit=limit)
    return json.dumps({
        "notifications": [n.model_dump(mode="json") for n in notifs],
        "count": len(notifs),
    }, ensure_ascii=False, default=str)


@mcp.tool()
def get_audit_log(admin_token: str, limit: int = 50, action: str = "", actor: str = "", berth: str = "") -> str:
    """查询审计日志（仅 admin）。需要 MCPHARBOR_ADMIN_TOKEN。支持按操作、操作者、Berth 过滤。"""
    err = _check_admin(admin_token)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    store = _get_store()
    entries = store.get_audit_log(
        limit=limit,
        action=action or None,
        actor=actor or None,
        berth=berth or None,
    )
    return json.dumps({
        "entries": [e.model_dump(mode="json") for e in entries],
        "count": len(entries),
    }, ensure_ascii=False, default=str)


# ═══════════════════════════════════════════════════════════════════
#  Resources - 只读
# ═══════════════════════════════════════════════════════════════════


@mcp.resource("harbor://berths")
def list_berths_resource() -> str:
    """列出所有活跃 Berth。"""
    store = _get_store()
    berths = store.list_berths()
    data = [
        {"berth": b.id, "owner": b.owner, "version": b.version,
         "capabilities": b.capabilities, "status": b.status.value}
        for b in berths
    ]
    return json.dumps({"berths": data}, ensure_ascii=False)


@mcp.resource("harbor://berths/{berth_id}/manifest")
def manifest_resource(berth_id: str) -> str:
    """获取指定 Berth 的最新 Manifest。"""
    store = _get_store()
    manifest = store.get_manifest(berth_id)
    if manifest is None:
        return json.dumps({"error": f"berth={berth_id} 的 manifest 未找到"}, ensure_ascii=False)
    return manifest.model_dump_json(indent=2)


@mcp.resource("harbor://berths/{berth_id}/contracts/{version}")
def contract_resource(berth_id: str, version: str) -> str:
    """获取指定 Berth 的指定版本 Contract。"""
    store = _get_store()
    contract = store.get_contract(berth_id, version)
    if contract is None:
        return json.dumps({"error": f"berth={berth_id} v{version} 的 contract 未找到"}, ensure_ascii=False)
    return contract.model_dump_json(indent=2)


# ═══════════════════════════════════════════════════════════════════
#  Admin 网页面板（仅在 streamable-http/sse 传输下可访问）
# ═══════════════════════════════════════════════════════════════════

@mcp.custom_route("/admin", methods=["GET"])
async def admin_dashboard(request):
    from starlette.responses import HTMLResponse

    admin_token = request.query_params.get("token", "")
    err = _check_admin(admin_token)
    if err:
        return HTMLResponse(f"<h1>403</h1><p>{html.escape(err)}</p>", status_code=403)

    data = _collect_admin_overview()
    mcp_url = str(request.base_url).rstrip("/") + "/mcp"
    return HTMLResponse(_render_admin_html(data, mcp_url))


# ═══════════════════════════════════════════════════════════════════
#  Prompts - 可选
# ═══════════════════════════════════════════════════════════════════


@mcp.prompt()
def onboard_berth() -> str:
    """引导新项目接入 Harbor 的流程。"""
    return """欢迎接入 MCP Harbor！

请按以下步骤操作：

1. 调用 register_agent(agent_id="你的团队名，如 auth-team", display_name="显示名",
   description="这个身份是干什么的") 获取 token（只显示一次，请立即保存到文件）
2. 确定你的 Berth ID（如 auth, order, payment）
3. 准备 Manifest 信息：
   - owner: 负责团队（必须等于第 1 步的 agent_id）
   - capabilities: 能力标签列表
   - protocol: 通信协议 (http/grpc/mqtt)
   - base_url: 服务地址
   - auth: 认证方式
   - requirements: 调用要求
   - errors: 错误码映射（key 必须是数字错误码的字符串，如 "401"）
   - events: 可发布的事件

4. 调用 publish_manifest 发布你的项目卡，带上第 1 步获取的 token

注意：工具名不带 harbor. 前缀，直接调裸名字（如 register_agent、publish_manifest）。

示例：
  register_agent(agent_id="auth-team", display_name="认证团队", description="负责登录认证与 token 签发")
  # -> 返回 token，请立即保存

  publish_manifest(
    berth="auth",
    version="1.0.0",
    owner="auth-team",
    token="<上一步返回的 token>",
    capabilities=["auth", "jwt", "token"],
    protocol="http",
    base_url="https://auth.internal",
    auth={"type": "bearer", "header": "Authorization"},
    requirements=["所有请求必须带 X-Request-Id"],
    errors={"401": "invalid_token", "403": "forbidden"},
    events=["user.created", "token.revoked"]
  )
"""


@mcp.prompt()
def check_compat_prompt(berth_a: str, berth_b: str) -> str:
    """检查两个 Berth 的兼容性。"""
    return f"""请检查 berth={berth_a} 和 berth={berth_b} 的契约兼容性。

调用 harbor.check_compat(berth_a="{berth_a}", berth_b="{berth_b}") 获取结果。

兼容性检查包括：
- 协议是否匹配
- 认证方式是否兼容
- 事件是否有交集
"""


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    """启动 MCP Harbor Server。

    默认 stdio（每个 agent 独立进程，harbor.publish_manifest 的原生推送只能推给
    同进程内的活跃会话，跨 agent 只能靠 harbor.check_updates 轮询兜底）。
    设置环境变量 MCPHARBOR_TRANSPORT=streamable-http 可让多个 agent 共享同一个
    Harbor 进程，这样 subscribe 后才能真正收到 notifications/resources/updated 推送。
    """
    transport = os.environ.get("MCPHARBOR_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
