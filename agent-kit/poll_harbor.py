#!/usr/bin/env python3
"""Harbor 轮询脚本（零依赖，标准库实现）—— 给没有原生推送能力的 Agent 会话做兜底收件。

用法：
    HARBOR_URL=http://127.0.0.1:8931/mcp \
    HARBOR_AGENT_ID=my-agent \
    HARBOR_TOKEN=<注册时拿到的 token> \
    python3 poll_harbor.py [--mark-read] [--watch INTERVAL]

做什么：
    1. get_conversations  —— 看每个对话对象的最新消息 + 未读数（消费者应关注最新消息）
    2. get_messages(unread_only=True) —— 拉全部未读正文
    3. check_updates      —— 顺带查契约版本有没有更新（轮询兜底）
    4. --mark-read        —— 拉完标记已读（建议确认"已处理"后再标，而不是拉到就标）
    5. --watch N          —— 常驻每 N 秒轮询一次（配合 systemd/cron 用一次性模式即可）

退出码：有未读=0，无未读=1，出错=2。配合 harbor_gate.sh 做空闲会话唤醒的门禁。
输出是紧凑 JSON，可直接作为"内部事件"注入会话上下文。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request

HARBOR_URL = os.environ.get("HARBOR_URL", "http://127.0.0.1:8931/mcp")
AGENT_ID = os.environ.get("HARBOR_AGENT_ID", "")
TOKEN = os.environ.get("HARBOR_TOKEN", "")
PROTOCOL_VERSION = "2024-11-05"


class HarborClient:
    """极简 MCP streamable-http 客户端：initialize -> tools/call，SSE 响应取最后一条 data。"""

    def __init__(self, url: str):
        self.url = url
        self.session_id = ""
        self._id = 0

    def _post(self, payload: dict) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            headers["mcp-session-id"] = self.session_id
        req = urllib.request.Request(
            self.url, data=json.dumps(payload).encode(), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            self.session_id = resp.headers.get("mcp-session-id", self.session_id)
            body = resp.read().decode()
        if not body.strip():
            # 通知类请求（无 id）标准应答是 202 空体
            return {}
        # SSE：可能多行 data: {...}，取最后一条非空
        data_lines = [ln[5:].strip() for ln in body.splitlines() if ln.startswith("data:")]
        if data_lines:
            return json.loads(data_lines[-1])
        return json.loads(body)  # 有些实现直接回 JSON

    def _call(self, method: str, params: dict | None = None) -> dict:
        self._id += 1
        payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}}
        return self._post(payload)

    def _notify(self, method: str) -> None:
        # 通知没有 id，服务器不应答
        self._post({"jsonrpc": "2.0", "method": method})

    def connect(self) -> None:
        self._call("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "harbor-agent-kit", "version": "1.0"},
        })
        self._notify("notifications/initialized")

    def tool(self, name: str, arguments: dict) -> dict:
        resp = self._call("tools/call", {"name": name, "arguments": arguments})
        if "error" in resp:
            raise RuntimeError(f"{name}: {resp['error']}")
        text = "".join(
            c.get("text", "") for c in resp.get("result", {}).get("content", [])
        )
        return json.loads(text)


def poll_once(mark_read: bool) -> int:
    client = HarborClient(HARBOR_URL)
    client.connect()

    convs = client.tool("get_conversations", {"agent_id": AGENT_ID, "token": TOKEN})
    unread = client.tool("get_messages", {
        "agent_id": AGENT_ID, "token": TOKEN, "unread_only": True, "limit": 50,
    })
    pinned = client.tool("get_my_pins", {"agent_id": AGENT_ID, "token": TOKEN})
    stale_pins = [p for p in pinned.get("pins", []) if p.get("stale")]

    has_unread = unread.get("count", 0) > 0 or bool(stale_pins)

    if mark_read and unread.get("count", 0) > 0:
        ids = [m["id"] for m in unread["messages"]]
        client.tool("mark_messages_read", {
            "agent_id": AGENT_ID, "token": TOKEN, "message_ids": ids,
        })

    print(json.dumps({
        "type": "harbor_poll",
        "agent_id": AGENT_ID,
        "unread_total": convs.get("unread_total", 0),
        "conversations": convs.get("conversations", []),
        "messages": unread.get("messages", []),
        "stale_contract_pins": stale_pins,
    }, ensure_ascii=False))
    return 0 if has_unread else 1


def main() -> int:
    if not AGENT_ID or not TOKEN:
        print("请设置 HARBOR_AGENT_ID 和 HARBOR_TOKEN 环境变量", file=sys.stderr)
        return 2
    ap = argparse.ArgumentParser()
    ap.add_argument("--mark-read", action="store_true", help="拉完未读即标记已读")
    ap.add_argument("--watch", type=int, default=0, metavar="SEC", help="常驻模式：每 SEC 秒轮询一次")
    args = ap.parse_args()

    if args.watch <= 0:
        try:
            return poll_once(args.mark_read)
        except Exception as exc:  # noqa: BLE001 —— 门禁脚本需要把错误打出来而不是崩栈
            print(json.dumps({"type": "harbor_poll", "error": str(exc)}, ensure_ascii=False))
            return 2

    while True:
        try:
            code = poll_once(args.mark_read)
            sys.stderr.write(f"[harbor] poll exit={code}\n")
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[harbor] poll error: {exc}\n")
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
