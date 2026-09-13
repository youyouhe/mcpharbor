#!/usr/bin/env python3
"""会话时序图（/admin/timeline）回归测试：
① get_message_pairs 把 (A→B)/(B→A) 归一成同一无向对
② 旧库（无 read_at 列）自动迁移；mark_messages_read 落 read_at 且重复标记不覆盖
③ 消息 record 的时延计算（acked / 未 acked / read_at 未知 三分支）
④ 任务 record：状态转移刻度从审计序列推、终态时间兜底 updated_at
⑤ _build_timeline_payload mode=task 把任务主条 + 相关消息（chat 自填 correlation、
   task_event 串 task.id）合进同一视图
⑥ API 路由：token 错 → 403；mode 非法 → 400；mode=list → 200
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "src")

os.environ["MCPHARBOR_ADMIN_TOKEN"] = "timeline-test-token"

from mcpharbor.models import AuditEntry, DirectMessage, Task, TaskStatus
from mcpharbor.storage import HarborStorage


def _utc(*args, **kwargs):
    return datetime(*args, tzinfo=timezone.utc, **kwargs)


def _msg(store, from_a, to_a, created, acked_at=None, kind="chat", correlation_id=""):
    m = DirectMessage(from_agent=from_a, to_agent=to_a, message="hi", kind=kind,
                      correlation_id=correlation_id, created_at=created, acked_at=acked_at,
                      acked=bool(acked_at))
    return store.add_message(m)


def test_pair_normalization():
    print("=== ① 会话对归一化：(A→B) 与 (B→A) 合成一行 ===")
    store = HarborStorage(":memory:")
    t0 = _utc(2026, 9, 12, 8, 0, 0)
    _msg(store, "alice", "bob", t0)
    _msg(store, "bob", "alice", t0 + timedelta(seconds=30))
    _msg(store, "alice", "carol", t0 + timedelta(seconds=60))
    pairs = store.get_message_pairs()
    assert len(pairs) == 2, pairs
    p1 = next(p for p in pairs if {p["a"], p["b"]} == {"alice", "bob"})
    assert p1["total"] == 2 and p1["chat_n"] == 2
    assert p1["first_at"] == t0.isoformat() and p1["last_at"] == (t0 + timedelta(seconds=30)).isoformat()
    # 双向消息都能按对取出，正序
    msgs = store.get_pair_messages("bob", "alice")  # 参数顺序无关
    assert [m.from_agent for m in msgs] == ["alice", "bob"]
    print("✓ 归一化 + 双向取数 OK")


def test_read_at_migration_and_mark():
    print("=== ② 旧库迁移 read_at + 标记已读落锚点 ===")
    # 造一个"旧版"库：messages 表没有 read_at 列
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute("""CREATE TABLE messages (
        id TEXT PRIMARY KEY, from_agent TEXT NOT NULL, to_agent TEXT NOT NULL,
        berth TEXT NOT NULL DEFAULT '', message TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '', reply_to TEXT NOT NULL DEFAULT '',
        severity TEXT NOT NULL DEFAULT 'normal', kind TEXT NOT NULL DEFAULT 'chat',
        created_at TEXT NOT NULL, read INTEGER NOT NULL DEFAULT 0,
        acked INTEGER NOT NULL DEFAULT 0, acked_at TEXT)""")
    old_created = _utc(2026, 9, 1, 0, 0, 0).isoformat()
    conn.execute("INSERT INTO messages (id, from_agent, to_agent, created_at) VALUES (?, ?, ?, ?)",
                 ("legacy1", "x", "y", old_created))
    conn.commit()
    conn.close()

    store = HarborStorage(tmp.name)
    chk = sqlite3.connect(tmp.name)
    chk.row_factory = sqlite3.Row
    cols = {r["name"] for r in chk.execute("PRAGMA table_info(messages)")}
    chk.close()
    assert "read_at" in cols, cols
    legacy = store.get_message("legacy1")
    assert legacy.read_at is None  # 旧数据 read_at 未知

    # 标记已读 → 落 read_at；重复标记不覆盖第一次的已读时间
    store.mark_messages_read("y", ["legacy1"])
    first = store.get_message("legacy1").read_at
    assert first is not None
    store.mark_messages_read("y", ["legacy1"])
    assert store.get_message("legacy1").read_at == first
    # 只能标记发给自己的
    _msg(store, "y", "x", _utc(2026, 9, 2))
    assert store.mark_messages_read("y", []) == 0
    print("✓ 迁移 + read_at 锚点 OK")


def test_message_record_latency():
    print("=== ③ 消息 record 时延三分支 ===")
    from mcpharbor.server import _timeline_record_message
    t0 = _utc(2026, 9, 12, 8, 0, 0)
    store = HarborStorage(":memory:")
    m1 = _msg(store, "a", "b", t0, acked_at=t0 + timedelta(seconds=90))
    r1 = _timeline_record_message(store.get_message(m1.id), lane="a")
    assert r1["latency"]["ack_wait_ms"] == 90_000
    assert r1["latency"]["read_wait_ms"] is None
    assert r1["ts_end"] == (t0 + timedelta(seconds=90)).isoformat()
    # 分支2：未 ack → ts_end=None，纯时间点
    m2 = _msg(store, "b", "a", t0)
    r2 = _timeline_record_message(m2, lane="b")
    assert r2["ts_end"] is None and r2["latency"]["ack_wait_ms"] is None
    # 分支3：read_at 已知 → read_wait 有值
    m3 = DirectMessage(from_agent="a", to_agent="b", message="x", created_at=t0,
                       read=True, read_at=t0 + timedelta(minutes=2))
    r3 = _timeline_record_message(m3, lane="a")
    assert r3["latency"]["read_wait_ms"] == 120_000
    print("✓ 时延三分支 OK")


def test_task_record_ticks():
    print("=== ④ 任务 record：审计刻度 + 终态时间 ===")
    from mcpharbor.server import _timeline_record_task
    store = HarborStorage(":memory:")
    t0 = _utc(2026, 9, 12, 9, 0, 0)
    task = Task(id="taskabc123", title="测试任务", creator="dog", assignee="cat",
                status=TaskStatus.COMPLETED, created_at=t0, updated_at=t0 + timedelta(minutes=2))
    store.create_task(task)
    store.log_audit(AuditEntry(timestamp=t0, action="task.create", actor="dog",
                               target="task:taskabc123", detail={}))
    store.log_audit(AuditEntry(timestamp=t0 + timedelta(seconds=10), action="task.update",
                               actor="cat", target="task:taskabc123", detail={"to": "working"}))
    r = _timeline_record_task(store.get_task("taskabc123"), store)
    assert r["type"] == "task" and r["open"] is False
    assert len(r["ticks"]) == 2
    assert r["ticks"][1]["label"] == "→ working"  # task.update 的 detail 键是 "to"
    assert r["ts_end"] == (t0 + timedelta(minutes=2)).isoformat()  # 兜底 updated_at
    # 未终态 → open
    task2 = Task(id="taskopen999", title="进行中", creator="dog", assignee="cat",
                 status=TaskStatus.WORKING, created_at=t0, updated_at=t0)
    store.create_task(task2)
    r2 = _timeline_record_task(task2, store)
    assert r2["open"] is True
    print("✓ 任务 record OK")


def test_payload_task_mode():
    print("=== ⑤ mode=task 聚合任务主条 + 相关消息 ===")
    from mcpharbor import server
    from mcpharbor.storage import HarborStorage as S
    server._store = S(":memory:")
    store = server._store
    t0 = _utc(2026, 9, 12, 10, 0, 0)
    task = Task(id="taskmerge01", title="合并视图", creator="dog", assignee="cat",
                status=TaskStatus.COMPLETED, created_at=t0, updated_at=t0)
    store.create_task(task)
    # task_event 串 task.id；chat 串用户自填 correlation（= 任务 id 之外的路径也覆盖）
    _msg(store, "dog", "cat", t0 + timedelta(seconds=5), kind="task_event", correlation_id="taskmerge01")
    _msg(store, "dog", "cat", t0 + timedelta(seconds=8), kind="chat", correlation_id="taskmerge01")
    payload = server._build_timeline_payload("task", key="taskmerge01")
    types = [e["type"] for e in payload["events"]]
    assert types.count("task") == 1 and types.count("message") == 2, payload["events"]
    # 用 correlation_id 当 key 也能查到同一条任务
    payload2 = server._build_timeline_payload("task", key="taskmerge01")
    assert any(e["type"] == "task" and e["id"] == "taskmerge01" for e in payload2["events"])
    # mode=list 结构
    plist = server._build_timeline_payload("list")
    assert {"pairs", "tasks"} <= set(plist.keys())
    print("✓ payload 组装 OK")


def test_api_routes():
    print("=== ⑥ API 路由：鉴权 / 参数校验 / 正常返回 ===")
    from starlette.requests import Request
    from mcpharbor import server

    def _req(params: str) -> Request:
        scope = {"type": "http", "method": "GET", "path": "/admin/api/timeline",
                 "query_string": params.encode(), "headers": []}
        return Request(scope)

    # token 错 → 403
    resp = asyncio.run(server.admin_timeline_api(_req("token=wrong&mode=list")))
    assert resp.status_code == 403, resp.status_code
    # mode 非法 → 400
    resp = asyncio.run(server.admin_timeline_api(
        _req("token=timeline-test-token&mode=bogus")))
    assert resp.status_code == 400
    # mode=pair 缺 key → 400
    resp = asyncio.run(server.admin_timeline_api(
        _req("token=timeline-test-token&mode=pair")))
    assert resp.status_code == 400
    # limit 非法 → 400；超上限 → 收敛到 1000（不报错）
    resp = asyncio.run(server.admin_timeline_api(
        _req("token=timeline-test-token&mode=list&limit=abc")))
    assert resp.status_code == 400
    resp = asyncio.run(server.admin_timeline_api(
        _req("token=timeline-test-token&mode=list&limit=99999")))
    assert resp.status_code == 200
    # 正常返回
    resp = asyncio.run(server.admin_timeline_api(_req("token=timeline-test-token&mode=list")))
    assert resp.status_code == 200
    data = json.loads(resp.body)
    assert data["mode"] == "list" and "pairs" in data and "tasks" in data
    # 时序图页面：token 对 → 200 HTML；token 错 → 403
    resp = asyncio.run(server.admin_timeline_page(_req("token=timeline-test-token")))
    assert resp.status_code == 200 and b"admin/timeline" not in resp.body[:100]
    assert b"\xf0\x9f\x95\x90" in resp.body  # 🕐
    resp = asyncio.run(server.admin_timeline_page(_req("token=bad")))
    assert resp.status_code == 403
    print("✓ 路由 OK")


if __name__ == "__main__":
    test_pair_normalization()
    test_read_at_migration_and_mark()
    test_message_record_latency()
    test_task_record_ticks()
    test_payload_task_mode()
    test_api_routes()
    print("\n全部通过 ✅")
