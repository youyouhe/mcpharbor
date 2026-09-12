#!/usr/bin/env python3
"""E2E 实测反馈的优化项回归测试：
① token 漏传时返回可操作的友好错误（而不是 schema 层 "required property" 裸报错）
② 自己发出的任务事件回执不再刷屏自己的收件箱/对话流（对方视角与 get_task 时间线不受影响）
③ get_audit_log 支持按 task_id 过滤出任务完整时间线
④ 事件名合法性校验（notify / subscribe / publish_manifest）
⑤ 旧库（无 kind 列）自动迁移
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, "src")

from mcpharbor.storage import HarborStorage


_ADMIN_TOKEN = "t-admin"


def _setup():
    from mcpharbor import server
    os.environ["MCPHARBOR_ADMIN_TOKEN"] = _ADMIN_TOKEN
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()
    return server, store


def _register(server, agent_id):
    resp = json.loads(server.register_agent.fn(
        agent_id, display_name=f"显示名-{agent_id}", description=f"E2E 优化测试身份 {agent_id}"))
    assert resp.get("status") == "ok", resp
    return resp["token"]


def test_missing_token_friendly_error():
    print("=== ① token 漏传返回友好错误 ===")
    server, store = _setup()
    _register(server, "alice")
    _register(server, "bob")

    # 以前：token 是 required property，MCP schema 层直接拒掉，报错不含任何下一步指引
    # 现在：签名给默认值，落到业务校验，返回可操作的提示
    resp = json.loads(asyncio.run(server.send_message.fn(from_agent="alice", message="hi", to_agent="bob")))
    assert "error" in resp and "register_agent" in resp["error"], resp
    print("✓ send_message 漏 token 提示先注册")

    resp = json.loads(asyncio.run(server.create_task.fn(creator="alice", assignee="bob", title="t")))
    assert "error" in resp and "register_agent" in resp["error"], resp
    print("✓ create_task 漏 token 同样有指引")

    resp = json.loads(server.get_audit_log.fn())  # 同步工具，不进 asyncio.run
    assert "error" in resp and "MCPHARBOR_ADMIN_TOKEN" in resp["error"], resp
    print("✓ admin 工具漏 admin_token 提示需要环境变量对应的 token")

    # 带上正确 token 后一切照常
    tok = json.loads(server.rotate_token.fn("alice", current_token="")).get("error")
    assert tok and "token" in tok
    store.close()
    print("✓ 缺 token 校验测试通过\n")


def test_own_task_events_hidden_from_inbox():
    print("=== ② 自己的任务事件回执不刷屏收件箱 ===")
    server, store = _setup()
    cat = _register(server, "cat")
    tiger = _register(server, "tiger")

    # cat 给 tiger 派任务，然后 tiger 走完生命周期——产生一串任务事件回执
    resp = json.loads(asyncio.run(server.create_task.fn(
        creator="cat", token=cat, assignee="tiger", title="E2E", detail="走完流程")))
    task_id = resp["task_id"]
    for status, note in (("accepted", "接单"), ("working", "开工"), ("completed", "done")):
        r = json.loads(asyncio.run(server.update_task.fn(
            task_id=task_id, agent_id="tiger", token=tiger, status=status, note=note)))
        assert r["status"] == "ok", r

    # cat 视角（操作方）：不出现自己发出的【任务·新交办】回执；
    # 但 tiger 发来的状态通知（对方动作）照常可见
    cat_msgs = json.loads(server.get_messages.fn(agent_id="cat", token=cat))
    msgs = cat_msgs["messages"]
    assert all(m["from_agent"] == "tiger" for m in msgs), f"不应有 cat 自己发出的回执: {msgs}"
    assert cat_msgs["count"] == 3 and all(m["kind"] == "task_event" for m in msgs), msgs
    print("✓ cat 的收件箱没有自己发出的【新交办】回执，tiger 的状态通知完整可见")

    cat_convs = json.loads(server.get_conversations.fn(agent_id="cat", token=cat))
    conv = cat_convs["conversations"][0]
    # 只有对方发来的 3 条状态通知；自己发出的【新交办】回执不再计入（旧语义这里是 4）
    assert conv["total"] == 3 and conv["unread"] == 3, conv
    print("✓ cat 的 get_conversations total 不再被自己的任务回执虚增（3 而不是 4）")

    # tiger 视角（接收方）：cat 发来的【新交办】照常可见（收信/条件门靠它触发）；
    # tiger 自己的 3 条状态回执同样不进他自己的收件箱
    tiger_msgs = json.loads(server.get_messages.fn(agent_id="tiger", token=tiger))
    kinds = [m["kind"] for m in tiger_msgs["messages"]]
    assert kinds == ["task_event"] and tiger_msgs["messages"][0]["from_agent"] == "cat", kinds
    print("✓ tiger 收件箱只留 cat 发来的【新交办】（自己的回执不刷屏），条件门触发不受影响")

    # get_task 的时间线（related_messages）双方视角都完整
    for viewer, token in (("cat", cat), ("tiger", tiger)):
        detail = json.loads(server.get_task.fn(task_id=task_id, agent_id=viewer, token=token))
        related = detail["related_messages"]
        assert len(related) == 4, f"{viewer} 的任务时间线应完整: {len(related)}"
    print("✓ get_task related_messages 双方视角均为完整 4 条")

    # 私信不受影响：cat 发真消息，双方都看得到
    asyncio.run(server.send_message.fn(from_agent="cat", token=cat, message="真消息", to_agent="tiger"))
    cat_msgs = json.loads(server.get_messages.fn(agent_id="cat", token=cat, with_agent="tiger"))
    # cat 视角 = 3 条收到的状态通知 + 1 条自己发的真消息（真消息是 chat，不过滤）
    assert cat_msgs["count"] == 4, cat_msgs
    assert cat_msgs["messages"][0]["message"] == "真消息"  # 时间倒序，最新在 前
    print("✓ 真人私信照常收发（与过滤互不干扰）")

    store.close()
    print("✓ 任务事件过滤测试通过\n")


def test_audit_log_filter_by_task_id():
    print("=== ③ 审计日志按 task_id 过滤 ===")
    server, store = _setup()
    cat = _register(server, "cat")
    tiger = _register(server, "tiger")

    resp = json.loads(asyncio.run(server.create_task.fn(
        creator="cat", token=cat, assignee="tiger", title="审计过滤测试")))
    task_id = resp["task_id"]
    asyncio.run(server.update_task.fn(
        task_id=task_id, agent_id="tiger", token=tiger, status="accepted", note="收到"))

    # 干扰项：另一个任务
    other = json.loads(asyncio.run(server.create_task.fn(
        creator="cat", token=cat, assignee="tiger", title="别的任务")))

    admin_token = _ADMIN_TOKEN
    resp = json.loads(server.get_audit_log.fn(admin_token=admin_token, task_id=task_id, limit=100))
    entries = resp["entries"]
    assert entries, "task_id 过滤不应为空"
    for e in entries:
        in_target = e["target"] == f"task:{task_id}"
        in_detail = e["detail"].get("task_id") == task_id
        assert in_target or in_detail, f"混入了别的任务的审计: {e}"
    actions = {e["action"] for e in entries}
    assert "task.create" in actions and "task.update" in actions and "task.notify" in actions, actions
    print(f"✓ task_id={task_id} 过滤出 {len(entries)} 条完整时间线（create/update/notify），无干扰项")

    store.close()
    print("✓ 审计过滤测试通过\n")


def test_event_name_validation():
    print("=== ④ 事件名合法性校验 ===")
    server, store = _setup()
    owner = _register(server, "ev-owner")
    sub = _register(server, "ev-sub")
    admin_token = _ADMIN_TOKEN

    asyncio.run(server.publish_manifest.fn(
        berth="ev", version="1.0.0", owner="ev-owner", token=owner))

    # notify：非法事件名被拒（此前会产生 change_type="x!" 之类的脏通知）
    for bad in ("X!!", "has space", "大写", "-lead", "trail-", "x" * 65):
        resp = json.loads(asyncio.run(server.notify.fn(
            berth="ev", event=bad, token=owner, message="m")))
        assert "error" in resp and "事件名" in resp["error"], (bad, resp)
    print("✓ notify 拒绝非法事件名（大写/空格/符号/超长）")

    # 合法事件名照常广播
    resp = json.loads(asyncio.run(server.notify.fn(
        berth="ev", event="user.created", token=owner, message="正常事件")))
    assert resp["status"] == "ok", resp
    print("✓ 合法事件名（user.created）照常广播")

    # subscribe：允许 * 通配，其余非法名被拒（同步工具，不进 asyncio.run）
    resp = json.loads(server.subscribe.fn(
        subscriber="ev-sub", token=sub, berth="ev", events=["*"]))
    assert resp["status"] == "ok", resp
    resp = json.loads(server.subscribe.fn(
        subscriber="ev-sub", token=sub, berth="ev", events=["BAD NAME"]))
    assert "error" in resp and "事件名" in resp["error"], resp
    print("✓ subscribe 允许 * 通配，拒绝非法事件名")

    # publish_manifest：声明的事件列表同样校验
    resp = json.loads(asyncio.run(server.publish_manifest.fn(
        berth="ev", version="1.1.0", owner="ev-owner", token=owner, events=["ok.event", "bad event!"])))
    assert "error" in resp and "事件名" in resp["error"], resp
    resp = json.loads(asyncio.run(server.publish_manifest.fn(
        berth="ev", version="1.1.0", owner="ev-owner", token=owner, events=["ok.event"])))
    assert resp["status"] == "ok", resp
    print("✓ publish_manifest 的事件列表校验，合法列表正常发布")

    store.close()
    print("✓ 事件名校验测试通过\n")


def test_old_db_migration_adds_kind():
    print("=== ⑤ 旧库自动补 kind 列 ===")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "old.db")
        # 手工建一个"kind 列出现之前"的旧库
        conn = sqlite3.connect(path)
        conn.execute("""CREATE TABLE messages (
            id TEXT PRIMARY KEY, from_agent TEXT NOT NULL, to_agent TEXT NOT NULL,
            berth TEXT NOT NULL DEFAULT '', message TEXT NOT NULL DEFAULT '',
            correlation_id TEXT NOT NULL DEFAULT '', severity TEXT NOT NULL DEFAULT 'normal',
            created_at TEXT NOT NULL, read INTEGER NOT NULL DEFAULT 0)""")
        conn.execute("INSERT INTO messages (id, from_agent, to_agent, message, created_at) "
                     "VALUES ('old1', 'x', 'y', '旧消息', '2026-01-01T00:00:00+00:00')")
        conn.commit()
        conn.close()

        store = HarborStorage(path)  # 打开即迁移，不应报错
        msgs = store.get_messages("y")
        assert len(msgs) == 1 and msgs[0].kind == "chat", msgs
        print("✓ 旧消息打开不报错，kind 自动视为 chat")

        store.close()
    print("✓ 旧库迁移测试通过\n")


if __name__ == "__main__":
    test_missing_token_friendly_error()
    test_own_task_events_hidden_from_inbox()
    test_audit_log_filter_by_task_id()
    test_event_name_validation()
    test_old_db_migration_adds_kind()
    print("全部 E2E 优化项回归测试通过 ✅")
