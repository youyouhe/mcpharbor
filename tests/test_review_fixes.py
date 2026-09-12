#!/usr/bin/env python3
"""代码审查发现问题的回归测试：notify 鉴权、publish_manifest 脏写、register_agent
竞态、任务 deadline 时区、收件人吊销检查、pin_contract id 一致性。"""

import asyncio
import json
import sys

sys.path.insert(0, "src")

from datetime import datetime, timedelta, timezone

from mcpharbor.storage import HarborStorage


def _setup():
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()
    return server, store


def _register(server, agent_id, **kwargs):
    resp = json.loads(server.register_agent.fn(agent_id, **kwargs))
    assert resp.get("status") == "ok", resp
    return resp["token"]


def test_notify_requires_owner_token():
    print("=== 测试 notify 需要 berth owner 的 token ===")
    server, store = _setup()

    owner_token = _register(server, "auth-team", display_name="认证团队", description="notify 鉴权测试owner")
    outsider_token = _register(server, "outsider", display_name="路人", description="notify 鉴权测试旁观者")

    asyncio.run(server.publish_manifest.fn(
        berth="auth", version="1.0.0", owner="auth-team", token=owner_token))
    json.loads(server.subscribe.fn(subscriber="outsider", token=outsider_token, berth="auth"))

    # 没带 token 完全无法调用（缺参数）；带错误 token 应被拒绝，且不能冒充 actor
    resp = json.loads(asyncio.run(server.notify.fn(
        berth="auth", event="user.created", token="wrong-token", actor="auth-team")))
    assert "error" in resp and "owner" in resp["error"]
    print("✓ 错误 token 广播被拒绝")

    # 用旁观者自己的（合法）token 广播 auth 的通知——不是 owner，应被拒绝
    resp = json.loads(asyncio.run(server.notify.fn(
        berth="auth", event="user.created", token=outsider_token, actor="auth-team")))
    assert "error" in resp and "owner" in resp["error"]
    print("✓ 非 owner 的合法 token 也不能广播别人的 berth（防冒充 actor）")

    # 不存在的 berth
    resp = json.loads(asyncio.run(server.notify.fn(berth="ghost", event="x", token=owner_token)))
    assert "error" in resp and "不存在" in resp["error"]
    print("✓ 不存在的 berth 被拒绝")

    # 用真正 owner 的 token 才能广播
    resp = json.loads(asyncio.run(server.notify.fn(
        berth="auth", event="user.created", token=owner_token, message="真实公告")))
    assert resp["status"] == "ok" and resp["notified"] == 1
    print("✓ owner 用自己的 token 广播成功")

    # 审计记录的 actor 是真实认证到的 owner，不是调用方随便填的字符串
    logs = store.get_audit_log(action="notify", limit=5)
    assert logs[0].actor == "auth-team"
    print("✓ 审计里的 actor 是认证结果，不是调用方自称的值")

    store.close()
    print("✓ notify 鉴权测试通过\n")


def test_publish_manifest_bad_errors_no_partial_write():
    print("=== 测试 publish_manifest 非法 errors 不留脏数据 ===")
    server, store = _setup()

    owner_token = _register(server, "b-team", display_name="B队", description="脏写测试用身份")

    resp = asyncio.run(server.publish_manifest.fn(
        berth="b1", version="1.0", owner="b-team", token=owner_token,
        errors={"not-a-number": "bad"}))
    resp = json.loads(resp)
    assert "error" in resp and "数字错误码" in resp["error"]
    print("✓ 非数字错误码返回友好错误（不是未捕获异常）")

    # 关键：berth 不应该被创建成"指向不存在版本"的半成品状态
    assert store.get_berth("b1") is None
    assert store.get_manifest("b1") is None
    print("✓ 校验失败时完全没有落库（berth 和 manifest 都不存在，不是脏数据）")

    # 正常调用应该照常成功
    resp = json.loads(asyncio.run(server.publish_manifest.fn(
        berth="b1", version="1.0", owner="b-team", token=owner_token,
        errors={"401": "invalid_token"})))
    assert resp["status"] == "ok"
    assert store.get_manifest("b1") is not None
    print("✓ 正常 errors 字典依然正常发布")

    store.close()
    print("✓ publish_manifest 脏写测试通过\n")


def test_register_agent_race_does_not_lie():
    print("=== 测试 register_agent 竞态不撒谎 ===")
    server, store = _setup()

    # 模拟并发：底层 create_agent_token 返回 False（另一个请求先赢了），
    # 但顶层 get_agent_token 的"是否已注册"检查还没看到那条记录（真实竞态里两者之间有时间窗口）。
    original = store.create_agent_token
    store.create_agent_token = lambda *a, **k: False

    resp = json.loads(server.register_agent.fn(
        "race-agent", display_name="竞态测试", description="竞态测试身份，验证不撒谎"))
    assert "error" in resp, resp
    assert "抢先注册" in resp["error"]
    print(f"✓ 底层写入失败时不会假装成功：{resp['error'][:40]}")

    store.create_agent_token = original
    store.close()
    print("✓ register_agent 竞态测试通过\n")


def test_task_deadline_naive_input_treated_as_utc():
    print("=== 测试任务 deadline 裸时间按 UTC 解释 ===")
    server, store = _setup()

    boss = _register(server, "boss", display_name="交办方", description="deadline 时区测试")
    worker = _register(server, "worker", display_name="受托方", description="deadline 时区测试对象")

    # 裸时间（不带时区）——1 分钟前的 UTC 时间点
    past_utc_naive = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")
    resp = json.loads(asyncio.run(server.create_task.fn(
        creator="boss", token=boss, assignee="worker", title="裸时间应按UTC",
        deadline=past_utc_naive)))
    assert resp["status"] == "ok"
    tid = resp["task_id"]

    # 存库的 deadline 必须带上 UTC 时区标记，不能是裸字符串——否则和 sweep 用的
    # now_iso（带 +00:00）做字符串比较时会产生时区歧义。
    task = store.get_task(tid)
    assert task.deadline.endswith("+00:00"), f"deadline 应带 UTC 标记，实际: {task.deadline}"
    print(f"✓ 裸时间入库后带上了 UTC 标记: {task.deadline}")

    # 既然是 1 分钟前的 UTC 时间，扫尾应该立刻判定超时
    swept = store.sweep_stale_tasks()
    assert any(t.id == tid and t.status.value == "failed" for t in swept)
    print("✓ 裸时间被正确当作 UTC 处理，扫尾立即判定超时（没有 8 小时时区偏移）")

    store.close()
    print("✓ deadline 时区测试通过\n")


def test_revoked_recipient_rejected():
    print("=== 测试收件人/受托方被吊销后拒绝新消息与任务 ===")
    server, store = _setup()
    import os
    os.environ["MCPHARBOR_ADMIN_TOKEN"] = "t-admin"

    a = _register(server, "sender", display_name="发件方", description="吊销收件人测试")
    b = _register(server, "revoked-target", display_name="被吊销方", description="吊销收件人测试对象")

    json.loads(server.admin_manage_agent.fn(admin_token="t-admin", agent_id="revoked-target", action="revoke"))

    resp = json.loads(asyncio.run(server.send_message.fn(
        from_agent="sender", token=a, to_agent="revoked-target", message="你好")))
    assert "error" in resp and "吊销" in resp["error"]
    print("✓ send_message 拒绝发给已吊销的收件人")

    resp = json.loads(asyncio.run(server.create_task.fn(
        creator="sender", token=a, assignee="revoked-target", title="派活")))
    assert "error" in resp and "吊销" in resp["error"]
    print("✓ create_task 拒绝派给已吊销的受托方")

    resp = json.loads(asyncio.run(server.admin_command.fn(
        admin_token="t-admin", to_agent="revoked-target", command="指令")))
    assert "error" in resp and "吊销" in resp["error"]
    print("✓ admin_command 拒绝下发给已吊销的 agent")

    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
    store.close()
    print("✓ 吊销收件人测试通过\n")


def test_pin_contract_id_consistency():
    print("=== 测试 pin_contract 返回的 id 与落库一致 ===")
    server, store = _setup()

    owner = _register(server, "owner", display_name="发布方", description="pin id 一致性测试")
    consumer = _register(server, "consumer", display_name="消费方", description="pin id 一致性测试对象")
    asyncio.run(server.publish_manifest.fn(berth="p1", version="1.0", owner="owner", token=owner))

    pin1 = store.pin_contract("consumer", "p1", "1.0", "task-x")
    row_id_1 = store.get_pins("consumer")[0].id
    assert pin1.id == row_id_1, f"首次钉：返回的 id={pin1.id} 应等于库里的 id={row_id_1}"
    print(f"✓ 首次 pin_contract 返回的 id 与库里一致: {pin1.id}")

    # 重复钉（更新版本）：ON CONFLICT 分支，id 应保持不变且和库里一致
    pin2 = store.pin_contract("consumer", "p1", "1.0", "task-x")
    row_id_2 = store.get_pins("consumer")[0].id
    assert pin2.id == row_id_2 == row_id_1, "重复钉不应产生新 id，且返回值要和库里一致"
    print(f"✓ 重复 pin_contract（更新版本）id 保持稳定且与库一致: {pin2.id}")

    store.close()
    print("✓ pin_contract id 一致性测试通过\n")


if __name__ == "__main__":
    test_notify_requires_owner_token()
    test_publish_manifest_bad_errors_no_partial_write()
    test_register_agent_race_does_not_lie()
    test_task_deadline_naive_input_treated_as_utc()
    test_revoked_recipient_rejected()
    test_pin_contract_id_consistency()
    print("🎉 代码审查回归测试全部通过。")
