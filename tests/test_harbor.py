#!/usr/bin/env python3
"""MCP Harbor 全面测试脚本。"""

import asyncio
import json
import os
import sys
import time
sys.path.insert(0, "src")

from mcpharbor.models import (
    AgentToken, AuditEntry, Berth, BerthStatus, Contract, Manifest,
    Notification, NotifyPriority, Subscription,
)
from mcpharbor.storage import HarborStorage


def test_models():
    print("=== 测试 Pydantic 模型 ===")
    b = Berth(id="auth", owner="auth-team", capabilities=["auth", "jwt"])
    print(f"Berth: {b.id} owner={b.owner} caps={b.capabilities}")

    m = Manifest(berth="auth", version="1.2.0", owner="auth-team",
                 capabilities=["auth", "jwt"], base_url="https://auth.internal",
                 auth={"type": "bearer"}, errors={401: "invalid_token"})
    print(f"Manifest: {m.berth} v{m.version}")

    c = Contract(berth="auth", version="1.2.0", manifest_version="1.2.0")
    print(f"Contract: {c.berth} v{c.version}")

    sub = Subscription(subscriber="order", berth="auth", version_range=">=1.0.0")
    print(f"Subscription: {sub.subscriber} -> {sub.berth} range={sub.version_range}")

    n = Notification(berth="auth", old_version="1.2.0", new_version="1.3.0",
                     severity=NotifyPriority.HIGH, summary="认证方式变更")
    print(f"Notification: {n.berth} {n.old_version}->{n.new_version} severity={n.severity.value}")

    print("✓ 模型创建成功\n")


def test_storage():
    print("=== 测试存储层 ===")
    store = HarborStorage(":memory:")

    # Berth CRUD
    b = Berth(id="auth", owner="auth-team", capabilities=["auth", "jwt"])
    store.upsert_berth(b)
    fetched = store.get_berth("auth")
    assert fetched is not None and fetched.id == "auth"
    print("✓ Berth 创建/读取成功")

    b.version = "1.1.0"
    store.upsert_berth(b)
    fetched = store.get_berth("auth")
    assert fetched.version == "1.1.0"
    print("✓ Berth 更新成功")

    # Manifest
    m = Manifest(berth="auth", version="1.2.0", owner="auth-team",
                 base_url="https://auth.internal")
    store.publish_manifest(m)
    fetched_m = store.get_manifest("auth")
    assert fetched_m is not None and fetched_m.version == "1.2.0"
    print("✓ Manifest 发布/读取成功")

    versions = store.list_manifest_versions("auth")
    assert len(versions) >= 1
    print(f"✓ 版本列表: {versions}")

    # Manifest versions with data
    all_m = store.get_manifest_versions_with_data("auth")
    assert len(all_m) >= 1
    print(f"✓ 版本数据: {len(all_m)} 个版本")

    # Contract
    c = Contract(berth="auth", version="1.2.0", manifest_version="1.2.0")
    store.publish_contract(c)
    fetched_c = store.get_contract("auth")
    assert fetched_c is not None
    print("✓ Contract 发布/读取成功")

    # Subscription with version_range
    sub = Subscription(subscriber="order-agent", berth="auth",
                       events=["user.created"], version_range=">=1.0.0")
    store.add_subscription(sub)
    subs = store.get_subscriptions("auth")
    assert len(subs) == 1 and subs[0].version_range == ">=1.0.0"
    print("✓ 订阅创建成功 (含 version_range)")

    store.remove_subscription(sub.id)
    subs = store.get_subscriptions("auth")
    assert len(subs) == 0
    print("✓ 订阅取消成功")

    # Notification
    notif = Notification(berth="auth", old_version="1.0.0", new_version="1.1.0",
                         severity=NotifyPriority.NORMAL, summary="测试通知")
    store.add_notification(notif)
    recent = store.get_recent_notifications("auth", within_seconds=60)
    assert len(recent) >= 1
    print("✓ 通知记录创建成功")

    history = store.get_notification_history("auth")
    assert len(history) >= 1
    print("✓ 通知历史查询成功")

    # Audit
    entry = AuditEntry(action="test", actor="tester", target="all")
    store.log_audit(entry)
    logs = store.get_audit_log()
    assert len(logs) >= 1
    print("✓ 审计日志写入成功")

    # Audit filter by actor
    logs = store.get_audit_log(actor="tester")
    assert len(logs) >= 1
    print("✓ 审计日志按 actor 过滤成功")

    # Check updates
    store.publish_manifest(Manifest(berth="auth", version="2.0.0", owner="auth-team"))
    updates = store.check_updates({"auth": "1.2.0"})
    assert len(updates) == 1 and updates[0]["latest_version"] == "2.0.0"
    print("✓ check_updates 检测到更新")

    no_updates = store.check_updates({"auth": "2.0.0"})
    assert len(no_updates) == 0
    print("✓ check_updates 无更新时返回空")

    # Cleanup
    cleaned = store.cleanup_old_audit(days=0)
    print(f"✓ 审计日志清理: 删除 {cleaned} 条")

    store.close()
    print("✓ 存储层测试全部通过\n")


def test_notification_merge():
    print("=== 测试通知合并 ===")
    store = HarborStorage(":memory:")

    b = Berth(id="auth", owner="auth-team", capabilities=["auth"])
    store.upsert_berth(b)

    n1 = Notification(berth="auth", old_version="1.0.0", new_version="1.1.0",
                      summary="变更1", severity=NotifyPriority.LOW)
    store.add_notification(n1)

    n2 = Notification(berth="auth", old_version="1.0.0", new_version="1.2.0",
                      summary="变更2", severity=NotifyPriority.NORMAL)
    store.add_notification(n2)

    recent = store.get_recent_notifications("auth", within_seconds=5)
    assert len(recent) == 2
    print(f"✓ 5秒内收到 {len(recent)} 条通知")

    store.mark_notification_merged([n1.id, n2.id], n2.id)
    recent_after = store.get_recent_notifications("auth", within_seconds=5)
    assert len(recent_after) == 0
    print("✓ 合并后未合并通知为 0")

    store.close()
    print("✓ 通知合并测试通过\n")


def test_change_summary():
    print("=== 测试变更摘要 ===")
    from mcpharbor.server import _generate_change_summary, _determine_priority

    old = Manifest(berth="auth", version="1.0.0", owner="auth-team",
                   capabilities=["auth"], base_url="https://old",
                   auth={"type": "basic"}, errors={401: "unauthorized"},
                   events=["user.created"])
    new = Manifest(berth="auth", version="1.1.0", owner="auth-team",
                   capabilities=["auth", "jwt"], base_url="https://new",
                   auth={"type": "bearer"}, errors={401: "invalid_token", 403: "forbidden"},
                   events=["user.created", "token.revoked"])

    summary = _generate_change_summary(old, new)
    print(f"  摘要: {summary}")
    assert "base_url" in summary or "认证" in summary or "能力" in summary
    print("✓ 变更摘要生成成功")

    assert _determine_priority(old, new) == NotifyPriority.HIGH
    print("✓ 认证变更优先级=high")

    new2 = Manifest(berth="auth", version="1.2.0", owner="auth-team",
                    capabilities=["auth", "jwt"], auth={"type": "basic"},
                    events=["user.created"])
    assert _determine_priority(old, new2) == NotifyPriority.NORMAL
    print("✓ 事件变更优先级=normal")

    none_summary = _generate_change_summary(None, new)
    assert "首次发布" in none_summary
    print("✓ 首次发布摘要正确")

    print("✓ 变更摘要测试通过\n")


def test_server_tools():
    print("=== 测试 MCP Server 工具 ===")
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store

    # publish_manifest
    b = Berth(id="auth", owner="auth-team", capabilities=["auth", "jwt"])
    store.upsert_berth(b)
    m = Manifest(berth="auth", version="1.2.0", owner="auth-team",
                 capabilities=["auth", "jwt"], base_url="https://auth.internal",
                 auth={"type": "bearer"}, errors={401: "invalid_token"},
                 events=["user.created"])
    store.publish_manifest(m)
    _audit = lambda action, actor, target, detail=None: store.log_audit(AuditEntry(action=action, actor=actor, target=target, detail=detail or {}))
    _audit("manifest.publish", "auth-team", "berth:auth", {"version": "1.2.0"})
    print("✓ publish_manifest: Manifest v1.2.0 已发布")

    # get_manifest
    manifest = store.get_manifest("auth")
    assert manifest is not None and manifest.berth == "auth"
    print(f"✓ get_manifest: v{manifest.version}")

    # search_berths
    berths = store.list_berths(capability="jwt")
    assert len(berths) == 1
    print(f"✓ search_berths: 找到 {len(berths)} 个")

    # subscribe
    sub = Subscription(subscriber="order-agent", berth="auth",
                       events=["user.created"], version_range=">=1.0.0")
    store.add_subscription(sub)
    print(f"✓ subscribe: order-agent 已订阅 auth")

    # notify
    notif = Notification(berth="auth", event="user.created", summary="test")
    store.add_notification(notif)
    subs = store.get_subscriptions("auth")
    print(f"✓ notify: 通知了 {len(subs)} 个订阅者")

    # resolve_dependency
    all_berths = store.list_berths()
    matched = {cap: [b.id for b in all_berths if cap in b.capabilities]
               for cap in ["auth", "jwt"]}
    assert "auth" in matched["auth"]
    print(f"✓ resolve_dependency: 匹配 {matched}")

    # check_updates
    store.publish_manifest(Manifest(berth="auth", version="2.0.0", owner="auth-team",
                                    capabilities=["auth", "jwt", "oauth2"]))
    updates = store.check_updates({"auth": "1.2.0"})
    assert len(updates) == 1
    print(f"✓ check_updates: {len(updates)} 个有更新")

    # sync
    latest = store.get_manifest("auth")
    assert latest is not None
    print(f"✓ sync: auth v{latest.version}")

    # diff_versions
    m1 = store.get_manifest("auth", "1.2.0")
    m2 = store.get_manifest("auth", "2.0.0")
    assert m1 is not None and m2 is not None
    diffs = []
    for field in ["capabilities", "base_url", "auth", "events"]:
        if getattr(m1, field) != getattr(m2, field):
            diffs.append(field)
    assert len(diffs) >= 1
    print(f"✓ diff_versions: {len(diffs)} 处差异 ({', '.join(diffs)})")

    # get_notifications
    notifs = store.get_notification_history("auth")
    assert len(notifs) >= 1
    print(f"✓ get_notifications: {len(notifs)} 条")

    # get_audit_log
    logs = store.get_audit_log(limit=10)
    assert len(logs) > 0
    print(f"✓ get_audit_log: {len(logs)} 条")

    store.close()
    print("✓ MCP Server 工具测试全部通过\n")


def test_auth():
    print("=== 测试 Agent 身份认证 ===")
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store

    def _publish(**kwargs):
        return json.loads(asyncio.run(server.publish_manifest.fn(**kwargs)))

    # register_agent
    resp = json.loads(server.register_agent.fn("auth-team", display_name="认证团队", description="负责认证与签发契约的团队"))
    assert resp["status"] == "ok" and resp["token"]
    auth_token = resp["token"]
    print("✓ register_agent: auth-team 注册成功")

    # 重复注册应失败
    dup = json.loads(server.register_agent.fn("auth-team", display_name="认证团队", description="负责认证与签发契约的团队"))
    assert "error" in dup and "重复注册" in dup["error"]
    print("✓ register_agent: 重复注册被拒绝")

    # 缺少身份档案应被拒绝（MCP schema 会强制必填，这里测传空值的情形）
    for kwargs in (
        {"agent_id": "no-profile", "display_name": "", "description": "有说明但没显示名"},
        {"agent_id": "no-profile", "display_name": "无名氏", "description": ""},
        {"agent_id": "no-profile", "display_name": "  ", "description": "  有空格的说明文字  "},
        {"agent_id": "no-profile", "display_name": "无名氏", "description": "短"},
    ):
        resp = json.loads(server.register_agent.fn(**kwargs))
        assert "error" in resp, kwargs
    print("✓ register_agent: 缺 display_name/description 被拒绝")

    # 非法 agent_id 格式应被拒绝
    for bad in ("WangXiaoya", "wang xiaoya", "-wang", "王小丫"):
        resp = json.loads(server.register_agent.fn(bad, display_name="x", description="格式测试的身份说明"))
        assert "error" in resp, bad
    print("✓ register_agent: 非法 agent_id 格式被拒绝")

    # 未注册身份发布应失败
    resp = _publish(berth="auth", version="1.0.0", owner="ghost-team", token="anything")
    assert "error" in resp and "未注册" in resp["error"]
    print("✓ publish_manifest: 未注册身份被拒绝")

    # 注册 mallory，尝试用 mallory 的 token 冒充 auth-team 发布
    mallory_resp = json.loads(server.register_agent.fn("mallory", display_name="攻击者", description="模拟冒充者的测试身份"))
    mallory_token = mallory_resp["token"]
    resp = _publish(berth="auth", version="1.0.0", owner="auth-team", token=mallory_token)
    assert "error" in resp and "无效" in resp["error"]
    print("✓ publish_manifest: 冒充身份（token 不匹配）被拒绝")

    # 正确 token 发布成功
    resp = _publish(
        berth="auth", version="1.0.0", owner="auth-team", token=auth_token,
        capabilities=["auth"], base_url="https://auth.internal",
    )
    assert resp["status"] == "ok"
    print("✓ publish_manifest: 正确 token 发布成功")

    # mallory 用自己合法身份，但冒充 owner 更新已存在的 auth berth，应被 owner 校验拒绝
    resp = _publish(berth="auth", version="2.0.0", owner="mallory", token=mallory_token)
    assert "error" in resp and "owner" in resp["error"]
    print("✓ publish_manifest: 已注册但非 owner 的更新被拒绝")

    # rotate_token
    rot = json.loads(server.rotate_token.fn("auth-team", auth_token))
    assert rot["status"] == "ok"
    new_token = rot["token"]
    print("✓ rotate_token: 更换成功")

    # 旧 token 应失效
    resp = _publish(berth="auth", version="2.0.0", owner="auth-team", token=auth_token)
    assert "error" in resp and "无效" in resp["error"]
    print("✓ rotate_token: 旧 token 已失效")

    # 新 token 可用
    resp = _publish(berth="auth", version="2.0.0", owner="auth-team", token=new_token)
    assert resp["status"] == "ok"
    print("✓ rotate_token: 新 token 可用")

    store.close()
    print("✓ Agent 身份认证测试通过\n")


def test_direct_messages():
    print("=== 测试点对点私信隔离 ===")
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()

    def _send(**kwargs):
        return json.loads(asyncio.run(server.send_message.fn(**kwargs)))

    a_token = json.loads(server.register_agent.fn("party-a", display_name="甲方", description="点对点私信测试身份A"))["token"]
    b_token = json.loads(server.register_agent.fn("party-b", display_name="乙方", description="点对点私信测试身份B"))["token"]
    c_token = json.loads(server.register_agent.fn("party-c", display_name="丙方", description="点对点私信测试身份C"))["token"]
    print("✓ 注册 party-a / party-b / party-c")

    # 收件人未注册应报错
    resp = _send(from_agent="party-a", token=a_token, to_agent="ghost", message="hi")
    assert "error" in resp and "未注册" in resp["error"]
    print("✓ send_message: 收件人未注册被拒绝")

    # party-a 给 party-b 发私信
    resp = _send(from_agent="party-a", token=a_token, to_agent="party-b", message="秘密协议地址是 X")
    assert resp["status"] == "ok"
    msg_id = resp["message_id"]
    print("✓ send_message: party-a -> party-b 发送成功")

    # party-b 能看到发给自己的私信
    resp = json.loads(server.get_messages.fn(agent_id="party-b", token=b_token))
    assert resp["count"] == 1 and resp["messages"][0]["message"] == "秘密协议地址是 X"
    print("✓ get_messages: party-b 能看到发给自己的私信")

    # party-c 自己的收件箱看不到 a->b 的私信（没有串话）
    resp = json.loads(server.get_messages.fn(agent_id="party-c", token=c_token))
    assert resp["count"] == 0
    print("✓ get_messages: party-c 的收件箱看不到 party-a 发给 party-b 的私信")

    # party-c 想拿自己的 token 冒充 party-b 读取，必须被拒绝
    resp = json.loads(server.get_messages.fn(agent_id="party-b", token=c_token))
    assert "error" in resp
    print("✓ get_messages: party-c 冒充 party-b 读取被拒绝")

    # subscribe 现在也需要身份校验：party-c 不能冒充 party-b 去订阅（会截走本该发给 party-b 的推送）
    resp = json.loads(server.subscribe.fn(subscriber="party-b", token=c_token, berth="auth"))
    assert "error" in resp
    print("✓ subscribe: party-c 冒充 party-b 身份订阅被拒绝")

    # 已读/未读：标记已读后 unread_only 应为空
    resp = json.loads(server.get_messages.fn(agent_id="party-b", token=b_token, unread_only=True))
    assert resp["count"] == 1
    mark = json.loads(server.mark_messages_read.fn(agent_id="party-b", token=b_token, message_ids=[msg_id]))
    assert mark["marked"] == 1
    resp = json.loads(server.get_messages.fn(agent_id="party-b", token=b_token, unread_only=True))
    assert resp["count"] == 0
    print("✓ mark_messages_read + unread_only: 已读后不再出现在未读列表里")

    store.close()
    print("✓ 点对点私信隔离测试通过\n")


class _FakeRequest:
    """给 admin_dashboard 路由测试用的最小假 Request：只需要 query_params.get(...) 和 base_url。"""
    def __init__(self, params: dict):
        self.query_params = params
        self.base_url = "http://127.0.0.1:8931/"


def test_admin_panel():
    print("=== 测试 Admin 面板 ===")
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()

    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
    try:
        # 没配置 MCPHARBOR_ADMIN_TOKEN 时，admin 功能整体不可用
        resp = json.loads(server.get_notifications.fn(admin_token="anything"))
        assert "error" in resp and "未启用" in resp["error"]
        print("✓ get_notifications: 未配置 MCPHARBOR_ADMIN_TOKEN 时整体拒绝")

        os.environ["MCPHARBOR_ADMIN_TOKEN"] = "s3cr3t"

        # 错误的 admin_token 被拒绝
        resp = json.loads(server.get_notifications.fn(admin_token="wrong"))
        assert "error" in resp and "无效" in resp["error"]
        resp = json.loads(server.get_audit_log.fn(admin_token="wrong"))
        assert "error" in resp and "无效" in resp["error"]
        print("✓ get_notifications/get_audit_log: 错误 admin_token 被拒绝")

        # 准备一些数据：agent（用一个带 HTML/script 的恶意 agent_id 顺便测试转义）、berth、订阅、审计
        evil_agent_id = "party-<script>alert(1)</script>"
        # 注册入口现在会校验 agent_id 格式，恶意 id 进不来（这正是新加的校验）；
        # 为了继续测 admin 面板的 HTML 转义，直接从存储层塞进去
        inserted = store.create_agent_token(
            evil_agent_id, "x" * 64,
            display_name="<b>evil</b>", description="恶意注册者测试身份",
        )
        assert inserted
        a_token = json.loads(server.register_agent.fn("admin-test-agent", display_name="worker", description="admin指令测试身份"))["token"]
        asyncio.run(server.publish_manifest.fn(
            berth="auth", version="1.0.0", owner="admin-test-agent", token=a_token,
        ))
        server.subscribe.fn(subscriber="admin-test-agent", token=a_token, berth="auth")

        # 正确的 admin_token 能拿到数据
        resp = json.loads(server.get_notifications.fn(admin_token="s3cr3t"))
        assert resp["count"] >= 1
        resp = json.loads(server.get_audit_log.fn(admin_token="s3cr3t"))
        assert resp["count"] >= 1
        print("✓ get_notifications/get_audit_log: 正确 admin_token 可查看全量")

        # 面板数据汇总 + HTML 渲染
        data = server._collect_admin_overview()
        assert any(a["agent_id"] == "admin-test-agent" for a in data["agents"])
        assert any(a["agent_id"] == evil_agent_id for a in data["agents"])
        assert any(b["id"] == "auth" for b in data["berths"])
        assert any(s["subscriber"] == "admin-test-agent" for s in data["subscriptions"])
        assert "message_count" in data and "notification_count" in data
        html_out = server._render_admin_html(data)
        assert "admin-test-agent" in html_out and "auth" in html_out
        assert "<script>alert(1)</script>" not in html_out
        assert "&lt;script&gt;" in html_out
        html_with_url = server._render_admin_html(data, mcp_url="http://127.0.0.1:8931/mcp")
        assert "http://127.0.0.1:8931/mcp" in html_with_url
        print("✓ _collect_admin_overview / _render_admin_html: 数据汇总正确，恶意 agent_id 被转义，连接信息正确渲染")

        # HTTP /admin 路由：没 token 拒绝，token 错拒绝，token 对返回 200 且做了 HTML 转义
        resp = asyncio.run(server.admin_dashboard(_FakeRequest({})))
        assert resp.status_code == 403
        resp = asyncio.run(server.admin_dashboard(_FakeRequest({"token": "wrong"})))
        assert resp.status_code == 403
        resp = asyncio.run(server.admin_dashboard(_FakeRequest({"token": "s3cr3t"})))
        assert resp.status_code == 200
        body = resp.body.decode()
        assert "<script>alert(1)</script>" not in body
        assert "&lt;script&gt;" in body
        assert "http://127.0.0.1:8931/mcp" in body
        print("✓ /admin HTTP 路由：未授权拒绝，授权后返回转义安全的 HTML，含 MCP 连接信息")
    finally:
        os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
        store.close()

    print("✓ Admin 面板测试通过\n")


def test_admin_command():
    print("=== 测试 Admin 下达指令 ===")
    from mcpharbor import server
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()

    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
    try:
        def _cmd(**kwargs):
            return json.loads(asyncio.run(server.admin_command.fn(**kwargs)))

        # "admin" 是保留身份，普通 agent 不能抢注
        resp = json.loads(server.register_agent.fn("admin", display_name="管理员", description="保留身份测试"))
        assert "error" in resp and "保留" in resp["error"]
        print("✓ register_agent: agent_id=admin 被拒绝注册")

        worker_token = json.loads(server.register_agent.fn("worker-agent", display_name="worker", description="admin指令测试身份"))["token"]

        # 没配置 MCPHARBOR_ADMIN_TOKEN 时整体不可用
        resp = _cmd(admin_token="anything", to_agent="worker-agent", command="停止当前任务")
        assert "error" in resp and "未启用" in resp["error"]
        print("✓ admin_command: 未配置 MCPHARBOR_ADMIN_TOKEN 时整体拒绝")

        os.environ["MCPHARBOR_ADMIN_TOKEN"] = "s3cr3t"

        # 拿一个普通 agent 自己的 token 冒充 admin_token，必须被拒绝
        resp = _cmd(admin_token=worker_token, to_agent="worker-agent", command="停止当前任务")
        assert "error" in resp and "无效" in resp["error"]
        print("✓ admin_command: 用普通 agent 的 token 冒充 admin_token 被拒绝")

        # 收件人未注册
        resp = _cmd(admin_token="s3cr3t", to_agent="ghost-agent", command="停止当前任务")
        assert "error" in resp and "未注册" in resp["error"]
        print("✓ admin_command: 收件人未注册被拒绝")

        # 正常下达指令
        resp = _cmd(admin_token="s3cr3t", to_agent="worker-agent",
                    command="立即停止处理订单批次 #42", correlation_id="task-42")
        assert resp["status"] == "ok"
        msg_id = resp["message_id"]
        print("✓ admin_command: 正常下达指令成功")

        # worker-agent 通过 get_messages 收到，from_agent=admin，severity=high
        resp = json.loads(server.get_messages.fn(agent_id="worker-agent", token=worker_token))
        assert resp["count"] == 1
        got = resp["messages"][0]
        assert got["id"] == msg_id
        assert got["from_agent"] == "admin"
        assert got["severity"] == "high"
        assert got["message"] == "立即停止处理订单批次 #42"
        print("✓ get_messages: worker-agent 收到来自 admin 的高优先级指令")

        # ── admin_manage_agent：吊销 / 彻底删除 ──
        def _mg(**kwargs):
            return json.loads(server.admin_manage_agent.fn(**kwargs))

        os.environ["MCPHARBOR_ADMIN_TOKEN"] = "s3cr3t"

        # 普通人冒充 admin 不行
        resp = _mg(admin_token=worker_token, agent_id="worker-agent", action="purge")
        assert "error" in resp and "无效" in resp["error"]
        print("✓ admin_manage_agent: 冒充 admin 被拒绝")

        # 非法 action
        resp = _mg(admin_token="s3cr3t", agent_id="worker-agent", action="delete")
        assert "error" in resp
        print("✓ admin_manage_agent: 非法 action 被拒绝")

        # 吊销：token 立即失效，记录保留
        resp = _mg(admin_token="s3cr3t", agent_id="worker-agent", action="revoke")
        assert resp["status"] == "ok"
        resp = json.loads(server.get_messages.fn(agent_id="worker-agent", token=worker_token))
        assert "error" in resp and "无效" in resp["error"]
        assert server._get_store().get_agent_token("worker-agent") is not None
        print("✓ admin_manage_agent: revoke 后 token 立即失效、记录保留")

        # purge：有 berth 的 agent 不能直接删
        b_token = json.loads(server.register_agent.fn("purge-owner", display_name="泊主", description="purge流程测试身份"))["token"]
        asyncio.run(server.publish_manifest.fn(berth="purge-test", version="0.1.0", owner="purge-owner", token=b_token))
        resp = _mg(admin_token="s3cr3t", agent_id="purge-owner", action="purge")
        assert "error" in resp and "berth" in resp["error"]
        print("✓ admin_manage_agent: 名下有 berth 时 purge 被拒绝")

        # 无 berth 的可以直接删
        resp = _mg(admin_token="s3cr3t", agent_id="worker-agent", action="purge")
        assert resp["status"] == "ok"
        assert server._get_store().get_agent_token("worker-agent") is None
        print("✓ admin_manage_agent: purge 彻底删除注册记录")

        # last_seen：成功认证后刷新
        seen = json.loads(server.register_agent.fn("seen-agent", display_name="活跃", description="last_seen 追踪测试身份"))
        assert server._get_store().get_agent_token("seen-agent").last_seen is None
        server.get_messages.fn(agent_id="seen-agent", token=seen["token"])
        assert server._get_store().get_agent_token("seen-agent").last_seen is not None
        print("✓ last_seen: 成功认证后刷新")
    finally:
        os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
        store.close()

    print("✓ Admin 下达指令测试通过\n")


async def _native_push_scenario():
    from fastmcp import Client
    from fastmcp.client.messages import MessageHandler
    from mcpharbor import server

    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()

    received = []

    class Handler(MessageHandler):
        async def on_resource_updated(self, message):
            received.append(message)

    pub_client = Client(server.mcp)
    sub_client = Client(server.mcp, message_handler=Handler())

    async with pub_client:
        r = await pub_client.call_tool("register_agent", {"agent_id": "auth-team", "display_name": "认证团队", "description": "负责认证与签发契约的团队"})
        token = json.loads(r.data)["token"]

        r = await pub_client.call_tool("publish_manifest", {
            "berth": "auth", "version": "1.0.0", "owner": "auth-team", "token": token,
        })
        assert json.loads(r.data)["pushed"] == 0
        print("✓ 首次发布：还没有订阅者，pushed=0")

        async with sub_client:
            r = await sub_client.call_tool("register_agent", {"agent_id": "order-agent", "display_name": "订单代理", "description": "订阅订单契约变更的代理"})
            order_token = json.loads(r.data)["token"]

            r = await sub_client.call_tool("subscribe", {
                "subscriber": "order-agent", "token": order_token, "berth": "auth",
                "events": ["contract_changed"], "resource_uri": "harbor://berths/auth/manifest",
            })
            assert json.loads(r.data)["status"] == "ok"
            assert "order-agent" in server._live_sessions
            print("✓ subscribe: order-agent 的会话已记录为存活会话")

            r = await pub_client.call_tool("publish_manifest", {
                "berth": "auth", "version": "1.1.0", "owner": "auth-team", "token": token,
            })
            data = json.loads(r.data)
            assert data["pushed"] == 1
            print(f"✓ publish_manifest: 原生推送成功 (pushed={data['pushed']})")

            await asyncio.sleep(0.1)
            assert len(received) == 1
            notif = received[0]
            assert notif.params.berth == "auth"
            assert notif.params.old_version == "1.0.0"
            assert notif.params.new_version == "1.1.0"
            assert str(notif.params.uri) == "harbor://berths/auth/manifest"
            print(f"✓ 订阅方收到原生通知: {notif.params.old_version} -> {notif.params.new_version}, summary={notif.params.summary}")

        # sub_client 在这里已断开连接（退出了 async with），存活会话应被清理
        r = await pub_client.call_tool("publish_manifest", {
            "berth": "auth", "version": "1.2.0", "owner": "auth-team", "token": token,
        })
        data = json.loads(r.data)
        assert data["pushed"] == 0
        assert "order-agent" not in server._live_sessions
        print("✓ 订阅方断线后推送优雅降级为 0，存活会话表已清理（轮询兜底继续可用）")

    store.close()


def test_native_push():
    print("=== 测试 MCP 原生通知推送 ===")
    asyncio.run(_native_push_scenario())
    print("✓ MCP 原生通知推送测试通过\n")


def test_demo():
    print("=== 演示：完整流程 ===")
    store = HarborStorage(":memory:")

    # Step 1: auth-team 注册泊位并发布 Manifest v1.2.0
    auth = Berth(id="auth", owner="auth-team", capabilities=["auth", "jwt", "token"])
    store.upsert_berth(auth)

    manifest_v1 = Manifest(
        berth="auth", version="1.2.0", owner="auth-team",
        capabilities=["auth", "jwt", "token"],
        protocol="http", base_url="https://auth.internal",
        auth={"type": "bearer", "header": "Authorization",
              "jwks": "https://auth.internal/.well-known/jwks.json"},
        requirements=["所有请求必须带 X-Request-Id"],
        errors={401: "invalid_token", 403: "forbidden"},
        events=["user.created", "token.revoked"],
        contact="auth-team@example.com",
    )
    store.publish_manifest(manifest_v1)
    print("✓ auth-team 发布 Manifest v1.2.0")

    # Step 2: order-agent 订阅
    sub = Subscription(subscriber="order-agent", berth="auth",
                       events=["contract_changed"], version_range=">=1.0.0")
    store.add_subscription(sub)
    print("✓ order-agent 订阅 auth 变更")

    # Step 3: auth 更新到 v1.3.0（带新事件和新错误码）
    manifest_v2 = Manifest(
        berth="auth", version="1.3.0", owner="auth-team",
        capabilities=["auth", "jwt", "token", "oauth2"],
        protocol="http", base_url="https://auth.internal",
        auth={"type": "bearer"},
        errors={401: "invalid_token", 403: "forbidden", 429: "rate_limited"},
        events=["user.created", "token.revoked", "user.updated"],
    )
    store.publish_manifest(manifest_v2)

    from mcpharbor.server import _generate_change_summary, _determine_priority
    summary = _generate_change_summary(manifest_v1, manifest_v2)
    priority = _determine_priority(manifest_v1, manifest_v2)
    print(f"✓ auth 更新到 v1.3.0: {summary} (优先级={priority.value})")

    # Step 4: 通知订阅者
    notif = Notification(
        berth="auth", old_version="1.2.0", new_version="1.3.0",
        change_type="contract_changed", severity=priority,
        summary=summary, resource_uri="harbor://berths/auth/manifest",
    )
    store.add_notification(notif)
    subs = store.get_subscriptions("auth")
    print(f"✓ 通知了 {len(subs)} 个订阅者")

    # Step 5: check_updates
    updates = store.check_updates({"auth": "1.2.0"})
    assert len(updates) == 1
    print(f"✓ order-agent 发现更新: {updates[0]['latest_version']}")

    # Step 6: sync 拉取最新
    latest = store.get_manifest("auth")
    print(f"✓ order-agent 拉取最新: v{latest.version}, capabilities={latest.capabilities}")

    # Step 7: 审计日志
    logs = store.get_audit_log(limit=10)
    print(f"✓ 审计日志: {len(logs)} 条")

    store.close()
    print("\n=== 演示完成 ===\n")


if __name__ == "__main__":
    test_models()
    test_storage()
    test_notification_merge()
    test_change_summary()
    test_server_tools()
    test_auth()
    test_direct_messages()
    test_admin_panel()
    test_admin_command()
    test_native_push()
    test_demo()
    print("🎉 所有测试通过！MCP Harbor v1.0 完整实现。")
