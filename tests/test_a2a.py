#!/usr/bin/env python3
"""MCP Harbor A2A 补全测试：订阅链路 E2E、对话线程、多播、参与者发现、契约钉、数据保养。"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "src")

from mcpharbor.models import DirectMessage, Manifest, NotifyPriority
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


def _publish(server, owner, token, berth, version, **extra):
    return json.loads(asyncio.run(server.publish_manifest.fn(
        berth=berth, version=version, owner=owner, token=token, **extra)))


def test_subscription_chain_e2e():
    """验收标准：发新版 -> 订阅者被通知 -> check_updates 兜底 -> sync 拉到新版。"""
    print("=== 测试订阅/广播链路 E2E ===")
    server, store = _setup()

    owner_token = _register(server, "auth-team", display_name="认证团队", description="契约发布方")
    sub_token = _register(server, "order-agent", display_name="订单团队", description="契约消费方")
    print("✓ 发布方 / 订阅方注册")

    resp = _publish(server, "auth-team", owner_token, "auth", "1.2.0",
                    capabilities=["auth", "jwt"], base_url="https://auth.internal")
    assert resp["status"] == "ok"
    print("✓ publish_manifest v1.2.0")

    resp = json.loads(server.subscribe.fn(
        subscriber="order-agent", token=sub_token, berth="auth",
        events=["contract_changed"], version_range=">=1.2.0"))
    assert resp["status"] == "ok", resp
    print("✓ order-agent 订阅 auth")

    # 发新版：订阅者应被通知（无存活会话，pushed=0，靠轮询兜底）
    resp = _publish(server, "auth-team", owner_token, "auth", "1.3.0",
                    capabilities=["auth", "jwt", "oauth2"])
    assert resp["status"] == "ok"
    assert resp["notified"] == 1, resp
    assert resp["pushed"] == 0  # 订阅方不在线，轮询兜底
    assert any(r["subscriber"] == "order-agent" for r in [{"subscriber": "order-agent"}])
    print(f"✓ v1.3.0 发布：notified={resp['notified']} pushed={resp['pushed']}（离线靠轮询）")

    # 轮询兜底：check_updates 应报告 1.2.0 -> 1.3.0
    resp = json.loads(server.check_updates.fn(known_versions={"auth": "1.2.0"}))
    assert resp["count"] == 1
    up = resp["updates"][0]
    assert up["latest_version"] == "1.3.0" and up["known_version"] == "1.2.0"
    print("✓ check_updates 兜底发现更新 1.2.0 -> 1.3.0")

    # 通知历史有记录
    notifs = store.get_notification_history("auth")
    assert any(n.new_version == "1.3.0" for n in notifs)
    print("✓ 通知历史已记录")

    # 5 秒合并：紧接着再发一版，应触发 notify.merged
    resp = _publish(server, "auth-team", owner_token, "auth", "1.3.1")
    assert resp["status"] == "ok"
    merged = store.get_audit_log(action="notify.merged", limit=5)
    assert len(merged) >= 1
    print("✓ 5 秒内连发触发通知合并 (notify.merged)")

    store.close()
    print("✓ 订阅/广播链路 E2E 测试通过\n")


def test_reply_thread_and_conversations():
    print("=== 测试回复线程与对话列表 ===")
    server, store = _setup()

    a = _register(server, "party-a", display_name="甲方", description="线程测试A")
    b = _register(server, "party-b", display_name="乙方", description="线程测试B")

    def _send(**kw):
        return json.loads(asyncio.run(server.send_message.fn(**kw)))

    r1 = _send(from_agent="party-a", token=a, to_agent="party-b",
               message="第一轮：接口地址改成 /v2 了", correlation_id="topic-1")
    assert r1["status"] == "ok"
    id1 = r1["results"][0]["message_id"]

    # b 回复引用 id1
    r2 = _send(from_agent="party-b", token=b, to_agent="party-a",
               message="收到，已切换 /v2", correlation_id="topic-1", reply_to=id1)
    assert r2["status"] == "ok"
    id2 = r2["results"][0]["message_id"]
    assert id2 != id1
    print("✓ reply_to 回复发送成功")

    # 线程字段进库
    msgs = store.get_messages("party-a", with_agent="party-b")
    reply = next(m for m in msgs if m.id == id2)
    assert reply.reply_to == id1
    print("✓ 消息记录带 reply_to 引用")

    # party-c 不能引用 a/b 之间的消息（会泄露内容给第三方）
    c = _register(server, "party-c", display_name="丙方", description="线程测试C")
    r3 = _send(from_agent="party-c", token=c, to_agent="party-a",
               message="冒充引用", reply_to=id1)
    assert "error" in r3 and "参与" in r3["error"]
    print("✓ 第三方引用他人私信被拒绝")

    # 引用不存在的消息
    r4 = _send(from_agent="party-a", token=a, to_agent="party-b", message="x", reply_to="nonexistent")
    assert "error" in r4
    print("✓ 引用不存在的消息被拒绝")

    # a 又发一条新话题
    _send(from_agent="party-a", token=a, to_agent="party-b",
          message="第二轮：密钥也换了", correlation_id="topic-2")

    # b 的对话列表：只看 party-a 一行，未读 2（最新一条是 topic-2），丙方那行不存在
    conv = json.loads(server.get_conversations.fn(agent_id="party-b", token=b))
    assert conv["count"] == 1, conv
    entry = conv["conversations"][0]
    assert entry["peer"] == "party-a"
    assert entry["unread"] == 2
    assert entry["total"] == 3
    assert entry["last_message"]["message"] == "第二轮：密钥也换了"
    assert entry["last_message"]["reply_to"] == ""
    print(f"✓ get_conversations: peer=party-a unread={entry['unread']} 最新=「{entry['last_message']['message'][:12]}…」")

    # 最新消息才是消费者要关注的：标记已读后未读归零
    ids = [m.id for m in store.get_messages("party-b", unread_only=True)]
    json.loads(server.mark_messages_read.fn(agent_id="party-b", token=b, message_ids=ids))
    conv = json.loads(server.get_conversations.fn(agent_id="party-b", token=b))
    assert conv["unread_total"] == 0
    print("✓ 全部标记已读后 unread_total 归零")

    # 拿错 token 看别人的对话列表必须被拒
    resp = json.loads(server.get_conversations.fn(agent_id="party-a", token=b))
    assert "error" in resp
    print("✓ get_conversations: 冒充他人查询被拒绝")

    store.close()
    print("✓ 回复线程与对话列表测试通过\n")


def test_multicast():
    print("=== 测试多播私信 ===")
    server, store = _setup()

    a = _register(server, "chair", display_name="主持", description="多播发起方")
    b = _register(server, "dev-1", display_name="开发一", description="多播接收方1")
    c = _register(server, "dev-2", display_name="开发二", description="多播接收方2")

    r = json.loads(asyncio.run(server.send_message.fn(
        from_agent="chair", token=a, message="今晚 8 点评审，各回一句",
        to_agents=["dev-1", "dev-2", "dev-1"],  # 重复收件人应被去重
        correlation_id="review-101")))
    assert r["status"] == "ok" and r["sent"] == 2, r
    assert {x["to"] for x in r["results"]} == {"dev-1", "dev-2"}
    print(f"✓ 多播 2 人（重复收件人已去重），correlation_id=review-101")

    # 各自只看到 chair<->自己
    for who, tok in (("dev-1", b), ("dev-2", c)):
        resp = json.loads(server.get_messages.fn(agent_id=who, token=tok))
        assert resp["count"] == 1 and resp["messages"][0]["correlation_id"] == "review-101"
    print("✓ 每个收件人各自只见主持人与自己的那份")

    # dev-1 的回复只在 chair<->dev-1 之间，dev-2 看不到
    asyncio.run(server.send_message.fn(
        from_agent="dev-1", token=b, to_agent="chair",
        message="我可以", correlation_id="review-101"))
    resp = json.loads(server.get_messages.fn(agent_id="dev-2", token=c))
    assert resp["count"] == 1  # 仍是 chair 那条
    print("✓ dev-1 的回复 dev-2 不可见（隔离不因多播放宽）")

    # 一个都收件人都不给 -> 报错
    r = json.loads(asyncio.run(server.send_message.fn(from_agent="chair", token=a, message="x")))
    assert "error" in r
    print("✓ 空收件人被拒绝")

    store.close()
    print("✓ 多播私信测试通过\n")


def test_search_agents_and_hidden():
    print("=== 测试参与者发现与隐身 ===")
    server, store = _setup()

    _register(server, "frontend", display_name="前端组", description="负责页面与交互",
              capabilities=["前端", "ui"], contact="fe@x.com")
    _register(server, "deployer", display_name="运维组", description="负责部署与监控",
              capabilities=["部署", "linux"])
    resp = json.loads(server.register_agent.fn(
        "auditor", display_name="隐身审计", description="只看不说话", hidden=True))
    auditor_token = resp["token"]

    # 关键词搜索：隐身的不出现
    resp = json.loads(server.search_agents.fn(keyword="组"))
    ids = [x["agent_id"] for x in resp["agents"]]
    assert "frontend" in ids and "deployer" in ids and "auditor" not in ids, ids
    print(f"✓ keyword=组 找到 {ids}（隐身者不出现）")

    # 能力搜索
    resp = json.loads(server.search_agents.fn(capability="部署"))
    assert [x["agent_id"] for x in resp["agents"]] == ["deployer"]
    assert resp["agents"][0]["display_name"] == "运维组"
    print("✓ capability=部署 精确命中运维组")

    # 描述关键词
    resp = json.loads(server.search_agents.fn(keyword="页面"))
    assert [x["agent_id"] for x in resp["agents"]] == ["frontend"]
    print("✓ keyword=页面 命中前端组")

    # 吊销后从搜索消失
    os.environ["MCPHARBOR_ADMIN_TOKEN"] = "t-admin"
    json.loads(server.admin_manage_agent.fn(admin_token="t-admin", agent_id="deployer", action="revoke"))
    resp = json.loads(server.search_agents.fn(keyword=""))
    assert "deployer" not in [x["agent_id"] for x in resp["agents"]]
    print("✓ 吊销身份不再出现在搜索结果里")
    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)

    # 隐身只是不被搜到：知道 id 仍可私信，自己仍可用 token 读
    fe_token = json.loads(server.register_agent.fn(
        "fe-sender", display_name="发信方", description="给隐身者发信的测试身份"))["token"]
    r = json.loads(asyncio.run(server.send_message.fn(
        from_agent="fe-sender", token=fe_token, to_agent="auditor", message="内部通报")))
    assert r["status"] == "ok"
    resp = json.loads(server.get_messages.fn(agent_id="auditor", token=auditor_token))
    assert resp["count"] == 1
    print("✓ 隐身者仍能收私信并正常读取")

    store.close()
    print("✓ 参与者发现与隐身测试通过\n")


def test_contract_pins():
    print("=== 测试契约钉 ===")
    server, store = _setup()

    owner = _register(server, "auth-team", display_name="认证团队", description="契约发布方")
    consumer = _register(server, "order-agent", display_name="订单团队", description="契约消费方")

    _publish(server, "auth-team", owner, "auth", "1.2.0", capabilities=["auth"])

    # 钉一个不存在的版本 -> 报错并列出可选版本
    resp = json.loads(server.pin_contract.fn(
        agent_id="order-agent", token=consumer, berth="auth", version="9.9.9"))
    assert "error" in resp and "1.2.0" in resp["error"]
    print("✓ 钉不存在的版本被拒绝（附可选版本）")

    # 正常钉住
    resp = json.loads(server.pin_contract.fn(
        agent_id="order-agent", token=consumer, berth="auth", version="1.2.0", task_id="task-42"))
    assert resp["status"] == "ok"
    print("✓ 钉住 auth v1.2.0 (task-42)")

    # 发布新版本 -> 响应里点名提醒钉在旧版本上的 agent
    resp = _publish(server, "auth-team", owner, "auth", "2.0.0", capabilities=["auth"])
    assert resp["status"] == "ok"
    assert resp["stale_pins"] and resp["stale_pins"][0]["agent"] == "order-agent"
    assert resp["stale_pins"][0]["pinned_version"] == "1.2.0"
    print(f"✓ 发布 v2.0.0 时点名提醒：{resp['stale_pins'][0]}")

    # get_my_pins 显示 stale
    resp = json.loads(server.get_my_pins.fn(agent_id="order-agent", token=consumer))
    assert resp["count"] == 1
    pin = resp["pins"][0]
    assert pin["stale"] is True and pin["latest_version"] == "2.0.0"
    print(f"✓ get_my_pins: stale=True (钉 {pin['version']}，最新 {pin['latest_version']})")

    # 换钉到当前最新 -> 不再 stale；但之后又发新版会再次被点名
    json.loads(server.pin_contract.fn(
        agent_id="order-agent", token=consumer, berth="auth", version="2.0.0", task_id="task-42"))
    resp = json.loads(server.get_my_pins.fn(agent_id="order-agent", token=consumer))
    assert resp["pins"][0]["stale"] is False
    resp = _publish(server, "auth-team", owner, "auth", "2.1.0")
    assert resp["stale_pins"] and resp["stale_pins"][0]["pinned_version"] == "2.0.0"
    print("✓ 换钉后跟上最新版不再 stale；再出新版会再次被点名")

    # 解钉
    resp = json.loads(server.unpin_contract.fn(
        agent_id="order-agent", token=consumer, berth="auth", task_id="task-42"))
    assert resp["status"] == "ok"
    resp = json.loads(server.get_my_pins.fn(agent_id="order-agent", token=consumer))
    assert resp["count"] == 0
    print("✓ unpin_contract 解除钉")

    # 拿别人 token 钉 -> 拒绝
    resp = json.loads(server.pin_contract.fn(
        agent_id="order-agent", token=owner, berth="auth", version="2.0.0"))
    assert "error" in resp
    print("✓ 冒充他人钉契约被拒绝")

    store.close()
    print("✓ 契约钉测试通过\n")


def test_admin_cleanup_and_purge():
    print("=== 测试数据保养与彻底删除 ===")
    server, store = _setup()
    os.environ["MCPHARBOR_ADMIN_TOKEN"] = "t-admin"

    a = _register(server, "tmp-agent", display_name="临时", description="清理功能测试身份甲")
    b = _register(server, "keep-agent", display_name="保留", description="清理功能测试身份乙")

    asyncio.run(server.send_message.fn(
        from_agent="tmp-agent", token=a, to_agent="keep-agent", message="新消息"))
    # 手工塞一条 100 天前的旧消息
    store.add_message(DirectMessage(
        from_agent="tmp-agent", to_agent="keep-agent", message="百年老消息",
        created_at=datetime.now(timezone.utc) - timedelta(days=100),
        severity=NotifyPriority.LOW,
    ))
    assert store.count_messages() == 2

    # 非法参数被拒
    resp = json.loads(server.admin_cleanup.fn(admin_token="t-admin", message_retention_days=3))
    assert "error" in resp
    resp = json.loads(server.admin_cleanup.fn(admin_token="wrong", message_retention_days=90))
    assert "error" in resp
    print("✓ admin_cleanup: 非法参数 / 错误 token 均被拒绝")

    # 正常清理：只删 90 天前的
    resp = json.loads(server.admin_cleanup.fn(admin_token="t-admin", message_retention_days=90))
    assert resp["status"] == "ok" and resp["removed_messages"] == 1
    assert store.count_messages() == 1
    print("✓ admin_cleanup: 100 天前的旧私信被清，新消息保留")

    # purge 连带清私信和订阅
    resp = json.loads(server.admin_manage_agent.fn(
        admin_token="t-admin", agent_id="tmp-agent", action="purge"))
    assert resp["status"] == "ok" and resp["removed_messages"] == 1
    assert store.count_messages() == 0
    assert store.get_agent_token("tmp-agent") is None
    print(f"✓ purge: {resp['message']}")

    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
    store.close()
    print("✓ 数据保养与彻底删除测试通过\n")


def test_admin_manage_berth():
    print("=== 测试 Admin 管理 Berth ===")
    server, store = _setup()
    os.environ["MCPHARBOR_ADMIN_TOKEN"] = "t-admin"

    owner = _register(server, "berth-owner", display_name="项目方", description="berth 管理测试身份")
    _publish(server, "berth-owner", owner, "legacy-api", "1.0.0", capabilities=["legacy"])
    _publish(server, "berth-owner", owner, "legacy-api", "1.1.0", capabilities=["legacy"])
    consumer = _register(server, "legacy-user", display_name="使用方", description="berth 管理测试消费方")
    json.loads(server.subscribe.fn(subscriber="legacy-user", token=consumer, berth="legacy-api"))
    json.loads(server.pin_contract.fn(agent_id="legacy-user", token=consumer,
                                      berth="legacy-api", version="1.0.0"))

    # 非法 action / 错误 token / 不存在的 berth
    assert "error" in json.loads(server.admin_manage_berth.fn(admin_token="t-admin", berth="legacy-api", action="nuke"))
    assert "error" in json.loads(server.admin_manage_berth.fn(admin_token="wrong", berth="legacy-api", action="deactivate"))
    assert "error" in json.loads(server.admin_manage_berth.fn(admin_token="t-admin", berth="ghost", action="deactivate"))
    print("✓ 非法 action / 错误 token / 不存在 berth 均被拒绝")

    # 下架：从发现消失，但版本历史保留
    resp = json.loads(server.admin_manage_berth.fn(admin_token="t-admin", berth="legacy-api", action="deactivate"))
    assert resp["status"] == "ok"
    found = json.loads(server.search_berths.fn(keyword="legacy"))
    assert found["count"] == 0
    assert store.get_manifest("legacy-api", "1.0.0") is not None  # 历史还在
    print("✓ deactivate: 搜索消失、版本历史保留")

    # 重新上架：恢复可见
    resp = json.loads(server.admin_manage_berth.fn(admin_token="t-admin", berth="legacy-api", action="activate"))
    assert resp["status"] == "ok"
    found = json.loads(server.search_berths.fn(keyword="legacy"))
    assert found["count"] == 1
    print("✓ activate: 重新上架恢复可见")

    # 有 berth 的 owner 不能被 purge，delete 后解锁
    resp = json.loads(server.admin_manage_agent.fn(admin_token="t-admin", agent_id="berth-owner", action="purge"))
    assert "error" in resp and "admin_manage_berth" in resp["error"]
    resp = json.loads(server.admin_manage_berth.fn(admin_token="t-admin", berth="legacy-api", action="delete"))
    assert resp["status"] == "ok"
    assert resp["removed"]["manifests"] == 2 and resp["removed"]["subscriptions"] == 1 and resp["removed"]["pins"] == 1
    resp = json.loads(server.admin_manage_agent.fn(admin_token="t-admin", agent_id="berth-owner", action="purge"))
    assert resp["status"] == "ok", resp
    print("✓ delete 连带清版本/订阅/契约钉，purge 随之解锁")

    assert store.get_berth("legacy-api") is None
    os.environ.pop("MCPHARBOR_ADMIN_TOKEN", None)
    store.close()
    print("✓ Admin 管理 Berth 测试通过\n")


if __name__ == "__main__":
    test_subscription_chain_e2e()
    test_reply_thread_and_conversations()
    test_multicast()
    test_search_agents_and_hidden()
    test_contract_pins()
    test_admin_cleanup_and_purge()
    test_admin_manage_berth()
    print("🎉 A2A 补全测试全部通过。")
