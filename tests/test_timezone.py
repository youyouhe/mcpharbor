#!/usr/bin/env python3
"""时区声明（register_agent 必填 / open_session 更正）回归测试：
① 注册必填 IANA 时区名：缺省/空串被拒；CST、GMT+8、+08:00 这类歧义写法被拒
② 合法声明（Asia/Shanghai / UTC / America/New_York）注册成功，落库、响应带 timezone
③ open_session 带 timezone 更正老注册的声明（老 agent 不必重新注册）；非法值不改库
④ get_conversations 响应带 server_time（UTC 锚点，可解析、与当前时刻接近）
⑤ 旧库迁移：agent_tokens 无 timezone 列的库自动补列，旧行读出为 ""
⑥ search_agents 名片带 timezone；admin 总览带 timezone（"未声明"兜底）
"""

import json
import sqlite3
import sys
import tempfile
import os
from datetime import datetime, timezone as dt_timezone

sys.path.insert(0, "src")

os.environ["MCPHARBOR_ADMIN_TOKEN"] = "tz-test-token"

from mcpharbor import server
from mcpharbor.storage import HarborStorage


def _setup():
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()
    return server, store


def _register(srv, agent_id, **kwargs):
    kwargs.setdefault("timezone", "Asia/Shanghai")
    kwargs.setdefault("display_name", agent_id)
    kwargs.setdefault("description", f"{agent_id} 的测试身份")
    resp = json.loads(srv.register_agent.fn(agent_id, **kwargs))
    assert resp.get("status") == "ok", resp
    return resp["token"]


def test_timezone_required():
    print("=== ① 注册必须声明 IANA 时区 ===")
    srv, store = _setup()
    base = dict(display_name="测试", description="时区校验测试身份")
    # 缺省（空串）被拒
    resp = json.loads(srv.register_agent.fn("no-tz", timezone="", **base))
    assert "error" in resp and "timezone" in resp["error"] and "Asia/Shanghai" in resp["error"], resp
    resp2 = json.loads(srv.register_agent.fn("no-tz2", **base))  # 参数没传同样落到空串
    assert "error" in resp2 and "IANA" in resp2["error"], resp2
    # 歧义写法被拒：缩写 / 偏移（尾随空格会被自动 strip，属宽容处理，不算错误）
    for bad in ("CST", "GMT+8", "+08:00", "utc/beijing"):
        r = json.loads(srv.register_agent.fn("bad-tz", timezone=bad, **base))
        assert "error" in r and "IANA" in r["error"], (bad, r)
    # 被拒后库中没有残留
    assert store.get_agent_token("no-tz") is None
    print("✓ 必填校验 OK")


def test_timezone_valid():
    print("=== ② 合法声明落库 ===")
    srv, store = _setup()
    for tz in ("Asia/Shanghai", "UTC", "America/New_York"):
        aid = f"tz-{tz.replace('/', '-').replace('_', '-').lower()}"
        r = json.loads(srv.register_agent.fn(aid, display_name="测试", description="合法时区测试身份",
                                             timezone=tz))
        assert r["status"] == "ok" and r["timezone"] == tz, r
        assert store.get_agent_token(aid).timezone == tz
    print("✓ 合法声明 OK")


def test_open_session_update():
    print("=== ③ open_session 更正声明 ===")
    srv, store = _setup()
    # 模拟老注册：库里没有声明（直接落库一条空 timezone）
    tok = _register(srv, "old-agent")
    conn = store._get_conn()
    conn.execute("UPDATE agent_tokens SET timezone='' WHERE agent_id='old-agent'")
    conn.commit()
    # open_session 带合法时区 → 更新
    r = json.loads(srv.open_session.fn(agent_id="old-agent", token=tok, timezone="Asia/Shanghai"))
    assert r["status"] == "ok" and "server_time" in r and "Asia/Shanghai" in r["message"], r
    assert store.get_agent_token("old-agent").timezone == "Asia/Shanghai"
    # 非法时区 → 报错且不覆盖已有声明
    r2 = json.loads(srv.open_session.fn(agent_id="old-agent", token=tok, timezone="Mars/Olympus"))
    assert "error" in r2 and "IANA" in r2["error"], r2
    assert store.get_agent_token("old-agent").timezone == "Asia/Shanghai"
    # 不带 timezone 的 open_session 照常，不动声明
    r3 = json.loads(srv.open_session.fn(agent_id="old-agent", token=tok))
    assert r3["status"] == "ok" and store.get_agent_token("old-agent").timezone == "Asia/Shanghai"
    # server_time 可解析且接近当前 UTC
    st = datetime.fromisoformat(r3["server_time"])
    assert abs((datetime.now(dt_timezone.utc) - st).total_seconds()) < 10
    print("✓ open_session 更正 OK")


def test_get_conversations_server_time():
    print("=== ④ get_conversations 带 UTC 锚点 ===")
    srv, store = _setup()
    a = json.loads(srv.register_agent.fn("alice", display_name="A", description="锚点测试A", timezone="UTC"))["token"]
    b = json.loads(srv.register_agent.fn("bob", display_name="B", description="锚点测试B", timezone="UTC"))["token"]
    import asyncio
    asyncio.run(srv.send_message.fn(from_agent="alice", token=a, message="hi", to_agent="bob"))
    conv = json.loads(srv.get_conversations.fn(agent_id="bob", token=b))
    st = datetime.fromisoformat(conv["server_time"])
    assert abs((datetime.now(dt_timezone.utc) - st).total_seconds()) < 10, conv["server_time"]
    assert conv["unread_total"] == 1
    print("✓ server_time OK")


def test_migration_old_db():
    print("=== ⑤ 旧库自动补列 ===")
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "old.db")
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE agent_tokens (agent_id TEXT PRIMARY KEY, token_hash TEXT NOT NULL,"
                 " created_at TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0,"
                 " display_name TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '',"
                 " contact TEXT NOT NULL DEFAULT '', capabilities TEXT NOT NULL DEFAULT '[]',"
                 " hidden INTEGER NOT NULL DEFAULT 0, last_seen TEXT)")
    conn.execute("INSERT INTO agent_tokens (agent_id, token_hash, created_at) VALUES"
                 " ('veteran', 'x', '2026-09-01T00:00:00+00:00')")
    conn.commit()
    conn.close()
    store = HarborStorage(db)
    cols = {r["name"] for r in store._get_conn().execute("PRAGMA table_info(agent_tokens)").fetchall()}
    assert "timezone" in cols, cols
    vet = store.get_agent_token("veteran")
    assert vet is not None and vet.timezone == ""
    # 补声明
    store.update_agent_timezone("veteran", "Asia/Shanghai")
    assert store.get_agent_token("veteran").timezone == "Asia/Shanghai"
    print("✓ 旧库迁移 OK")


def test_search_and_admin():
    print("=== ⑥ 名片/总览带时区 ===")
    srv, store = _setup()
    _register(srv, "card-agent", timezone="Asia/Shanghai")
    r = json.loads(srv.search_agents.fn(keyword="card-agent"))
    assert r["count"] == 1 and r["agents"][0]["timezone"] == "Asia/Shanghai", r
    # 未声明 → 空串（名片）/ "未声明"（admin 总览）
    conn = store._get_conn()
    conn.execute("INSERT INTO agent_tokens (agent_id, token_hash, display_name, description, created_at, revoked)"
                 " VALUES ('ghost', 'h', '老注册', '没有时区列时代的老身份', '2026-09-01T00:00:00+00:00', 0)")
    conn.commit()
    r2 = json.loads(srv.search_agents.fn(keyword="ghost"))
    assert r2["agents"][0]["timezone"] == "", r2
    overview = srv._collect_admin_overview()
    tz_map = {a["agent_id"]: a["timezone"] for a in overview["agents"]}
    assert tz_map["card-agent"] == "Asia/Shanghai" and tz_map["ghost"] == "未声明", tz_map
    print("✓ 名片/总览 OK")


if __name__ == "__main__":
    test_timezone_required()
    test_timezone_valid()
    test_open_session_update()
    test_get_conversations_server_time()
    test_migration_old_db()
    test_search_and_admin()
    print("\n全部通过 ✅")
