#!/usr/bin/env python3
"""健康检查（GET /health）回归测试：
① 正常：200，status=ok，db=ok，tool_count=36，server_time 可解析，uptime 非负
② 无鉴权：不带任何 token 直接可访问（探活端点的行业惯例）
③ 隐私：响应文本不含任何 agent_id / 参与者数据
④ DB 故障：503，status=unhealthy，db 带错误原因
⑤ 心跳字段：interval 恒 30；last_tick 为空（刚启动）或 ISO 时间串
⑥ harbor.sh status 走 /health 探活（shell 逻辑，跑一遍确认输出语义）
"""

import asyncio
import json
import sys
from datetime import datetime

sys.path.insert(0, "src")

from mcpharbor import server
from mcpharbor.storage import HarborStorage


def _req():
    from starlette.requests import Request
    scope = {"type": "http", "method": "GET", "path": "/health",
             "query_string": b"", "headers": []}
    return Request(scope)


def _call_health():
    return asyncio.run(server.health_check(_req()))


def _body(resp):
    return json.loads(resp.body)


def test_ok():
    print("=== ① 正常返回 ===")
    server._store = HarborStorage(":memory:")
    resp = _call_health()
    assert resp.status_code == 200, resp.status_code
    body = _body(resp)
    assert body["status"] == "ok" and body["db"] == "ok", body
    assert body["service"] == "mcpharbor"
    assert body["tool_count"] == 36, body["tool_count"]
    assert body["uptime_seconds"] >= 0
    datetime.fromisoformat(body["server_time"])  # UTC 锚点，可解析
    print("✓ 200/ok OK")


def test_no_auth():
    print("=== ② 无鉴权可访问 ===")
    resp = _call_health()
    assert resp.status_code == 200
    print("✓ 无鉴权 OK")


def test_privacy():
    print("=== ③ 响应不含参与者数据 ===")
    tok = json.loads(server.register_agent.fn("alice", timezone="Asia/Shanghai",
                                              display_name="alice", description="健康检查测试身份"))["token"]
    asyncio.run(server.send_message.fn(from_agent="alice", token=tok, message="secret", to_agent="alice"))
    body = _body(_call_health())
    text = json.dumps(body, ensure_ascii=False)
    for leak in ("alice", "secret", "agent_id", "participants"):
        assert leak not in text, (leak, text)
    print("✓ 隐私 OK")


def test_db_failure():
    print("=== ④ DB 故障 → 503 ===")
    real_store = server._store

    class BrokenStore:
        def _get_conn(self):
            raise RuntimeError("disk I/O error (模拟)")

    server._store = BrokenStore()
    try:
        resp = _call_health()
        assert resp.status_code == 503, resp.status_code
        body = _body(resp)
        assert body["status"] == "unhealthy" and "disk I/O error" in body["db"], body
    finally:
        server._store = real_store
    print("✓ 503 OK")


def test_heartbeat_field():
    print("=== ⑤ 心跳字段 ===")
    body = _body(_call_health())
    hb = body["heartbeat"]
    assert hb["interval_seconds"] == 30
    assert hb["last_tick"] is None or isinstance(hb["last_tick"], str), hb
    # 手动推进一轮后 last_tick 变成 ISO 串
    server._last_heartbeat_tick = datetime.now().isoformat()
    hb2 = _body(_call_health())["heartbeat"]
    datetime.fromisoformat(hb2["last_tick"])
    server._last_heartbeat_tick = ""
    print("✓ 心跳字段 OK")


def test_harbor_sh_status():
    print("=== ⑥ harbor.sh status 探活 ===")
    import subprocess
    r = subprocess.run(["./harbor.sh", "status"], capture_output=True, text=True)
    out = r.stdout
    # 服务在跑时应报 /health OK（若 harbor 未启动则提示未运行，也算正确语义）
    assert ("/health OK" in out) or ("未运行" in out) or ("不通" in out), out
    print("  harbor.sh status 输出:", out.strip().splitlines()[0])
    print("✓ status 语义 OK")


if __name__ == "__main__":
    test_ok()
    test_no_auth()
    test_privacy()
    test_db_failure()
    test_heartbeat_field()
    test_harbor_sh_status()
    print("\n全部通过 ✅")
