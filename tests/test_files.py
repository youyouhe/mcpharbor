#!/usr/bin/env python3
"""寄存文件（send_file/get_file/list_files）回归测试：
① 多播只落盘一份，每个收件人一条短通知（含 file_id、不含 content）
② 权限矩阵：收件人/sender 可取、第三方被拒、无 token 被拒；"不存在/无权"同措辞防探测
③ fetched 记录与 fetch_count；list_files 的 direction 与过期不可见
④ 过期清理：行删 + 磁盘 unlink；磁盘文件已被手工删不报错
⑤ :memory: tempfile 兜底；真实/相对 db 路径的目录解析
⑥ send_message >48KB 带 hint 仍发送；<阈值无 hint；推送 summary 截断
⑦ 边界：content=""、filename 净化（../ 与超长）、file_id 不存在/已过期报错
"""

import asyncio
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "src")

os.environ["MCPHARBOR_ADMIN_TOKEN"] = "files-test-token"

from mcpharbor import server
from mcpharbor.storage import HarborStorage


def _setup():
    store = HarborStorage(":memory:")
    server._store = store
    server._live_sessions.clear()
    return server, store


def _register(srv, agent_id):
    resp = json.loads(srv.register_agent.fn(agent_id, timezone="Asia/Shanghai", display_name=agent_id,
                                            description=f"{agent_id} 的测试身份"))
    assert resp.get("status") == "ok", resp
    return resp["token"]


def _call(fn, **kwargs):
    return json.loads(asyncio.run(fn(**kwargs)) if asyncio.iscoroutinefunction(fn) else fn(**kwargs))


def test_send_and_fetch():
    print("=== ① 多播单份落盘 + 短通知 ===")
    srv, store = _setup()
    a = _register(srv, "alice")
    b = _register(srv, "bob")
    c = _register(srv, "charlie")
    content = "#!/bin/bash\n" + "echo hello\n" * 100

    resp = _call(srv.send_file.fn, from_agent="alice", token=a, filename="setup.sh",
                 content=content, to_agents=["bob", "charlie"], note="免密登录脚本")
    assert resp["status"] == "ok" and resp["sent"] == 2, resp
    fid = resp["file_id"]
    assert resp["sha256"] == hashlib.sha256(content.encode()).hexdigest()
    # 磁盘只一份
    assert (store.files_dir / fid).read_text(encoding="utf-8") == content
    assert len(list(store.files_dir.glob("*"))) == 1
    # 每个收件人一条短通知：含 file_id、不含 content
    for who, tok in (("bob", b), ("charlie", c)):
        msgs = json.loads(srv.get_messages.fn(agent_id=who, token=tok))["messages"]
        assert len(msgs) == 1, msgs
        assert fid in msgs[0]["message"] and "setup.sh" in msgs[0]["message"]
        assert "echo hello" not in msgs[0]["message"]  # 全文不进私信
    print("✓ 落盘/通知 OK")

    print("=== ② 权限矩阵 ===")
    got = _call(srv.get_file.fn, agent_id="bob", token=b, file_id=fid)
    assert got["status"] == "ok" and got["content"] == content and got["filename"] == "setup.sh"
    got_a = _call(srv.get_file.fn, agent_id="alice", token=a, file_id=fid)  # sender 也能取
    assert got_a["status"] == "ok"
    d = _register(srv, "dave")
    denied = _call(srv.get_file.fn, agent_id="dave", token=d, file_id=fid)
    assert "error" in denied and "不存在或你不是" in denied["error"]
    # 未带 token
    denied2 = _call(srv.get_file.fn, agent_id="bob", token="", file_id=fid)
    assert "error" in denied2
    # 防探测：不存在的 file_id 与无权的 file_id 拒绝理由一致（文案里的 file_id 是各自查询值）
    miss = _call(srv.get_file.fn, agent_id="dave", token=d, file_id="nonexistent0000")
    assert "不存在或你不是" in miss["error"] == denied["error"].replace(fid, "nonexistent0000")
    print("✓ 权限矩阵 OK")
    return srv, store, a, b, fid


def test_fetched_and_list():
    print("=== ③ fetched 记录 + list_files ===")
    srv, store, a, b, fid = test_send_and_fetch()
    _call(srv.get_file.fn, agent_id="bob", token=b, file_id=fid)  # bob 第二次取
    mine_b = _call(srv.list_files.fn, agent_id="bob", token=b)["files"]
    assert len(mine_b) == 1 and mine_b[0]["direction"] == "received"
    # fetch_count 是全体当事人的取用次数：bob 2 次 + alice（sender）1 次
    assert mine_b[0]["fetch_count"] == 3, mine_b[0]
    mine_a = _call(srv.list_files.fn, agent_id="alice", token=a)["files"]
    assert mine_a[0]["direction"] == "sent" and mine_a[0]["fetch_count"] == 3
    print("✓ fetched/list OK")


def test_expiry_cleanup():
    print("=== ④ 过期清理 ===")
    srv, store = _setup()
    a = _register(srv, "alice")
    _register(srv, "bob")
    resp = _call(srv.send_file.fn, from_agent="alice", token=a, filename="old.txt",
                 content="x", to_agents=["bob"])
    fid = resp["file_id"]
    # 手工把 expires_at 改到过去
    conn = store._get_conn()
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    conn.execute("UPDATE files SET expires_at=? WHERE id=?", (past, fid))
    conn.commit()
    # 过期后 list 不可见、get 拒绝
    assert _call(srv.list_files.fn, agent_id="alice", token=a)["count"] == 0
    assert "已过期" in _call(srv.get_file.fn, agent_id="alice", token=a, file_id=fid)["error"]
    # 清理：行删 + 盘删
    assert store.cleanup_expired_files() == 1
    assert not (store.files_dir / fid).exists()
    assert store.cleanup_expired_files() == 0
    # 磁盘文件被手工删掉后再清理不报错
    resp2 = _call(srv.send_file.fn, from_agent="alice", token=a, filename="y.txt",
                  content="y", to_agents=["bob"])
    (store.files_dir / resp2["file_id"]).unlink()
    conn.execute("UPDATE files SET expires_at=? WHERE id=?",
                 ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(), resp2["file_id"]))
    conn.commit()
    assert store.cleanup_expired_files() == 1
    print("✓ 过期清理 OK")


def test_dir_resolution():
    print("=== ⑤ 目录解析 ===")
    # :memory: → tempfile
    s1 = HarborStorage(":memory:")
    assert "harbor_files_" in str(s1.files_dir), s1.files_dir
    # 真实路径 → 同级 harbor_files
    tmp = tempfile.mkdtemp()
    s2 = HarborStorage(os.path.join(tmp, "x.db"))
    assert s2.files_dir == __import__("pathlib").Path(tmp) / "harbor_files"
    # save_file 懒创建
    s2.save_file(__import__("mcpharbor.models", fromlist=["HarborFile"]).HarborFile(
        filename="t.txt", sender="a"), "hi")
    assert (s2.files_dir / s2.list_files_for("a")[0].id).exists()
    print("✓ 目录解析 OK")


def test_send_message_hint():
    print("=== ⑥ send_message 大文本 hint + 推送截断 ===")
    srv, store = _setup()
    a = _register(srv, "alice")
    _register(srv, "bob")
    big = "x" * 50_000
    resp = _call(srv.send_message.fn, from_agent="alice", token=a, message=big, to_agent="bob")
    assert resp["status"] == "ok" and "hint" in resp and "send_file" in resp["hint"]
    small = _call(srv.send_message.fn, from_agent="alice", token=a, message="hi", to_agent="bob")
    assert "hint" not in small
    # 推送 summary 截断：注册一个在线会话不可行（需真 MCP session），退而验证 _trunc 本身
    assert server._trunc("a" * 3000) == "a" * 2000 + "…（已截断）"
    assert server._trunc("short") == "short"
    print("✓ hint/截断 OK")


def test_edges():
    print("=== ⑦ 边界 ===")
    srv, store = _setup()
    a = _register(srv, "alice")
    _register(srv, "bob")
    # content 为空可发可取
    resp = _call(srv.send_file.fn, from_agent="alice", token=a, filename="empty.txt",
                 content="", to_agent="bob")
    assert resp["status"] == "ok" and resp["size"] == 0
    got = _call(srv.get_file.fn, agent_id="alice", token=a, file_id=resp["file_id"])
    assert got["content"] == ""
    # filename 净化：../ 与控制字符与超长
    resp2 = _call(srv.send_file.fn, from_agent="alice", token=a,
                  filename="../../etc/passwd\x01", content="x", to_agent="bob")
    assert "/" not in resp2["filename"] and "\\" not in resp2["filename"]
    assert "\x01" not in resp2["filename"]
    resp3 = _call(srv.send_file.fn, from_agent="alice", token=a, filename="长" * 300,
                  content="x", to_agent="bob")
    assert len(resp3["filename"]) == 255
    # 空文件名兜底
    resp4 = _call(srv.send_file.fn, from_agent="alice", token=a, filename="  ",
                  content="x", to_agent="bob")
    assert resp4["filename"] == "file.txt"
    # 收件人无效
    err = _call(srv.send_file.fn, from_agent="alice", token=a, filename="f",
                content="x", to_agent="ghost")
    assert "error" in err
    print("✓ 边界 OK")


if __name__ == "__main__":
    test_fetched_and_list()
    test_expiry_cleanup()
    test_dir_resolution()
    test_send_message_hint()
    test_edges()
    print("\n全部通过 ✅")
