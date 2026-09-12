"""SQLite storage layer for MCP Harbor."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .models import (
    AgentToken, AuditEntry, Berth, BerthStatus, Contract, ContractPin, DirectMessage,
    Manifest, Notification, NotifyPriority, Subscription,
)


class HarborStorage:
    def __init__(self, db_path: str | Path = "harbor.db"):
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS berths (
                id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                version TEXT NOT NULL DEFAULT '0.1.0',
                capabilities TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'active',
                contact TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                berth TEXT NOT NULL,
                version TEXT NOT NULL,
                data TEXT NOT NULL,
                published_at TEXT NOT NULL,
                FOREIGN KEY (berth) REFERENCES berths(id),
                UNIQUE(berth, version)
            );

            CREATE TABLE IF NOT EXISTS contracts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                berth TEXT NOT NULL,
                version TEXT NOT NULL,
                manifest_version TEXT NOT NULL,
                data TEXT NOT NULL,
                published_at TEXT NOT NULL,
                FOREIGN KEY (berth) REFERENCES berths(id),
                UNIQUE(berth, version)
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                subscriber TEXT NOT NULL,
                berth TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT '[]',
                version_range TEXT NOT NULL DEFAULT '*',
                callback TEXT NOT NULL DEFAULT '',
                resource_uri TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id TEXT PRIMARY KEY,
                berth TEXT NOT NULL,
                old_version TEXT NOT NULL DEFAULT '',
                new_version TEXT NOT NULL DEFAULT '',
                change_type TEXT NOT NULL DEFAULT 'contract_changed',
                severity TEXT NOT NULL DEFAULT 'normal',
                summary TEXT NOT NULL DEFAULT '',
                resource_uri TEXT NOT NULL DEFAULT '',
                event TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                merged INTEGER NOT NULL DEFAULT 0,
                merged_ids TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS agent_tokens (
                agent_id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                contact TEXT NOT NULL DEFAULT '',
                last_seen TEXT,
                created_at TEXT NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                berth TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                reply_to TEXT NOT NULL DEFAULT '',
                severity TEXT NOT NULL DEFAULT 'normal',
                created_at TEXT NOT NULL,
                read INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS contract_pins (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                berth TEXT NOT NULL,
                version TEXT NOT NULL,
                task_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(agent_id, berth, task_id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_agent);
            CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_agent);

            CREATE TABLE IF NOT EXISTS audit_log (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                target TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_manifests_berth ON manifests(berth);
            CREATE INDEX IF NOT EXISTS idx_contracts_berth ON contracts(berth);
            CREATE INDEX IF NOT EXISTS idx_subscriptions_berth ON subscriptions(berth);
            CREATE INDEX IF NOT EXISTS idx_notifications_berth ON notifications(berth);
            CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
        """)
        # 旧库迁移：agent_tokens 补齐身份档案列（display_name/description/contact/capabilities/hidden）
        existing_cols = {r["name"] for r in conn.execute("PRAGMA table_info(agent_tokens)").fetchall()}
        for col in ("display_name", "description", "contact"):
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE agent_tokens ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
        if "last_seen" not in existing_cols:
            conn.execute("ALTER TABLE agent_tokens ADD COLUMN last_seen TEXT")
        if "capabilities" not in existing_cols:
            conn.execute("ALTER TABLE agent_tokens ADD COLUMN capabilities TEXT NOT NULL DEFAULT '[]'")
        if "hidden" not in existing_cols:
            conn.execute("ALTER TABLE agent_tokens ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0")
        # messages 补 reply_to（对旧消息引用链路向下兼容，旧行默认空串）
        msg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(messages)").fetchall()}
        if "reply_to" not in msg_cols:
            conn.execute("ALTER TABLE messages ADD COLUMN reply_to TEXT NOT NULL DEFAULT ''")
        conn.commit()

    # ── Berth CRUD ──

    def upsert_berth(self, berth: Berth) -> Berth:
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        berth.updated_at = datetime.now(timezone.utc)
        conn.execute("""
            INSERT INTO berths (id, owner, version, capabilities, status, contact, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                owner=excluded.owner, version=excluded.version,
                capabilities=excluded.capabilities, status=excluded.status,
                contact=excluded.contact, updated_at=excluded.updated_at
        """, (
            berth.id, berth.owner, berth.version,
            json.dumps(berth.capabilities), berth.status.value,
            berth.contact, berth.created_at.isoformat(), now
        ))
        conn.commit()
        return berth

    def get_berth(self, berth_id: str) -> Berth | None:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM berths WHERE id=?", (berth_id,)).fetchone()
        if not row:
            return None
        return Berth(
            id=row["id"], owner=row["owner"], version=row["version"],
            capabilities=json.loads(row["capabilities"]),
            status=BerthStatus(row["status"]), contact=row["contact"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def list_berths(self, capability: str | None = None) -> list[Berth]:
        conn = self._get_conn()
        if capability:
            rows = conn.execute(
                "SELECT * FROM berths WHERE capabilities LIKE ? AND status='active'",
                (f'%"{capability}"%',)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM berths WHERE status='active'").fetchall()
        return [
            Berth(
                id=r["id"], owner=r["owner"], version=r["version"],
                capabilities=json.loads(r["capabilities"]),
                status=BerthStatus(r["status"]), contact=r["contact"],
                created_at=datetime.fromisoformat(r["created_at"]),
                updated_at=datetime.fromisoformat(r["updated_at"]),
            )
            for r in rows
        ]

    def deactivate_berth(self, berth_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "UPDATE berths SET status='inactive', updated_at=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), berth_id)
        )
        conn.commit()
        return cur.rowcount > 0

    def list_all_berths(self) -> list[Berth]:
        """管理视角：不管状态，返回全部 berth（含 inactive/deprecated）。"""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM berths").fetchall()
        return [
            Berth(
                id=r["id"], owner=r["owner"], version=r["version"],
                capabilities=json.loads(r["capabilities"]),
                status=BerthStatus(r["status"]), contact=r["contact"],
                created_at=datetime.fromisoformat(r["created_at"]),
                updated_at=datetime.fromisoformat(r["updated_at"]),
            )
            for r in rows
        ]

    # ── Manifest ──

    def publish_manifest(self, manifest: Manifest) -> Manifest:
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        manifest.published_at = datetime.now(timezone.utc)
        conn.execute("""
            INSERT OR REPLACE INTO manifests (berth, version, data, published_at)
            VALUES (?, ?, ?, ?)
        """, (manifest.berth, manifest.version, manifest.model_dump_json(), now))
        conn.commit()
        return manifest

    def get_manifest(self, berth_id: str, version: str | None = None) -> Manifest | None:
        conn = self._get_conn()
        if version:
            row = conn.execute(
                "SELECT data FROM manifests WHERE berth=? AND version=?",
                (berth_id, version)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT data FROM manifests WHERE berth=? ORDER BY published_at DESC LIMIT 1",
                (berth_id,)
            ).fetchone()
        if not row:
            return None
        return Manifest.model_validate_json(row["data"])

    def list_manifest_versions(self, berth_id: str) -> list[str]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT version FROM manifests WHERE berth=? ORDER BY published_at DESC",
            (berth_id,)
        ).fetchall()
        return [r["version"] for r in rows]

    def get_manifest_versions_with_data(self, berth_id: str) -> list[Manifest]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM manifests WHERE berth=? ORDER BY published_at DESC",
            (berth_id,)
        ).fetchall()
        return [Manifest.model_validate_json(r["data"]) for r in rows]

    # ── Contract ──

    def publish_contract(self, contract: Contract) -> Contract:
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        contract.published_at = datetime.now(timezone.utc)
        conn.execute("""
            INSERT OR REPLACE INTO contracts (berth, version, manifest_version, data, published_at)
            VALUES (?, ?, ?, ?, ?)
        """, (contract.berth, contract.version, contract.manifest_version,
              contract.model_dump_json(), now))
        conn.commit()
        return contract

    def get_contract(self, berth_id: str, version: str | None = None) -> Contract | None:
        conn = self._get_conn()
        if version:
            row = conn.execute(
                "SELECT data FROM contracts WHERE berth=? AND version=?",
                (berth_id, version)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT data FROM contracts WHERE berth=? ORDER BY published_at DESC LIMIT 1",
                (berth_id,)
            ).fetchone()
        if not row:
            return None
        return Contract.model_validate_json(row["data"])

    # ── Subscription ──

    def add_subscription(self, sub: Subscription) -> Subscription:
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO subscriptions (id, subscriber, berth, events, version_range, callback, resource_uri, created_at, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sub.id, sub.subscriber, sub.berth,
            json.dumps(sub.events), sub.version_range,
            sub.callback, sub.resource_uri,
            sub.created_at.isoformat(), int(sub.active)
        ))
        conn.commit()
        return sub

    def get_subscriptions(self, berth_id: str) -> list[Subscription]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM subscriptions WHERE berth=? AND active=1",
            (berth_id,)
        ).fetchall()
        return [
            Subscription(
                id=r["id"], subscriber=r["subscriber"], berth=r["berth"],
                events=json.loads(r["events"]), version_range=r["version_range"],
                callback=r["callback"], resource_uri=r["resource_uri"],
                created_at=datetime.fromisoformat(r["created_at"]),
                active=bool(r["active"]),
            )
            for r in rows
        ]

    def remove_subscription(self, sub_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute("UPDATE subscriptions SET active=0 WHERE id=?", (sub_id,))
        conn.commit()
        return cur.rowcount > 0

    def list_all_subscriptions(self) -> list[Subscription]:
        """管理视角：跨所有 berth 的全部活跃订阅关系。"""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM subscriptions WHERE active=1").fetchall()
        return [
            Subscription(
                id=r["id"], subscriber=r["subscriber"], berth=r["berth"],
                events=json.loads(r["events"]), version_range=r["version_range"],
                callback=r["callback"], resource_uri=r["resource_uri"],
                created_at=datetime.fromisoformat(r["created_at"]),
                active=bool(r["active"]),
            )
            for r in rows
        ]

    # ── Notification ──

    def add_notification(self, notif: Notification) -> Notification:
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO notifications (id, berth, old_version, new_version, change_type,
                severity, summary, resource_uri, event, created_at, merged, merged_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            notif.id, notif.berth, notif.old_version, notif.new_version,
            notif.change_type, notif.severity.value, notif.summary,
            notif.resource_uri, notif.event, notif.created_at.isoformat(),
            int(notif.merged), json.dumps(notif.merged_ids)
        ))
        conn.commit()
        return notif

    def get_recent_notifications(self, berth_id: str, within_seconds: int = 5) -> list[Notification]:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=within_seconds)).isoformat()
        rows = conn.execute(
            "SELECT * FROM notifications WHERE berth=? AND created_at>=? AND merged=0 ORDER BY created_at DESC",
            (berth_id, cutoff)
        ).fetchall()
        return [
            Notification(
                id=r["id"], berth=r["berth"], old_version=r["old_version"],
                new_version=r["new_version"], change_type=r["change_type"],
                severity=NotifyPriority(r["severity"]), summary=r["summary"],
                resource_uri=r["resource_uri"], event=r["event"],
                created_at=datetime.fromisoformat(r["created_at"]),
                merged=bool(r["merged"]),
                merged_ids=json.loads(r["merged_ids"]),
            )
            for r in rows
        ]

    def mark_notification_merged(self, notif_ids: list[str], merged_into: str) -> None:
        conn = self._get_conn()
        for nid in notif_ids:
            conn.execute(
                "UPDATE notifications SET merged=1 WHERE id=?",
                (nid,)
            )
        conn.commit()

    def get_notification_history(self, berth_id: str | None = None, limit: int = 50) -> list[Notification]:
        conn = self._get_conn()
        if berth_id:
            rows = conn.execute(
                "SELECT * FROM notifications WHERE berth=? ORDER BY created_at DESC LIMIT ?",
                (berth_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM notifications ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [
            Notification(
                id=r["id"], berth=r["berth"], old_version=r["old_version"],
                new_version=r["new_version"], change_type=r["change_type"],
                severity=NotifyPriority(r["severity"]), summary=r["summary"],
                resource_uri=r["resource_uri"], event=r["event"],
                created_at=datetime.fromisoformat(r["created_at"]),
                merged=bool(r["merged"]),
                merged_ids=json.loads(r["merged_ids"]),
            )
            for r in rows
        ]

    def count_notifications(self) -> int:
        conn = self._get_conn()
        return conn.execute("SELECT COUNT(*) AS n FROM notifications").fetchone()["n"]

    # ── Agent Token ──

    def _row_to_agent_token(self, row: sqlite3.Row) -> AgentToken:
        return AgentToken(
            agent_id=row["agent_id"], token_hash=row["token_hash"],
            display_name=row["display_name"], description=row["description"],
            contact=row["contact"],
            capabilities=json.loads(row["capabilities"]) if "capabilities" in row.keys() else [],
            hidden=bool(row["hidden"]) if "hidden" in row.keys() else False,
            last_seen=datetime.fromisoformat(row["last_seen"]) if row["last_seen"] else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            revoked=bool(row["revoked"]),
        )

    def get_agent_token(self, agent_id: str) -> AgentToken | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM agent_tokens WHERE agent_id=?", (agent_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_agent_token(row)

    def create_agent_token(
        self, agent_id: str, token_hash: str,
        display_name: str = "", description: str = "", contact: str = "",
        capabilities: list[str] | None = None, hidden: bool = False,
    ) -> bool:
        """注册新 agent_id 的令牌（带身份档案）。若 agent_id 已存在则返回 False。"""
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT INTO agent_tokens (agent_id, token_hash, display_name, description, contact, capabilities, hidden, created_at, revoked)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
                (agent_id, token_hash, display_name, description, contact,
                 json.dumps(capabilities or []), int(hidden),
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def touch_agent_last_seen(self, agent_id: str) -> None:
        """记录 agent 最后一次成功通过认证的时间，用于识别僵尸注册。"""
        conn = self._get_conn()
        conn.execute(
            "UPDATE agent_tokens SET last_seen=? WHERE agent_id=?",
            (datetime.now(timezone.utc).isoformat(), agent_id),
        )
        conn.commit()

    def revoke_agent(self, agent_id: str) -> bool:
        """吊销 agent：token 立即失效，注册记录保留可追溯。"""
        conn = self._get_conn()
        cur = conn.execute(
            "UPDATE agent_tokens SET revoked=1 WHERE agent_id=?",
            (agent_id,),
        )
        conn.commit()
        return cur.rowcount > 0

    def purge_agent(self, agent_id: str) -> tuple[int, int]:
        """彻底删除 agent 的注册记录及其全部订阅、私信、契约钉，返回 (删除订阅数, 删除私信数)。"""
        conn = self._get_conn()
        subs = conn.execute("SELECT COUNT(*) AS n FROM subscriptions WHERE subscriber=?", (agent_id,)).fetchone()["n"]
        msgs = conn.execute(
            "SELECT COUNT(*) AS n FROM messages WHERE from_agent=? OR to_agent=?", (agent_id, agent_id)
        ).fetchone()["n"]
        conn.execute("DELETE FROM subscriptions WHERE subscriber=?", (agent_id,))
        conn.execute("DELETE FROM messages WHERE from_agent=? OR to_agent=?", (agent_id, agent_id))
        conn.execute("DELETE FROM contract_pins WHERE agent_id=?", (agent_id,))
        conn.execute("DELETE FROM agent_tokens WHERE agent_id=?", (agent_id,))
        conn.commit()
        return subs, msgs

    def rotate_agent_token(self, agent_id: str, new_token_hash: str) -> bool:
        """轮换已存在 agent_id 的令牌。若 agent_id 不存在则返回 False。"""
        conn = self._get_conn()
        cur = conn.execute(
            "UPDATE agent_tokens SET token_hash=?, revoked=0 WHERE agent_id=?",
            (new_token_hash, agent_id),
        )
        conn.commit()
        return cur.rowcount > 0

    def verify_agent_token(self, agent_id: str, token_hash: str) -> bool:
        token = self.get_agent_token(agent_id)
        if token is None or token.revoked:
            return False
        return token.token_hash == token_hash

    def list_agents(self) -> list[AgentToken]:
        """管理视角：全部已注册的 agent（含 token_hash，调用方展示前应自行去掉）。"""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM agent_tokens ORDER BY created_at ASC").fetchall()
        return [self._row_to_agent_token(r) for r in rows]

    def search_agents(self, keyword: str = "", capability: str = "") -> list[AgentToken]:
        """公开视角：搜索可见的 agent（排除已吊销和隐身注册的），不含 token_hash 也一样返回，
        由调用方决定展示哪些字段。keyword 匹配 agent_id/显示名/描述/联系方式。"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM agent_tokens WHERE revoked=0 AND hidden=0 ORDER BY created_at ASC"
        ).fetchall()
        agents = [self._row_to_agent_token(r) for r in rows]
        if keyword:
            kw = keyword.lower()
            agents = [
                a for a in agents
                if kw in a.agent_id.lower()
                or kw in a.display_name.lower()
                or kw in a.description.lower()
                or kw in a.contact.lower()
                or any(kw in c.lower() for c in a.capabilities)
            ]
        if capability:
            cap = capability.lower()
            agents = [a for a in agents if any(cap == c.lower() for c in a.capabilities)]
        return agents

    # ── Direct Message ──

    def _row_to_message(self, r: sqlite3.Row) -> DirectMessage:
        return DirectMessage(
            id=r["id"], from_agent=r["from_agent"], to_agent=r["to_agent"],
            berth=r["berth"], message=r["message"], correlation_id=r["correlation_id"],
            reply_to=r["reply_to"] if "reply_to" in r.keys() else "",
            severity=NotifyPriority(r["severity"]),
            created_at=datetime.fromisoformat(r["created_at"]), read=bool(r["read"]),
        )

    def add_message(self, msg: DirectMessage) -> DirectMessage:
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO messages (id, from_agent, to_agent, berth, message,
                correlation_id, reply_to, severity, created_at, read)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            msg.id, msg.from_agent, msg.to_agent, msg.berth, msg.message,
            msg.correlation_id, msg.reply_to, msg.severity.value,
            msg.created_at.isoformat(), int(msg.read),
        ))
        conn.commit()
        return msg

    def get_message(self, message_id: str) -> DirectMessage | None:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
        return self._row_to_message(row) if row else None

    def get_messages(
        self, agent_id: str, with_agent: str | None = None,
        unread_only: bool = False, limit: int = 50,
    ) -> list[DirectMessage]:
        """返回 agent_id 作为收件人或发件人的私信，按时间倒序。"""
        conn = self._get_conn()
        conditions = ["(to_agent=? OR from_agent=?)"]
        params: list[Any] = [agent_id, agent_id]
        if with_agent:
            conditions.append("(to_agent=? OR from_agent=?)")
            params.extend([with_agent, with_agent])
        if unread_only:
            conditions.append("to_agent=? AND read=0")
            params.append(agent_id)
        where = " AND ".join(conditions)
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM messages WHERE {where} ORDER BY created_at DESC LIMIT ?",
            params,
        ).fetchall()
        return [self._row_to_message(r) for r in rows]

    def get_conversations(self, agent_id: str) -> list[dict[str, Any]]:
        """按对话对象聚合：每个对方一条最新消息 + 未读数。

        消费者应关注最新消息——多轮往来后从这里一眼看清"谁发了什么、几条没读"，
        再决定用 get_messages(with_agent=...) 展开哪段对话。
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM messages WHERE to_agent=? OR from_agent=? ORDER BY created_at DESC",
            (agent_id, agent_id),
        ).fetchall()
        by_peer: dict[str, dict[str, Any]] = {}
        for r in rows:
            msg = self._row_to_message(r)
            peer = msg.from_agent if msg.to_agent == agent_id else msg.to_agent
            entry = by_peer.get(peer)
            if entry is None:
                by_peer[peer] = {
                    "peer": peer,
                    "last_message": {
                        "id": msg.id, "from_agent": msg.from_agent,
                        "to_agent": msg.to_agent, "message": msg.message,
                        "correlation_id": msg.correlation_id,
                        "reply_to": msg.reply_to,
                        "severity": msg.severity.value,
                        "created_at": msg.created_at.isoformat(),
                        "read": msg.read,
                    },
                    "unread": 0,
                    "total": 0,
                }
                entry = by_peer[peer]
            entry["total"] += 1
            if msg.to_agent == agent_id and not msg.read:
                entry["unread"] += 1
        # 有未读的对话排最前（未读多者优先），其余按最新消息时间倒序
        entries = sorted(by_peer.values(),
                         key=lambda e: e["last_message"]["created_at"], reverse=True)
        entries.sort(key=lambda e: e["unread"], reverse=True)
        return entries

    def mark_messages_read(self, agent_id: str, message_ids: list[str]) -> int:
        """只能标记发给自己（to_agent=agent_id）的私信为已读。"""
        conn = self._get_conn()
        count = 0
        for mid in message_ids:
            cur = conn.execute(
                "UPDATE messages SET read=1 WHERE id=? AND to_agent=?",
                (mid, agent_id),
            )
            count += cur.rowcount
        conn.commit()
        return count

    def count_messages(self) -> int:
        conn = self._get_conn()
        return conn.execute("SELECT COUNT(*) AS n FROM messages").fetchone()["n"]

    # ── Audit ──

    def log_audit(self, entry: AuditEntry) -> None:
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO audit_log (id, timestamp, action, actor, target, detail)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            entry.id, entry.timestamp.isoformat(),
            entry.action, entry.actor, entry.target,
            json.dumps(entry.detail)
        ))
        conn.commit()

    def get_audit_log(self, limit: int = 100, action: str | None = None,
                      actor: str | None = None, berth: str | None = None) -> list[AuditEntry]:
        conn = self._get_conn()
        conditions = []
        params: list[Any] = []
        if action:
            conditions.append("action=?")
            params.append(action)
        if actor:
            conditions.append("actor=?")
            params.append(actor)
        if berth:
            conditions.append("target LIKE ?")
            params.append(f"%berth:{berth}%")

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM audit_log{where} ORDER BY timestamp DESC LIMIT ?",
            params
        ).fetchall()
        return [
            AuditEntry(
                id=r["id"],
                timestamp=datetime.fromisoformat(r["timestamp"]),
                action=r["action"], actor=r["actor"], target=r["target"],
                detail=json.loads(r["detail"]),
            )
            for r in rows
        ]

    def cleanup_old_audit(self, days: int = 90) -> int:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = conn.execute("DELETE FROM audit_log WHERE timestamp<?", (cutoff,))
        conn.commit()
        return cur.rowcount

    def cleanup_old_notifications(self, days: int = 90) -> int:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = conn.execute("DELETE FROM notifications WHERE created_at<?", (cutoff,))
        conn.commit()
        return cur.rowcount

    def cleanup_old_messages(self, days: int = 90) -> int:
        """删除超过保留期的私信（已读未读一起删），返回删除条数。"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = conn.execute("DELETE FROM messages WHERE created_at<?", (cutoff,))
        conn.commit()
        return cur.rowcount

    # ── Contract Pin ──

    def pin_contract(self, agent_id: str, berth: str, version: str, task_id: str = "") -> ContractPin:
        """钉住某 berth 的某版本（同一 agent+berth+task_id 重复钉 = 更新版本）。"""
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO contract_pins (id, agent_id, berth, version, task_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(agent_id, berth, task_id) DO UPDATE SET
                version=excluded.version, created_at=excluded.created_at
        """, (
            uuid4().hex[:12], agent_id, berth, version, task_id,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        return ContractPin(agent_id=agent_id, berth=berth, version=version, task_id=task_id)

    def unpin_contract(self, agent_id: str, berth: str, task_id: str = "") -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM contract_pins WHERE agent_id=? AND berth=? AND task_id=?",
            (agent_id, berth, task_id),
        )
        conn.commit()
        return cur.rowcount > 0

    def get_pins(self, agent_id: str) -> list[ContractPin]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM contract_pins WHERE agent_id=? ORDER BY created_at DESC",
            (agent_id,),
        ).fetchall()
        return [
            ContractPin(id=r["id"], agent_id=r["agent_id"], berth=r["berth"],
                        version=r["version"], task_id=r["task_id"],
                        created_at=datetime.fromisoformat(r["created_at"]))
            for r in rows
        ]

    def get_pins_for_berth(self, berth: str, exclude_version: str = "") -> list[ContractPin]:
        """某 berth 的全部钉子；exclude_version 非空时只返回钉在其它（旧）版本上的。"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM contract_pins WHERE berth=?", (berth,)
        ).fetchall()
        pins = [
            ContractPin(id=r["id"], agent_id=r["agent_id"], berth=r["berth"],
                        version=r["version"], task_id=r["task_id"],
                        created_at=datetime.fromisoformat(r["created_at"]))
            for r in rows
        ]
        if exclude_version:
            pins = [p for p in pins if p.version != exclude_version]
        return pins

    # ── Check Updates ──

    def check_updates(self, known_versions: dict[str, str]) -> list[dict[str, Any]]:
        """检查已知版本是否有更新。

        known_versions: {berth_id: known_version}
        返回有更新的 berth 列表。
        """
        conn = self._get_conn()
        updates = []
        for berth_id, known_ver in known_versions.items():
            row = conn.execute(
                "SELECT version FROM manifests WHERE berth=? ORDER BY published_at DESC LIMIT 1",
                (berth_id,)
            ).fetchone()
            if row and row["version"] != known_ver:
                updates.append({
                    "berth": berth_id,
                    "known_version": known_ver,
                    "latest_version": row["version"],
                })
        return updates

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
