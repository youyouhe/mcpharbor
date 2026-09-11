"""Pydantic schemas for MCP Harbor."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class BerthStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"


class NotifyPriority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class Berth(BaseModel):
    """泊位 - 项目在 Harbor 的注册点。"""

    id: str = Field(..., min_length=1, max_length=64, pattern=r"^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$")
    owner: str
    version: str = "0.1.0"
    capabilities: list[str] = Field(default_factory=list)
    status: BerthStatus = BerthStatus.ACTIVE
    contact: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Manifest(BaseModel):
    """项目卡 - 元数据、能力、协议、认证等。"""

    berth: str
    version: str
    owner: str
    capabilities: list[str] = Field(default_factory=list)
    protocol: str = "http"
    base_url: str = ""
    auth: dict[str, Any] = Field(default_factory=dict)
    requirements: list[str] = Field(default_factory=list)
    errors: dict[int, str] = Field(default_factory=dict)
    events: list[str] = Field(default_factory=list)
    contact: str = ""
    published_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Contract(BaseModel):
    """契约 - 接口、协议、认证方式、通信要求、错误码、事件。"""

    berth: str
    version: str
    manifest_version: str
    endpoints: list[dict[str, Any]] = Field(default_factory=list)
    protocol: str = "http"
    auth: dict[str, Any] = Field(default_factory=dict)
    requirements: list[str] = Field(default_factory=list)
    errors: dict[int, str] = Field(default_factory=dict)
    events: list[str] = Field(default_factory=list)
    compatibility: dict[str, Any] = Field(default_factory=dict)
    published_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Subscription(BaseModel):
    """订阅 - 谁关心哪个契约变更。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:12])
    subscriber: str
    berth: str
    events: list[str] = Field(default_factory=list)
    version_range: str = "*"
    callback: str = ""
    resource_uri: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    active: bool = True


class Notification(BaseModel):
    """通知记录 - 存储发送过的通知。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    berth: str
    old_version: str = ""
    new_version: str = ""
    change_type: str = "contract_changed"
    severity: NotifyPriority = NotifyPriority.NORMAL
    summary: str = ""
    resource_uri: str = ""
    event: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    merged: bool = False
    merged_ids: list[str] = Field(default_factory=list)


class AgentToken(BaseModel):
    """Agent 身份令牌 - 证明某个 agent_id 的调用者持有正确的密钥。"""

    agent_id: str = Field(..., min_length=1, max_length=128)
    token_hash: str
    display_name: str = ""
    description: str = ""
    contact: str = ""
    last_seen: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    revoked: bool = False


class DirectMessage(BaseModel):
    """点对点私信 - 只有 from_agent/to_agent 双方可见，与 berth 广播通知隔离。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    from_agent: str
    to_agent: str
    berth: str = ""
    message: str = ""
    correlation_id: str = ""
    severity: NotifyPriority = NotifyPriority.NORMAL
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    read: bool = False


class AuditEntry(BaseModel):
    """审计日志条目。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    action: str
    actor: str
    target: str
    detail: dict[str, Any] = Field(default_factory=dict)
