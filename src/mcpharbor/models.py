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
    capabilities: list[str] = Field(default_factory=list)
    hidden: bool = False
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
    reply_to: str = ""
    severity: NotifyPriority = NotifyPriority.NORMAL
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    read: bool = False
    # ack = 收件方明确确认"收到并认领"（比已读更强：已读只代表看到，ack 代表对这个消息负责）
    acked: bool = False
    acked_at: datetime | None = None


class TaskStatus(str, Enum):
    """任务状态机的全部状态。终态：completed / failed / canceled / rejected。"""

    CREATED = "created"          # 已交办，等受托方响应
    ACCEPTED = "accepted"        # 受托方接单
    WORKING = "working"          # 进行中
    INPUT_REQUIRED = "input_required"  # 卡住，等交办方补充信息
    COMPLETED = "completed"      # 完成（终态，带 result）
    FAILED = "failed"            # 失败（终态，带 result）
    CANCELED = "canceled"        # 取消（终态）
    REJECTED = "rejected"        # 拒单（终态）


class Task(BaseModel):
    """任务 - 把"说一句话"升级成"托付一件事"：有双方认账的生命周期、可查、可催、可撤。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:12])
    title: str
    creator: str
    assignee: str
    detail: str = ""
    berth: str = ""
    correlation_id: str = ""
    status: TaskStatus = TaskStatus.CREATED
    result: str = ""
    deadline: str = ""  # ISO 时间，空=不限；超时未到终态由心跳扫尾自动标 failed
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ContractPin(BaseModel):
    """契约版本钉子 - agent 声明"当前任务固定用某 berth 的某版本"。

    任务进行中契约变更时，Harbor 据此提醒钉在旧版本上的 agent（避免同一任务
    一半用旧契约、一半用新契约）；任务结束应由 agent 自己解除。
    """

    id: str = Field(default_factory=lambda: uuid4().hex[:12])
    agent_id: str
    berth: str
    version: str
    task_id: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AuditEntry(BaseModel):
    """审计日志条目。"""

    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    action: str
    actor: str
    target: str
    detail: dict[str, Any] = Field(default_factory=dict)
