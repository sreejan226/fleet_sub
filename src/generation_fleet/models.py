from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class InstanceState(str, Enum):
    PENDING = "PENDING"
    BOOTING = "BOOTING"
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    DRAINING = "DRAINING"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


class SpotStatus(str, Enum):
    NONE = "none"
    MARKED = "marked"
    WARNING = "warning"
    INTERRUPTED = "interrupted"


class JobState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"


class Pool(str, Enum):
    IO = "io"
    CPU = "cpu"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


MANDATORY_TAGS = ("env", "service", "owner")

INSTANCE_PRICES = {
    "gen.small": 0.10,
    "gen.large": 0.40,
    "gen.xlarge": 1.20,
    "gen.gpu": 3.50,
}

AUTHORIZED_TERMINATE = "ops:terminate:authorized"


class Instance(BaseModel):
    id: str
    instance_type: str
    state: InstanceState = InstanceState.PENDING
    launch_time: float
    boot_time: float = 0.0
    boot_delay: float = 30.0
    ready_at: float = 0.0
    utilization: float = 0.0
    tags: dict[str, str] = Field(default_factory=dict)
    image_digest: str
    environment_keys: list[str] = Field(default_factory=list)
    environment_hash: str
    env_key_hashes: dict[str, str] = Field(default_factory=dict)
    hourly_price: float
    spot_status: SpotStatus = SpotStatus.NONE
    last_heartbeat: float = 0.0
    ttl_seconds: float = 3600.0
    keep_until: bool = False
    warm: bool = False
    image_cached: bool = False
    security_group_id: str = "sg-default"
    interrupt_warn_at: float | None = None
    interrupt_at: float | None = None
    quarantined: bool = False
    quarantine_reason: str | None = None
    worker_id: str | None = None
    pool: Pool = Pool.IO
    last_billing_time: float = 0.0
    accrued_cost: float = 0.0


class Job(BaseModel):
    id: str
    pool: Pool
    payload: dict[str, Any] = Field(default_factory=dict)
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_attempts: int = 3
    created_at: float
    leased_at: float | None = None
    completed_at: float | None = None
    worker_id: str | None = None
    work_seconds: float = 0.2
    model: str = "gen-base"
    resolution_tier: str = "standard"
    result: str | None = None
    error: str | None = None


class WorkerRecord(BaseModel):
    id: str
    pool: Pool
    instance_id: str
    claiming: bool = True
    draining: bool = False
    last_heartbeat: float
    current_job_id: str | None = None
    jobs_completed: int = 0


class CommandDocument(BaseModel):
    document_id: str
    content_hash: str
    permitted_action: str
    allowed_environment: str
    required_tags: dict[str, str] = Field(default_factory=dict)
    workflow_identity: str
    body: dict[str, Any] = Field(default_factory=dict)


class ScalingDecision(BaseModel):
    ts: float
    scaler: str
    pending_jobs: int
    jobs_per_second: float
    backlog_time: float
    current_capacity: int
    desired_capacity: int
    booting: int
    draining: int
    warm: int
    reason: str
    action: str


class SecurityRule(BaseModel):
    port: int
    cidr: str
    description: str = ""


class SecurityGroup(BaseModel):
    id: str
    rules: list[SecurityRule] = Field(default_factory=list)
    internal_service: bool = False


class Waiver(BaseModel):
    id: str
    finding_key: str
    expires_at: float
    reason: str


class Alert(BaseModel):
    id: str
    name: str
    severity: AlertSeverity
    group: str
    message: str
    runbook: str
    ts: float
    silenced: bool = False
