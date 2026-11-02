from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GF_", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    sqlite_path: str = "/data/fleet.db"
    role: str = "control-plane"
    worker_pool: str = "io"
    worker_io_concurrency: int = 4
    worker_cpu_concurrency: int = 2
    lease_ttl_seconds: int = 30
    heartbeat_seconds: int = 5
    target_backlog_seconds: float = 15.0
    min_capacity: int = 1
    max_capacity: int = 20
    warm_capacity: int = 2
    cooldown_seconds: float = 20.0
    hysteresis: int = 1
    cost_util_threshold: float = 0.05
    default_ttl_seconds: float = 3600.0
    boot_delay_min: float = 20.0
    boot_delay_max: float = 60.0
    interrupt_warning_seconds: float = 120.0
    default_image_digest: str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    default_env_hash: str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    credit_price_standard: float = 0.05
    credit_price_high: float = 0.20
    slo_success_rate: float = 0.99
    fleet_yaml: str = "/app/fleet.yaml"
    log_level: str = "INFO"
