from __future__ import annotations

import os

os.environ["GF_TESTING"] = "1"

import pytest
from fakeredis import FakeRedis

from generation_fleet.clock import FakeClock
from generation_fleet.settings import Settings
from generation_fleet.world import build_world


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def world(clock, redis, tmp_path):
    settings = Settings(
        sqlite_path=str(tmp_path / "fleet.db"),
        fleet_yaml=str(tmp_path / "missing.yaml"),
        lease_ttl_seconds=30,
        cooldown_seconds=5,
        min_capacity=1,
        max_capacity=12,
        warm_capacity=2,
        boot_delay_min=20,
        boot_delay_max=60,
        target_backlog_seconds=15,
    )
    w = build_world(settings, redis, clock=clock, sqlite_path=settings.sqlite_path, fleet_path="fleet.yaml")
    w.backlog.cooldown = 5
    return w
