from __future__ import annotations

from generation_fleet.models import Pool
from generation_fleet.world import World


def submit_n(world: World, n: int, work_seconds: float = 0.05, **kwargs) -> None:
    for _ in range(n):
        world.queue.submit(Pool.IO, {"kind": "generate"}, work_seconds=work_seconds, **kwargs)


def run_for(world: World, seconds: float, step: float = 1.0) -> None:
    end = world.clock.now() + seconds
    while world.clock.now() < end:
        world.tick()
        world.clock.sleep(step)


def trace_steady(world: World, duration: float = 30, rate: int = 2) -> None:
    elapsed = 0.0
    while elapsed < duration:
        submit_n(world, rate)
        world.tick()
        world.clock.sleep(1)
        elapsed += 1


def trace_spike(world: World, boot_delay: float = 45.0) -> None:
    world.warm.forced_boot_delay = boot_delay
    submit_n(world, 3)
    run_for(world, 5)
    submit_n(world, 30)
    run_for(world, 90)


def trace_oscillate(world: World) -> None:
    for i in range(40):
        submit_n(world, 8 if i % 8 < 4 else 1)
        world.tick()
        world.clock.sleep(1)


def trace_ramp(world: World) -> None:
    for i in range(1, 21):
        submit_n(world, i)
        world.tick()
        world.clock.sleep(1)


def chaos_interrupt_fraction(world: World, fraction: float = 0.3) -> int:
    running = [i for i in world.cloud.list_instances() if i.state.value == "RUNNING"]
    n = max(1, int(len(running) * fraction))
    for inst in running[:n]:
        world.cloud.schedule_spot_interruption(inst.id, warning_seconds=world.settings.interrupt_warning_seconds)
    return n
