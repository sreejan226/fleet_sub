from __future__ import annotations


def test_plan_apply_idempotent(world):
    plan1 = world.reconciler.plan(world.desired)
    apply1 = world.reconciler.apply(world.desired)
    assert apply1["applied"] is True
    apply2 = world.reconciler.apply(world.desired)
    assert apply2["applied"] is True
    plan2 = world.reconciler.plan(world.desired)
    assert plan1["min"] == plan2["min"]


def test_destructive_needs_confirmation(world):
    world.desired["fleet"]["capacity"]["max"] = 0
    world.desired["fleet"]["capacity"]["min"] = 0
    inst = world.warm.launch_cold(boot_delay=0)
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    out = world.reconciler.apply(world.desired, confirm_destructive=False)
    assert out.get("needs_confirmation") is True
