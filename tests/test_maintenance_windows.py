from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.compute.maintenance import MaintenanceWindowService, window_batch_key
from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import close_connection, get_connection, init_db, transaction


@pytest.fixture(autouse=True)
def isolated_database(tmp_path: Path):
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "maintenance.db")
    close_connection()
    yield
    close_connection()


TEMPLATE_A = {
    "code": "solver-a",
    "name": "方程求解模板A",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}
TEMPLATE_B = {
    "code": "solver-b",
    "name": "方程求解模板B",
    "algorithm": "solver-b",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def build(clock: FrozenClock) -> tuple[ComputeOperationsService, MaintenanceWindowService]:
    init_db()
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE_A, "administrator")
    service.create_template(TEMPLATE_B, "administrator")
    return service, MaintenanceWindowService(get_connection(), clock)


def submit(service: ComputeOperationsService, key: str, *, template: str = "solver-a", project: str = "project-a", user: str = "researcher-1", priority: int = 50) -> dict:
    return service.submit({
        "template_code": template, "project_code": project, "requested_by": user,
        "parameters": {"iterations": 100}, "priority": priority, "idempotency_key": key,
    })


def window_payload(base: datetime, *, _json: bool = False, **overrides) -> dict:
    def stamp(value: datetime) -> str:
        return value.isoformat() if _json else value

    payload = {
        "code": "upgrade-solver-a",
        "name": "求解器A升级",
        "scope_type": "algorithm",
        "scope_value": "solver-a",
        "drain_at": stamp(base + timedelta(minutes=1)),
        "deadline_at": stamp(base + timedelta(minutes=10)),
        "recover_at": stamp(base + timedelta(minutes=20)),
        "deadline_policy": "cancel",
        "block_reason": "求解器升级，暂停相关任务领取",
    }
    payload.update(overrides)
    return payload


def test_announced_window_does_not_block_claims():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    maintenance.create_window(window_payload(clock.now()), "administrator")
    task = submit(service, "announce-001")
    claimed = service.claim("worker-1", ["solver-a"], 60)
    assert claimed is not None and claimed["id"] == task["id"]
    view = maintenance.get_window(1)
    assert view["state"] == "announced"
    assert view["blocked_reason"] == ""
    assert view["progress"]["safe_to_upgrade"] is True


def test_draining_blocks_matched_claims_but_keeps_other_queues_moving():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    blocked_task = submit(service, "drain-001", template="solver-a", priority=90)
    free_task = submit(service, "drain-002", template="solver-b", priority=10)
    maintenance.create_window(window_payload(clock.now()), "administrator")
    clock.advance(minutes=2)
    view = maintenance.advance_window(1)
    assert view["state"] == "draining"
    assert view["affected_task_ids"] == [blocked_task["id"]]  # 排空开始时匹配的排队任务
    # 即使匹配任务优先级更高，工作者也只能领到其他队列的任务
    claimed = service.claim("worker-1", ["solver-a", "solver-b"], 60)
    assert claimed["id"] == free_task["id"]
    assert service.claim("worker-2", ["solver-a"], 60) is None
    gate = maintenance.claim_gate(["solver-a"])
    assert gate == {
        "task_id": blocked_task["id"], "window_id": 1, "window_code": "upgrade-solver-a",
        "window_name": "求解器A升级", "window_state": "draining",
        "reason": "求解器升级，暂停相关任务领取", "deadline_at": view["deadline_at"],
    }
    refreshed = maintenance.get_window(1)
    assert refreshed["progress"]["queued_blocked"] == 1
    assert refreshed["progress"]["running_leases"] == 0
    tracked = [item for item in refreshed["affected_tasks"] if item["task_id"] == blocked_task["id"]][0]
    assert tracked["block_count"] == 2
    assert tracked["outcome"] == "blocked"


def test_running_short_task_completes_naturally_and_progress_reflects_safety():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    running_task = submit(service, "natural-001")
    claimed = service.claim("worker-1", ["solver-a"], 60)
    maintenance.create_window(window_payload(clock.now()), "administrator")
    clock.advance(minutes=2)
    view = maintenance.advance_window(1)
    assert view["affected_task_ids"] == [running_task["id"]]
    assert view["progress"] == {
        "phase": "draining", "snapshot_task_count": 1, "natural_completed": 0,
        "queued_blocked": 0, "running_leases": 1,
        "enforced_task_count": 0, "cancelled_task_count": 0, "requeued_task_count": 0,
        "remaining": 1, "safe_to_upgrade": False,
    }
    service.complete(claimed["id"], "worker-1", {"value": 1}, {})
    view = maintenance.get_window(1)
    assert view["progress"]["natural_completed"] == 1
    assert view["progress"]["running_leases"] == 0
    assert view["progress"]["safe_to_upgrade"] is True


def test_deadline_cancels_matched_tasks_once_and_releases_leases():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    running_task = submit(service, "force-001")
    service.claim("worker-1", ["solver-a"], 60)
    queued_task = submit(service, "force-002")
    maintenance.create_window(window_payload(clock.now()), "administrator")
    clock.advance(minutes=11)
    view = maintenance.advance_window(1)
    assert view["state"] == "enforcing"
    assert service.get_task(running_task["id"])["status"] == "cancelled"
    assert service.get_task(queued_task["id"])["status"] == "cancelled"
    assert service.get_task(running_task["id"])["lease_owner"] == ""
    assert view["progress"]["cancelled_task_count"] == 2
    assert view["progress"]["requeued_task_count"] == 0
    assert view["progress"]["safe_to_upgrade"] is True
    for task_id in (running_task["id"], queued_task["id"]):
        interventions = service.get_task(task_id)["interventions"]
        assert [item["action"] for item in interventions] == ["maintenance_cancel"]
        assert interventions[0]["batch_key"] == window_batch_key("upgrade-solver-a")
        assert interventions[0]["actor"] == "maintenance-window:upgrade-solver-a"
    # 重复推进不能产生二次干预记录
    maintenance.advance_window(1)
    maintenance.advance_window(1)
    for task_id in (running_task["id"], queued_task["id"]):
        assert len(service.get_task(task_id)["interventions"]) == 1


def test_requeue_policy_returns_running_lease_to_queue_with_original_priority():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    running_task = submit(service, "requeue-001", priority=77)
    service.claim("worker-1", ["solver-a"], 60)
    queued_task = submit(service, "requeue-002", priority=55)
    maintenance.create_window(window_payload(clock.now(), code="requeue-win", deadline_policy="requeue"), "administrator")
    clock.advance(minutes=11)
    view = maintenance.advance_window(1)
    assert view["state"] == "enforcing"
    running_after = service.get_task(running_task["id"])
    queued_after = service.get_task(queued_task["id"])
    assert running_after["status"] == "queued" and running_after["priority"] == 77
    assert running_after["lease_owner"] == "" and running_after["lease_expires_at"] == ""
    assert queued_after["status"] == "queued" and queued_after["priority"] == 55
    interventions = running_after["interventions"]
    assert [item["action"] for item in interventions] == ["maintenance_requeue"]
    # 排队任务没有被强制干预
    assert queued_after["interventions"] == []
    assert view["progress"]["requeued_task_count"] == 1
    # 恢复后按原优先级竞争：77 的任务先被领取
    clock.advance(minutes=20)
    maintenance.advance_window(1)
    claimed = service.claim("worker-2", ["solver-a"], 60)
    assert claimed["id"] == running_task["id"]


def test_overlapping_windows_apply_stricter_cancel_rule():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    task = submit(service, "overlap-001")
    service.claim("worker-1", ["solver-a"], 60)
    base = clock.now()
    maintenance.create_window(window_payload(base, code="project-requeue", scope_type="project", scope_value="project-a", deadline_policy="requeue"), "administrator")
    maintenance.create_window(window_payload(base, code="algo-cancel", scope_type="algorithm", scope_value="solver-a", deadline_policy="cancel"), "administrator")
    clock.advance(minutes=11)
    # 无论先推进哪个窗口，都必须采用取消策略，且只有一条干预记录
    requeue_view = maintenance.advance_window(1)
    cancel_view = maintenance.advance_window(2)
    assert service.get_task(task["id"])["status"] == "cancelled"
    intervention = service.get_task(task["id"])["interventions"][0]
    assert intervention["action"] == "maintenance_cancel"
    assert intervention["batch_key"] == window_batch_key("algo-cancel")
    assert cancel_view["progress"]["cancelled_task_count"] == 1
    # 反向顺序结果一致（另一个任务、新数据库由另一个用例覆盖推进顺序的对称性）
    requeue_view = maintenance.advance_window(1)
    cancel_view = maintenance.advance_window(2)
    assert len(service.get_task(task["id"])["interventions"]) == 1
    assert requeue_view["progress"]["cancelled_task_count"] == 0


def test_overlap_strictness_holds_when_cancel_window_advances_first():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    task = submit(service, "overlap-002")
    service.claim("worker-1", ["solver-a"], 60)
    base = clock.now()
    maintenance.create_window(window_payload(base, code="project-requeue-2", scope_type="project", scope_value="project-a", deadline_policy="requeue"), "administrator")
    maintenance.create_window(window_payload(base, code="algo-cancel-2", scope_type="algorithm", scope_value="solver-a", deadline_policy="cancel"), "administrator")
    clock.advance(minutes=11)
    cancel_view = maintenance.advance_window(2)  # 先推进取消窗口
    requeue_view = maintenance.advance_window(1)
    assert service.get_task(task["id"])["status"] == "cancelled"
    interventions = service.get_task(task["id"])["interventions"]
    assert len(interventions) == 1
    assert interventions[0]["batch_key"] == window_batch_key("algo-cancel-2")
    assert cancel_view["progress"]["cancelled_task_count"] == 1
    assert requeue_view["progress"]["cancelled_task_count"] == 0
    # 再来一轮重复推进仍然只有一条干预
    maintenance.advance_window(1)
    maintenance.advance_window(2)
    assert len(service.get_task(task["id"])["interventions"]) == 1


def test_window_cancel_abandons_schedule_and_claims_resume():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    maintenance.create_window(window_payload(clock.now()), "administrator")
    clock.advance(minutes=2)
    maintenance.advance_window(1)
    task = submit(service, "abandon-001")
    assert service.claim("worker-1", ["solver-a"], 60) is None
    view = maintenance.cancel_window(1, "administrator", "升级延期")
    assert view["state"] == "cancelled"
    claimed = service.claim("worker-1", ["solver-a"], 60)
    assert claimed["id"] == task["id"]
    # 取消后推进不会复活窗口
    again = maintenance.advance_window(1)
    assert again["state"] == "cancelled"


def test_manual_recovery_skips_enforcement_and_other_scope_stays_claimable():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    matched = submit(service, "manual-001", template="solver-a")
    other = submit(service, "manual-002", template="solver-b")
    maintenance.create_window(window_payload(clock.now(), scope_type="template", scope_value="solver-a"), "administrator")
    clock.advance(minutes=2)
    maintenance.advance_window(1)
    assert service.claim("worker-1", ["solver-a", "solver-b"], 60)["id"] == other["id"]
    assert service.claim("worker-2", ["solver-a"], 60) is None
    view = maintenance.recover_window(1, "administrator", "升级提前完成")
    assert view["state"] == "recovered"
    assert service.claim("worker-3", ["solver-a"], 60)["id"] == matched["id"]
    assert service.get_task(matched["id"])["interventions"] == []


def test_window_validation_rejects_bad_timeline_and_unknown_scope():
    clock = FrozenClock(datetime(2026, 9, 26, 0, 0, tzinfo=UTC))
    service, maintenance = build(clock)
    base = clock.now()
    payload = window_payload(base, drain_at=base + timedelta(minutes=20), deadline_at=base + timedelta(minutes=10))
    try:
        maintenance.create_window(payload, "administrator")
    except Exception as exc:
        assert exc.code == "validation_error"
    else:
        raise AssertionError("时间线非法时应当拒绝创建")
    payload = window_payload(base, code="bad-algo", scope_type="algorithm", scope_value="missing-algorithm")
    try:
        maintenance.create_window(payload, "administrator")
    except Exception as exc:
        assert exc.code == "validation_error"
    else:
        raise AssertionError("不存在的算法应当拒绝创建")


def test_window_api_lifecycle_and_block_reason(client):
    from app.core.clock import to_storage

    now = datetime.now(UTC)
    for template in (TEMPLATE_A, TEMPLATE_B):
        response = client.post("/api/compute/templates?actor=administrator", json=template)
        assert response.status_code == 201, response.text
    created = client.post("/api/compute/maintenance/windows?actor=administrator", json=window_payload(now, _json=True))
    assert created.status_code == 201
    window_id = created.json()["id"]
    submitted = client.post("/api/compute/tasks", json={
        "template_code": "solver-a", "project_code": "project-a", "requested_by": "researcher-1",
        "parameters": {"iterations": 100}, "priority": 50, "idempotency_key": "api-window-001",
    })
    assert submitted.status_code == 202
    other = client.post("/api/compute/tasks", json={
        "template_code": "solver-b", "project_code": "project-a", "requested_by": "researcher-1",
        "parameters": {"iterations": 100}, "priority": 50, "idempotency_key": "api-window-002",
    })
    assert other.status_code == 202
    # 排空尚未开始时领取不受影响
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == submitted.json()["id"]
    # 把排空开始点推到过去、截止与恢复点留在未来，再推进窗口
    with transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE compute_maintenance_windows SET drain_at=?,deadline_at=?,recover_at=? WHERE id=?",
            (to_storage(now - timedelta(minutes=1)), to_storage(now + timedelta(hours=1)), to_storage(now + timedelta(hours=2)), window_id),
        )
    queued = client.post("/api/compute/tasks", json={
        "template_code": "solver-a", "project_code": "project-a", "requested_by": "researcher-1",
        "parameters": {"iterations": 100}, "priority": 90, "idempotency_key": "api-window-003",
    }).json()
    drained = client.post(f"/api/compute/maintenance/windows/{window_id}/advance")
    assert drained.json()["state"] == "draining"
    blocked_claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert blocked_claim.json()["task"] is None
    assert blocked_claim.json()["blocked"]["window_code"] == "upgrade-solver-a"
    gate = client.post("/api/compute/maintenance/claim-check", json={"capabilities": ["solver-a"]})
    assert gate.json()["blocked"]["task_id"] == queued["id"]
    details = client.get(f"/api/compute/maintenance/windows/{window_id}")
    body = details.json()
    assert body["blocked_reason"].startswith("求解器升级")
    assert body["progress"]["running_leases"] == 1
    assert body["progress"]["safe_to_upgrade"] is False
    assert body["progress"]["queued_blocked"] == 1
    assert {item["state"] for item in client.get("/api/compute/maintenance/windows").json()["items"]} == {"draining"}
