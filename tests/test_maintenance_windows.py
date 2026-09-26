from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.compute.service import ComputeOperationsService, window_batch_key
from app.core.clock import FrozenClock
from app.database import get_connection, init_db

TEMPLATE_A = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}

TEMPLATE_B = {
    "code": "solver-b",
    "name": "蒙特卡洛模板",
    "algorithm": "solver-b",
    "parameter_schema": {
        "samples": {"type": "integer", "required": True, "minimum": 10, "maximum": 100000},
    },
    "default_parameters": {},
    "max_runtime_seconds": 120,
    "max_attempts": 3,
}


def future_deadline(**kwargs: int) -> str:
    return (datetime.now(UTC) + timedelta(**kwargs)).isoformat()


def submit_payload(key: str, *, template: str = "solver-a", project: str = "project-a", user: str = "researcher-1", priority: int = 50) -> dict:
    parameters = {"iterations": 100, "mode": "accurate"} if template == "solver-a" else {"samples": 100}
    return {
        "template_code": template,
        "project_code": project,
        "requested_by": user,
        "parameters": parameters,
        "priority": priority,
        "idempotency_key": key,
    }


def window_payload(code: str, *, scope_type: str = "algorithm", scope_value: str = "solver-a", policy: str = "requeue") -> dict:
    return {
        "code": code,
        "name": f"窗口-{code}",
        "scope_type": scope_type,
        "scope_value": scope_value,
        "drain_policy": policy,
        "drain_deadline": future_deadline(hours=1),
        "reason": "求解器版本升级",
    }


def create_templates(client) -> None:
    for template in (TEMPLATE_A, TEMPLATE_B):
        response = client.post("/api/compute/templates?actor=administrator", json=template)
        assert response.status_code == 201, response.text


def create_window(client, code: str, **overrides) -> dict:
    response = client.post("/api/compute/maintenance-windows?actor=operator", json=window_payload(code, **overrides))
    assert response.status_code == 201, response.text
    return response.json()


def advance(client, window_id: int, target: str) -> dict:
    response = client.post(f"/api/compute/maintenance-windows/{window_id}/advance", json={"actor": "operator", "target": target})
    assert response.status_code == 200, response.text
    return response.json()


def test_announced_window_does_not_block_claims(client):
    create_templates(client)
    task = client.post("/api/compute/tasks", json=submit_payload("announce-000001")).json()
    window = create_window(client, "upgrade-announced")
    assert window["status"] == "announced" and window["blocking"]["active"] is False
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task["id"]


def test_draining_blocks_only_matching_tasks_and_reports_progress(client):
    create_templates(client)
    task_a = client.post("/api/compute/tasks", json=submit_payload("drain-a", template="solver-a", project="project-a")).json()
    task_b = client.post("/api/compute/tasks", json=submit_payload("drain-b", template="solver-b", project="project-b")).json()
    window = advance(client, create_window(client, "upgrade-draining")["id"], "draining")
    assert window["blocking"]["active"] is True and "排空" in window["blocking"]["message"]

    # 排空阶段只阻止新领取，不阻止新提交。
    extra = client.post("/api/compute/tasks", json=submit_payload("drain-extra", template="solver-a", project="project-a"))
    assert extra.status_code == 202

    # 其他算法队列不受影响，匹配任务留在队列中。
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a", "solver-b"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == task_b["id"]
    blocked_claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert blocked_claim.status_code == 200 and blocked_claim.json()["task"] is None

    # 任务详情返回阻塞原因。
    details = client.get(f"/api/compute/task-details/{task_a['id']}").json()
    assert details["blocking"]["code"] == "upgrade-draining"
    assert details["blocking"]["status"] == "draining"
    assert "求解器版本升级" in details["blocking"]["reason"]

    # 窗口详情返回受影响任务与执行进度。
    detail = client.get(f"/api/compute/maintenance-windows/{window['id']}").json()
    assert detail["progress"]["matched_active"] == 2
    assert detail["progress"]["queued"] == 2
    assert detail["progress"]["leased"] == 0
    assert detail["progress"]["drained"] is True
    assert {item["id"] for item in detail["affected_tasks"]} == {task_a["id"], extra.json()["id"]}

    listed = client.get("/api/compute/maintenance-windows", params={"status": "draining"}).json()["items"]
    assert [item["code"] for item in listed] == ["upgrade-draining"]
    assert listed[0]["blocking"]["active"] is True


def test_enforce_requeue_is_idempotent_and_resume_keeps_priority(client):
    create_templates(client)
    low = client.post("/api/compute/tasks", json=submit_payload("enforce-low", priority=10)).json()
    high = client.post("/api/compute/tasks", json=submit_payload("enforce-high", priority=90)).json()
    for worker in ("w1", "w2"):
        claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
        assert claimed.json()["task"] is not None

    window = create_window(client, "upgrade-requeue", scope_type="project", scope_value="project-a", policy="requeue")
    advance(client, window["id"], "draining")
    enforced = advance(client, window["id"], "enforcing")
    assert enforced["status"] == "enforcing"
    assert enforced["progress"]["enforced_requeued"] == 2
    assert enforced["progress"]["drained"] is True

    for task_id in (low["id"], high["id"]):
        details = client.get(f"/api/compute/task-details/{task_id}").json()
        assert details["status"] == "queued" and details["lease_owner"] == ""
        maintenance = [item for item in details["interventions"] if item["batch_key"] == window_batch_key(window["id"])]
        assert [item["action"] for item in maintenance] == ["maintenance_requeue"]

    # 重复推进同一窗口不产生二次干预记录。
    again = advance(client, window["id"], "enforcing")
    assert again["progress"]["enforced_requeued"] == 2
    due = client.post("/api/compute/maintenance-windows/process-due").json()
    assert due["enforced"] == []
    for task_id in (low["id"], high["id"]):
        details = client.get(f"/api/compute/task-details/{task_id}").json()
        assert len([item for item in details["interventions"] if item["action"] == "maintenance_requeue"]) == 1

    # 恢复后任务按原有优先级重新竞争。
    advance(client, window["id"], "resumed")
    first = client.post("/api/compute/tasks/claim", json={"worker_id": "w3", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert first.json()["task"]["id"] == high["id"]
    assert first.json()["task"]["priority"] == 90
    second = client.post("/api/compute/tasks/claim", json={"worker_id": "w4", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert second.json()["task"]["id"] == low["id"]


def test_overlapping_windows_apply_stricter_policy(client):
    create_templates(client)
    task = client.post("/api/compute/tasks", json=submit_payload("overlap-000001")).json()
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == task["id"]

    requeue_window = create_window(client, "overlap-requeue", scope_type="algorithm", scope_value="solver-a", policy="requeue")
    cancel_window = create_window(client, "overlap-cancel", scope_type="project", scope_value="project-a", policy="cancel")
    advance(client, requeue_window["id"], "draining")
    advance(client, cancel_window["id"], "draining")

    # 更严格的取消策略先生效，任务被直接取消。
    enforced = advance(client, cancel_window["id"], "enforcing")
    assert enforced["progress"]["enforced_cancelled"] == 1
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "cancelled"
    assert [item["action"] for item in details["interventions"]] == ["maintenance_cancel"]

    # 另一个窗口随后推进时任务已不再持有租约，不会产生新的干预。
    follow = advance(client, requeue_window["id"], "enforcing")
    assert follow["progress"]["enforced_requeued"] == 0
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["interventions"]) == 1


def test_first_enforcing_window_applies_its_own_policy(client):
    create_templates(client)
    task = client.post("/api/compute/tasks", json=submit_payload("overlap-000002")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})

    requeue_window = create_window(client, "order-requeue", scope_type="algorithm", scope_value="solver-a", policy="requeue")
    cancel_window = create_window(client, "order-cancel", scope_type="project", scope_value="project-a", policy="cancel")
    advance(client, requeue_window["id"], "draining")
    advance(client, cancel_window["id"], "draining")

    # 重新排队窗口先进入强制停止时，取消策略尚未生效。
    advance(client, requeue_window["id"], "enforcing")
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "queued"
    assert [item["action"] for item in details["interventions"]] == ["maintenance_requeue"]

    follow = advance(client, cancel_window["id"], "enforcing")
    assert follow["progress"]["enforced_cancelled"] == 0
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["status"] == "queued"


def test_process_due_enforces_after_deadline(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE_A, "administrator")
    queued = service.submit(submit_payload("due-000001"))
    claimed = service.claim("worker-a", ["solver-a"], 30)
    assert claimed and claimed["id"] == queued["id"]

    window = service.create_window({**window_payload("due-window"), "drain_deadline": clock.now() + timedelta(seconds=60)}, "operator")
    service.advance_window(window["id"], "operator", "draining")
    assert service.claim("worker-b", ["solver-a"], 30) is None

    clock.advance(seconds=61)
    due = service.process_due_windows()
    assert due["enforced"] == [window["id"]]
    details = service.get_task(queued["id"])
    assert details["status"] == "queued" and details["lease_owner"] == ""
    assert [item["action"] for item in details["interventions"]] == ["maintenance_requeue"]

    # 到期处理与重复推进都是幂等的。
    assert service.process_due_windows()["enforced"] == []
    service.advance_window(window["id"], "operator", "enforcing")
    assert len(service.get_task(queued["id"])["interventions"]) == 1

    service.advance_window(window["id"], "operator", "resumed")
    reclaimed = service.claim("worker-c", ["solver-a"], 30)
    assert reclaimed and reclaimed["id"] == queued["id"]


def test_cancelled_window_reopens_queue(client):
    create_templates(client)
    task = client.post("/api/compute/tasks", json=submit_payload("cancel-window-1")).json()
    window = create_window(client, "upgrade-aborted")
    advance(client, window["id"], "draining")
    blocked = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert blocked.json()["task"] is None

    cancelled = advance(client, window["id"], "cancelled")
    assert cancelled["blocking"]["active"] is False
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == task["id"]


def test_window_validation_and_transition_errors(client):
    create_templates(client)
    create_window(client, "upgrade-validation")
    duplicate = client.post("/api/compute/maintenance-windows?actor=operator", json=window_payload("upgrade-validation"))
    assert duplicate.status_code == 409

    past = window_payload("upgrade-past")
    past["drain_deadline"] = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    assert client.post("/api/compute/maintenance-windows?actor=operator", json=past).status_code == 422

    missing_template = window_payload("upgrade-missing", scope_type="template", scope_value="no-such-template")
    assert client.post("/api/compute/maintenance-windows?actor=operator", json=missing_template).status_code == 404

    window = create_window(client, "upgrade-transitions")
    illegal = client.post(f"/api/compute/maintenance-windows/{window['id']}/advance", json={"actor": "operator", "target": "resumed"})
    assert illegal.status_code == 409
    assert client.post("/api/compute/maintenance-windows/99999/advance", json={"actor": "operator", "target": "draining"}).status_code == 404
    assert client.get("/api/compute/maintenance-windows/99999").status_code == 404
