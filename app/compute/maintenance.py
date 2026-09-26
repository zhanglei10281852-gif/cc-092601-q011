from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.compute.repository import (
    WINDOW_SCOPE_ALGORITHM,
    WINDOW_SCOPE_PROJECT,
    WINDOW_SCOPE_TEMPLATE,
    WINDOW_SCOPE_TYPES,
    ComputeRepository,
)
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction

ACTIVE_STATES = ("draining", "enforcing")
TERMINAL_TASK_STATUSES = ("succeeded", "failed", "cancelled")


def window_batch_key(code: str) -> str:
    """窗口截止干预的确定性批次键，重复推进时据此识别已经发生过的干预。"""
    return f"maintenance-window:{code}:deadline"


class MaintenanceWindowService:
    """管理按算法、模板或项目定义的求解器维护窗口。

    状态流转：announced（预告）→ draining（排空）→ enforcing（强制停止）
    → recovered（恢复）；announced/draining 阶段可由操作者取消（cancelled）。
    所有流转都通过带条件的状态更新完成，重复推进不会产生二次任务干预。
    """

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()

    # ---- 查询 ----

    def list_windows(self, *, state: str | None = None, scope_type: str | None = None, scope_value: str | None = None) -> list[dict[str, Any]]:
        repository = ComputeRepository(self.connection)
        return [self._view(repository, dict(row)) for row in repository.list_windows(state=state, scope_type=scope_type, scope_value=scope_value)]

    def get_window(self, window_id: int) -> dict[str, Any]:
        repository = ComputeRepository(self.connection)
        window = repository.window_by_id(window_id)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return self._view(repository, dict(window))

    def claim_gate(self, capabilities: list[str]) -> dict[str, Any] | None:
        """返回当前能力下队首任务被拦截的阻塞原因，没有拦截时返回 None。"""
        now = to_storage(self.clock.now())
        blocked = ComputeRepository(self.connection).blocked_queued_head(capabilities, now)
        if blocked is None:
            return None
        return {
            "task_id": blocked["id"],
            "window_id": blocked["window_id"],
            "window_code": blocked["window_code"],
            "window_name": blocked["window_name"],
            "window_state": blocked["window_state"],
            "reason": blocked["window_reason"],
            "deadline_at": blocked["window_deadline_at"],
        }

    # ---- 写操作 ----

    def create_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        scope_type = payload["scope_type"]
        if scope_type not in WINDOW_SCOPE_TYPES:
            raise ValidationError("维护窗口范围类型不合法")
        drain_at_value = payload["drain_at"]
        deadline_value = payload["deadline_at"]
        recover_value = payload["recover_at"]
        if not (drain_at_value <= deadline_value <= recover_value):
            raise ValidationError("维护窗口时间需要满足 排空开始 <= 截止时间 <= 恢复时间")
        now = to_storage(self.clock.now())
        drain_at = to_storage(drain_at_value)
        deadline_at = to_storage(deadline_value)
        recover_at = to_storage(recover_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.window_by_code(payload["code"]) is not None:
                raise ConflictError("维护窗口编码已存在")
            self._validate_scope(repository, scope_type, payload["scope_value"])
            window = repository.create_window(
                code=payload["code"], name=payload["name"], scope_type=scope_type,
                scope_value=payload["scope_value"], drain_at=drain_at, deadline_at=deadline_at,
                recover_at=recover_at, deadline_policy=payload["deadline_policy"],
                block_reason=payload["block_reason"], created_by=actor, now=now,
            )
            return self._view(repository, window)

    def advance_window(self, window_id: int, actor: str = "maintenance-scheduler") -> dict[str, Any]:
        """按当前时钟推进窗口：到点进入排空、截止执行策略、到点恢复。

        可被调度器或运营接口重复调用；只有状态真正翻转的那一次会写入干预记录。
        """
        del actor  # 窗口流转本身不产生任务级干预，操作者仅体现在显式取消/恢复上
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            window = repository.window_by_id(window_id)
            if window is None:
                raise NotFoundError("维护窗口不存在")
            window = self._enter_draining(repository, window, now)
            if window["state"] == "draining":
                window = self._enter_enforcing(repository, window, now)
            if window["state"] == "enforcing":
                window = self._enter_recovered(repository, window, now)
            return self._view(repository, dict(window))

    def cancel_window(self, window_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            window = repository.window_by_id(window_id)
            if window is None:
                raise NotFoundError("维护窗口不存在")
            if window["state"] not in ("announced", "draining"):
                raise ConflictError("只有预告或排空阶段的维护窗口可以取消")
            cursor = connection.execute(
                "UPDATE compute_maintenance_windows SET state='cancelled',cancel_reason=?,cancelled_by=?,cancelled_at=?,updated_at=?,version=version+1 WHERE id=? AND state IN ('announced','draining')",
                (reason, actor, now, now, window_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("维护窗口状态已变化，取消失败")
            return self._view(repository, dict(repository.window_by_id(window_id)))

    def recover_window(self, window_id: int, actor: str, reason: str) -> dict[str, Any]:
        """操作者确认升级提前结束，手动恢复领取；排空阶段恢复意味着跳过强制停止。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            window = repository.window_by_id(window_id)
            if window is None:
                raise NotFoundError("维护窗口不存在")
            if window["state"] not in ("draining", "enforcing"):
                raise ConflictError("只有排空或强制停止阶段的维护窗口可以恢复")
            cursor = connection.execute(
                "UPDATE compute_maintenance_windows SET state='recovered',recovered_at=?,recovered_by=?,updated_at=?,version=version+1 WHERE id=? AND state IN ('draining','enforcing')",
                (now, actor, now, window_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("维护窗口状态已变化，恢复失败")
            return self._view(repository, dict(repository.window_by_id(window_id)))

    # ---- 内部流转 ----

    def _enter_draining(self, repository: ComputeRepository, window: sqlite3.Row, now: str) -> sqlite3.Row:
        if window["state"] != "announced" or now < window["drain_at"]:
            return window
        cursor = repository.connection.execute(
            "UPDATE compute_maintenance_windows SET state='draining',drained_at=?,updated_at=?,version=version+1 WHERE id=? AND state='announced'",
            (now, now, window["id"]),
        )
        if cursor.rowcount != 1:
            return repository.window_by_id(window["id"])
        # 记录排空开始时受影响的任务快照，用于计算自然排空进度
        tasks = repository.matching_tasks(window["scope_type"], window["scope_value"], statuses=("queued", "running"))
        snapshot = [int(task["id"]) for task in tasks]
        repository.touch_window(window["id"], now, affected_task_ids_json=json.dumps(snapshot, ensure_ascii=False))
        return repository.window_by_id(window["id"])

    def _enter_enforcing(self, repository: ComputeRepository, window: sqlite3.Row, now: str) -> sqlite3.Row:
        if now < window["deadline_at"]:
            return window
        batch_key = window_batch_key(window["code"])
        cursor = repository.connection.execute(
            "UPDATE compute_maintenance_windows SET state='enforcing',enforced_at=?,enforced_batch_key=?,updated_at=?,version=version+1 WHERE id=? AND state='draining'",
            (now, batch_key, now, window["id"]),
        )
        if cursor.rowcount != 1:
            return repository.window_by_id(window["id"])
        window = repository.window_by_id(window["id"])
        self._enforce_deadline(repository, window, now, batch_key)
        return repository.window_by_id(window["id"])

    def _enforce_deadline(self, repository: ComputeRepository, window: sqlite3.Row, now: str, batch_key: str) -> None:
        """截止时刻对匹配任务执行取消或重新排队，整个过程只在状态翻转时执行一次。

        重叠窗口采用更严格规则：若任一同样到点的重叠窗口策略为取消，则取消；
        干预记录归属更严格的窗口（确定性批次键），保证两个窗口各自重复推进时
        都只产生同一条干预，不会二次干预。
        """
        base_reason = window["block_reason"] or f"维护窗口 {window['code']} 截止"
        tasks = repository.matching_tasks(window["scope_type"], window["scope_value"], statuses=("queued", "running"))
        for row in tasks:
            task = dict(row)
            owner = window
            owner_batch = batch_key
            reason = base_reason
            stricter = repository.stricter_cancel_window(window["id"], row, now)
            if window["deadline_policy"] == "requeue" and stricter is not None:
                owner = stricter
                owner_batch = window_batch_key(stricter["code"])
                reason = f"{stricter['block_reason'] or '重叠维护窗口要求取消'}（覆盖窗口 {window['code']} 的重新排队策略）"
            if owner["deadline_policy"] == "cancel":
                repository.connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',finished_at=?,lease_owner='',lease_expires_at='',last_error_code='maintenance_window',last_error_message=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, reason[:2000], now, task["id"]),
                )
                action = "maintenance_cancel"
                outcome = "cancelled"
            else:
                # 排队中的任务在 requeue 策略下保持排队，恢复后按原优先级继续竞争
                if task["status"] != "running":
                    continue
                repository.connection.execute(
                    "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',available_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task["id"]),
                )
                action = "maintenance_requeue"
                outcome = "requeued"
            after = dict(repository.task_by_id(task["id"]))
            repository.add_intervention(
                task_id=task["id"], actor=f"maintenance-window:{owner['code']}", action=action,
                reason=reason, before=task, after=after, batch_key=owner_batch, now=now,
            )
            repository.mark_window_task(window["id"], task["id"], outcome, now)
            if int(owner["id"]) != int(window["id"]):
                repository.mark_window_task(int(owner["id"]), task["id"], outcome, now)

    def _enter_recovered(self, repository: ComputeRepository, window: sqlite3.Row, now: str) -> sqlite3.Row:
        if now < window["recover_at"]:
            return window
        cursor = repository.connection.execute(
            "UPDATE compute_maintenance_windows SET state='recovered',recovered_at=?,updated_at=?,version=version+1 WHERE id=? AND state='enforcing'",
            (now, now, window["id"]),
        )
        if cursor.rowcount != 1:
            return repository.window_by_id(window["id"])
        return repository.window_by_id(window["id"])

    # ---- 视图 ----

    def _view(self, repository: ComputeRepository, window: dict[str, Any]) -> dict[str, Any]:
        snapshot = [int(value) for value in json.loads(window.get("affected_task_ids_json") or "[]")]
        window_tasks = {item["task_id"]: item for item in repository.window_tasks(window["id"])}
        task_ids = list(dict.fromkeys([*snapshot, *sorted(window_tasks)]))
        affected: list[dict[str, Any]] = []
        natural_completed = 0
        for task_id in task_ids:
            task = repository.task_by_id(task_id)
            tracked = window_tasks.get(task_id)
            status = None if task is None else task["status"]
            outcome = None if tracked is None else tracked["outcome"]
            entry = {
                "task_id": task_id,
                "status": status,
                "project_code": None if task is None else task["project_code"],
                "template_code": None if task is None else task["template_code"],
                "algorithm": None if task is None else task["template_algorithm"],
                "outcome": outcome,
                "block_count": 0 if tracked is None else tracked["block_count"],
                "first_blocked_at": None if tracked is None else tracked["first_blocked_at"],
                "last_blocked_at": None if tracked is None else tracked["last_blocked_at"],
                "resolved_at": None if tracked is None else tracked["resolved_at"],
            }
            if task is not None and task_id in snapshot and status in TERMINAL_TASK_STATUSES and outcome != "cancelled":
                natural_completed += 1
            affected.append(entry)
        live_matching = repository.matching_tasks(window["scope_type"], window["scope_value"])
        active = window["state"] in ACTIVE_STATES
        queued_blocked = sum(1 for task in live_matching if task["status"] == "queued") if active else 0
        running_leases = sum(1 for task in live_matching if task["status"] == "running") if active else 0
        enforcement = repository.intervention_counts(window_batch_key(window["code"]))
        cancelled_count = enforcement.get("maintenance_cancel", 0)
        requeued_count = enforcement.get("maintenance_requeue", 0)
        view = {key: window[key] for key in window}
        view["affected_task_ids"] = snapshot
        view.pop("affected_task_ids_json", None)
        view["affected_tasks"] = affected
        view["blocked_reason"] = window["block_reason"] if active else ""
        view["progress"] = {
            "phase": window["state"],
            "snapshot_task_count": len(snapshot),
            "natural_completed": natural_completed,
            "queued_blocked": queued_blocked,
            "running_leases": running_leases,
            "enforced_task_count": cancelled_count + requeued_count,
            "cancelled_task_count": cancelled_count,
            "requeued_task_count": requeued_count,
            "remaining": running_leases,
            "safe_to_upgrade": window["state"] in ("recovered", "cancelled") or running_leases == 0,
        }
        return view

    @staticmethod
    def _validate_scope(repository: ComputeRepository, scope_type: str, scope_value: str) -> None:
        if scope_type == WINDOW_SCOPE_ALGORITHM:
            row = repository.connection.execute("SELECT 1 FROM compute_templates WHERE algorithm=? LIMIT 1", (scope_value,)).fetchone()
            if row is None:
                raise ValidationError("指定算法尚未被任何参数模板使用")
        elif scope_type == WINDOW_SCOPE_TEMPLATE:
            if repository.template_by_code(scope_value) is None:
                raise ValidationError("指定参数模板不存在")
        elif scope_type == WINDOW_SCOPE_PROJECT:
            # 项目没有独立注册表，只校验非空（由 schema 层保证）
            return
