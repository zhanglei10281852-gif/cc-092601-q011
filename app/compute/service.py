from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import BLOCKING_WINDOW_STATES, ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


WINDOW_STATUS_LABELS = {
    "announced": "预告",
    "draining": "排空",
    "enforcing": "强制停止",
    "resumed": "已恢复",
    "cancelled": "已取消",
}

# 允许的状态推进方向；目标状态与当前状态相同时为幂等空操作。
WINDOW_TRANSITIONS = {
    "announced": {"draining", "enforcing", "cancelled"},
    "draining": {"enforcing", "resumed", "cancelled"},
    "enforcing": {"resumed"},
    "resumed": set(),
    "cancelled": set(),
}

# 策略严格程度：取消比重新排队更严格，窗口重叠时取数值更大者。
WINDOW_POLICY_RANK = {"requeue": 1, "cancel": 2}

# 推进到各状态时写入的时间戳列。
WINDOW_TIMESTAMP_COLUMNS = {
    "draining": "draining_at",
    "enforcing": "enforced_at",
    "resumed": "resumed_at",
    "cancelled": "cancelled_at",
}


def window_batch_key(window_id: int) -> str:
    return f"maintenance-window:{window_id}"


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["blocking"] = None
        if result["status"] == "queued":
            window = self.repository.blocking_window_for_task(task_id)
            if window is not None:
                label = WINDOW_STATUS_LABELS[window["status"]]
                result["blocking"] = {
                    "window_id": window["id"],
                    "code": window["code"],
                    "name": window["name"],
                    "status": window["status"],
                    "status_label": label,
                    "reason": window["reason"],
                    "message": f"维护窗口 {window['code']}（{window['name']}）处于{label}阶段，匹配任务暂停新领取",
                }
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def create_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        deadline = to_storage(payload["drain_deadline"])
        if deadline <= now:
            raise ValidationError("维护窗口截止时间必须晚于当前时间")
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.window_by_code(payload["code"]):
                raise ConflictError("维护窗口编码已存在")
            if payload["scope_type"] == "template" and repository.template_by_code(payload["scope_value"]) is None:
                raise NotFoundError("参数模板不存在")
            window = repository.create_window(
                code=payload["code"], name=payload["name"], scope_type=payload["scope_type"],
                scope_value=payload["scope_value"], drain_policy=payload["drain_policy"],
                drain_deadline=deadline, reason=payload["reason"], created_by=actor, now=now,
            )
            return self._window_view(repository, window, include_tasks=True)

    def list_windows(self, *, status: str | None = None) -> list[dict[str, Any]]:
        return [self._window_view(self.repository, row, include_tasks=False) for row in self.repository.list_windows(status=status)]

    def window_detail(self, window_id: int) -> dict[str, Any]:
        window = self.repository.window_by_id(window_id)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return self._window_view(self.repository, dict(window), include_tasks=True)

    def advance_window(self, window_id: int, actor: str, target: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            window = repository.window_by_id(window_id)
            if window is None:
                raise NotFoundError("维护窗口不存在")
            if window["status"] == target:
                # 幂等推进：重复推进到当前状态不产生状态变更和二次干预记录。
                return self._window_view(repository, dict(window), include_tasks=True)
            self._transition_window(connection, repository, dict(window), actor, target, now)
            return self._window_view(repository, dict(repository.window_by_id(window_id)), include_tasks=True)

    def process_due_windows(self, actor: str = "maintenance-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        enforced: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            for window in repository.due_draining_windows(now):
                self._transition_window(connection, repository, dict(window), actor, "enforcing", now)
                enforced.append(int(window["id"]))
        return {"enforced": enforced}

    def _transition_window(self, connection: sqlite3.Connection, repository: ComputeRepository, window: dict[str, Any], actor: str, target: str, now: str) -> None:
        current = window["status"]
        if target not in WINDOW_TRANSITIONS.get(current, set()):
            raise ConflictError(f"维护窗口不能从{WINDOW_STATUS_LABELS[current]}推进到{WINDOW_STATUS_LABELS[target]}")
        column = WINDOW_TIMESTAMP_COLUMNS[target]
        cursor = connection.execute(
            f"UPDATE compute_maintenance_windows SET status=?,{column}=?,updated_at=?,version=version+1 WHERE id=? AND status=?",
            (target, now, now, window["id"], current),
        )
        if cursor.rowcount != 1:
            raise ConflictError("维护窗口状态已变化，请刷新后重试")
        if target == "enforcing":
            self._enforce_window(connection, repository, dict(repository.window_by_id(window["id"])), actor, now)

    def _enforce_window(self, connection: sqlite3.Connection, repository: ComputeRepository, window: dict[str, Any], actor: str, now: str) -> None:
        batch_key = window_batch_key(window["id"])
        for task in repository.tasks_matching_window(window, ("running", "cancel_requested")):
            if repository.intervention_exists(task["id"], batch_key):
                continue
            policy, stricter_sources = self._effective_policy(repository, window, task)
            before = dict(task)
            status = "cancelled" if policy == "cancel" else "queued"
            message = f"维护窗口 {window['code']} 强制{'停止' if policy == 'cancel' else '重新排队'}"
            connection.execute(
                "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='maintenance_window',last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, now, message, now if policy == "cancel" else None, now, task["id"]),
            )
            after = dict(repository.task_by_id(task["id"]))
            reason = f"维护窗口 {window['code']}（{window['name']}）强制停止：{window['reason']}"
            if stricter_sources and policy != window["drain_policy"]:
                reason += f"；与窗口 {','.join(stricter_sources)} 重叠，采用更严格策略"
            repository.add_intervention(task_id=task["id"], actor=actor, action=f"maintenance_{policy}", reason=reason, before=before, after=after, batch_key=batch_key, now=now)

    @staticmethod
    def _effective_policy(repository: ComputeRepository, window: dict[str, Any], task: dict[str, Any]) -> tuple[str, list[str]]:
        overlapping = repository.enforcing_windows_matching_task(task)
        strictest = max(WINDOW_POLICY_RANK[item["drain_policy"]] for item in overlapping)
        policy = "cancel" if strictest >= WINDOW_POLICY_RANK["cancel"] else "requeue"
        sources = sorted(item["code"] for item in overlapping if item["id"] != window["id"] and WINDOW_POLICY_RANK[item["drain_policy"]] == strictest)
        return policy, sources

    def _window_view(self, repository: ComputeRepository, window: dict[str, Any], *, include_tasks: bool) -> dict[str, Any]:
        data = dict(window)
        label = WINDOW_STATUS_LABELS[window["status"]]
        data["status_label"] = label
        blocking = window["status"] in BLOCKING_WINDOW_STATES
        data["blocking"] = {
            "active": blocking,
            "reason": window["reason"] if blocking else "",
            "message": f"维护窗口 {window['code']}（{window['name']}）处于{label}阶段，匹配任务暂停新领取" if blocking else "",
        }
        affected = repository.tasks_matching_window(window, ("queued", "running", "cancel_requested"))
        counts = repository.intervention_counts(window_batch_key(window["id"]))
        leased = sum(1 for task in affected if task["status"] in {"running", "cancel_requested"})
        data["progress"] = {
            "matched_active": len(affected),
            "queued": sum(1 for task in affected if task["status"] == "queued"),
            "running": sum(1 for task in affected if task["status"] == "running"),
            "cancel_requested": sum(1 for task in affected if task["status"] == "cancel_requested"),
            "leased": leased,
            "drained": leased == 0,
            "enforced_cancelled": counts.get("maintenance_cancel", 0),
            "enforced_requeued": counts.get("maintenance_requeue", 0),
        }
        if include_tasks:
            data["affected_tasks"] = affected
        return data

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
