from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

# 任务与维护窗口范围匹配的关联条件，供各类查询复用。
WINDOW_MATCH_CONDITION = (
    "(w.scope_type='algorithm' AND w.scope_value=tpl.algorithm)"
    " OR (w.scope_type='template' AND w.scope_value=tpl.code)"
    " OR (w.scope_type='project' AND w.scope_value=t.project_code)"
)

# 处于这些状态的窗口会阻止匹配任务被新领取。
BLOCKING_WINDOW_STATES = ("draining", "enforcing")

_SCOPE_CLAUSES = {
    "algorithm": "tpl.algorithm=?",
    "template": "tpl.code=?",
    "project": "t.project_code=?",
}


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        blocking = ",".join("?" for _ in BLOCKING_WINDOW_STATES)
        params.extend(BLOCKING_WINDOW_STATES)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?"
            + condition
            + f" AND NOT EXISTS (SELECT 1 FROM compute_maintenance_windows w WHERE w.status IN ({blocking}) AND ({WINDOW_MATCH_CONDITION}))"
            + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE id=?", (window_id,)).fetchone()

    def window_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE code=?", (code,)).fetchone()

    def create_window(self, *, code: str, name: str, scope_type: str, scope_value: str, drain_policy: str, drain_deadline: str, reason: str, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_maintenance_windows(code,name,scope_type,scope_value,status,drain_policy,drain_deadline,reason,created_by,announced_at,created_at,updated_at) VALUES(?,?,?,?,'announced',?,?,?,?,?,?,?)",
            (code, name, scope_type, scope_value, drain_policy, drain_deadline, reason, created_by, now, now, now),
        )
        return dict(self.window_by_id(cursor.lastrowid))

    def list_windows(self, *, status: str | None) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE status=? ORDER BY drain_deadline,id", (status,)).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM compute_maintenance_windows ORDER BY drain_deadline,id").fetchall()
        return [dict(row) for row in rows]

    def due_draining_windows(self, now: str) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE status='draining' AND drain_deadline<=? ORDER BY drain_deadline,id", (now,)).fetchall()

    def tasks_matching_window(self, window: sqlite3.Row | dict[str, Any], statuses: tuple[str, ...]) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = list(statuses)
        params.append(window["scope_value"])
        rows = self.connection.execute(
            f"SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status IN ({placeholders}) AND {_SCOPE_CLAUSES[window['scope_type']]} ORDER BY t.priority DESC,t.created_at ASC,t.id ASC",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def blocking_window_for_task(self, task_id: int) -> sqlite3.Row | None:
        blocking = ",".join("?" for _ in BLOCKING_WINDOW_STATES)
        # 参数顺序需与 SQL 中 ? 的出现顺序一致：JOIN 先于 WHERE。
        params: list[Any] = [task_id, *BLOCKING_WINDOW_STATES]
        return self.connection.execute(
            f"SELECT w.* FROM compute_maintenance_windows w JOIN compute_tasks t ON t.id=? JOIN compute_templates tpl ON tpl.id=t.template_id WHERE w.status IN ({blocking}) AND ({WINDOW_MATCH_CONDITION}) ORDER BY CASE w.status WHEN 'enforcing' THEN 0 ELSE 1 END,w.drain_deadline,w.id LIMIT 1",
            params,
        ).fetchone()

    def enforcing_windows_matching_task(self, task: sqlite3.Row | dict[str, Any]) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM compute_maintenance_windows WHERE status='enforcing' AND ((scope_type='algorithm' AND scope_value=?) OR (scope_type='template' AND scope_value=?) OR (scope_type='project' AND scope_value=?)) ORDER BY id",
            (task["template_algorithm"], task["template_code"], task["project_code"]),
        ).fetchall()
        return [dict(row) for row in rows]

    def intervention_exists(self, task_id: int, batch_key: str) -> bool:
        return self.connection.execute("SELECT 1 FROM compute_interventions WHERE task_id=? AND batch_key=? LIMIT 1", (task_id, batch_key)).fetchone() is not None

    def intervention_counts(self, batch_key: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT action,COUNT(*) AS amount FROM compute_interventions WHERE batch_key=? GROUP BY action", (batch_key,)).fetchall()
        return {str(row["action"]): int(row["amount"]) for row in rows}
