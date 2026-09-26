from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

WINDOW_SCOPE_ALGORITHM = "algorithm"
WINDOW_SCOPE_TEMPLATE = "template"
WINDOW_SCOPE_PROJECT = "project"
WINDOW_SCOPE_TYPES = (WINDOW_SCOPE_ALGORITHM, WINDOW_SCOPE_TEMPLATE, WINDOW_SCOPE_PROJECT)


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
        rows = self.queued_heads(capabilities, now, blocked=False, limit=1)
        return rows[0] if rows else None

    def blocked_queued_head(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        rows = self.queued_heads(capabilities, now, blocked=True, limit=1)
        return rows[0] if rows else None

    def queued_heads(self, capabilities: Iterable[str], now: str, *, blocked: bool, limit: int) -> list[sqlite3.Row]:
        """返回队首任务；blocked=True 时只返回被维护窗口拦截的任务，否则跳过被拦截任务。"""
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        capability_condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            capability_condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        window_join = (
            "JOIN compute_maintenance_windows w ON w.state IN ('draining','enforcing') AND ("
            "(w.scope_type='algorithm' AND w.scope_value=tpl.algorithm)"
            " OR (w.scope_type='template' AND w.scope_value=tpl.code)"
            " OR (w.scope_type='project' AND w.scope_value=t.project_code))"
        )
        strictness = "CASE w.state WHEN 'enforcing' THEN 0 ELSE 1 END,w.drain_at,w.id"
        if blocked:
            sql = (
                "SELECT t.*,tpl.algorithm AS template_algorithm,tpl.code AS template_code,w.id AS window_id,w.code AS window_code,w.name AS window_name,w.state AS window_state,w.block_reason AS window_reason,w.deadline_at AS window_deadline_at "
                "FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id " + window_join
                + " WHERE t.status='queued' AND t.available_at<=?" + capability_condition
                + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC," + strictness + " LIMIT ?"
            )
        else:
            sql = (
                "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id "
                "WHERE t.status='queued' AND t.available_at<=?" + capability_condition
                + " AND NOT EXISTS (SELECT 1 FROM compute_maintenance_windows w WHERE w.state IN ('draining','enforcing') AND ("
                "(w.scope_type='algorithm' AND w.scope_value=tpl.algorithm)"
                " OR (w.scope_type='template' AND w.scope_value=tpl.code)"
                " OR (w.scope_type='project' AND w.scope_value=t.project_code))) "
                "ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT ?"
            )
        params.append(limit)
        return list(self.connection.execute(sql, params).fetchall())

    def matching_tasks(self, scope_type: str, scope_value: str, *, statuses: Iterable[str] | None = None) -> list[sqlite3.Row]:
        if scope_type == WINDOW_SCOPE_ALGORITHM:
            scope_condition = "tpl.algorithm=?"
        elif scope_type == WINDOW_SCOPE_TEMPLATE:
            scope_condition = "tpl.code=?"
        else:
            scope_condition = "t.project_code=?"
        sql = (
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm "
            "FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE " + scope_condition
        )
        params: list[Any] = [scope_value]
        statuses = list(statuses) if statuses is not None else None
        if statuses:
            sql += " AND t.status IN (" + ",".join("?" for _ in statuses) + ")"
            params.extend(statuses)
        sql += " ORDER BY t.id"
        return list(self.connection.execute(sql, params).fetchall())

    # ---- 维护窗口 ----

    def create_window(self, *, code: str, name: str, scope_type: str, scope_value: str, drain_at: str, deadline_at: str, recover_at: str, deadline_policy: str, block_reason: str, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_maintenance_windows(code,name,scope_type,scope_value,drain_at,deadline_at,recover_at,deadline_policy,state,block_reason,created_by,created_at,updated_at,version) VALUES(?,?,?,?,?,?,?,?,'announced',?,?,?,?,1)",
            (code, name, scope_type, scope_value, drain_at, deadline_at, recover_at, deadline_policy, block_reason, created_by, now, now),
        )
        return dict(self.window_by_id(cursor.lastrowid))

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE id=?", (window_id,)).fetchone()

    def window_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE code=?", (code,)).fetchone()

    def list_windows(self, *, state: str | None = None, scope_type: str | None = None, scope_value: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if state:
            clauses.append("state=?")
            values.append(state)
        if scope_type:
            clauses.append("scope_type=?")
            values.append(scope_type)
        if scope_value:
            clauses.append("scope_value=?")
            values.append(scope_value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute("SELECT * FROM compute_maintenance_windows" + where + " ORDER BY created_at DESC,id DESC", values).fetchall()
        return [dict(row) for row in rows]

    def touch_window(self, window_id: int, now: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ",".join(f"{key}=?" for key in fields)
        values = [*fields.values(), now, window_id]
        self.connection.execute(
            f"UPDATE compute_maintenance_windows SET {assignments},updated_at=?,version=version+1 WHERE id=?",
            values,
        )

    def window_task_outcome(self, window_id: int, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_maintenance_window_tasks WHERE window_id=? AND task_id=?",
            (window_id, task_id),
        ).fetchone()

    def record_window_block(self, window_id: int, task_id: int, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_maintenance_window_tasks(window_id,task_id,first_blocked_at,last_blocked_at,block_count,outcome) VALUES(?,?,?,?,1,'blocked') "
            "ON CONFLICT(window_id,task_id) DO UPDATE SET last_blocked_at=excluded.last_blocked_at,block_count=compute_maintenance_window_tasks.block_count+1 "
            "WHERE compute_maintenance_window_tasks.outcome='blocked'",
            (window_id, task_id, now, now),
        )

    def mark_window_task(self, window_id: int, task_id: int, outcome: str, now: str) -> None:
        # 截止时直接干预的任务可能从未被领取拦截（例如截止时仍持租约的运行任务），
        # 此时阻塞时间留空；若已有拦截记录，冲突更新保留原拦截时间与次数。
        self.connection.execute(
            "INSERT INTO compute_maintenance_window_tasks(window_id,task_id,first_blocked_at,last_blocked_at,block_count,outcome,resolved_at) VALUES(?,?,NULL,NULL,0,?,?) "
            "ON CONFLICT(window_id,task_id) DO UPDATE SET outcome=excluded.outcome,resolved_at=excluded.resolved_at",
            (window_id, task_id, outcome, now),
        )

    def window_tasks(self, window_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT wt.*,t.status AS task_status,t.project_code AS task_project_code,tpl.code AS task_template_code,tpl.algorithm AS task_algorithm "
            "FROM compute_maintenance_window_tasks wt "
            "JOIN compute_tasks t ON t.id=wt.task_id JOIN compute_templates tpl ON tpl.id=t.template_id "
            "WHERE wt.window_id=? ORDER BY wt.task_id",
            (window_id,),
        ).fetchall()]

    def stricter_cancel_window(self, exclude_window_id: int, task: sqlite3.Row, now: str) -> sqlite3.Row | None:
        """重叠窗口中是否存在同样到点、且策略为取消的更严格窗口（与窗口推进先后无关）。"""
        return self.connection.execute(
            "SELECT w.* FROM compute_maintenance_windows w WHERE w.id<>? AND w.state IN ('announced','draining','enforcing') "
            "AND w.deadline_at<=? AND w.deadline_policy='cancel' AND ("
            "(w.scope_type='algorithm' AND w.scope_value=?) OR (w.scope_type='template' AND w.scope_value=?) "
            "OR (w.scope_type='project' AND w.scope_value=?)) "
            "ORDER BY CASE w.scope_type WHEN 'algorithm' THEN 0 WHEN 'template' THEN 1 ELSE 2 END,w.id LIMIT 1",
            (exclude_window_id, now, task["template_algorithm"], task["template_code"], task["project_code"]),
        ).fetchone()

    def intervention_counts(self, batch_key: str) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT action,COUNT(*) AS amount FROM compute_interventions WHERE batch_key=? GROUP BY action",
            (batch_key,),
        ).fetchall()
        return {str(row["action"]): int(row["amount"]) for row in rows}


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
