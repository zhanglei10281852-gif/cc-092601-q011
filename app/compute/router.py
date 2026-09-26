from __future__ import annotations

from fastapi import APIRouter, Query

from app.compute.maintenance import MaintenanceWindowService
from app.compute.schemas import (
    BatchOperation,
    CancelRequest,
    MaintenanceClaimCheck,
    MaintenanceWindowCancel,
    MaintenanceWindowCreate,
    PriorityRequest,
    QuotaSet,
    RetryRequest,
    TaskClaim,
    TaskFailure,
    TaskResult,
    TaskSubmit,
    TemplateCreate,
)
from app.compute.service import ComputeOperationsService

router = APIRouter(prefix="/api/compute", tags=["科学计算任务运营"])


def service() -> ComputeOperationsService:
    return ComputeOperationsService()


def maintenance_service() -> MaintenanceWindowService:
    return MaintenanceWindowService()


@router.get("/templates")
def list_templates():
    return {"items": service().list_templates()}


@router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, actor: str = Query(..., min_length=1)):
    return service().create_template(payload.model_dump(), actor)


@router.put("/quotas")
def set_quota(payload: QuotaSet, actor: str = Query(..., min_length=1)):
    return service().set_quota(payload.model_dump(), actor)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=limit)}


@router.get("/task-details/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    operations = service()
    task = operations.claim(payload.worker_id, payload.capabilities, payload.lease_seconds)
    response: dict = {"task": task}
    if task is None:
        # 没有领到任务时，告知运营侧是否因维护窗口排空而被拦截
        response["blocked"] = MaintenanceWindowService(operations.connection, operations.clock).claim_gate(payload.capabilities)
    return response


@router.post("/maintenance/claim-check")
def maintenance_claim_check(payload: MaintenanceClaimCheck):
    return {"blocked": maintenance_service().claim_gate(payload.capabilities)}


@router.get("/maintenance/windows")
def list_maintenance_windows(state: str | None = None, scope_type: str | None = None, scope_value: str | None = None):
    return {"items": maintenance_service().list_windows(state=state, scope_type=scope_type, scope_value=scope_value)}


@router.post("/maintenance/windows", status_code=201)
def create_maintenance_window(payload: MaintenanceWindowCreate, actor: str = Query(..., min_length=1)):
    return maintenance_service().create_window(payload.model_dump(), actor)


@router.get("/maintenance/windows/{window_id}")
def get_maintenance_window(window_id: int):
    return maintenance_service().get_window(window_id)


@router.post("/maintenance/windows/{window_id}/advance")
def advance_maintenance_window(window_id: int, actor: str = Query(default="maintenance-scheduler", min_length=1)):
    return maintenance_service().advance_window(window_id, actor)


@router.post("/maintenance/windows/{window_id}/cancel")
def cancel_maintenance_window(window_id: int, payload: MaintenanceWindowCancel):
    return maintenance_service().cancel_window(window_id, payload.actor, payload.reason)


@router.post("/maintenance/windows/{window_id}/recover")
def recover_maintenance_window(window_id: int, payload: MaintenanceWindowCancel):
    return maintenance_service().recover_window(window_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskClaim):
    return service().heartbeat(task_id, payload.worker_id, payload.lease_seconds)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    return service().complete(task_id, payload.worker_id, payload.result, payload.metrics)


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: CancelRequest):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, payload: RetryRequest):
    return service().retry(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/{task_id}/priority")
def set_priority(task_id: int, payload: PriorityRequest):
    return service().set_priority(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/batch")
def batch_operation(payload: BatchOperation):
    return service().batch_operation(payload.model_dump())


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.get("/summary")
def summary():
    return service().summary()
