from fastapi import APIRouter, Body, Request
from fastapi.responses import RedirectResponse, Response
from app.errors import DomainError
from . import service
from .service import execute
router = APIRouter(tags=["M6-批次导出"])

@router.get("/runs")
def page(request: Request):
    store = request.app.state.store
    execute({"store": store})
    service.ingest({}, store)  # 样例批次入库：幂等，重放不复活已撤回记录
    data = service.overview(store)
    return request.app.state.templates.TemplateResponse(request, "m6_runs.html", {"title": "批次导出", "data": data})

@router.get("/api/runs")
def runs(request: Request):
    execute({"store": request.app.state.store})
    return {"runs": request.app.state.store.all()}

@router.post("/api/batches")
def ingest(request: Request, payload: dict = Body(default={})):
    return service.ingest(payload or {}, request.app.state.store)

@router.get("/api/batches")
def batches(request: Request):
    return service.overview(request.app.state.store)

@router.post("/api/batches/{batch_id}/revoke")
def revoke(batch_id: str, request: Request, payload: dict = Body(default={})):
    return service.revoke(batch_id, (payload or {}).get("reason"), request.app.state.store)

@router.post("/api/batches/{batch_id}/purge")
def purge(batch_id: str, request: Request, payload: dict = Body(default={})):
    payload = payload or {}
    return service.purge(batch_id, request.app.state.store,
                         chunk_size=payload.get("chunk_size", 256),
                         max_chunks=payload.get("max_chunks"))

@router.get("/api/batches/{batch_id}/audit")
def audit(batch_id: str, request: Request):
    return service.audit(batch_id, request.app.state.store)

@router.get("/batches/{batch_id}")
def audit_page(batch_id: str, request: Request):
    try:
        data = service.audit(batch_id, request.app.state.store)
    except DomainError:
        return RedirectResponse("/runs", status_code=303)
    return request.app.state.templates.TemplateResponse(request, "m6_audit.html", {"title": "批次审计", "data": data})

@router.get("/api/export/evidence.csv")
def export(request: Request):
    text = service.export_csv(request.app.state.store)
    return Response(text, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=evidence.csv"})
