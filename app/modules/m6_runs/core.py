import csv, io
from datetime import datetime, timezone
from app.errors import DomainError
from app.manifest import canonical_digest, run_id
SAMPLE={"store":None,"rows":[{"source_line":2,"archive_id":"000123","reason":"OK"},{"source_line":5,"archive_id":"BAD","reason":"BAD_COLUMNS"}]}
def csv_text(rows):
    out=io.StringIO(); writer=csv.DictWriter(out,fieldnames=["source_line","archive_id","reason"],lineterminator="\n"); writer.writeheader(); writer.writerows(rows); return out.getvalue()
def run(payload):
    rows=payload.get("rows",SAMPLE["rows"]); digest=canonical_digest(rows); rid=run_id(rows); store=payload.get("store")
    result={"input_count":len(rows),"valid_count":sum(r["reason"]=="OK" for r in rows),"quarantine_count":sum(r["reason"]!="OK" for r in rows)}
    if store: rid=store.record(digest,rid,result)["run_id"]
    return {"status":"READY","run_id":rid,"digest":digest,"csv":csv_text(rows),**result,"reason":"OK"}

# ---- 批次档案：入库 / 撤回 / 清除 / 审计 ----

ARCHIVE_SAMPLE={"rows":[
    {"source_line":2,"archive_id":"000123","raw":"000123,07,年度 汇编","reason":"OK"},
    {"source_line":3,"archive_id":"000124","raw":"000124,08,会议记录","reason":"OK"},
    {"source_line":5,"archive_id":"9007199254740993","raw":"9007199254740993,08,会议","reason":"OK"},
    {"source_line":7,"archive_id":"BAD","raw":"BAD,09","reason":"BAD_COLUMNS"},
]}
EXPORT_FIELDS=["batch_id","source_line","archive_id","reason","status"]

class PurgeInterrupted(RuntimeError):
    """清除中途崩溃（模拟）：已提交分块保留，重启后可从游标续跑。"""

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _validate_rows(rows) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise DomainError("INVALID_VALUE", "rows 必须是非空列表")
    clean=[]; seen=set()
    for i, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise DomainError("INVALID_VALUE", f"第 {i} 行不是对象")
        line=row.get("source_line")
        if isinstance(line, bool) or not isinstance(line, int) or line < 1:
            raise DomainError("INVALID_VALUE", f"第 {i} 行 source_line 必须是正整数")
        if line in seen:
            raise DomainError("INVALID_VALUE", f"source_line {line} 重复，批次内行号必须唯一")
        seen.add(line)
        archive_id=row.get("archive_id")
        if not isinstance(archive_id, str) or not archive_id:
            raise DomainError("INVALID_VALUE", f"第 {i} 行 archive_id 必须是非空字符串")
        reason=row.get("reason", "OK"); raw=row.get("raw", "")
        if not isinstance(reason, str) or not isinstance(raw, str):
            raise DomainError("INVALID_VALUE", f"第 {i} 行 reason/raw 必须是字符串")
        clean.append({"source_line":line,"archive_id":archive_id,"raw":raw,"reason":reason})
    return clean

def record_digest(row: dict) -> str:
    """内容身份：档号 + 原始内容。撤回墓碑按此登记，行号漂移的重放同样被拦截。"""
    return canonical_digest({"archive_id":row["archive_id"],"raw":row["raw"]})

def batch_id_for(rows) -> str:
    return "batch-" + canonical_digest(rows)[:12]

def _counts(rows) -> dict:
    return {"input_count":len(rows),
            "valid_count":sum(1 for r in rows if r["reason"]=="OK"),
            "quarantine_count":sum(1 for r in rows if r["reason"]!="OK")}

def ingest(payload: dict, store, now: str | None = None) -> dict:
    """入库：同内容重放复用原批次（created=False），且不会复活已撤回记录。"""
    rows=_validate_rows(payload.get("rows", ARCHIVE_SAMPLE["rows"]))
    bid=batch_id_for(rows)
    batch={"batch_id":bid,"digest":canonical_digest(rows),
           "created_at":now or _utcnow(),**_counts(rows)}
    records=[{"batch_id":bid,"source_line":r["source_line"],"record_digest":record_digest(r),
              "reason_code":r["reason"],"archive_id":r["archive_id"],"raw_value":r["raw"]}
             for r in rows]
    created=store.ingest_batch(batch, records)
    return {"status":"READY","created":created,"batch":store.batch_view(bid),"reason":"OK"}

def revoke(batch_id: str, reason: str | None, store, now: str | None = None) -> dict:
    """撤回：即刻生效（读侧不再给出敏感值），重试不重复记账（changed=False）。"""
    changed=store.revoke_batch(batch_id, reason or "MANUAL_REVOKE", now or _utcnow())
    if changed is None:
        raise DomainError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在")
    return {"status":"READY","changed":changed,"batch":store.batch_view(batch_id),"reason":"OK"}

def purge(batch_id: str, store, chunk_size: int = 256, max_chunks: int | None = None,
          fail_after: int | None = None, now: str | None = None) -> dict:
    """清除敏感值：分块提交，中断/重启后可续跑；无剩余时重试为 no-op。"""
    if store.batch_view(batch_id) is None:
        raise DomainError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在")
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise DomainError("INVALID_VALUE", "chunk_size 必须是正整数")
    if max_chunks is not None and (isinstance(max_chunks, bool) or not isinstance(max_chunks, int) or max_chunks < 1):
        raise DomainError("INVALID_VALUE", "max_chunks 必须是正整数")
    purged=0; chunks=0
    while True:
        out=store.purge_chunk(batch_id, chunk_size, now or _utcnow())
        purged+=out["purged"]; chunks+=1
        if out["purged"]==0 or out["remaining"]==0:
            break
        if fail_after is not None and chunks >= fail_after:
            raise PurgeInterrupted(f"清除在 {chunks} 个分块后中断，已提交 {purged} 行，可续跑")
        if max_chunks is not None and chunks >= max_chunks:
            break
    view=store.batch_view(batch_id)
    return {"status":"READY","batch_id":batch_id,"purged":purged,
            "remaining":view["purge"]["pending"],"reason":"OK"}

def audit(batch_id: str, store) -> dict:
    view=store.batch_view(batch_id)
    if view is None:
        raise DomainError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在")
    return view

def overview(store) -> dict:
    batches=store.list_batches()
    return {"status":"READY","batches":batches,
            "input_count":sum(b["input_count"] for b in batches),
            "valid_count":sum(b["valid_count"] for b in batches),
            "quarantine_count":sum(b["quarantine_count"] for b in batches),
            "reason":"OK"}

def export_csv(store) -> str:
    """证据导出：实时反映库内状态。已撤回记录只出行号、理由与状态，不出档号。"""
    out=io.StringIO()
    writer=csv.DictWriter(out, fieldnames=EXPORT_FIELDS, lineterminator="\n")
    writer.writeheader()
    for r in store.export_rows():
        writer.writerow({"batch_id":r["batch_id"],"source_line":r["source_line"],
                         "archive_id":r["archive_id"] or "","reason":r["reason_code"],
                         "status":r["status"]})
    return out.getvalue()
