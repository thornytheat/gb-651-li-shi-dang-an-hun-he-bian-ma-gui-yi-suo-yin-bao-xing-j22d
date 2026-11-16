from __future__ import annotations
import json
from pathlib import Path
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  digest TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  result TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  digest TEXT NOT NULL,
  input_count INTEGER NOT NULL,
  valid_count INTEGER NOT NULL,
  quarantine_count INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TEXT NOT NULL,
  revoked_at TEXT,
  revoke_reason TEXT
);
CREATE TABLE IF NOT EXISTS records (
  batch_id TEXT NOT NULL,
  source_line INTEGER NOT NULL,
  record_digest TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  archive_id TEXT,
  raw_value TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  purged INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (batch_id, source_line)
);
CREATE TABLE IF NOT EXISTS tombstones (
  record_digest TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  revoked_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS purge_ledger (
  record_key TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  source_line INTEGER NOT NULL,
  purged_at TEXT NOT NULL
);
"""

class Store:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)

    def connect(self):
        return sqlite3.connect(self.path)

    # ---- 运行登记（legacy） ----

    def record(self, digest: str, run_id: str, result: dict) -> dict:
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?, ?)",
                       (digest, run_id, json.dumps(result, ensure_ascii=False, sort_keys=True)))
            row = db.execute("SELECT run_id, result FROM runs WHERE digest=?", (digest,)).fetchone()
        return {"run_id": row[0], "result": json.loads(row[1])}

    def count(self) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]

    def all(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT run_id, digest, result FROM runs ORDER BY run_id").fetchall()
        return [{"run_id": r[0], "digest": r[1], "result": json.loads(r[2])} for r in rows]

    # ---- 批次档案：入库 / 撤回 / 清除 ----

    def ingest_batch(self, batch: dict, records: list[dict]) -> bool:
        """入库一个批次。幂等：同内容重放复用原批次；已被撤回的内容以无敏感值的
        撤回态落行（保留行号与理由供对账），不会复活档号与原始内容。
        返回是否为新建批次。"""
        with self.connect() as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO batches "
                "(batch_id, digest, input_count, valid_count, quarantine_count, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?)",
                (batch["batch_id"], batch["digest"], batch["input_count"],
                 batch["valid_count"], batch["quarantine_count"], batch["created_at"]))
            created = cur.rowcount > 0
            tombstoned = {r[0] for r in db.execute("SELECT record_digest FROM tombstones")}
            for rec in records:
                if rec["record_digest"] in tombstoned:
                    rec = {**rec, "archive_id": None, "raw_value": None,
                           "status": "REVOKED", "purged": 1}
                else:
                    rec = {**rec, "status": "ACTIVE", "purged": 0}
                db.execute(
                    "INSERT OR IGNORE INTO records "
                    "(batch_id, source_line, record_digest, reason_code, archive_id, raw_value, status, purged) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (rec["batch_id"], rec["source_line"], rec["record_digest"], rec["reason_code"],
                     rec["archive_id"], rec["raw_value"], rec["status"], rec["purged"]))
        return created

    def revoke_batch(self, batch_id: str, reason: str, revoked_at: str):
        """按批次撤回。幂等：重复撤回返回 False 且不改账。
        批次不存在返回 None。撤回即刻生效（读侧不再给出敏感值），
        并为记录内容登记全局墓碑，阻断后续重放复活。"""
        with self.connect() as db:
            row = db.execute("SELECT status FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                return None
            if row[0] != "ACTIVE":
                return False
            db.execute("UPDATE batches SET status='REVOKED', revoked_at=?, revoke_reason=? "
                       "WHERE batch_id=?", (revoked_at, reason, batch_id))
            digests = [r[0] for r in db.execute(
                "SELECT record_digest FROM records WHERE batch_id=?", (batch_id,)).fetchall()]
            db.executemany(
                "INSERT OR IGNORE INTO tombstones (record_digest, batch_id, revoked_at) VALUES (?, ?, ?)",
                [(d, batch_id, revoked_at) for d in digests])
            # 同一内容无论落在哪个批次都一并撤回，避免敏感值经其他批次继续可读
            db.executemany("UPDATE records SET status='REVOKED' WHERE record_digest=? AND status='ACTIVE'",
                           [(d,) for d in digests])
        return True

    def purge_chunk(self, batch_id: str, limit: int, purged_at: str) -> dict:
        """清除一个分块的敏感值（档号与原始内容），行号与理由保留。
        每个分块一个事务：中断后已提交部分保留，可从游标续跑；
        台账按记录主键 INSERT OR IGNORE，重试不重复记账。"""
        with self.connect() as db:
            rows = db.execute(
                "SELECT rowid, source_line FROM records "
                "WHERE batch_id=? AND status='REVOKED' AND purged=0 "
                "ORDER BY source_line LIMIT ?", (batch_id, limit)).fetchall()
            for rowid, line in rows:
                db.execute("UPDATE records SET archive_id=NULL, raw_value=NULL, purged=1 WHERE rowid=?",
                           (rowid,))
                db.execute(
                    "INSERT OR IGNORE INTO purge_ledger (record_key, batch_id, source_line, purged_at) "
                    "VALUES (?, ?, ?, ?)", (f"{batch_id}:{line}", batch_id, line, purged_at))
            remaining = db.execute(
                "SELECT COUNT(*) FROM records WHERE batch_id=? AND status='REVOKED' AND purged=0",
                (batch_id,)).fetchone()[0]
        return {"purged": len(rows), "remaining": remaining}

    def batch_view(self, batch_id: str) -> dict | None:
        """审计视图：计数、行号、理由始终可见；已撤回记录的档号与原始内容
        在读取边界一律隐去（即使清除尚未跑完）。"""
        with self.connect() as db:
            b = db.execute(
                "SELECT batch_id, digest, input_count, valid_count, quarantine_count, "
                "status, created_at, revoked_at, revoke_reason FROM batches WHERE batch_id=?",
                (batch_id,)).fetchone()
            if b is None:
                return None
            recs = db.execute(
                "SELECT source_line, reason_code, archive_id, raw_value, status, purged "
                "FROM records WHERE batch_id=? ORDER BY source_line", (batch_id,)).fetchall()
            ledger = db.execute(
                "SELECT source_line, purged_at FROM purge_ledger WHERE batch_id=? ORDER BY source_line",
                (batch_id,)).fetchall()
        records = []
        for line, reason_code, archive_id, raw_value, status, purged in recs:
            revoked = status == "REVOKED"
            records.append({
                "source_line": line, "reason_code": reason_code, "status": status,
                "purged": bool(purged),
                "archive_id": None if revoked else archive_id,
                "raw_value": None if revoked else raw_value,
            })
        return {
            "batch_id": b[0], "digest": b[1],
            "input_count": b[2], "valid_count": b[3], "quarantine_count": b[4],
            "status": b[5], "created_at": b[6], "revoked_at": b[7], "revoke_reason": b[8],
            "records": records,
            "purge": {
                "purged": sum(1 for r in recs if r[5]),
                "pending": sum(1 for r in recs if r[4] == "REVOKED" and not r[5]),
                "ledger": [{"source_line": line, "purged_at": at} for line, at in ledger],
            },
        }

    def list_batches(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT b.batch_id, b.input_count, b.valid_count, b.quarantine_count, "
                "b.status, b.created_at, b.revoked_at, b.revoke_reason, "
                "(SELECT COUNT(*) FROM records r WHERE r.batch_id=b.batch_id AND r.purged=1), "
                "(SELECT COUNT(*) FROM records r WHERE r.batch_id=b.batch_id "
                "  AND r.status='REVOKED' AND r.purged=0) "
                "FROM batches b ORDER BY b.created_at, b.batch_id").fetchall()
        return [{"batch_id": r[0], "input_count": r[1], "valid_count": r[2],
                 "quarantine_count": r[3], "status": r[4], "created_at": r[5],
                 "revoked_at": r[6], "revoke_reason": r[7],
                 "purged": r[8], "pending": r[9]} for r in rows]

    def export_rows(self) -> list[dict]:
        """证据导出行：与审计视图同一投影，已撤回记录不输出档号。"""
        with self.connect() as db:
            rows = db.execute(
                "SELECT batch_id, source_line, reason_code, archive_id, status "
                "FROM records ORDER BY batch_id, source_line").fetchall()
        return [{"batch_id": b, "source_line": line, "reason_code": rc,
                 "archive_id": None if status == "REVOKED" else archive_id, "status": status}
                for b, line, rc, archive_id, status in rows]
