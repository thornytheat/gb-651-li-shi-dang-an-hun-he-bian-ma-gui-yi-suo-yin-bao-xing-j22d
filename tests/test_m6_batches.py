import pytest
from app.errors import DomainError
from app.store import Store
from app.modules.m6_runs import core

@pytest.fixture
def store(tmp_path):
    return Store(str(tmp_path / "batch.db"))

def ingest_sample(store):
    return core.ingest({}, store)["batch"]["batch_id"]

# ---- 入库 ----

def test_ingest_creates_batch_with_counts(store):
    out = core.ingest({}, store)
    assert out["created"] is True
    b = out["batch"]
    assert b["status"] == "ACTIVE"
    assert (b["input_count"], b["valid_count"], b["quarantine_count"]) == (4, 3, 1)
    assert [r["source_line"] for r in b["records"]] == [2, 3, 5, 7]
    assert b["records"][0]["archive_id"] == "000123"
    assert b["records"][0]["raw_value"] == "000123,07,年度 汇编"

def test_ingest_idempotent_same_digest(store):
    bid = ingest_sample(store)
    again = core.ingest({}, store)
    assert again["created"] is False
    assert again["batch"]["batch_id"] == bid
    assert len(store.list_batches()) == 1
    assert len(again["batch"]["records"]) == 4

def test_ingest_rejects_bad_rows(store):
    with pytest.raises(DomainError):
        core.ingest({"rows": []}, store)
    with pytest.raises(DomainError):
        core.ingest({"rows": [{"source_line": "2", "archive_id": "000123"}]}, store)
    with pytest.raises(DomainError):
        core.ingest({"rows": [{"source_line": 2, "archive_id": "A"},
                              {"source_line": 2, "archive_id": "B"}]}, store)

# ---- 撤回：计数、行号、理由留账；档号与原始内容不再可读 ----

def test_revoke_keeps_ledger_hides_sensitive(store):
    bid = ingest_sample(store)
    out = core.revoke(bid, "LEGAL_HOLD", store)
    assert out["changed"] is True
    b = out["batch"]
    assert b["status"] == "REVOKED"
    assert (b["input_count"], b["valid_count"], b["quarantine_count"]) == (4, 3, 1)
    assert [r["source_line"] for r in b["records"]] == [2, 3, 5, 7]
    assert {r["reason_code"] for r in b["records"]} == {"OK", "BAD_COLUMNS"}
    assert all(r["archive_id"] is None and r["raw_value"] is None for r in b["records"])

def test_revoke_retry_no_double_count(store):
    bid = ingest_sample(store)
    core.revoke(bid, "FIRST", store)
    again = core.revoke(bid, "SECOND", store)
    assert again["changed"] is False
    b = again["batch"]
    assert b["revoke_reason"] == "FIRST"  # 首次记账不被重试覆盖
    assert sum(1 for r in b["records"] if r["status"] == "REVOKED") == 4

def test_revoke_unknown_batch(store):
    with pytest.raises(DomainError):
        core.revoke("batch-none", "X", store)
    with pytest.raises(DomainError):
        core.audit("batch-none", store)

# ---- 清除：中断可续跑，重试不重复记账 ----

def test_purge_resumes_after_interruption_and_restart(store):
    bid = ingest_sample(store)
    core.revoke(bid, "R", store)
    with pytest.raises(core.PurgeInterrupted):
        core.purge(bid, store, chunk_size=1, fail_after=1)
    mid = core.audit(bid, store)
    assert mid["purge"]["purged"] == 1 and mid["purge"]["pending"] == 3
    restarted = Store(store.path)  # 模拟重启：同一库文件新建实例续跑
    done = core.purge(bid, restarted, chunk_size=1)
    assert done["remaining"] == 0
    view = core.audit(bid, restarted)
    assert view["purge"]["purged"] == 4
    assert len(view["purge"]["ledger"]) == 4

def test_purge_retry_no_double_count(store):
    bid = ingest_sample(store)
    core.revoke(bid, "R", store)
    first = core.purge(bid, store, chunk_size=2)
    assert first["purged"] == 4 and first["remaining"] == 0
    again = core.purge(bid, store, chunk_size=2)
    assert again["purged"] == 0 and again["remaining"] == 0
    assert len(core.audit(bid, store)["purge"]["ledger"]) == 4

def test_purge_erases_storage_but_keeps_lines(store):
    bid = ingest_sample(store)
    core.revoke(bid, "R", store)
    core.purge(bid, store)
    with store.connect() as db:
        rows = db.execute(
            "SELECT archive_id, raw_value, reason_code, source_line "
            "FROM records WHERE batch_id=?", (bid,)).fetchall()
    assert all(a is None and raw is None for a, raw, _, _ in rows)  # 敏感值已物理清除
    assert sorted(line for *_, line in rows) == [2, 3, 5, 7]          # 行号留账
    assert {rc for _, _, rc, _ in rows} == {"OK", "BAD_COLUMNS"}      # 理由留账

def test_purge_unknown_batch(store):
    with pytest.raises(DomainError):
        core.purge("batch-none", store)

# ---- 重放防护 ----

def test_replay_does_not_revive_revoked_batch(store):
    bid = ingest_sample(store)
    core.revoke(bid, "R", store)
    core.purge(bid, store)
    out = core.ingest({}, store)  # 旧采信请求原样重放
    assert out["created"] is False
    b = out["batch"]
    assert b["status"] == "REVOKED"
    assert all(r["archive_id"] is None and r["raw_value"] is None for r in b["records"])
    assert b["purge"]["purged"] == 4

def test_tombstone_blocks_reingest_with_shifted_lines(store):
    bid = ingest_sample(store)
    core.revoke(bid, "R", store)
    core.purge(bid, store)
    shifted = {"rows": [dict(r, source_line=r["source_line"] + 100)
                        for r in core.ARCHIVE_SAMPLE["rows"]]}
    out = core.ingest(shifted, store)  # 同一内容换行号再采信
    assert out["created"] is True
    assert out["batch"]["batch_id"] != bid
    assert all(r["status"] == "REVOKED" and r["archive_id"] is None
               for r in out["batch"]["records"])
    with store.connect() as db:  # 敏感值不曾再次落库
        left = db.execute("SELECT COUNT(*) FROM records WHERE archive_id IS NOT NULL").fetchone()[0]
    assert left == 0

# ---- 证据导出同步反映 ----

def test_export_reflects_revocation(store):
    bid = ingest_sample(store)
    before = core.export_csv(store)
    assert "000123" in before and "ACTIVE" in before
    core.revoke(bid, "R", store)
    after = core.export_csv(store)
    assert "000123" not in after and "9007199254740993" not in after  # 档号不再出现
    assert "BAD_COLUMNS" in after                                       # 理由留账
    lines = after.strip().splitlines()
    assert lines[0] == "batch_id,source_line,archive_id,reason,status"
    assert len(lines) == 5                                              # 删行不失账：墓碑行仍在
    assert all(line.endswith(",REVOKED") for line in lines[1:])
    assert f"{bid},2,,OK,REVOKED" in lines

# ---- API 端到端：批次页完成入库、撤回、审计，导出同步反映 ----

def test_api_full_flow(client):
    r = client.post("/api/batches", json={})
    assert r.status_code == 200
    bid = r.json()["batch"]["batch_id"]
    assert r.json()["created"] is True

    audit = client.get(f"/api/batches/{bid}/audit")
    assert audit.status_code == 200
    assert audit.json()["records"][0]["archive_id"] == "000123"

    csv1 = client.get("/api/export/evidence.csv").text
    assert "000123" in csv1

    rv = client.post(f"/api/batches/{bid}/revoke", json={"reason": "LEGAL_HOLD"})
    assert rv.status_code == 200 and rv.json()["changed"] is True
    # 撤回生效后，旧导出请求（同一地址重放）不再读到档号
    csv2 = client.get("/api/export/evidence.csv").text
    assert "000123" not in csv2 and "9007199254740993" not in csv2
    assert "BAD_COLUMNS" in csv2 and f"{bid},2,,OK,REVOKED" in csv2
    # 撤回重试不重复记账
    rv2 = client.post(f"/api/batches/{bid}/revoke", json={"reason": "OTHER"})
    assert rv2.json()["changed"] is False
    assert rv2.json()["batch"]["revoke_reason"] == "LEGAL_HOLD"

    p = client.post(f"/api/batches/{bid}/purge", json={})
    assert p.status_code == 200 and p.json()["remaining"] == 0
    # 旧采信请求重放不能复活已撤回记录
    again = client.post("/api/batches", json={}).json()
    assert again["created"] is False and again["batch"]["status"] == "REVOKED"
    assert all(rec["archive_id"] is None for rec in again["batch"]["records"])
    # 审计页与批次页
    assert client.get(f"/batches/{bid}").status_code == 200
    assert client.get("/runs").status_code == 200
    overview = client.get("/api/batches").json()
    assert overview["input_count"] == 4 and overview["quarantine_count"] == 1

def test_api_unknown_batch(client):
    assert client.get("/api/batches/batch-none/audit").status_code == 422
    assert client.post("/api/batches/batch-none/revoke", json={}).status_code == 422
    assert client.post("/api/batches/batch-none/purge", json={}).status_code == 422

def test_api_invalid_ingest(client):
    assert client.post("/api/batches", json={"rows": []}).status_code == 422
    assert client.post("/api/batches", json={"rows": [{"source_line": -1, "archive_id": "A"}]}).status_code == 422

def test_unknown_batch_page_redirects(client):
    r = client.get("/batches/batch-none", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/runs"
