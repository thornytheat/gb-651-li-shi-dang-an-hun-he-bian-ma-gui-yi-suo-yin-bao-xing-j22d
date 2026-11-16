from . import core

def execute(payload: dict | None = None) -> dict:
    return core.run(payload or core.SAMPLE)

def overview(store) -> dict:
    return core.overview(store)

def ingest(payload: dict, store) -> dict:
    return core.ingest(payload or {}, store)

def revoke(batch_id: str, reason: str | None, store) -> dict:
    return core.revoke(batch_id, reason, store)

def purge(batch_id: str, store, chunk_size: int = 256, max_chunks: int | None = None) -> dict:
    return core.purge(batch_id, store, chunk_size=chunk_size, max_chunks=max_chunks)

def audit(batch_id: str, store) -> dict:
    return core.audit(batch_id, store)

def export_csv(store) -> str:
    return core.export_csv(store)
