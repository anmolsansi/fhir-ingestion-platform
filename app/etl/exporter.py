import json
import os
from datetime import date, datetime, timezone


def run_dir(base: str = "exports", run_id: int | None = None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = f"run_{run_id}_{timestamp}" if run_id is not None else f"run_{timestamp}"
    path = os.path.join(base, name)
    os.makedirs(path, exist_ok=True)
    return path


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def write_ndjson(path: str, filename: str, items: list[dict]) -> str:
    os.makedirs(path, exist_ok=True)
    full_path = os.path.join(path, filename)
    with open(full_path, "w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=False, default=_json_default))
            handle.write("\n")
    return full_path
