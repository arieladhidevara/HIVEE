import json
import sqlite3
import time
from typing import Any, Dict, Optional

from core.workspace_paths import new_id


def _dump_json(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False)
    except Exception:
        return "{}"


def append_project_activity_log_entry(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    actor_type: str,
    actor_id: Optional[str],
    actor_label: Optional[str],
    event_type: str,
    summary: str,
    payload: Optional[Dict[str, Any]] = None,
    created_at: Optional[int] = None,
) -> str:
    event_id = new_id("act")
    ts = int(time.time()) if created_at is None else int(created_at)
    conn.execute(
        """
        INSERT INTO project_activity_log (
            id, project_id, actor_type, actor_id, actor_label, event_type, summary, payload_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            str(event_id),
            str(project_id or "").strip(),
            str(actor_type or "").strip() or "unknown",
            str(actor_id or "").strip() or None,
            str(actor_label or "").strip() or None,
            str(event_type or "").strip() or "event",
            str(summary or "").strip()[:1000] or "-",
            _dump_json(payload),
            ts,
        ),
    )
    return str(event_id)
