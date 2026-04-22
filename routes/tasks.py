from hivee_shared import *


from services.project_activity import append_project_activity_log_entry


TASK_BLUEPRINTS: List[Dict[str, Any]] = [
    {
        "id": "mvp_delivery",
        "name": "MVP Delivery",
        "description": "Discovery -> build -> QA -> release handoff",
        "tasks": [
            {"title": "Define scope and acceptance criteria", "description": "Lock target outcomes and success metrics.", "priority": TASK_PRIORITY_HIGH},
            {"title": "Design technical approach", "description": "Document architecture and rollout approach.", "priority": TASK_PRIORITY_HIGH},
            {"title": "Implement core user flow", "description": "Build and verify the main end-to-end scenario.", "priority": TASK_PRIORITY_URGENT},
            {"title": "QA and stabilization pass", "description": "Run QA checklist and resolve critical issues.", "priority": TASK_PRIORITY_MEDIUM},
            {"title": "Release and handoff notes", "description": "Prepare release summary and operational handoff.", "priority": TASK_PRIORITY_MEDIUM},
        ],
        "dependencies": [(1, 0), (2, 1), (3, 2), (4, 3)],
    },
    {
        "id": "incident_response",
        "name": "Incident Response",
        "description": "Containment -> diagnosis -> fix -> postmortem",
        "tasks": [
            {"title": "Triage and impact assessment", "description": "Identify scope, severity, and affected users.", "priority": TASK_PRIORITY_URGENT},
            {"title": "Containment actions", "description": "Apply temporary controls to stop further impact.", "priority": TASK_PRIORITY_URGENT},
            {"title": "Root-cause analysis", "description": "Collect evidence and determine root cause.", "priority": TASK_PRIORITY_HIGH},
            {"title": "Permanent remediation", "description": "Implement and verify long-term fix.", "priority": TASK_PRIORITY_HIGH},
            {"title": "Postmortem and follow-ups", "description": "Document lessons learned and preventive actions.", "priority": TASK_PRIORITY_MEDIUM},
        ],
        "dependencies": [(1, 0), (2, 1), (3, 2), (4, 3)],
    },
    {
        "id": "research_spike",
        "name": "Research Spike",
        "description": "Question framing -> evidence -> recommendation",
        "tasks": [
            {"title": "Frame research questions", "description": "Define assumptions, constraints, and goals.", "priority": TASK_PRIORITY_HIGH},
            {"title": "Collect references and data", "description": "Gather docs, benchmarks, and implementation options.", "priority": TASK_PRIORITY_MEDIUM},
            {"title": "Evaluate trade-offs", "description": "Compare options across risk, effort, and impact.", "priority": TASK_PRIORITY_MEDIUM},
            {"title": "Recommendation memo", "description": "Summarize findings and propose next steps.", "priority": TASK_PRIORITY_HIGH},
        ],
        "dependencies": [(1, 0), (2, 1), (3, 2)],
    },
]

def _coerce_task_status(raw_status: Any, *, required: bool = False) -> str:
    status = str(raw_status or "").strip().lower()
    if not status:
        if required:
            raise HTTPException(400, "Task status is required")
        return TASK_STATUS_TODO
    if status not in TASK_STATUSES:
        raise HTTPException(400, f"Invalid task status. Allowed: {', '.join(TASK_STATUSES)}")
    return status


def _coerce_task_priority(raw_priority: Any, *, required: bool = False) -> str:
    priority = str(raw_priority or "").strip().lower()
    if not priority:
        if required:
            raise HTTPException(400, "Task priority is required")
        return TASK_PRIORITY_MEDIUM
    if priority not in TASK_PRIORITIES:
        raise HTTPException(400, f"Invalid task priority. Allowed: {', '.join(TASK_PRIORITIES)}")
    return priority


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _parse_json_object(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _dump_json(payload: Optional[Dict[str, Any]]) -> str:
    if not isinstance(payload, dict):
        return "{}"
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return "{}"


def _checkout_from_row(row: Optional[sqlite3.Row], *, now: Optional[int] = None) -> Optional[ProjectTaskCheckoutOut]:
    if not row:
        return None
    current_ts = int(time.time()) if now is None else int(now)
    expires_at = _safe_int(row["expires_at"])
    is_active = expires_at > current_ts
    return ProjectTaskCheckoutOut(
        owner_type=str(row["owner_type"] or ""),
        owner_id=str(row["owner_id"] or ""),
        owner_label=str(row["owner_label"] or "").strip() or None,
        note=str(row["checkout_note"] or "").strip(),
        checked_out_at=_safe_int(row["checked_out_at"]),
        expires_at=expires_at,
        is_active=is_active,
    )


def _task_from_row(task_row: sqlite3.Row, checkout_row: Optional[sqlite3.Row] = None, *, now: Optional[int] = None) -> ProjectTaskOut:
    return ProjectTaskOut(
        id=str(task_row["id"]),
        project_id=str(task_row["project_id"]),
        created_by_user_id=str(task_row["created_by_user_id"] or "").strip() or None,
        created_by_agent_id=str(task_row["created_by_agent_id"] or "").strip() or None,
        title=str(task_row["title"] or ""),
        description=str(task_row["description"] or ""),
        status=_coerce_task_status(task_row["status"]),
        priority=_coerce_task_priority(task_row["priority"]),
        assignee_agent_id=str(task_row["assignee_agent_id"] or "").strip() or None,
        due_at=_safe_int(task_row["due_at"]) if task_row["due_at"] is not None else None,
        weight_pct=max(0, min(100, int(task_row["weight_pct"] or 0))),
        metadata=_parse_json_object(task_row["metadata_json"]),
        created_at=_safe_int(task_row["created_at"]),
        updated_at=_safe_int(task_row["updated_at"]),
        closed_at=_safe_int(task_row["closed_at"]) if task_row["closed_at"] is not None else None,
        checkout=_checkout_from_row(checkout_row, now=now),
    )


def _require_task_read_access(access: Dict[str, Any]) -> None:
    if str(access.get("mode") or "") == "owner":
        return
    perms = access.get("permissions") or {}
    if not bool(perms.get("can_chat_project")):
        raise HTTPException(403, "This project agent cannot access project tasks")


def _require_task_write_access(access: Dict[str, Any]) -> None:
    _require_task_read_access(access)


def _actor_from_access(access: Dict[str, Any]) -> Dict[str, str]:
    mode = str(access.get("mode") or "").strip().lower()
    if mode == "owner":
        uid = str(access.get("user_id") or "").strip()
        return {"type": "user", "id": uid, "label": "owner"}

    aid = str(access.get("agent_id") or "").strip()
    uid = str(access.get("user_id") or "").strip()
    if aid:
        label_prefix = "member" if mode == "member" else "agent"
        return {"type": "project_agent", "id": aid, "label": f"{label_prefix}:{aid}"}
    return {"type": "user", "id": uid, "label": mode or "user"}


def _assert_assignee_exists(conn: sqlite3.Connection, *, project_id: str, assignee_agent_id: Optional[str]) -> Optional[str]:
    aid = str(assignee_agent_id or "").strip()
    if not aid:
        return None
    row = conn.execute(
        "SELECT agent_id FROM project_agents WHERE project_id = ? AND agent_id = ? LIMIT 1",
        (project_id, aid),
    ).fetchone()
    if not row:
        raise HTTPException(400, "assignee_agent_id is not part of this project")
    return aid


def _resolve_task_project_id(task_id: str) -> str:
    conn = db()
    row = conn.execute("SELECT project_id FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Task not found")
    return str(row["project_id"])


def _task_comment_from_row(row: sqlite3.Row) -> ProjectTaskCommentOut:
    return ProjectTaskCommentOut(
        id=str(row["id"]),
        task_id=str(row["task_id"]),
        project_id=str(row["project_id"]),
        author_type=str(row["author_type"] or ""),
        author_id=str(row["author_id"] or "").strip() or None,
        author_label=str(row["author_label"] or "").strip() or None,
        body=str(row["body"] or ""),
        created_at=_safe_int(row["created_at"]),
        updated_at=_safe_int(row["updated_at"]),
    )


def _can_mutate_task_comment(*, access: Dict[str, Any], actor: Dict[str, str], comment_row: sqlite3.Row) -> bool:
    if str(access.get("mode") or "").strip().lower() == "owner":
        return True
    author_type = str(comment_row["author_type"] or "").strip()
    author_id = str(comment_row["author_id"] or "").strip()
    actor_type = str(actor.get("type") or "").strip()
    actor_id = str(actor.get("id") or "").strip()
    return bool(author_id and actor_id and author_type == actor_type and author_id == actor_id)



def _get_task_blueprint(blueprint_id: str) -> Optional[Dict[str, Any]]:
    key = str(blueprint_id or "").strip().lower()
    if not key:
        return None
    for item in TASK_BLUEPRINTS:
        if str(item.get("id") or "").strip().lower() == key:
            return item
    return None


def _assert_task_row_in_project(conn: sqlite3.Connection, *, project_id: str, task_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, title, status FROM project_tasks WHERE project_id = ? AND id = ? LIMIT 1",
        (project_id, task_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Task not found")
    return row


def _dependency_stats_by_task(conn: sqlite3.Connection, *, project_id: str, task_ids: List[str]) -> Dict[str, Dict[str, int]]:
    ids = [str(tid).strip() for tid in (task_ids or []) if str(tid).strip()]
    if not ids:
        return {}
    placeholders = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""
        SELECT
            d.task_id AS task_id,
            COUNT(*) AS total_count,
            SUM(CASE WHEN COALESCE(t.status, '') != ? THEN 1 ELSE 0 END) AS open_count
        FROM project_task_dependencies d
        JOIN project_tasks t
            ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
        WHERE d.project_id = ?
          AND d.task_id IN ({placeholders})
        GROUP BY d.task_id
        """,
        (TASK_STATUS_DONE, project_id, *ids),
    ).fetchall()
    out: Dict[str, Dict[str, int]] = {}
    for row in rows:
        tid = str(row["task_id"] or "").strip()
        if not tid:
            continue
        out[tid] = {
            "total": max(0, _safe_int(row["total_count"])),
            "open": max(0, _safe_int(row["open_count"])),
        }
    return out


def _decorate_task_with_dependency_stats(task: ProjectTaskOut, stats: Optional[Dict[str, int]]) -> ProjectTaskOut:
    src_meta = task.metadata if isinstance(task.metadata, dict) else {}
    meta = dict(src_meta)
    total_count = max(0, _safe_int((stats or {}).get("total", 0)))
    open_count = max(0, _safe_int((stats or {}).get("open", 0)))
    meta["_system_dependency_total"] = total_count
    meta["_system_dependency_open"] = open_count
    task.metadata = meta
    return task


def _list_open_dependencies(conn: sqlite3.Connection, *, project_id: str, task_id: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT d.depends_on_task_id, t.title, t.status
        FROM project_task_dependencies d
        JOIN project_tasks t
            ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
        WHERE d.project_id = ?
          AND d.task_id = ?
          AND COALESCE(t.status, '') != ?
        ORDER BY t.updated_at DESC
        """,
        (project_id, task_id, TASK_STATUS_DONE),
    ).fetchall()


def _ensure_status_transition_allowed(conn: sqlite3.Connection, *, project_id: str, task_id: str, next_status: str) -> None:
    target = str(next_status or "").strip().lower()
    if target not in {TASK_STATUS_IN_PROGRESS, TASK_STATUS_REVIEW, TASK_STATUS_DONE}:
        return
    blockers = _list_open_dependencies(conn, project_id=project_id, task_id=task_id)
    if not blockers:
        return
    preview = ", ".join(
        [str(row["title"] or row["depends_on_task_id"]).strip()[:80] for row in blockers[:3] if str(row["title"] or row["depends_on_task_id"]).strip()]
    )
    if not preview:
        preview = "dependency tasks"
    raise HTTPException(409, f"Task has open dependencies: {preview}. Complete dependencies first.")


def _would_create_dependency_cycle(conn: sqlite3.Connection, *, project_id: str, task_id: str, depends_on_task_id: str) -> bool:
    origin = str(task_id or "").strip()
    start = str(depends_on_task_id or "").strip()
    if not origin or not start:
        return False
    stack: List[str] = [start]
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current == origin:
            return True
        if current in seen:
            continue
        seen.add(current)
        rows = conn.execute(
            """
            SELECT depends_on_task_id
            FROM project_task_dependencies
            WHERE project_id = ? AND task_id = ?
            """,
            (project_id, current),
        ).fetchall()
        for row in rows:
            nxt = str(row["depends_on_task_id"] or "").strip()
            if nxt and nxt not in seen:
                stack.append(nxt)
    return False


def register_routes(app: FastAPI) -> None:
    @app.get("/api/projects/{project_id}/tasks", response_model=List[ProjectTaskOut])
    async def list_project_tasks(
        request: Request,
        project_id: str,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee_agent_id: Optional[str] = None,
        include_closed: bool = False,
        limit: int = 100,
    ):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)

        status_filter = str(status or "").strip().lower() or None
        if status_filter and status_filter not in TASK_STATUSES:
            raise HTTPException(400, f"Invalid status filter. Allowed: {', '.join(TASK_STATUSES)}")
        priority_filter = str(priority or "").strip().lower() or None
        if priority_filter and priority_filter not in TASK_PRIORITIES:
            raise HTTPException(400, f"Invalid priority filter. Allowed: {', '.join(TASK_PRIORITIES)}")
        assignee_filter = str(assignee_agent_id or "").strip() or None
        cap = max(1, min(int(limit or 100), 300))
        now = int(time.time())

        conn = db()
        conn.execute("DELETE FROM project_task_checkouts WHERE project_id = ? AND expires_at <= ?", (project_id, now))
        sql = (
            """
            SELECT t.*, c.owner_type, c.owner_id, c.owner_label, c.checkout_note, c.checked_out_at, c.expires_at
            FROM project_tasks t
            LEFT JOIN project_task_checkouts c ON c.task_id = t.id
            WHERE t.project_id = ?
            """
        )
        params: List[Any] = [project_id]
        if status_filter:
            sql += " AND t.status = ?"
            params.append(status_filter)
        if priority_filter:
            sql += " AND t.priority = ?"
            params.append(priority_filter)
        if assignee_filter:
            sql += " AND t.assignee_agent_id = ?"
            params.append(assignee_filter)
        if not include_closed:
            sql += " AND t.status != ?"
            params.append(TASK_STATUS_DONE)
        sql += " ORDER BY CASE t.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, t.updated_at DESC LIMIT ?"
        params.append(cap)
        rows = conn.execute(sql, tuple(params)).fetchall()
        task_ids = [str(r["id"]) for r in rows]
        dep_stats = _dependency_stats_by_task(conn, project_id=project_id, task_ids=task_ids)
        conn.commit()
        conn.close()

        out: List[ProjectTaskOut] = []
        for row in rows:
            checkout_row = row if row["owner_type"] is not None else None
            task = _task_from_row(row, checkout_row=checkout_row, now=now)
            stats = dep_stats.get(str(task.id), {"total": 0, "open": 0})
            out.append(_decorate_task_with_dependency_stats(task, stats))
        return out

    @app.post("/api/projects/{project_id}/tasks", response_model=ProjectTaskOut)
    async def create_project_task(request: Request, project_id: str, payload: ProjectTaskCreateIn):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)
        now = int(time.time())

        conn = db()
        _assert_assignee_exists(conn, project_id=project_id, assignee_agent_id=payload.assignee_agent_id)
        task_id = new_id("tsk")
        status_value = _coerce_task_status(payload.status, required=True)
        priority_value = _coerce_task_priority(payload.priority, required=True)
        conn.execute(
            """
            INSERT INTO project_tasks (
                id, project_id, created_by_user_id, created_by_agent_id,
                title, description, status, priority, assignee_agent_id,
                due_at, weight_pct, metadata_json, created_at, updated_at, closed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                task_id,
                project_id,
                actor["id"] if actor["type"] == "user" else None,
                actor["id"] if actor["type"] == "project_agent" else None,
                str(payload.title or "").strip()[:TASK_TITLE_MAX_CHARS],
                str(payload.description or "")[:TASK_DESCRIPTION_MAX_CHARS],
                status_value,
                priority_value,
                str(payload.assignee_agent_id or "").strip() or None,
                _safe_int(payload.due_at) if payload.due_at is not None else None,
                0,
                _dump_json(payload.metadata),
                now,
                now,
                now if status_value == TASK_STATUS_DONE else None,
            ),
        )
        row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
        append_project_activity_log_entry(
            conn,
            project_id=project_id,
            actor_type=actor["type"],
            actor_id=actor["id"],
            actor_label=actor["label"],
            event_type="task.created",
            summary=f"Task created: {str(payload.title or '').strip()[:120]}",
            payload={
                "task_id": task_id,
                "status": status_value,
                "priority": priority_value,
                "assignee_agent_id": str(payload.assignee_agent_id or "").strip() or None,
            },
            created_at=now,
        )
        conn.commit()
        conn.close()

        await emit(project_id, "project.task.created", {"task_id": task_id})
        return _task_from_row(row, checkout_row=None, now=now)

    @app.get("/api/tasks/{task_id}", response_model=ProjectTaskOut)
    async def get_task_detail(request: Request, task_id: str):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)

        now = int(time.time())
        conn = db()
        conn.execute("DELETE FROM project_task_checkouts WHERE task_id = ? AND expires_at <= ?", (task_id, now))
        row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
        checkout_row = conn.execute(
            "SELECT * FROM project_task_checkouts WHERE task_id = ? LIMIT 1",
            (task_id,),
        ).fetchone()
        dep_stats = _dependency_stats_by_task(conn, project_id=project_id, task_ids=[task_id])
        conn.commit()
        conn.close()
        if not row:
            raise HTTPException(404, "Task not found")
        task = _task_from_row(row, checkout_row=checkout_row, now=now)
        return _decorate_task_with_dependency_stats(task, dep_stats.get(task_id, {"total": 0, "open": 0}))

    @app.patch("/api/tasks/{task_id}", response_model=ProjectTaskOut)
    async def update_task_detail(request: Request, task_id: str, payload: ProjectTaskUpdateIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)

        conn = db()
        current = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
        if not current:
            conn.close()
            raise HTTPException(404, "Task not found")

        updates: List[str] = []
        params: List[Any] = []
        changed_fields: List[str] = []

        if payload.title is not None:
            title_value = str(payload.title or "").strip()
            if not title_value:
                conn.close()
                raise HTTPException(400, "title cannot be empty")
            updates.append("title = ?")
            params.append(title_value[:TASK_TITLE_MAX_CHARS])
            changed_fields.append("title")

        if payload.description is not None:
            updates.append("description = ?")
            params.append(str(payload.description or "")[:TASK_DESCRIPTION_MAX_CHARS])
            changed_fields.append("description")

        if payload.status is not None:
            next_status = _coerce_task_status(payload.status, required=True)
            _ensure_status_transition_allowed(conn, project_id=project_id, task_id=task_id, next_status=next_status)
            updates.append("status = ?")
            params.append(next_status)
            changed_fields.append("status")
            if next_status == TASK_STATUS_DONE:
                updates.append("closed_at = ?")
                params.append(int(time.time()))
            else:
                updates.append("closed_at = NULL")

        if payload.priority is not None:
            updates.append("priority = ?")
            params.append(_coerce_task_priority(payload.priority, required=True))
            changed_fields.append("priority")

        if payload.clear_assignee:
            updates.append("assignee_agent_id = NULL")
            changed_fields.append("assignee_agent_id")
        elif payload.assignee_agent_id is not None:
            aid = _assert_assignee_exists(conn, project_id=project_id, assignee_agent_id=payload.assignee_agent_id)
            updates.append("assignee_agent_id = ?")
            params.append(aid)
            changed_fields.append("assignee_agent_id")

        if payload.clear_due_at:
            updates.append("due_at = NULL")
            changed_fields.append("due_at")
        elif payload.due_at is not None:
            updates.append("due_at = ?")
            params.append(_safe_int(payload.due_at))
            changed_fields.append("due_at")

        if payload.metadata is not None:
            updates.append("metadata_json = ?")
            params.append(_dump_json(payload.metadata))
            changed_fields.append("metadata")

        now = int(time.time())
        if updates:
            updates.append("updated_at = ?")
            params.append(now)
            params.append(task_id)
            conn.execute(f"UPDATE project_tasks SET {', '.join(updates)} WHERE id = ?", tuple(params))
            append_project_activity_log_entry(
                conn,
                project_id=project_id,
                actor_type=actor["type"],
                actor_id=actor["id"],
                actor_label=actor["label"],
                event_type="task.updated",
                summary=f"Task updated: {task_id}",
                payload={"task_id": task_id, "changed_fields": changed_fields[:20]},
                created_at=now,
            )

        row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
        checkout_row = conn.execute("SELECT * FROM project_task_checkouts WHERE task_id = ? LIMIT 1", (task_id,)).fetchone()
        dep_stats = _dependency_stats_by_task(conn, project_id=project_id, task_ids=[task_id])
        conn.commit()
        conn.close()

        await emit(project_id, "project.task.updated", {"task_id": task_id, "changed_fields": changed_fields[:20]})
        task = _task_from_row(row, checkout_row=checkout_row, now=now)
        return _decorate_task_with_dependency_stats(task, dep_stats.get(task_id, {"total": 0, "open": 0}))

    @app.post("/api/tasks/{task_id}/checkout", response_model=ProjectTaskOut)
    async def checkout_task(request: Request, task_id: str, payload: ProjectTaskCheckoutIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)

        ttl_sec = max(TASK_CHECKOUT_MIN_TTL_SEC, min(_safe_int(payload.ttl_sec, TASK_CHECKOUT_DEFAULT_TTL_SEC), TASK_CHECKOUT_MAX_TTL_SEC))
        note = str(payload.note or "").strip()[:300]
        now = int(time.time())
        expires_at = now + ttl_sec

        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT id FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
            if not row:
                raise HTTPException(404, "Task not found")

            existing = conn.execute(
                "SELECT owner_type, owner_id, owner_label, checkout_note, checked_out_at, expires_at FROM project_task_checkouts WHERE task_id = ? LIMIT 1",
                (task_id,),
            ).fetchone()
            if existing and _safe_int(existing["expires_at"]) > now:
                same_owner = (
                    str(existing["owner_type"] or "") == actor["type"]
                    and str(existing["owner_id"] or "") == actor["id"]
                )
                if not same_owner and not bool(payload.force):
                    raise HTTPException(409, "Task is currently checked out by another actor")

            conn.execute(
                """
                INSERT OR REPLACE INTO project_task_checkouts (
                    task_id, project_id, owner_type, owner_id, owner_label, checkout_note, checked_out_at, expires_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (task_id, project_id, actor["type"], actor["id"], actor["label"], note, now, expires_at),
            )
            conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
            append_project_activity_log_entry(
                conn,
                project_id=project_id,
                actor_type=actor["type"],
                actor_id=actor["id"],
                actor_label=actor["label"],
                event_type="task.checkout",
                summary=f"Task checkout: {task_id}",
                payload={"task_id": task_id, "ttl_sec": ttl_sec, "force": bool(payload.force)},
                created_at=now,
            )
            task_row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
            checkout_row = conn.execute("SELECT * FROM project_task_checkouts WHERE task_id = ? LIMIT 1", (task_id,)).fetchone()
            dep_stats = _dependency_stats_by_task(conn, project_id=project_id, task_ids=[task_id])
            conn.commit()
        except HTTPException:
            conn.rollback()
            conn.close()
            raise
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to checkout task: {detail_to_text(exc)}")
        conn.close()

        await emit(project_id, "project.task.checkout", {"task_id": task_id, "owner_id": actor["id"]})
        task = _task_from_row(task_row, checkout_row=checkout_row, now=now)
        return _decorate_task_with_dependency_stats(task, dep_stats.get(task_id, {"total": 0, "open": 0}))

    @app.post("/api/tasks/{task_id}/release", response_model=ProjectTaskOut)
    async def release_task_checkout(request: Request, task_id: str, payload: ProjectTaskReleaseIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)
        owner_mode = str(access.get("mode") or "") == "owner"
        now = int(time.time())

        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            task_row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
            if not task_row:
                raise HTTPException(404, "Task not found")

            existing = conn.execute(
                "SELECT * FROM project_task_checkouts WHERE task_id = ? LIMIT 1",
                (task_id,),
            ).fetchone()
            if existing:
                is_owner = (
                    str(existing["owner_type"] or "") == actor["type"]
                    and str(existing["owner_id"] or "") == actor["id"]
                )
                if not is_owner and not owner_mode and not bool(payload.force):
                    raise HTTPException(403, "Only checkout owner (or project owner) can release this checkout")
                conn.execute("DELETE FROM project_task_checkouts WHERE task_id = ?", (task_id,))
                conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
                append_project_activity_log_entry(
                    conn,
                    project_id=project_id,
                    actor_type=actor["type"],
                    actor_id=actor["id"],
                    actor_label=actor["label"],
                    event_type="task.release",
                    summary=f"Task checkout released: {task_id}",
                    payload={"task_id": task_id, "force": bool(payload.force), "reason": str(payload.reason or "").strip()[:300]},
                    created_at=now,
                )
            task_row = conn.execute("SELECT * FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
            dep_stats = _dependency_stats_by_task(conn, project_id=project_id, task_ids=[task_id])
            conn.commit()
        except HTTPException:
            conn.rollback()
            conn.close()
            raise
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to release task checkout: {detail_to_text(exc)}")
        conn.close()

        await emit(project_id, "project.task.release", {"task_id": task_id})
        task = _task_from_row(task_row, checkout_row=None, now=now)
        return _decorate_task_with_dependency_stats(task, dep_stats.get(task_id, {"total": 0, "open": 0}))

    @app.get("/api/tasks/{task_id}/dependencies", response_model=List[ProjectTaskDependencyOut])
    async def list_task_dependencies(request: Request, task_id: str):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)

        conn = db()
        try:
            _assert_task_row_in_project(conn, project_id=project_id, task_id=task_id)
            rows = conn.execute(
                """
                SELECT d.task_id, d.depends_on_task_id, t.title AS depends_on_title, t.status AS depends_on_status, d.created_at
                FROM project_task_dependencies d
                JOIN project_tasks t
                    ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
                WHERE d.project_id = ? AND d.task_id = ?
                ORDER BY d.created_at ASC
                """,
                (project_id, task_id),
            ).fetchall()
        finally:
            conn.close()
        return [
            ProjectTaskDependencyOut(
                task_id=str(r["task_id"]),
                depends_on_task_id=str(r["depends_on_task_id"]),
                depends_on_title=str(r["depends_on_title"] or ""),
                depends_on_status=_coerce_task_status(r["depends_on_status"]),
                created_at=_safe_int(r["created_at"]),
            )
            for r in rows
        ]
    @app.post("/api/tasks/{task_id}/dependencies", response_model=ProjectTaskDependencyOut)
    async def add_task_dependency(request: Request, task_id: str, payload: ProjectTaskDependencyCreateIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)

        depends_on_task_id = str(payload.depends_on_task_id or "").strip()
        if not depends_on_task_id:
            raise HTTPException(400, "depends_on_task_id is required")
        if depends_on_task_id == task_id:
            raise HTTPException(400, "Task cannot depend on itself")

        now = int(time.time())
        conn = db()
        created = False
        try:
            conn.execute("BEGIN IMMEDIATE")
            _assert_task_row_in_project(conn, project_id=project_id, task_id=task_id)
            _assert_task_row_in_project(conn, project_id=project_id, task_id=depends_on_task_id)

            dep_row = conn.execute(
                """
                SELECT d.task_id, d.depends_on_task_id, t.title AS depends_on_title, t.status AS depends_on_status, d.created_at
                FROM project_task_dependencies d
                JOIN project_tasks t
                    ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
                WHERE d.project_id = ? AND d.task_id = ? AND d.depends_on_task_id = ?
                LIMIT 1
                """,
                (project_id, task_id, depends_on_task_id),
            ).fetchone()
            if dep_row:
                conn.commit()
            else:
                if _would_create_dependency_cycle(conn, project_id=project_id, task_id=task_id, depends_on_task_id=depends_on_task_id):
                    raise HTTPException(409, "Dependency would create a cycle")

                conn.execute(
                    """
                    INSERT INTO project_task_dependencies (
                        project_id, task_id, depends_on_task_id, created_at
                    ) VALUES (?,?,?,?)
                    """,
                    (project_id, task_id, depends_on_task_id, now),
                )
                conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
                dep_row = conn.execute(
                    """
                    SELECT d.task_id, d.depends_on_task_id, t.title AS depends_on_title, t.status AS depends_on_status, d.created_at
                    FROM project_task_dependencies d
                    JOIN project_tasks t
                        ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
                    WHERE d.project_id = ? AND d.task_id = ? AND d.depends_on_task_id = ?
                    LIMIT 1
                    """,
                    (project_id, task_id, depends_on_task_id),
                ).fetchone()
                append_project_activity_log_entry(
                    conn,
                    project_id=project_id,
                    actor_type=actor["type"],
                    actor_id=actor["id"],
                    actor_label=actor["label"],
                    event_type="task.dependency.added",
                    summary=f"Task dependency added: {task_id} -> {depends_on_task_id}",
                    payload={"task_id": task_id, "depends_on_task_id": depends_on_task_id},
                    created_at=now,
                )
                conn.commit()
                created = True
        except HTTPException:
            conn.rollback()
            conn.close()
            raise
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to add dependency: {detail_to_text(exc)}")
        conn.close()

        if created:
            await emit(project_id, "project.task.dependency.added", {"task_id": task_id, "depends_on_task_id": depends_on_task_id})
        if not dep_row:
            raise HTTPException(500, "Failed to create dependency")
        return ProjectTaskDependencyOut(
            task_id=str(dep_row["task_id"]),
            depends_on_task_id=str(dep_row["depends_on_task_id"]),
            depends_on_title=str(dep_row["depends_on_title"] or ""),
            depends_on_status=_coerce_task_status(dep_row["depends_on_status"]),
            created_at=_safe_int(dep_row["created_at"]),
        )
    @app.delete("/api/tasks/{task_id}/dependencies/{depends_on_task_id}")
    async def remove_task_dependency(request: Request, task_id: str, depends_on_task_id: str):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)

        dep_id = str(depends_on_task_id or "").strip()
        if not dep_id:
            raise HTTPException(400, "depends_on_task_id is required")

        now = int(time.time())
        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT 1
                FROM project_task_dependencies
                WHERE project_id = ? AND task_id = ? AND depends_on_task_id = ?
                LIMIT 1
                """,
                (project_id, task_id, dep_id),
            ).fetchone()
            if not existing:
                raise HTTPException(404, "Dependency not found")

            conn.execute(
                "DELETE FROM project_task_dependencies WHERE project_id = ? AND task_id = ? AND depends_on_task_id = ?",
                (project_id, task_id, dep_id),
            )
            conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
            append_project_activity_log_entry(
                conn,
                project_id=project_id,
                actor_type=actor["type"],
                actor_id=actor["id"],
                actor_label=actor["label"],
                event_type="task.dependency.removed",
                summary=f"Task dependency removed: {task_id} -> {dep_id}",
                payload={"task_id": task_id, "depends_on_task_id": dep_id},
                created_at=now,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            conn.close()
            raise
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to remove dependency: {detail_to_text(exc)}")
        conn.close()

        await emit(project_id, "project.task.dependency.removed", {"task_id": task_id, "depends_on_task_id": dep_id})
        return {"ok": True, "task_id": task_id, "depends_on_task_id": dep_id}
    @app.get("/api/projects/{project_id}/task-blueprints", response_model=List[ProjectTaskBlueprintOut])
    async def list_task_blueprints(request: Request, project_id: str):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)
        return [
            ProjectTaskBlueprintOut(
                id=str(item.get("id") or ""),
                name=str(item.get("name") or ""),
                description=str(item.get("description") or ""),
                tasks_count=len(item.get("tasks") or []),
            )
            for item in TASK_BLUEPRINTS
        ]

    @app.post("/api/projects/{project_id}/tasks/apply-blueprint", response_model=ProjectTaskBlueprintApplyOut)
    async def apply_task_blueprint(request: Request, project_id: str, payload: ProjectTaskBlueprintApplyIn):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)

        blueprint = _get_task_blueprint(payload.blueprint_id)
        if not blueprint:
            raise HTTPException(404, "Task blueprint not found")

        now = int(time.time())
        title_prefix = str(payload.title_prefix or "").strip()[:80]
        include_dependencies = bool(payload.include_dependencies)

        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            assignee = _assert_assignee_exists(conn, project_id=project_id, assignee_agent_id=payload.assignee_agent_id)

            created_task_ids: List[str] = []
            for idx, spec in enumerate(blueprint.get("tasks") or []):
                task_id = new_id("tsk")
                base_title = str(spec.get("title") or "Task").strip()[:TASK_TITLE_MAX_CHARS]
                full_title = f"{title_prefix} {base_title}".strip()[:TASK_TITLE_MAX_CHARS] if title_prefix else base_title
                description = str(spec.get("description") or "")[:TASK_DESCRIPTION_MAX_CHARS]
                priority = _coerce_task_priority(spec.get("priority"), required=False)
                metadata = {
                    "blueprint_id": str(blueprint.get("id") or ""),
                    "blueprint_step": idx + 1,
                }
                conn.execute(
                    """
                    INSERT INTO project_tasks (
                        id, project_id, created_by_user_id, created_by_agent_id,
                        title, description, status, priority, assignee_agent_id,
                        due_at, weight_pct, metadata_json, created_at, updated_at, closed_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        task_id,
                        project_id,
                        actor["id"] if actor["type"] == "user" else None,
                        actor["id"] if actor["type"] == "project_agent" else None,
                        full_title,
                        description,
                        TASK_STATUS_TODO,
                        priority,
                        assignee,
                        None,
                        0,
                        _dump_json(metadata),
                        now,
                        now,
                        None,
                    ),
                )
                created_task_ids.append(task_id)

            if include_dependencies:
                for pair in blueprint.get("dependencies") or []:
                    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                        continue
                    task_idx = _safe_int(pair[0], -1)
                    depends_on_idx = _safe_int(pair[1], -1)
                    if task_idx < 0 or depends_on_idx < 0:
                        continue
                    if task_idx >= len(created_task_ids) or depends_on_idx >= len(created_task_ids):
                        continue
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO project_task_dependencies (
                            project_id, task_id, depends_on_task_id, created_at
                        ) VALUES (?,?,?,?)
                        """,
                        (project_id, created_task_ids[task_idx], created_task_ids[depends_on_idx], now),
                    )

            append_project_activity_log_entry(
                conn,
                project_id=project_id,
                actor_type=actor["type"],
                actor_id=actor["id"],
                actor_label=actor["label"],
                event_type="task.blueprint.applied",
                summary=f"Task blueprint applied: {str(blueprint.get('name') or blueprint.get('id') or 'blueprint')}",
                payload={
                    "blueprint_id": str(blueprint.get("id") or ""),
                    "created_task_ids": created_task_ids,
                    "include_dependencies": include_dependencies,
                },
                created_at=now,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            conn.close()
            raise
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to apply task blueprint: {detail_to_text(exc)}")
        conn.close()

        await emit(
            project_id,
            "project.task.blueprint.applied",
            {
                "blueprint_id": str(blueprint.get("id") or ""),
                "task_ids": created_task_ids,
                "created_count": len(created_task_ids),
            },
        )
        return ProjectTaskBlueprintApplyOut(
            ok=True,
            project_id=project_id,
            blueprint_id=str(blueprint.get("id") or ""),
            created_task_ids=created_task_ids,
            created_count=len(created_task_ids),
        )
    @app.get("/api/tasks/{task_id}/comments", response_model=List[ProjectTaskCommentOut])
    async def list_task_comments(request: Request, task_id: str, limit: int = 200):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)
        cap = max(1, min(int(limit or 200), 400))

        conn = db()
        rows = conn.execute(
            """
            SELECT id, task_id, project_id, author_type, author_id, author_label, body, created_at, updated_at
            FROM project_task_comments
            WHERE task_id = ?
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (task_id, cap),
        ).fetchall()
        conn.close()
        return [_task_comment_from_row(r) for r in rows]

    @app.post("/api/tasks/{task_id}/comments", response_model=ProjectTaskCommentOut)
    async def create_task_comment(request: Request, task_id: str, payload: ProjectTaskCommentCreateIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)
        now = int(time.time())

        conn = db()
        task_row = conn.execute("SELECT id FROM project_tasks WHERE id = ? LIMIT 1", (task_id,)).fetchone()
        if not task_row:
            conn.close()
            raise HTTPException(404, "Task not found")

        comment_id = new_id("tcm")
        comment_text = str(payload.body or "").strip()
        if not comment_text:
            conn.close()
            raise HTTPException(400, "Comment body cannot be empty")
        conn.execute(
            """
            INSERT INTO project_task_comments (
                id, task_id, project_id, author_type, author_id, author_label, body, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                comment_id,
                task_id,
                project_id,
                actor["type"],
                actor["id"] or None,
                actor["label"] or None,
                comment_text,
                now,
                now,
            ),
        )
        conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
        append_project_activity_log_entry(
            conn,
            project_id=project_id,
            actor_type=actor["type"],
            actor_id=actor["id"],
            actor_label=actor["label"],
            event_type="task.comment",
            summary=f"Task comment added: {task_id}",
            payload={"task_id": task_id, "comment_id": comment_id},
            created_at=now,
        )
        conn.commit()
        conn.close()

        await emit(project_id, "project.task.comment", {"task_id": task_id, "comment_id": comment_id})
        return ProjectTaskCommentOut(
            id=comment_id,
            task_id=task_id,
            project_id=project_id,
            author_type=actor["type"],
            author_id=actor["id"] or None,
            author_label=actor["label"] or None,
            body=comment_text,
            created_at=now,
            updated_at=now,
        )

    @app.patch("/api/tasks/{task_id}/comments/{comment_id}", response_model=ProjectTaskCommentOut)
    async def update_task_comment(request: Request, task_id: str, comment_id: str, payload: ProjectTaskCommentUpdateIn):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)
        now = int(time.time())
        comment_text = str(payload.body or "").strip()
        if not comment_text:
            raise HTTPException(400, "Comment body cannot be empty")

        conn = db()
        comment_row = conn.execute(
            """
            SELECT id, task_id, project_id, author_type, author_id, author_label, body, created_at, updated_at
            FROM project_task_comments
            WHERE id = ? AND task_id = ?
            LIMIT 1
            """,
            (comment_id, task_id),
        ).fetchone()
        if not comment_row:
            conn.close()
            raise HTTPException(404, "Comment not found")
        if not _can_mutate_task_comment(access=access, actor=actor, comment_row=comment_row):
            conn.close()
            raise HTTPException(403, "Only comment author (or project owner) can edit this comment")

        conn.execute(
            "UPDATE project_task_comments SET body = ?, updated_at = ? WHERE id = ?",
            (comment_text, now, comment_id),
        )
        conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
        append_project_activity_log_entry(
            conn,
            project_id=project_id,
            actor_type=actor["type"],
            actor_id=actor["id"],
            actor_label=actor["label"],
            event_type="task.comment.updated",
            summary=f"Task comment updated: {task_id}",
            payload={"task_id": task_id, "comment_id": comment_id},
            created_at=now,
        )
        updated_row = conn.execute(
            """
            SELECT id, task_id, project_id, author_type, author_id, author_label, body, created_at, updated_at
            FROM project_task_comments
            WHERE id = ?
            LIMIT 1
            """,
            (comment_id,),
        ).fetchone()
        conn.commit()
        conn.close()
        if not updated_row:
            raise HTTPException(404, "Comment not found")

        await emit(project_id, "project.task.comment.updated", {"task_id": task_id, "comment_id": comment_id})
        return _task_comment_from_row(updated_row)

    @app.delete("/api/tasks/{task_id}/comments/{comment_id}")
    async def delete_task_comment(request: Request, task_id: str, comment_id: str):
        project_id = _resolve_task_project_id(task_id)
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        _require_task_write_access(access)
        actor = _actor_from_access(access)
        now = int(time.time())

        conn = db()
        comment_row = conn.execute(
            """
            SELECT id, task_id, project_id, author_type, author_id, author_label, body, created_at, updated_at
            FROM project_task_comments
            WHERE id = ? AND task_id = ?
            LIMIT 1
            """,
            (comment_id, task_id),
        ).fetchone()
        if not comment_row:
            conn.close()
            raise HTTPException(404, "Comment not found")
        if not _can_mutate_task_comment(access=access, actor=actor, comment_row=comment_row):
            conn.close()
            raise HTTPException(403, "Only comment author (or project owner) can delete this comment")

        conn.execute("DELETE FROM project_task_comments WHERE id = ?", (comment_id,))
        conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
        append_project_activity_log_entry(
            conn,
            project_id=project_id,
            actor_type=actor["type"],
            actor_id=actor["id"],
            actor_label=actor["label"],
            event_type="task.comment.deleted",
            summary=f"Task comment deleted: {task_id}",
            payload={"task_id": task_id, "comment_id": comment_id},
            created_at=now,
        )
        conn.commit()
        conn.close()

        await emit(project_id, "project.task.comment.deleted", {"task_id": task_id, "comment_id": comment_id})
        return {"ok": True, "task_id": task_id, "comment_id": comment_id}

    @app.get("/api/projects/{project_id}/activity", response_model=List[ProjectActivityEventOut])
    async def list_project_activity(
        request: Request,
        project_id: str,
        limit: int = 60,
        before: Optional[int] = None,
        event_type: Optional[str] = None,
    ):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_task_read_access(access)

        cap = max(1, min(int(limit or 60), PROJECT_ACTIVITY_MAX_LIMIT))
        before_ts = _safe_int(before, 0) if before is not None else 0
        event_filter = str(event_type or "").strip()

        conn = db()
        sql = (
            """
            SELECT id, project_id, actor_type, actor_id, actor_label, event_type, summary, payload_json, created_at
            FROM project_activity_log
            WHERE project_id = ?
            """
        )
        params: List[Any] = [project_id]
        if before_ts > 0:
            sql += " AND created_at < ?"
            params.append(before_ts)
        if event_filter:
            sql += " AND event_type = ?"
            params.append(event_filter)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(cap)

        rows = conn.execute(sql, tuple(params)).fetchall()
        conn.close()
        return [
            ProjectActivityEventOut(
                id=str(r["id"]),
                project_id=str(r["project_id"]),
                actor_type=str(r["actor_type"] or ""),
                actor_id=str(r["actor_id"] or "").strip() or None,
                actor_label=str(r["actor_label"] or "").strip() or None,
                event_type=str(r["event_type"] or ""),
                summary=str(r["summary"] or ""),
                payload=_parse_json_object(r["payload_json"]),
                created_at=_safe_int(r["created_at"]),
            )
            for r in rows
        ]


__all__ = [name for name in globals() if not name.startswith('__')]













