from schemas import *

def _bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "").strip()
    if token:
        return token
    return str(request.cookies.get(SESSION_COOKIE_NAME) or "").strip()

def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=str(token or "").strip(),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=SESSION_COOKIE_MAX_AGE_SEC,
        path="/",
    )

def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/", samesite="lax")

def get_optional_session_user(request: Request) -> Optional[str]:
    token = _bearer_token(request)
    if not token:
        return None
    conn = db()
    row = conn.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid session token")
    return row["user_id"]

def get_session_user(request: Request) -> str:
    token = _bearer_token(request)
    if not token:
        raise HTTPException(401, "Missing Authorization: Bearer <token>")
    conn = db()
    row = conn.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid session token")
    return row["user_id"]

def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(10)}"

def _project_out_from_row(row: sqlite3.Row) -> ProjectOut:
    payload = dict(row)
    setup_raw = payload.pop("setup_json", None)
    payload["setup_details"] = _normalize_setup_details(_parse_setup_json(setup_raw))
    payload["plan_status"] = str(payload.get("plan_status") or PLAN_STATUS_PENDING)
    payload["plan_text"] = str(payload.get("plan_text") or "")
    payload["plan_updated_at"] = payload.get("plan_updated_at")
    payload["plan_approved_at"] = payload.get("plan_approved_at")
    payload["execution_status"] = _coerce_execution_status(payload.get("execution_status"))
    payload["progress_pct"] = _clamp_progress(payload.get("progress_pct"))
    payload["execution_updated_at"] = payload.get("execution_updated_at")
    payload["usage_prompt_tokens"] = max(0, _to_int(payload.get("usage_prompt_tokens")))
    payload["usage_completion_tokens"] = max(0, _to_int(payload.get("usage_completion_tokens")))
    payload["usage_total_tokens"] = max(0, _to_int(payload.get("usage_total_tokens")))
    payload["usage_updated_at"] = payload.get("usage_updated_at")
    return ProjectOut(**payload)

def _resolve_project_workspace_access(request: Request, project_id: str) -> Dict[str, Any]:
    session_user = get_optional_session_user(request)
    conn = db()
    project = conn.execute(
        "SELECT id, user_id, project_root, workspace_root FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not project:
        conn.close()
        raise HTTPException(404, "Project not found")

    if session_user and project["user_id"] == session_user:
        conn.close()
        return {"mode": "owner", "project": dict(project), "user_id": session_user}

    agent_id = (request.headers.get("X-Project-Agent-Id") or "").strip()
    agent_token = (request.headers.get("X-Project-Agent-Token") or "").strip()
    if agent_id and agent_token:
        token_hash = _hash_access_token(agent_token)
        row = conn.execute(
            """
            SELECT 1
            FROM project_agents pa
            JOIN project_agent_access_tokens pat
                ON pat.project_id = pa.project_id AND pat.agent_id = pa.agent_id
            WHERE pa.project_id = ? AND pa.agent_id = ? AND pat.token_hash = ?
            """,
            (project_id, agent_id, token_hash),
        ).fetchone()
        if row:
            conn.close()
            return {"mode": "agent", "project": dict(project), "agent_id": agent_id}

    conn.close()
    if session_user:
        raise HTTPException(403, "Only project owner or invited agent can access this folder")
    raise HTTPException(401, "Missing authorization. Use owner token or agent access headers.")

def _clean_relative_project_path(raw_path: Optional[str]) -> str:
    raw = str(raw_path or "").strip().replace("\\", "/")
    while raw.startswith("/"):
        raw = raw[1:]
    return raw

def _resolve_project_relative_path(
    owner_user_id: str,
    project_root: str,
    relative_path: Optional[str],
    *,
    require_exists: bool = True,
    require_dir: bool = False,
) -> Path:
    project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    rel = _clean_relative_project_path(relative_path)
    target = (project_dir / rel).resolve() if rel else project_dir
    if not _path_within(target, project_dir):
        raise HTTPException(400, "Path is outside project root")
    if require_exists and not target.exists():
        raise HTTPException(404, "Path not found")
    if require_dir and target.exists() and not target.is_dir():
        raise HTTPException(400, "Path is not a directory")
    return target

def _resolve_workspace_relative_path(
    user_id: str,
    relative_path: Optional[str],
    *,
    require_exists: bool = True,
    require_dir: bool = False,
) -> Tuple[Path, Path]:
    workspace_root = _user_home_dir(user_id).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    rel = _clean_relative_project_path(relative_path)
    target = (workspace_root / rel).resolve() if rel else workspace_root
    if not _path_within(target, workspace_root):
        raise HTTPException(400, "Path is outside workspace root")
    if require_exists and not target.exists():
        raise HTTPException(404, "Path not found")
    if require_dir and target.exists() and not target.is_dir():
        raise HTTPException(400, "Path is not a directory")
    return workspace_root, target

__all__ = [name for name in globals() if not name.startswith('__')]
