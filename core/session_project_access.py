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


def _default_project_agent_write_paths(source_type: str, agent_id: str) -> List[str]:
    source = str(source_type or "owner").strip().lower() or "owner"
    if source == "external":
        safe_agent = _safe_agent_filename(agent_id)
        return [f"{USER_OUTPUTS_DIRNAME}/external/{safe_agent}"]
    return ["*"]


def _normalize_permission_write_paths(raw_paths: Any, *, fallback: Optional[List[str]] = None) -> List[str]:
    candidate_values: List[Any] = []
    if isinstance(raw_paths, list):
        candidate_values = raw_paths
    elif isinstance(raw_paths, tuple):
        candidate_values = list(raw_paths)
    elif isinstance(raw_paths, str):
        text = raw_paths.strip()
        if text:
            parsed: Any = None
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                candidate_values = parsed
            else:
                candidate_values = [text]
    elif raw_paths is not None:
        candidate_values = [raw_paths]

    cleaned: List[str] = []
    seen: set[str] = set()
    for item in candidate_values:
        raw_item = str(item or "").strip()
        if raw_item in {"*", "all", "project", "project_root"}:
            if "*" not in seen:
                seen.add("*")
                cleaned.append("*")
            continue
        rel = _clean_relative_project_path(raw_item)
        if not rel:
            continue
        rel = _remap_legacy_project_doc_rel_path(rel)
        if rel.lower() == LEGACY_OUTPUTS_DIRNAME:
            rel = USER_OUTPUTS_DIRNAME
        elif _rel_path_startswith(rel, LEGACY_OUTPUTS_DIRNAME):
            suffix = rel[len(LEGACY_OUTPUTS_DIRNAME) :].lstrip("/\\")
            rel = _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
        path_parts = [p for p in Path(rel).parts if p not in {"", "."}]
        if any(p == ".." for p in path_parts):
            continue
        low = rel.lower()
        if low in seen:
            continue
        seen.add(low)
        cleaned.append(rel)
        if len(cleaned) >= 40:
            break

    if cleaned:
        return cleaned
    if fallback is not None:
        return _normalize_permission_write_paths(fallback, fallback=None)
    return []


def _default_project_agent_permissions(*, source_type: str, agent_id: str) -> Dict[str, Any]:
    return {
        "can_chat_project": True,
        "can_read_files": True,
        "can_write_files": True,
        "write_paths": _normalize_permission_write_paths(
            _default_project_agent_write_paths(source_type, agent_id),
            fallback=None,
        ),
        "has_custom": False,
    }


def _get_project_agent_permissions(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    agent_id: str,
    source_type: str,
) -> Dict[str, Any]:
    defaults = _default_project_agent_permissions(source_type=source_type, agent_id=agent_id)
    row = conn.execute(
        """
        SELECT can_chat_project, can_read_files, can_write_files, write_paths_json
        FROM project_agent_permissions
        WHERE project_id = ? AND agent_id = ?
        """,
        (project_id, agent_id),
    ).fetchone()
    if not row:
        return defaults
    write_paths = _normalize_permission_write_paths(row["write_paths_json"], fallback=defaults["write_paths"])
    source = str(source_type or "owner").strip().lower() or "owner"
    legacy_owner_defaults = _normalize_permission_write_paths(
        [USER_OUTPUTS_DIRNAME, PROJECT_INFO_DIRNAME, "agents", "logs"],
        fallback=None,
    )
    if source != "external" and write_paths == legacy_owner_defaults:
        write_paths = ["*"]
    return {
        "can_chat_project": bool(_to_int(row["can_chat_project"])),
        "can_read_files": bool(_to_int(row["can_read_files"])),
        "can_write_files": bool(_to_int(row["can_write_files"])),
        "write_paths": write_paths,
        "has_custom": True,
    }


def _project_path_allowed_for_agent(rel_path: str, allow_paths: List[str]) -> bool:
    rel = _clean_relative_project_path(rel_path)
    if not rel:
        return False
    rel = _remap_legacy_project_doc_rel_path(rel)
    normalized_allow = _normalize_permission_write_paths(allow_paths, fallback=[])
    if not normalized_allow:
        return False
    if "*" in normalized_allow:
        return True
    return any(_rel_path_startswith(rel, root) for root in normalized_allow)

def _parse_a2a_session_scopes(raw_scopes: Any) -> List[str]:
    if isinstance(raw_scopes, list):
        return [str(s).strip() for s in raw_scopes if str(s).strip()]
    text = str(raw_scopes or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(s).strip() for s in parsed if str(s).strip()]


def _resolve_optional_a2a_agent_session(
    request: Request,
    *,
    required_scope: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    token = str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip()
    if not token:
        return None
    expected_agent_id = str(request.headers.get(ENV_AGENT_ID_HEADER) or "").strip()
    now = int(time.time())
    token_hash = _hash_access_token(token)

    conn = db()
    row = conn.execute(
        """
        SELECT eas.id, eas.env_id, eas.agent_id, eas.scopes_json, eas.status, eas.expires_at,
               e.owner_user_id
        FROM environment_agent_sessions eas
        JOIN environments e ON e.id = eas.env_id
        WHERE eas.token_hash = ?
        ORDER BY eas.created_at DESC
        LIMIT 1
        """,
        (token_hash,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(401, "Invalid A2A agent session")

    agent_id = str(row["agent_id"] or "").strip()
    if expected_agent_id and expected_agent_id != agent_id:
        conn.close()
        raise HTTPException(403, "A2A agent header does not match session agent")

    status = str(row["status"] or "").strip().lower()
    if status not in {ENV_AGENT_SESSION_STATUS_ACTIVE, ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING}:
        conn.close()
        raise HTTPException(403, "A2A agent session is not active")

    expires_at = _to_int(row["expires_at"])
    if expires_at <= now:
        conn.execute(
            "UPDATE environment_agent_sessions SET status = ?, revoked_at = ? WHERE id = ?",
            (ENV_AGENT_SESSION_STATUS_EXPIRED, now, str(row["id"])),
        )
        conn.commit()
        conn.close()
        raise HTTPException(401, "A2A agent session expired")

    scopes = _parse_a2a_session_scopes(row["scopes_json"])
    if required_scope and required_scope not in scopes and "*" not in scopes:
        conn.close()
        raise HTTPException(403, f"Missing required scope: {required_scope}")

    user_id = str(row["owner_user_id"] or "").strip()
    if not user_id:
        conn.close()
        raise HTTPException(409, "Environment is not claimed by a user yet")

    conn.execute(
        "UPDATE environment_agent_sessions SET last_seen_at = ? WHERE id = ?",
        (now, str(row["id"])),
    )
    conn.commit()
    conn.close()
    return {
        "session_id": str(row["id"] or "").strip(),
        "env_id": str(row["env_id"] or "").strip(),
        "agent_id": agent_id,
        "user_id": user_id,
        "scopes": scopes,
        "status": status,
        "expires_at": expires_at,
    }


def _require_project_read_access(access: Dict[str, Any]) -> None:
    if str(access.get("mode") or "") == "owner":
        return
    perms = access.get("permissions") or {}
    if not bool(perms.get("can_read_files")):
        raise HTTPException(403, "This project agent cannot read project files")


def _require_project_write_access(access: Dict[str, Any], rel_path: str) -> None:
    if str(access.get("mode") or "") == "owner":
        return
    perms = access.get("permissions") or {}
    if not bool(perms.get("can_write_files")):
        raise HTTPException(403, "This project agent cannot write project files")
    allow_paths = perms.get("write_paths") or []
    if not _project_path_allowed_for_agent(rel_path, allow_paths):
        raise HTTPException(403, "Path is outside allowed write paths for this project agent")


def _require_project_chat_access(access: Dict[str, Any]) -> None:
    if str(access.get("mode") or "") == "owner":
        return
    perms = access.get("permissions") or {}
    if not bool(perms.get("can_chat_project")):
        raise HTTPException(403, "This project agent cannot use project chat")


def _resolve_project_workspace_access(
    request: Request,
    project_id: str,
    *,
    required_scope: Optional[str] = None,
) -> Dict[str, Any]:
    session_user: Optional[str] = None
    bearer_raw = _bearer_token(request)
    has_project_agent_headers = bool(
        str(request.headers.get("X-Project-Agent-Id") or "").strip()
        and str(request.headers.get("X-Project-Agent-Token") or "").strip()
    )
    bearer_matches_agent_token = False
    if bearer_raw and not has_project_agent_headers:
        # Agents sometimes send the project agent token via Authorization: Bearer
        # instead of the X-Project-Agent-Token header. Treat that as valid project
        # agent auth so we don't 401 a request that has the right credentials in
        # the wrong header.
        _conn = db()
        _row = _conn.execute(
            "SELECT 1 FROM project_agent_access_tokens WHERE project_id = ? AND token_hash = ? LIMIT 1",
            (project_id, _hash_access_token(bearer_raw)),
        ).fetchone()
        _conn.close()
        bearer_matches_agent_token = bool(_row)
    try:
        session_user = get_optional_session_user(request)
    except HTTPException:
        if (
            not str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip()
            and not has_project_agent_headers
            and not bearer_matches_agent_token
        ):
            raise
        session_user = None
    a2a_access = _resolve_optional_a2a_agent_session(request, required_scope=required_scope or "env.read")
    conn = db()
    project = conn.execute(
        "SELECT id, user_id, project_root, workspace_root FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not project:
        conn.close()
        raise HTTPException(404, "Project not found")

    project_owner_user_id = str(project["user_id"] or "").strip()

    if session_user and project_owner_user_id == session_user:
        conn.close()
        return {
            "mode": "owner",
            "project": dict(project),
            "user_id": session_user,
            "agent_id": None,
            "source_type": "owner",
            "permissions": {
                "can_chat_project": True,
                "can_read_files": True,
                "can_write_files": True,
                "write_paths": _normalize_permission_write_paths(
                    [USER_OUTPUTS_DIRNAME, PROJECT_INFO_DIRNAME, "agents", "logs"],
                    fallback=None,
                ),
                "has_custom": False,
            },
        }

    if session_user:
        member_rows = conn.execute(
            """
            SELECT pem.id AS membership_id,
                   pem.agent_id,
                   pem.member_connection_id,
                   COALESCE(pa.source_type, 'external') AS source_type
            FROM project_external_agent_memberships pem
            JOIN project_agents pa
                ON pa.project_id = pem.project_id AND pa.agent_id = pem.agent_id
            WHERE pem.project_id = ? AND pem.member_user_id = ? AND pem.status = 'active'
            ORDER BY pem.updated_at DESC, pem.created_at DESC
            """,
            (project_id, session_user),
        ).fetchall()
        if member_rows:
            requested_member_agent_id = (request.headers.get("X-Project-Agent-Id") or "").strip()
            selected: Optional[sqlite3.Row] = None
            if requested_member_agent_id:
                selected = next(
                    (
                        r
                        for r in member_rows
                        if str(r["agent_id"] or "").strip() == requested_member_agent_id
                    ),
                    None,
                )
                if not selected:
                    conn.close()
                    raise HTTPException(403, "This user is not an active member for the requested project agent")
            else:
                selected = member_rows[0]

            member_agent_id = str(selected["agent_id"] or "").strip()
            source_type = str(selected["source_type"] or "external").strip() or "external"
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=member_agent_id,
                source_type=source_type,
            )
            conn.close()
            return {
                "mode": "member",
                "project": dict(project),
                "user_id": session_user,
                "membership_id": str(selected["membership_id"] or "").strip() or None,
                "member_connection_id": str(selected["member_connection_id"] or "").strip() or None,
                "agent_id": member_agent_id,
                "source_type": source_type,
                "auth_mode": "user_session",
                "permissions": perms,
            }

    if a2a_access:
        a2a_env_id = str(a2a_access.get("env_id") or "").strip()
        a2a_user_id = str(a2a_access.get("user_id") or "").strip()
        a2a_agent_id = str(a2a_access.get("agent_id") or "").strip()
        row = conn.execute(
            """
            SELECT pem.id AS membership_id,
                   pem.member_connection_id,
                   COALESCE(pa.source_type, 'external') AS source_type
            FROM project_external_agent_memberships pem
            JOIN project_agents pa
                ON pa.project_id = pem.project_id AND pa.agent_id = pem.agent_id
            JOIN openclaw_connections oc
                ON oc.id = pem.member_connection_id
            WHERE pem.project_id = ?
              AND pem.member_user_id = ?
              AND pem.agent_id = ?
              AND pem.status = 'active'
              AND oc.env_id = ?
            ORDER BY pem.updated_at DESC, pem.created_at DESC
            LIMIT 1
            """,
            (project_id, a2a_user_id, a2a_agent_id, a2a_env_id),
        ).fetchone()
        if row:
            source_type = str(row["source_type"] or "external").strip() or "external"
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=a2a_agent_id,
                source_type=source_type,
            )
            conn.close()
            return {
                "mode": "member",
                "project": dict(project),
                "user_id": a2a_user_id,
                "membership_id": str(row["membership_id"] or "").strip() or None,
                "member_connection_id": str(row["member_connection_id"] or "").strip() or None,
                "agent_id": a2a_agent_id,
                "source_type": source_type,
                "auth_mode": "a2a_session",
                "a2a_env_id": a2a_env_id,
                "a2a_session_id": str(a2a_access.get("session_id") or "").strip() or None,
                "permissions": perms,
            }
    agent_id = (request.headers.get("X-Project-Agent-Id") or "").strip()
    agent_token = (request.headers.get("X-Project-Agent-Token") or "").strip()
    if agent_id and agent_token:
        token_hash = _hash_access_token(agent_token)
        row = conn.execute(
            """
            SELECT COALESCE(pa.source_type, 'owner') AS source_type
            FROM project_agents pa
            JOIN project_agent_access_tokens pat
                ON pat.project_id = pa.project_id AND pat.agent_id = pa.agent_id
            WHERE pa.project_id = ? AND pa.agent_id = ? AND pat.token_hash = ?
            """,
            (project_id, agent_id, token_hash),
        ).fetchone()
        if row:
            source_type = str(row["source_type"] or "owner").strip() or "owner"
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                source_type=source_type,
            )
            conn.close()
            return {
                "mode": "agent",
                "project": dict(project),
                "agent_id": agent_id,
                "source_type": source_type,
                "auth_mode": "project_agent_token",
                "permissions": perms,
            }

    # Bearer-token fallback: an agent that sends its project token via
    # `Authorization: Bearer <token>` (instead of X-Project-Agent-Token) still
    # authenticates as that agent. The token must match a project_agent row.
    if bearer_matches_agent_token and bearer_raw:
        bearer_hash = _hash_access_token(bearer_raw)
        row = conn.execute(
            """
            SELECT pa.agent_id AS agent_id,
                   COALESCE(pa.source_type, 'owner') AS source_type
            FROM project_agents pa
            JOIN project_agent_access_tokens pat
                ON pat.project_id = pa.project_id AND pat.agent_id = pa.agent_id
            WHERE pa.project_id = ? AND pat.token_hash = ?
            LIMIT 1
            """,
            (project_id, bearer_hash),
        ).fetchone()
        if row:
            resolved_agent_id = str(row["agent_id"] or "").strip()
            source_type = str(row["source_type"] or "owner").strip() or "owner"
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=resolved_agent_id,
                source_type=source_type,
            )
            conn.close()
            return {
                "mode": "agent",
                "project": dict(project),
                "agent_id": resolved_agent_id,
                "source_type": source_type,
                "auth_mode": "project_agent_bearer",
                "permissions": perms,
            }

    conn.close()
    if session_user or a2a_access:
        raise HTTPException(403, "Only project owner or invited agent can access this folder")
    raise HTTPException(
        401,
        "Missing authorization. Use owner session, project agent headers, or A2A agent session headers.",
    )


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
