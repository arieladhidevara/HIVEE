from .security_auth import *

def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(10)}"


def _environment_home_dir(env_id: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(env_id or "").strip()).strip("-") or "env"
    return SERVER_WORKSPACES_DIR / f"env_{safe}"

def _environment_workspace_root_dir(env_id: str) -> Path:
    return _environment_home_dir(env_id) / HIVEE_ROOT

def _environment_templates_dir(env_id: str) -> Path:
    return _environment_workspace_root_dir(env_id) / "TEMPLATES"

def _environment_projects_dir(env_id: str) -> Path:
    return _environment_workspace_root_dir(env_id) / "PROJECTS"

def _user_home_dir(user_id: str) -> Path:
    return SERVER_WORKSPACES_DIR / user_id

def _user_workspace_root_dir(user_id: str) -> Path:
    return _user_home_dir(user_id) / HIVEE_ROOT

def _user_templates_dir(user_id: str) -> Path:
    return _user_workspace_root_dir(user_id) / "TEMPLATES"

def _user_projects_dir(user_id: str) -> Path:
    return _user_workspace_root_dir(user_id) / "PROJECTS"

def _safe_filesystem_token(raw: Any, fallback: str = "item", max_len: int = 72) -> str:
    token = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(raw or "").strip()).strip("-._")
    if not token:
        token = fallback
    if len(token) > max_len:
        token = token[:max_len].rstrip("-._") or fallback
    return token

def _user_agents_root_dir(user_id: str) -> Path:
    return _user_workspace_root_dir(user_id) / AGENTS_ROOT_DIRNAME

def _user_connection_agents_dir(user_id: str, connection_id: str) -> Path:
    return _user_agents_root_dir(user_id) / _safe_filesystem_token(connection_id, "connection")

def _user_agent_runtime_dir(user_id: str, connection_id: str, agent_id: str) -> Path:
    return _user_connection_agents_dir(user_id, connection_id) / _safe_filesystem_token(agent_id, "agent")

def _agent_component_paths(user_id: str, connection_id: str, agent_id: str) -> Dict[str, Path]:
    root = _user_agent_runtime_dir(user_id, connection_id, agent_id)
    return {
        "root": root,
        "card": root / "card",
        "memory": root / "memory",
        "history": root / "history",
        "checkpoints": root / "checkpoints",
        "metrics": root / "metrics",
        "approvals": root / "approvals",
    }

def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")

def _read_json_file(path: Path, fallback: Any) -> Any:
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback

def _path_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False

def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0

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

def _build_claim_url(request: Request, env_id: str, code: str) -> str:
    base = _request_origin(request)
    return (
        f"{base}/?claim_env_id={url_quote(env_id, safe='')}"
        f"&claim_code={url_quote(code, safe='')}"
    )

def _encode_rel_path_for_url_path(path: str) -> str:
    clean = str(path or "").replace("\\", "/").strip("/")
    if not clean:
        return ""
    return "/".join(url_quote(part, safe="") for part in clean.split("/") if part)

def _latest_file_relative_path(root_dir: Path, base_dir: Path) -> Optional[str]:
    if not root_dir.exists() or not root_dir.is_dir():
        return None
    latest_path: Optional[Path] = None
    latest_mtime = -1.0
    for candidate in root_dir.rglob("*"):
        if not candidate.is_file():
            continue
        try:
            mtime = float(candidate.stat().st_mtime)
        except OSError:
            continue
        if mtime >= latest_mtime:
            latest_path = candidate
            latest_mtime = mtime
    if not latest_path:
        return None
    try:
        return latest_path.relative_to(base_dir).as_posix()
    except Exception:
        return None

def _project_info_dir(project_dir: Path) -> Path:
    return (project_dir / PROJECT_INFO_DIRNAME).resolve()

def _legacy_project_doc_paths(project_dir: Path) -> List[Path]:
    names = [
        "README.md",
        "brief.md",
        "goal.md",
        "project-setup.md",
        "setup-chat-history.txt",
        "SETUP-CHAT.txt",
        "PROJECT-INFO.MD",
        "PROJECT-DELEGATION.MD",
        "overview.md",
        "project-plan.md",
        "project-delegation.md",
        "usage.md",
        "tracker.md",
        "chat-hivee.md",
        "project.md",
    ]
    out: List[Path] = []
    for name in names:
        out.append((project_dir / name).resolve())
    return out

def _rel_path_startswith(rel: str, prefix: str) -> bool:
    left = str(rel or "").strip().replace("\\", "/").strip("/")
    right = str(prefix or "").strip().replace("\\", "/").strip("/")
    if not left or not right:
        return False
    left_low = left.lower()
    right_low = right.lower()
    return left_low == right_low or left_low.startswith(right_low + "/")

def _normalize_user_outputs_prefix(raw_prefix: Optional[str]) -> str:
    raw = _clean_relative_project_path(raw_prefix or "")
    if not raw:
        return f"{USER_OUTPUTS_DIRNAME}/generated"
    if raw.lower() == LEGACY_OUTPUTS_DIRNAME:
        return USER_OUTPUTS_DIRNAME
    if _rel_path_startswith(raw, f"{LEGACY_OUTPUTS_DIRNAME}/"):
        suffix = raw[len(LEGACY_OUTPUTS_DIRNAME):].lstrip("/\\")
        return _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
    if _rel_path_startswith(raw, USER_OUTPUTS_DIRNAME):
        return raw
    return _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{raw}")

def _remap_legacy_project_doc_rel_path(rel: str) -> str:
    normalized = _clean_relative_project_path(rel)
    if not normalized:
        return normalized
    low = normalized.lower()
    legacy_map = {
        "readme.md": README_FILE,
        "brief.md": BRIEF_FILE,
        "goal.md": GOAL_FILE,
        "project-setup.md": PROJECT_SETUP_FILE,
        "setup-chat-history.txt": SETUP_CHAT_HISTORY_FILE,
        "setup-chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "set-up-chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "set_up_chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "project-info.md": PROJECT_INFO_FILE,
        "project-info.mdx": PROJECT_INFO_FILE,
        "project-info.txt": PROJECT_INFO_FILE,
        "project-info": PROJECT_INFO_FILE,
        "project-delegation.md": PROJECT_DELEGATION_FILE,
        "overview.md": OVERVIEW_FILE,
        "project-plan.md": PROJECT_PLAN_FILE,
        "usage.md": USAGE_FILE,
        "tracker.md": TRACKER_FILE,
        "chat-hivee.md": CHAT_HIVEE_FILE,
        "project.md": PROJECT_PLAN_FILE,
    }
    return legacy_map.get(low, normalized)

def _render_tree(root: Path, *, max_depth: int = 4, max_entries: int = MAX_TREE_ENTRIES) -> str:
    root = root.resolve()
    if not root.exists():
        return f"{root.name}/\n  (missing)"

    lines: List[str] = [f"{root.name}/"]
    shown = 0

    def _walk(node: Path, prefix: str, depth: int) -> None:
        nonlocal shown
        if depth > max_depth or shown >= max_entries:
            return
        try:
            children = sorted(node.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except Exception:
            lines.append(f"{prefix}(unreadable)")
            return

        for child in children:
            if shown >= max_entries:
                lines.append(f"{prefix}...")
                return
            shown += 1
            suffix = "/" if child.is_dir() else ""
            lines.append(f"{prefix}{child.name}{suffix}")
            if child.is_dir() and depth < max_depth:
                _walk(child, prefix + "  ", depth + 1)

    _walk(root, "  ", 1)
    return "\n".join(lines)[:MAX_WORKSPACE_TREE_CHARS]
def _collect_new_user_templates() -> Dict[str, Any]:
    directories: List[str] = []
    files: List[Dict[str, str]] = []
    warnings: List[str] = []
    total_payload_bytes = 0

    if not NEW_USER_ASSETS_DIR.exists():
        warnings.append(f"Directory not found: {NEW_USER_ASSETS_DIR.as_posix()}")
    else:
        for node in sorted(NEW_USER_ASSETS_DIR.rglob("*"), key=lambda p: p.as_posix()):
            rel = node.relative_to(NEW_USER_ASSETS_DIR).as_posix()
            if not rel:
                continue
            if node.is_dir():
                directories.append(rel)
                continue

            if len(files) >= MAX_TEMPLATE_FILES:
                warnings.append(f"Skipped remaining files after {MAX_TEMPLATE_FILES} items.")
                break

            try:
                raw = node.read_bytes()
            except Exception as e:
                warnings.append(f"Skipped {rel}: {str(e)}")
                continue
            if len(raw) > MAX_TEMPLATE_FILE_BYTES:
                warnings.append(f"Skipped {rel}: {len(raw)} bytes exceeds max {MAX_TEMPLATE_FILE_BYTES}.")
                continue
            if (total_payload_bytes + len(raw)) > MAX_TEMPLATE_PAYLOAD_BYTES:
                warnings.append(f"Skipped {rel}: aggregate payload exceeds {MAX_TEMPLATE_PAYLOAD_BYTES} bytes.")
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                warnings.append(f"Skipped {rel}: non UTF-8 file.")
                continue
            files.append({"path": rel, "content": text})
            total_payload_bytes += len(raw)

    has_setup = any((f.get("path") or "").upper() == "PROJECT-SETUP.MD" for f in files)
    if not has_setup:
        files.append({"path": "PROJECT-SETUP.MD", "content": DEFAULT_PROJECT_SETUP_MD})
        warnings.append("PROJECT-SETUP.MD missing in assets/new_user; fallback template was injected.")

    return {"directories": directories, "files": files, "warnings": warnings}


def _ensure_user_workspace(user_id: str) -> Dict[str, Any]:
    SERVER_WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
    home = _user_home_dir(user_id)
    workspace_root = _user_workspace_root_dir(user_id)
    templates_root = _user_templates_dir(user_id)
    projects_root = _user_projects_dir(user_id)
    agents_root = _user_agents_root_dir(user_id)

    home.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    projects_root.mkdir(parents=True, exist_ok=True)
    agents_root.mkdir(parents=True, exist_ok=True)

    payload = _collect_new_user_templates()
    for rel_dir in payload.get("directories") or []:
        target_dir = (templates_root / rel_dir).resolve()
        if not _path_within(target_dir, templates_root):
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
    for item in payload.get("files") or []:
        rel_path = str(item.get("path") or "").strip()
        if not rel_path:
            continue
        target_file = (templates_root / rel_path).resolve()
        if not _path_within(target_file, templates_root):
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        if not target_file.exists():
            target_file.write_text(str(item.get("content") or ""), encoding="utf-8")

    return {
        "workspace_root": workspace_root.as_posix(),
        "templates_root": templates_root.as_posix(),
        "projects_root": projects_root.as_posix(),
        "agents_root": agents_root.as_posix(),
        "workspace_tree": _render_tree(workspace_root),
        "template_warnings": payload.get("warnings") or [],
    }

def _ensure_environment_workspace(env_id: str) -> Dict[str, Any]:
    SERVER_WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
    home = _environment_home_dir(env_id)
    workspace_root = _environment_workspace_root_dir(env_id)
    templates_root = _environment_templates_dir(env_id)
    projects_root = _environment_projects_dir(env_id)
    agents_root = workspace_root / AGENTS_ROOT_DIRNAME

    home.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    projects_root.mkdir(parents=True, exist_ok=True)
    agents_root.mkdir(parents=True, exist_ok=True)

    payload = _collect_new_user_templates()
    for rel_dir in payload.get("directories") or []:
        target_dir = (templates_root / rel_dir).resolve()
        if not _path_within(target_dir, templates_root):
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
    for item in payload.get("files") or []:
        rel_path = str(item.get("path") or "").strip()
        if not rel_path:
            continue
        target_file = (templates_root / rel_path).resolve()
        if not _path_within(target_file, templates_root):
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        if not target_file.exists():
            target_file.write_text(str(item.get("content") or ""), encoding="utf-8")

    return {
        "workspace_root": workspace_root.as_posix(),
        "templates_root": templates_root.as_posix(),
        "projects_root": projects_root.as_posix(),
        "agents_root": agents_root.as_posix(),
        "workspace_tree": _render_tree(workspace_root),
        "template_warnings": payload.get("warnings") or [],
    }

def _issue_environment_claim_code(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    ttl_sec: int = ENV_CLAIM_CODE_TTL_SEC,
    created_by_agent_id: Optional[str] = None,
) -> Tuple[str, int]:
    now = int(time.time())
    ttl = max(60, min(int(ttl_sec or ENV_CLAIM_CODE_TTL_SEC), 60 * 60 * 24))
    expires_at = now + ttl
    raw_code = _new_environment_claim_code()
    conn.execute(
        """
        INSERT INTO environment_claim_codes (id, env_id, code_hash, created_at, expires_at, used_at, used_by_user_id, created_by_agent_id)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            new_id("clm"),
            env_id,
            _hash_access_token(raw_code),
            now,
            expires_at,
            None,
            None,
            (str(created_by_agent_id or "").strip() or None),
        ),
    )
    return raw_code, expires_at

def _issue_environment_agent_session(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    agent_id: str,
    scopes: List[str],
    ttl_sec: int = ENV_AGENT_SESSION_TTL_SEC,
) -> Tuple[str, int]:
    now = int(time.time())
    ttl = max(60, min(int(ttl_sec or ENV_AGENT_SESSION_TTL_SEC), 60 * 60 * 24 * 7))
    expires_at = now + ttl
    raw_token = _new_environment_agent_session_token()
    cleaned_scopes = [str(s).strip() for s in (scopes or []) if str(s).strip()]
    if not cleaned_scopes:
        cleaned_scopes = ["env.read"]
    conn.execute(
        """
        INSERT INTO environment_agent_sessions (
            id, env_id, agent_id, token_hash, scopes_json, status, created_at, expires_at, revoked_at, last_seen_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            new_id("a2a"),
            env_id,
            str(agent_id or "agent").strip() or "agent",
            _hash_access_token(raw_token),
            json.dumps(cleaned_scopes, ensure_ascii=False),
            ENV_AGENT_SESSION_STATUS_ACTIVE,
            now,
            expires_at,
            None,
            now,
        ),
    )
    return raw_token, expires_at

def _issue_environment_agent_link_token(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    agent_id: str,
    ttl_sec: int = ENV_AGENT_LINK_TOKEN_TTL_SEC,
) -> Tuple[str, int]:
    now = int(time.time())
    ttl = max(60 * 60, min(int(ttl_sec or ENV_AGENT_LINK_TOKEN_TTL_SEC), 60 * 60 * 24 * 365))
    expires_at = now + ttl
    normalized_agent = str(agent_id or "agent").strip() or "agent"
    conn.execute(
        """
        UPDATE environment_agent_links
        SET status = ?, revoked_at = ?
        WHERE env_id = ? AND agent_id = ? AND status = ?
        """,
        (
            ENV_AGENT_LINK_STATUS_REVOKED,
            now,
            env_id,
            normalized_agent,
            ENV_AGENT_LINK_STATUS_ACTIVE,
        ),
    )
    raw_token = _new_environment_agent_link_token()
    conn.execute(
        """
        INSERT INTO environment_agent_links (
            id, env_id, agent_id, token_hash, status, created_at, expires_at, revoked_at, last_used_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            new_id("alk"),
            env_id,
            normalized_agent,
            _hash_access_token(raw_token),
            ENV_AGENT_LINK_STATUS_ACTIVE,
            now,
            expires_at,
            None,
            now,
        ),
    )
    return raw_token, expires_at

def _transition_environment_agent_sessions_to_handoff(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    ttl_sec: int = ENV_AGENT_HANDOFF_TTL_SEC,
) -> int:
    now = int(time.time())
    ttl = max(60, min(int(ttl_sec or ENV_AGENT_HANDOFF_TTL_SEC), 60 * 30))
    handoff_expires_at = now + ttl
    read_only_scopes = json.dumps(["env.read", "env.handoff.wait", "env.handoff.ack"], ensure_ascii=False)
    conn.execute(
        """
        UPDATE environment_agent_sessions
        SET status = ?,
            scopes_json = ?,
            expires_at = CASE WHEN expires_at < ? THEN expires_at ELSE ? END,
            revoked_at = NULL,
            last_seen_at = ?
        WHERE env_id = ? AND status = ?
        """,
        (
            ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING,
            read_only_scopes,
            handoff_expires_at,
            handoff_expires_at,
            now,
            env_id,
            ENV_AGENT_SESSION_STATUS_ACTIVE,
        ),
    )
    return handoff_expires_at

def _parse_scopes_json(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if str(s).strip()]
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(s).strip() for s in parsed if str(s).strip()]
    except Exception:
        pass
    return []

def _resolve_environment_agent_access(
    request: Request,
    env_id: str,
    *,
    required_scope: Optional[str] = None,
) -> Dict[str, Any]:
    token = str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip()
    if not token:
        raise HTTPException(401, f"Missing {ENV_AGENT_SESSION_HEADER} header")
    token_hash = _hash_access_token(token)
    now = int(time.time())
    conn = db()
    row = conn.execute(
        """
        SELECT id, env_id, agent_id, scopes_json, status, expires_at
        FROM environment_agent_sessions
        WHERE env_id = ? AND token_hash = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (env_id, token_hash),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(401, "Invalid A2A agent session")
    session_status = str(row["status"] or "").strip().lower()
    if session_status not in {ENV_AGENT_SESSION_STATUS_ACTIVE, ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING}:
        conn.close()
        raise HTTPException(403, "A2A agent session is not active")
    if _to_int(row["expires_at"]) <= now:
        conn.execute(
            "UPDATE environment_agent_sessions SET status = ?, revoked_at = ? WHERE id = ?",
            (ENV_AGENT_SESSION_STATUS_EXPIRED, now, row["id"]),
        )
        conn.commit()
        conn.close()
        raise HTTPException(401, "A2A agent session expired")

    scopes = _parse_scopes_json(row["scopes_json"])
    if required_scope and (required_scope not in scopes) and ("*" not in scopes):
        conn.close()
        raise HTTPException(403, f"Missing required scope: {required_scope}")
    conn.execute(
        "UPDATE environment_agent_sessions SET last_seen_at = ? WHERE id = ?",
        (now, row["id"]),
    )
    conn.commit()
    conn.close()
    return {
        "session_id": str(row["id"]),
        "env_id": str(row["env_id"]),
        "agent_id": str(row["agent_id"]),
        "scopes": scopes,
        "status": session_status,
        "expires_at": _to_int(row["expires_at"]),
    }

def _get_primary_environment_for_user(user_id: str) -> Optional[Dict[str, Any]]:
    conn = db()
    row = conn.execute(
        """
        SELECT id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at, archived_at
        FROM environments
        WHERE owner_user_id = ?
        ORDER BY
            CASE
                WHEN status = ? THEN 0
                WHEN status = ? THEN 1
                ELSE 2
            END,
            COALESCE(claimed_at, created_at) DESC
        LIMIT 1
        """,
        (user_id, ENV_STATUS_ACTIVE, ENV_STATUS_PENDING_CLAIM),
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def _ensure_primary_environment_for_user(user_id: str, *, email: Optional[str] = None) -> Dict[str, Any]:
    existing = _get_primary_environment_for_user(user_id)
    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace.get("workspace_root") or _user_workspace_root_dir(user_id).as_posix())
    now = int(time.time())
    if existing:
        env_id = str(existing.get("id") or "").strip()
        conn = db()
        conn.execute(
            """
            UPDATE environments
            SET status = ?, workspace_root = ?, claimed_at = COALESCE(claimed_at, ?)
            WHERE id = ? AND owner_user_id = ?
            """,
            (ENV_STATUS_ACTIVE, workspace_root, now, env_id, user_id),
        )
        conn.commit()
        conn.close()
        existing["status"] = ENV_STATUS_ACTIVE
        existing["workspace_root"] = workspace_root
        existing["claimed_at"] = existing.get("claimed_at") or now
        return existing

    env_id = new_id("env")
    display_name = ""
    if email:
        display_name = str(email).split("@")[0].strip()[:120]
    conn = db()
    conn.execute(
        """
        INSERT INTO environments (id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at, archived_at)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            env_id,
            user_id,
            display_name or f"user-{user_id[-6:]}",
            ENV_STATUS_ACTIVE,
            workspace_root,
            now,
            now,
            None,
        ),
    )
    conn.commit()
    conn.close()
    return {
        "id": env_id,
        "owner_user_id": user_id,
        "display_name": display_name,
        "status": ENV_STATUS_ACTIVE,
        "workspace_root": workspace_root,
        "created_at": now,
        "claimed_at": now,
        "archived_at": None,
    }

def _merge_environment_workspace_into_user_workspace(env_id: str, user_id: str) -> Dict[str, Any]:
    env_root = _environment_workspace_root_dir(env_id).resolve()
    if not env_root.exists():
        return {"copied_files": 0, "copied_dirs": 0}
    user_root = _user_workspace_root_dir(user_id).resolve()
    _ensure_user_workspace(user_id)
    copied_files = 0
    copied_dirs = 0
    for node in sorted(env_root.rglob("*"), key=lambda p: p.as_posix()):
        try:
            rel = node.relative_to(env_root)
        except Exception:
            continue
        target = (user_root / rel).resolve()
        if not _path_within(target, user_root):
            continue
        if node.is_dir():
            if not target.exists():
                target.mkdir(parents=True, exist_ok=True)
                copied_dirs += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            continue
        shutil.copy2(node, target)
        copied_files += 1
    return {"copied_files": copied_files, "copied_dirs": copied_dirs}

def _project_setup_template_file(user_id: str) -> Path:
    return _user_templates_dir(user_id) / "PROJECT-SETUP.MD"

def _read_project_setup_template(user_id: str) -> str:
    _ensure_user_workspace(user_id)
    template_file = _project_setup_template_file(user_id)
    if not _path_within(template_file, _user_workspace_root_dir(user_id)):
        return DEFAULT_PROJECT_SETUP_MD
    try:
        content = template_file.read_text(encoding="utf-8")
    except Exception:
        return DEFAULT_PROJECT_SETUP_MD
    cleaned = content.strip()
    if not cleaned:
        return DEFAULT_PROJECT_SETUP_MD
    return cleaned[:MAX_TEMPLATE_PAYLOAD_BYTES]

def _resolve_owner_project_dir(owner_user_id: str, project_root: str) -> Path:
    owner_workspace = _user_workspace_root_dir(owner_user_id).resolve()
    raw = str(project_root or "").strip()
    if not raw:
        raise HTTPException(400, "Project root is not initialized")

    # Preferred path: directly inside owner's server workspace.
    direct = Path(raw).resolve()
    if _path_within(direct, owner_workspace):
        return direct

    # Backward compatibility: map legacy "HIVEE/..." roots into owner workspace.
    normalized = raw.replace("\\", "/")
    if normalized == HIVEE_ROOT:
        mapped = owner_workspace
    elif normalized.startswith(f"{HIVEE_ROOT}/"):
        mapped = (owner_workspace / normalized[len(HIVEE_ROOT) + 1 :]).resolve()
    else:
        raise HTTPException(400, "Project folder is outside owner workspace")

    if not _path_within(mapped, owner_workspace):
        raise HTTPException(400, "Invalid project folder path")
    return mapped

def _delete_project_with_resources(*, owner_user_id: str, project_id: str) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        "SELECT id, title, project_root FROM projects WHERE id = ? AND user_id = ?",
        (project_id, owner_user_id),
    ).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Project not found"}

    project_title = str(row["title"] or project_id)
    project_root = str(row["project_root"] or "")
    conn.execute("DELETE FROM project_agent_access_tokens WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM project_agents WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM projects WHERE id = ? AND user_id = ?", (project_id, owner_user_id))
    conn.commit()
    conn.close()

    deleted_dir = False
    folder_error = ""
    folder_path = ""
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
        folder_path = project_dir.as_posix()
        owner_workspace = _user_workspace_root_dir(owner_user_id).resolve()
        if project_dir.exists() and _path_within(project_dir, owner_workspace):
            shutil.rmtree(project_dir)
            deleted_dir = True
    except Exception as e:
        folder_error = str(e)[:500]

    project_queues.pop(project_id, None)
    return {
        "ok": True,
        "project_id": project_id,
        "title": project_title,
        "project_root": project_root,
        "folder_path": folder_path,
        "folder_deleted": deleted_dir,
        "folder_error": folder_error or None,
    }

def _delete_account_with_resources(*, user_id: str) -> Dict[str, Any]:
    conn = db()
    user_row = conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user_row:
        conn.close()
        return {"ok": False, "error": "User not found"}

    project_rows = conn.execute(
        "SELECT id FROM projects WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    connection_rows = conn.execute(
        "SELECT id FROM openclaw_connections WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    environment_rows = conn.execute(
        "SELECT id FROM environments WHERE owner_user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()

    project_ids = [str(r["id"]) for r in project_rows]
    connection_ids = [str(r["id"]) for r in connection_rows]
    environment_ids = [str(r["id"]) for r in environment_rows]
    deleted_projects = 0
    for project_id in project_ids:
        deleted = _delete_project_with_resources(owner_user_id=user_id, project_id=project_id)
        if deleted.get("ok"):
            deleted_projects += 1
        project_queues.pop(project_id, None)

    conn = db()
    conn.execute(
        "DELETE FROM project_agent_access_tokens WHERE project_id IN (SELECT id FROM projects WHERE user_id = ?)",
        (user_id,),
    )
    conn.execute(
        "DELETE FROM project_agents WHERE project_id IN (SELECT id FROM projects WHERE user_id = ?)",
        (user_id,),
    )
    conn.execute("DELETE FROM projects WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM connection_policies WHERE user_id = ?", (user_id,))
    if connection_ids:
        placeholders = ",".join(["?"] * len(connection_ids))
        conn.execute(f"DELETE FROM connection_policies WHERE connection_id IN ({placeholders})", tuple(connection_ids))
    conn.execute("DELETE FROM openclaw_connections WHERE user_id = ?", (user_id,))
    conn.execute(
        "DELETE FROM environment_claim_codes WHERE env_id IN (SELECT id FROM environments WHERE owner_user_id = ?)",
        (user_id,),
    )
    conn.execute(
        "DELETE FROM environment_agent_links WHERE env_id IN (SELECT id FROM environments WHERE owner_user_id = ?)",
        (user_id,),
    )
    conn.execute(
        "DELETE FROM environment_agent_sessions WHERE env_id IN (SELECT id FROM environments WHERE owner_user_id = ?)",
        (user_id,),
    )
    conn.execute("DELETE FROM managed_agent_approval_rules WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agent_metrics WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agent_permissions WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agent_checkpoints WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agent_history WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agent_memory WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM managed_agents WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM environments WHERE owner_user_id = ?", (user_id,))
    conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    workspace_deleted = False
    workspace_error = ""
    try:
        user_home = _user_home_dir(user_id).resolve()
        workspaces_root = SERVER_WORKSPACES_DIR.resolve()
        if user_home.exists() and _path_within(user_home, workspaces_root):
            shutil.rmtree(user_home)
            workspace_deleted = True
    except Exception as e:
        workspace_error = str(e)[:500]

    deleted_env_workspaces = 0
    for env_id in environment_ids:
        try:
            env_home = _environment_home_dir(env_id).resolve()
            workspaces_root = SERVER_WORKSPACES_DIR.resolve()
            if env_home.exists() and _path_within(env_home, workspaces_root):
                shutil.rmtree(env_home)
                deleted_env_workspaces += 1
        except Exception:
            continue

    return {
        "ok": True,
        "user_id": user_id,
        "projects_deleted": deleted_projects,
        "connections_deleted": len(connection_ids),
        "environments_deleted": len(environment_ids),
        "environment_workspaces_deleted": deleted_env_workspaces,
        "workspace_deleted": workspace_deleted,
        "workspace_error": workspace_error or None,
    }


__all__ = [name for name in globals() if not name.startswith('__')]
