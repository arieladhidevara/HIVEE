import os

from services.project_utils import *
def _upsert_connection_policy(
    connection_id: str,
    user_id: str,
    *,
    main_agent_id: Optional[str],
    main_agent_name: Optional[str],
    bootstrap_status: str,
    bootstrap_error: Optional[str],
    workspace_tree: Optional[str] = None,
    workspace_root: str = HIVEE_ROOT,
    templates_root: str = HIVEE_TEMPLATES_ROOT,
) -> None:
    conn = db()
    conn.execute(
        """
        INSERT INTO connection_policies (
            connection_id, user_id, main_agent_id, main_agent_name, workspace_root, templates_root, bootstrap_status, bootstrap_error, workspace_tree, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(connection_id) DO UPDATE SET
            user_id=excluded.user_id,
            main_agent_id=excluded.main_agent_id,
            main_agent_name=excluded.main_agent_name,
            workspace_root=excluded.workspace_root,
            templates_root=excluded.templates_root,
            bootstrap_status=excluded.bootstrap_status,
            bootstrap_error=excluded.bootstrap_error,
            workspace_tree=excluded.workspace_tree,
            updated_at=excluded.updated_at
        """,
        (
            connection_id,
            user_id,
            main_agent_id,
            main_agent_name,
            workspace_root,
            templates_root,
            bootstrap_status,
            bootstrap_error,
            workspace_tree,
            int(time.time()),
        ),
    )
    conn.commit()
    conn.close()

async def _bootstrap_connection_workspace(user_id: str, base_url: str, api_key: str) -> Dict[str, Any]:
    main_agent_id: Optional[str] = None
    main_agent_name: Optional[str] = None
    discovered_agents: List[Dict[str, Any]] = []
    probe = await openclaw_list_agents(base_url, api_key)
    if not probe.get("ok") and probe.get("error_code") == "missing_operator_write":
        # Token is valid (health likely works) but lacks operator.write — surface this
        # as a specific error code so callers can set connection_state accurately.
        health_check = await openclaw_health(base_url, api_key)
        return {
            "ok": False,
            "error_code": "missing_operator_write",
            "error": probe.get("error"),
            "hint": probe.get("hint"),
            "health_ok": health_check.get("ok"),
            "main_agent_id": None,
            "main_agent_name": None,
            "agents": [],
        }
    if not probe.get("ok"):
        fallback_health = await openclaw_health(base_url, api_key)
        fallback_agents: List[Dict[str, Any]] = []
        if fallback_health.get("ok"):
            fallback_payload = fallback_health.get("payload")
            fallback_raw_agents = _extract_agents_list(fallback_payload) or []
            if fallback_raw_agents:
                fallback_agents = _normalize_agents(fallback_raw_agents)
        if fallback_agents:
            probe = {
                "ok": True,
                "transport": "health-fallback",
                "path": str(fallback_health.get("path") or ""),
                "agents": fallback_agents,
                "warning": "Agent list resolved from health payload fallback.",
                "original_error": detail_to_text(probe.get("error") or probe.get("details") or probe)[:1000],
            }
        elif fallback_health.get("ok"):
            probe = {
                "ok": True,
                "transport": "health-only",
                "path": str(fallback_health.get("path") or ""),
                "agents": [],
                "warning": "Agent list endpoint unavailable; using health-only bootstrap fallback.",
                "original_error": detail_to_text(probe.get("error") or probe.get("details") or probe)[:1000],
            }
        else:
            probe_error = detail_to_text(probe.get("error") or probe.get("details") or probe)[:1200]
            probe_hint = detail_to_text(probe.get("hint") or "")[:400]
            health_error = detail_to_text(fallback_health.get("error") or fallback_health.get("details") or "")[:600]
            composed_error = probe_error or "Could not verify OpenClaw agent endpoint. Check base_url and API key/token."
            if probe_hint:
                composed_error = f"{composed_error} Hint: {probe_hint}"
            if health_error and health_error not in composed_error:
                composed_error = f"{composed_error} Health: {health_error}"
            return {
                "ok": False,
                "error": composed_error[:1800],
                "main_agent_id": None,
                "main_agent_name": None,
                "agent_probe": probe,
                "health_probe": fallback_health,
                "agents": [],
            }
    if probe.get("ok"):
        discovered_agents = [dict(a) for a in (probe.get("agents") or []) if isinstance(a, dict)]
        picked = _pick_main_agent(probe.get("agents") or [])
        if picked:
            picked_id = str(picked.get("id") or "").strip()
            main_agent_id = picked_id or None
            main_agent_name = str(picked.get("name") or main_agent_id)
    if (not discovered_agents) and main_agent_id:
        discovered_agents = [{"id": main_agent_id, "name": main_agent_name or main_agent_id}]

    try:
        workspace = _ensure_user_workspace(user_id)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to provision server workspace: {str(e)}",
            "main_agent_id": main_agent_id,
            "main_agent_name": main_agent_name,
            "agent_probe": probe,
            "agents": discovered_agents,
        }

    return {
        "ok": True,
        "main_agent_id": main_agent_id,
        "main_agent_name": main_agent_name,
        "agent_probe": probe,
        "agents": discovered_agents,
        "workspace_tree": workspace["workspace_tree"],
        "workspace_root": workspace["workspace_root"],
        "templates_root": workspace["templates_root"],
        "projects_root": workspace["projects_root"],
        "template_warnings": workspace.get("template_warnings") or [],
    }

def _normalize_managed_agent_candidates(
    raw_agents: Any,
    *,
    fallback_agent_id: Optional[str] = None,
    fallback_agent_name: Optional[str] = None,
) -> List[Dict[str, str]]:
    cleaned: List[Dict[str, str]] = []
    seen: set[str] = set()
    candidates = raw_agents if isinstance(raw_agents, list) else []
    for row in candidates:
        if isinstance(row, str):
            aid = str(row).strip()
            nm = aid
        elif isinstance(row, dict):
            aid = str(row.get("id") or row.get("agent_id") or row.get("name") or "").strip()
            nm = str(row.get("name") or row.get("title") or aid).strip()
        else:
            continue
        if not aid:
            continue
        if aid in seen:
            continue
        seen.add(aid)
        cleaned.append({"id": aid[:180], "name": (nm or aid)[:220]})
    fallback_id = str(fallback_agent_id or "").strip()
    if fallback_id and fallback_id not in seen:
        cleaned.append(
            {
                "id": fallback_id[:180],
                "name": (str(fallback_agent_name or fallback_id).strip() or fallback_id)[:220],
            }
        )
    return cleaned

def _build_managed_agent_card(
    *,
    agent_id: str,
    agent_name: str,
    base_url: str,
    connection_id: str,
    env_id: Optional[str],
    root_path: str,
) -> Dict[str, Any]:
    now = int(time.time())
    safe_skill_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-") or "agent"
    return {
        "schemaVersion": MANAGED_AGENT_CARD_VERSION,
        "name": str(agent_name or agent_id),
        "description": f"Hivee managed profile for agent `{agent_id}`.",
        "version": "1.0.0",
        "provider": {"organization": "Hivee"},
        "supportedInterfaces": [
            {
                "url": str(base_url or "").rstrip("/"),
                "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0",
            }
        ],
        "capabilities": {
            "streaming": True,
            "pushNotifications": False,
            "stateTransitionHistory": True,
        },
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "skills": [
            {
                "id": f"{safe_skill_id}.execute",
                "name": "Project Execution",
                "description": "Executes scoped project tasks and reports progress.",
                "tags": ["execution", "workflow", "collaboration"],
            }
        ],
        "securityRequirements": [{"type": "bearer", "scopes": ["env.read"]}],
        "metadata": {
            "managedBy": "hivee",
            "connectionId": connection_id,
            "environmentId": env_id,
            "rootPath": root_path,
            "provisionedAt": now,
        },
    }

def _append_managed_agent_history_record(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    env_id: Optional[str],
    connection_id: str,
    agent_id: str,
    event_kind: str,
    event_text: str,
    event_payload: Optional[Dict[str, Any]],
    history_file: Optional[Path] = None,
) -> None:
    payload_json = json.dumps(event_payload or {}, ensure_ascii=False)
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO managed_agent_history (
            id, user_id, env_id, connection_id, agent_id, event_kind, event_text, event_payload_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            new_id("mgh"),
            user_id,
            env_id,
            connection_id,
            agent_id,
            str(event_kind or "event")[:120],
            str(event_text or "")[:2000],
            payload_json,
            now,
        ),
    )
    if history_file:
        history_file.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": now,
            "event_kind": str(event_kind or "event")[:120],
            "event_text": str(event_text or "")[:2000],
            "payload": event_payload or {},
        }
        with history_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def _refresh_managed_agents_index(user_id: str) -> None:
    conn = db()
    rows = conn.execute(
        """
        SELECT connection_id, agent_id, agent_name, status, root_path, updated_at
        FROM managed_agents
        WHERE user_id = ?
        ORDER BY updated_at DESC, agent_name ASC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    payload = {
        "generated_at": int(time.time()),
        "count": len(rows),
        "agents": [
            {
                "connection_id": str(r["connection_id"] or ""),
                "agent_id": str(r["agent_id"] or ""),
                "agent_name": str(r["agent_name"] or ""),
                "status": str(r["status"] or ""),
                "root_path": str(r["root_path"] or ""),
                "updated_at": _to_int(r["updated_at"]),
            }
            for r in rows
        ],
    }
    _write_json_file(_user_agents_root_dir(user_id) / "index.json", payload)

def _provision_managed_agents_for_connection(
    *,
    user_id: str,
    env_id: Optional[str],
    connection_id: str,
    base_url: str,
    raw_agents: Any,
    fallback_agent_id: Optional[str] = None,
    fallback_agent_name: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_user_workspace(user_id)
    now = int(time.time())
    workspace_root = _user_workspace_root_dir(user_id).resolve()
    normalized_agents = _normalize_managed_agent_candidates(
        raw_agents,
        fallback_agent_id=fallback_agent_id,
        fallback_agent_name=fallback_agent_name,
    )
    if not normalized_agents:
        return {
            "ok": False,
            "error": "No agents available for managed provisioning",
            "provisioned": 0,
            "updated": 0,
            "failed": 0,
            "agents": [],
            "errors": [],
        }

    conn = db()
    provisioned = 0
    updated = 0
    failed = 0
    errors: List[str] = []
    output_agents: List[Dict[str, Any]] = []
    for agent in normalized_agents:
        agent_id = str(agent.get("id") or "").strip()
        agent_name = str(agent.get("name") or agent_id).strip() or agent_id
        if not agent_id:
            failed += 1
            errors.append("Missing agent id in candidate entry")
            continue
        try:
            parts = _agent_component_paths(user_id, connection_id, agent_id)
            for path in parts.values():
                if path == parts["root"]:
                    path.mkdir(parents=True, exist_ok=True)
                    continue
                path.mkdir(parents=True, exist_ok=True)

            root_path = parts["root"].resolve().as_posix()
            card_payload = _build_managed_agent_card(
                agent_id=agent_id,
                agent_name=agent_name,
                base_url=base_url,
                connection_id=connection_id,
                env_id=env_id,
                root_path=root_path,
            )
            memory_payloads = {
                "working": {
                    "scope": "working",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
                "project": {
                    "scope": "project",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
                "long_term": {
                    "scope": "long_term",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
            }
            checkpoint_state = {
                "checkpoint_key": "latest",
                "status": "ready",
                "notes": "Auto-generated checkpoint seed.",
                "updated_at": now,
            }
            permissions_payload = {
                "scopes": ["env.read", "project.read", "project.write"],
                "tools": ["workspace.read", "workspace.write", "chat.send", "project.control"],
                "path_allowlist": [workspace_root.as_posix(), root_path],
                "secrets_policy": {
                    "mode": "connection-bound",
                    "connection_id": connection_id,
                },
                "approval_required": True,
                "updated_at": now,
            }
            metrics_payload = {
                "success_count": 0,
                "failure_count": 0,
                "total_calls": 0,
                "total_prompt_tokens": 0,
                "total_completion_tokens": 0,
                "total_latency_ms": 0,
                "last_error": None,
                "last_seen_at": None,
                "updated_at": now,
            }
            approval_rules = {
                "destructive_write": {
                    "description": "Require owner approval for destructive file actions.",
                    "required": True,
                    "patterns": ["delete", "remove", "truncate", "reset", "drop table"],
                },
                "outside_workspace": {
                    "description": "Require owner approval for access outside workspace root.",
                    "required": True,
                    "workspace_root": workspace_root.as_posix(),
                },
                "high_token_budget": {
                    "description": "Require owner approval for very large token usage.",
                    "required": True,
                    "max_total_tokens": 120000,
                },
            }

            existing = conn.execute(
                "SELECT id FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id = ?",
                (user_id, connection_id, agent_id),
            ).fetchone()
            card_json = json.dumps(card_payload, ensure_ascii=False)
            if existing:
                conn.execute(
                    """
                    UPDATE managed_agents
                    SET env_id = ?, agent_name = ?, status = ?, card_version = ?, card_json = ?, root_path = ?, updated_at = ?
                    WHERE user_id = ? AND connection_id = ? AND agent_id = ?
                    """,
                    (
                        env_id,
                        agent_name,
                        "active",
                        MANAGED_AGENT_CARD_VERSION,
                        card_json,
                        root_path,
                        now,
                        user_id,
                        connection_id,
                        agent_id,
                    ),
                )
                updated += 1
                event_kind = "agent.synced"
                event_text = "Managed agent resources refreshed."
            else:
                conn.execute(
                    """
                    INSERT INTO managed_agents (
                        id, user_id, env_id, connection_id, agent_id, agent_name, status,
                        card_version, card_json, root_path, provisioned_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        new_id("mga"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        agent_name,
                        "active",
                        MANAGED_AGENT_CARD_VERSION,
                        card_json,
                        root_path,
                        now,
                        now,
                    ),
                )
                provisioned += 1
                event_kind = "agent.provisioned"
                event_text = "Managed agent resources initialized."

            for scope in MANAGED_AGENT_MEMORY_SCOPES:
                scope_payload = memory_payloads.get(scope, {"scope": scope, "summary": "", "entries": [], "updated_at": now})
                conn.execute(
                    """
                    INSERT INTO managed_agent_memory (
                        id, user_id, env_id, connection_id, agent_id, memory_scope, summary, payload_json, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, connection_id, agent_id, memory_scope) DO UPDATE SET
                        env_id=excluded.env_id,
                        summary=CASE
                            WHEN managed_agent_memory.summary IS NULL OR managed_agent_memory.summary = '' THEN excluded.summary
                            ELSE managed_agent_memory.summary
                        END,
                        payload_json=CASE
                            WHEN managed_agent_memory.payload_json IS NULL OR managed_agent_memory.payload_json = '' THEN excluded.payload_json
                            ELSE managed_agent_memory.payload_json
                        END,
                        updated_at=excluded.updated_at
                    """,
                    (
                        new_id("mgm"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        scope,
                        str(scope_payload.get("summary") or ""),
                        json.dumps(scope_payload, ensure_ascii=False),
                        now,
                    ),
                )

            conn.execute(
                """
                INSERT INTO managed_agent_checkpoints (
                    id, user_id, env_id, connection_id, agent_id, checkpoint_key, state_json, status, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id, checkpoint_key) DO UPDATE SET
                    env_id=excluded.env_id,
                    state_json=excluded.state_json,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgc"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    "latest",
                    json.dumps(checkpoint_state, ensure_ascii=False),
                    "ready",
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO managed_agent_permissions (
                    id, user_id, env_id, connection_id, agent_id, scopes_json, tools_json, path_allowlist_json, secrets_policy_json, approval_required, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id) DO UPDATE SET
                    env_id=excluded.env_id,
                    scopes_json=excluded.scopes_json,
                    tools_json=excluded.tools_json,
                    path_allowlist_json=excluded.path_allowlist_json,
                    secrets_policy_json=excluded.secrets_policy_json,
                    approval_required=excluded.approval_required,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgp"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    json.dumps(permissions_payload.get("scopes") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("tools") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("path_allowlist") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("secrets_policy") or {}, ensure_ascii=False),
                    1 if _coerce_bool(permissions_payload.get("approval_required")) else 0,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO managed_agent_metrics (
                    id, user_id, env_id, connection_id, agent_id, success_count, failure_count, total_calls,
                    total_prompt_tokens, total_completion_tokens, total_latency_ms, last_error, last_seen_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id) DO UPDATE SET
                    env_id=excluded.env_id,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgt"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    _to_int(metrics_payload.get("success_count")),
                    _to_int(metrics_payload.get("failure_count")),
                    _to_int(metrics_payload.get("total_calls")),
                    _to_int(metrics_payload.get("total_prompt_tokens")),
                    _to_int(metrics_payload.get("total_completion_tokens")),
                    _to_int(metrics_payload.get("total_latency_ms")),
                    metrics_payload.get("last_error"),
                    metrics_payload.get("last_seen_at"),
                    now,
                ),
            )
            for rule_key, policy in approval_rules.items():
                conn.execute(
                    """
                    INSERT INTO managed_agent_approval_rules (
                        id, user_id, env_id, connection_id, agent_id, rule_key, policy_json, is_enabled, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, connection_id, agent_id, rule_key) DO UPDATE SET
                        env_id=excluded.env_id,
                        policy_json=excluded.policy_json,
                        is_enabled=excluded.is_enabled,
                        updated_at=excluded.updated_at
                    """,
                    (
                        new_id("mga"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        rule_key,
                        json.dumps(policy, ensure_ascii=False),
                        1 if _coerce_bool(policy.get("required", True)) else 0,
                        now,
                        now,
                    ),
                )

            _write_json_file(parts["card"] / AGENT_CARD_FILENAME, card_payload)
            for scope in MANAGED_AGENT_MEMORY_SCOPES:
                _write_json_file(parts["memory"] / f"{scope}.json", memory_payloads.get(scope, {}))
            _write_json_file(parts["checkpoints"] / "latest.json", checkpoint_state)
            _write_json_file(parts["metrics"] / "summary.json", metrics_payload)
            _write_json_file(parts["approvals"] / "rules.json", approval_rules)
            _write_json_file(parts["approvals"] / "permissions.json", permissions_payload)

            _append_managed_agent_history_record(
                conn,
                user_id=user_id,
                env_id=env_id,
                connection_id=connection_id,
                agent_id=agent_id,
                event_kind=event_kind,
                event_text=event_text,
                event_payload={
                    "agent_name": agent_name,
                    "root_path": root_path,
                    "connection_id": connection_id,
                },
                history_file=parts["history"] / "events.jsonl",
            )
            output_agents.append(
                {
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "root_path": root_path,
                    "status": "active",
                }
            )
        except Exception as e:
            failed += 1
            errors.append(f"{agent_id or 'unknown'}: {str(e)[:220]}")

    conn.commit()
    conn.close()
    _refresh_managed_agents_index(user_id)
    return {
        "ok": failed == 0,
        "provisioned": provisioned,
        "updated": updated,
        "failed": failed,
        "agents": output_agents,
        "errors": errors[:20],
    }

async def try_get_json(
    client: httpx.AsyncClient, url: str
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    try:
        r = await client.get(url, timeout=10)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text[:2000]
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return True, r.json(), r.status_code, None
        return True, {"raw": r.text[:2000]}, r.status_code, None
    except Exception as e:
        return False, None, None, str(e)

def _is_openclaw_login_html(payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False
    raw = payload.get("raw")
    if not isinstance(raw, str):
        return False
    marker = raw.lower()
    return ("welcome to openclaw" in marker) and ('action="/login"' in marker or "gateway token" in marker)


def _is_openclaw_starting_html(payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False
    raw = payload.get("raw")
    if not isinstance(raw, str):
        return False
    marker = raw.lower()
    return (
        "starting openclaw" in marker
        or "please wait while we set up your environment" in marker
        or ('id="log-output"' in marker and "/api/logs" in marker)
    )


def _response_looks_like_login_html(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    text = (resp.text or "").lower()
    return ("welcome to openclaw" in text) and ('action="/login"' in text or "gateway token" in text)


def _response_looks_like_starting_html(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    text = (resp.text or "").lower()
    return (
        "starting openclaw" in text
        or "please wait while we set up your environment" in text
        or ('id="log-output"' in text and "/api/logs" in text)
    )

def _safe_json_response(resp: httpx.Response) -> Tuple[Optional[Any], Optional[str]]:
    text = (resp.text or "").strip()
    if not text:
        return None, None
    try:
        return resp.json(), None
    except Exception as e:
        return None, str(e)

def _extract_agents_list(data: Any) -> Optional[List[Any]]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None

    for key in ["agents", "nodes", "subagents", "list", "data", "items", "results", "models"]:
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            # Support map-style config payloads, e.g. {"agents": {"main": {...}, "qa": {...}}}
            if key in {"agents", "subagents"} and value:
                if all(isinstance(v, dict) for v in value.values()):
                    mapped: List[Dict[str, Any]] = []
                    for map_key, map_val in value.items():
                        row = dict(map_val)
                        row.setdefault("id", str(map_key))
                        row.setdefault("name", row.get("id") or str(map_key))
                        mapped.append(row)
                    return mapped
            for nested_key in ["agents", "subagents", "list", "items", "results", "models", "data"]:
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return nested_value

    if any(k in data for k in ["id", "agent_id", "name", "slug", "model"]):
        return [data]
    return None

def _normalize_agents(agents: List[Any]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for a in agents:
        if isinstance(a, str):
            norm.append({"id": a, "name": a})
        elif isinstance(a, dict):
            aid = (
                a.get("id")
                or a.get("agent_id")
                or a.get("name")
                or a.get("slug")
                or a.get("model")
                or "unknown"
            )
            nm = a.get("name") or a.get("title") or a.get("label") or aid
            norm.append({"id": str(aid), "name": str(nm), "raw": a})
    return norm

def _merge_unique_agents(
    target: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
    *,
    seen_ids: Optional[set[str]] = None,
) -> int:
    if seen_ids is None:
        seen_ids = {
            str(item.get("id") or "").strip().lower()
            for item in target
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
    added = 0
    for item in incoming:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("id") or "").strip()
        if not aid:
            continue
        key = aid.lower()
        if key in seen_ids:
            continue
        seen_ids.add(key)
        target.append(item)
        added += 1
    return added

async def _request_openclaw_with_auth(
    client: httpx.AsyncClient,
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    *,
    timeout: int = 15,
    json_body: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> httpx.Response:
    url = base_url.rstrip("/") + path
    headers = {"Authorization": f"Bearer {api_key}"}
    if extra_headers:
        headers.update(extra_headers)
    res = await client.request(
        method=method,
        url=url,
        headers=headers,
        json=json_body,
        timeout=timeout,
    )

    if res.status_code >= 400:
        print(f"[openclaw] {res.status_code} on {method} {url} — token prefix: {api_key[:6]}... body: {res.text[:400]}", flush=True)
    if res.status_code == 401 or _response_looks_like_login_html(res):
        login = await client.post(base_url.rstrip("/") + "/login", data={"token": api_key}, timeout=timeout)
        if login.status_code < 400:
            res = await client.request(method=method, url=url, headers=headers, json=json_body, timeout=timeout)
    return res

async def openclaw_health(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_status: Optional[int] = None
        last_err: str = ""

        # 1) Prefer explicit health/status routes.
        for p in HEALTH_PATHS:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=10)
            except Exception as e:
                last_err = f"{p}: {str(e)}"
                continue

            last_status = r.status_code
            if _response_looks_like_login_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _response_looks_like_starting_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code == 401:
                return {
                    "ok": False,
                    "error": "Unauthorized (401). Token/API key invalid.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code >= 400:
                last_err = f"{p}: {r.status_code} {r.text[:300]}"
                continue

            ct = r.headers.get("content-type", "")
            if "application/json" in ct:
                payload = r.json()
            else:
                payload = {"raw": r.text[:2000]}
            if _is_openclaw_login_html(payload):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _is_openclaw_starting_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            return {
                "ok": True,
                "path": p,
                "status": r.status_code,
                "payload": payload,
                "probe": "health",
            }

        # 2) Some OpenClaw deployments don't expose /health publicly; allow read-only fallback probes.
        fallback_paths = [
            "/v1/models",
            "/models",
            "/api/models",
            "/v1/agents",
            "/agents",
            "/api/agents",
        ]
        for p in fallback_paths:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=10)
            except Exception as e:
                last_err = f"{p}: {str(e)}"
                continue

            last_status = r.status_code
            if _response_looks_like_login_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _response_looks_like_starting_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code == 401:
                return {
                    "ok": False,
                    "error": "Unauthorized (401). Token/API key invalid.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code >= 400:
                last_err = f"{p}: {r.status_code} {r.text[:300]}"
                continue

            data, parse_err = _safe_json_response(r)
            payload: Any = data if data is not None else {"raw": (r.text or "")[:2000]}
            if _is_openclaw_login_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _is_openclaw_starting_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            out: Dict[str, Any] = {
                "ok": True,
                "path": p,
                "status": r.status_code,
                "payload": payload,
                "probe": "fallback",
            }
            if parse_err:
                out["warning"] = f"Fallback endpoint returned non-JSON payload: {parse_err}"
            return out

        return {
            "ok": False,
            "error": (
                "Could not verify OpenClaw health/reachability on common paths. "
                "Gateway may be restarting/crashing, or reverse proxy path is incomplete."
            ),
            "last_status": last_status,
            "last_error": last_err[:600],
            "hint": (
                "If OpenClaw logs show ECONNREFUSED to 127.0.0.1:18789, fix upstream service/config first. "
                "Then ensure at least one of: /health, /status, /v1/models is reachable."
            ),
        }

async def openclaw_list_agents(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err: Optional[str] = None
        rest_agents: List[Dict[str, Any]] = []
        rest_seen_ids: set[str] = set()
        rest_model_agents: List[Dict[str, Any]] = []
        rest_model_seen_ids: set[str] = set()
        rest_ok_paths: List[str] = []
        rest_model_source_path: str = ""

        for p in AGENTS_PATHS:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=15)
                if _response_looks_like_login_html(r):
                    return {"ok": False, "error": "OpenClaw returned login page. Gateway token is invalid or missing.", "path": p}
                if _response_looks_like_starting_html(r):
                    return {
                        "ok": False,
                        "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                        "hint": "Wait until startup completes in OpenClaw, then retry listing agents.",
                        "path": p,
                    }
                if r.status_code == 401:
                    return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                if r.status_code == 403:
                    body = r.text[:600]
                    if _is_missing_operator_write_error(body):
                        return {
                            "ok": False,
                            "error": "Token is valid but missing operator.write scope. Agent listing and chat require an operator token.",
                            "error_code": "missing_operator_write",
                            "hint": "In OpenClaw: ensure your gateway token has operator.write scope (gateway.auth.mode=token, operator role).",
                            "path": p,
                        }
                    return {"ok": False, "error": f"Forbidden (403). Token lacks required permissions. {body}", "path": p}
                if r.status_code >= 400:
                    last_err = f"{r.status_code}: {r.text[:500]}"
                    continue

                data, parse_err = _safe_json_response(r)
                if data is None:
                    raw = (r.text or "").strip()
                    if not raw:
                        rest_ok_paths.append(p)
                        continue
                    ctype = r.headers.get("content-type") or "unknown"
                    last_err = f"{p}: expected JSON but got {ctype}; body={raw[:300]}"
                    if parse_err:
                        last_err = f"{last_err}; parse_error={parse_err}"
                    continue

                agents = _extract_agents_list(data) or []
                norm = _normalize_agents(agents)
                print(f"[openclaw] 200 on GET {base_url}{p} - keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__} agents_found={len(norm)}", flush=True)
                if norm:
                    if "/model" in p.lower():
                        # Model listing paths - keep separate; only use as fallback if no real agents found.
                        _merge_unique_agents(rest_model_agents, norm, seen_ids=rest_model_seen_ids)
                        if not rest_model_source_path:
                            rest_model_source_path = p
                    else:
                        _merge_unique_agents(rest_agents, norm, seen_ids=rest_seen_ids)
                rest_ok_paths.append(p)
            except Exception as e:
                last_err = str(e)

        if rest_agents:
            rest_agents.sort(key=lambda a: (str(a.get("name") or "").lower(), str(a.get("id") or "").lower()))
            return {"ok": True, "path": rest_ok_paths[0] if rest_ok_paths else AGENTS_PATHS[0], "agents": rest_agents}

        # No real agents found - fall back to model names if available.
        if rest_model_agents:
            rest_model_agents.sort(key=lambda a: (str(a.get("name") or "").lower(), str(a.get("id") or "").lower()))
            return {
                "ok": True,
                "transport": "rest-models-fallback",
                "path": rest_model_source_path or AGENTS_PATHS[0],
                "agents": rest_model_agents,
                "warning": "Only /models endpoint available; using model names as chat targets.",
            }

        return {
            "ok": False,
            "error": f"Could not list agents on common paths. Last error: {last_err}",
            "hint": (
                "This OpenClaw likely does not expose REST JSON agent listing on your base_url path. "
                "Enable /agents or /v1/models over HTTP on the gateway."
            ),
        }
def _extract_chat_text(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        return payload
    if not isinstance(payload, dict):
        return None

    # OpenAI-style chat completions
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict) and isinstance(msg.get("content"), str):
            return msg["content"]

    # OpenAI responses-style
    output = payload.get("output")
    if isinstance(output, list):
        chunks: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, list):
                for c in content:
                    if isinstance(c, dict) and isinstance(c.get("text"), str):
                        chunks.append(c["text"])
        if chunks:
            return "\n".join(chunks)

    # Generic fallback fields
    for key in ["text", "message", "response", "answer", "content"]:
        val = payload.get(key)
        if isinstance(val, str):
            return val
    return None

def _is_credit_or_max_token_error(detail: Any) -> bool:
    low = detail_to_text(detail).lower()
    if not low:
        return False
    markers = [
        "requires more credits",
        "fewer max_tokens",
        "requested up to",
        "can only afford",
        "insufficient credits",
        "max_tokens",
        "monthly limit",
    ]
    return any(m in low for m in markers)


def _is_missing_operator_write_error(text: str) -> bool:
    low = text.lower()
    return "missing scope" in low and "operator.write" in low


def _candidate_openclaw_model_hints(agent_id: Optional[str]) -> List[str]:
    raw = str(agent_id or "").strip()
    hints: List[str] = []

    def _push(value: str) -> None:
        item = str(value or "").strip()
        if not item or item in hints:
            return
        hints.append(item)

    if not raw:
        _push("openclaw/default")
        _push("openclaw:default")
        _push("openclaw")
        return hints

    _push(raw)
    if raw.startswith("openclaw/"):
        suffix = raw.split("/", 1)[1].strip()
        if suffix:
            _push(f"openclaw:{suffix}")
    elif raw.startswith("openclaw:"):
        suffix = raw.split(":", 1)[1].strip()
        if suffix:
            _push(f"openclaw/{suffix}")
    else:
        _push(f"openclaw/{raw}")
        _push(f"openclaw:{raw}")
    return hints


def _is_model_resolution_error(status_code: int, raw_text: str) -> bool:
    if status_code != 400:
        return False
    low = str(raw_text or "").lower()
    if "model" not in low:
        return False
    return any(
        marker in low
        for marker in [
            "not found",
            "unknown",
            "invalid model",
            "unsupported model",
            "does not exist",
        ]
    )


async def openclaw_chat(
    base_url: str,
    api_key: str,
    message: str,
    agent_id: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
    session_key: Optional[str] = None,
    user_id: Optional[str] = None,
    timeout_sec: int = 90,
) -> Dict[str, Any]:
    cap = _to_int(max_output_tokens) if max_output_tokens is not None else 0
    if cap <= 0:
        cap = 0
    model_hints = _candidate_openclaw_model_hints(agent_id)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err = None
        saw_405 = False
        saw_502: Optional[str] = None  # first path that returned 502
        saw_404_paths: List[str] = []

        for p in CHAT_PATHS:
            extra_headers: Dict[str, str] = {}
            if agent_id:
                extra_headers["x-openclaw-agent-id"] = agent_id
            if session_key:
                extra_headers["x-openclaw-session-key"] = str(session_key)[:240]

            for model_idx, model_hint in enumerate(model_hints):
                if p.endswith("/responses"):
                    body: Dict[str, Any] = {"model": model_hint, "input": message}
                    if agent_id:
                        body["agent_id"] = agent_id
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
                    if cap > 0:
                        body["max_output_tokens"] = cap
                        # Compatibility fallback for providers/gateways expecting chat-completions naming.
                        body["max_tokens"] = cap
                elif "chat/completions" in p:
                    body = {
                        "model": model_hint,
                        "messages": [{"role": "user", "content": message}],
                    }
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
                    if cap > 0:
                        body["max_tokens"] = cap
                else:
                    body = {"model": model_hint, "message": message, "prompt": message, "input": message}
                    if agent_id:
                        body["agent_id"] = agent_id
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
                    if cap > 0:
                        body["max_output_tokens"] = cap
                        body["max_tokens"] = cap

                try:
                    r = await _request_openclaw_with_auth(
                        client,
                        "POST",
                        base_url,
                        p,
                        api_key,
                        json_body=body,
                        timeout=30,
                        extra_headers=extra_headers,
                    )
                    if _response_looks_like_login_html(r):
                        last_err = f"{p}: OpenClaw returned login page. Gateway token is invalid or missing."
                        break
                    if _response_looks_like_starting_html(r):
                        last_err = f"{p}: OpenClaw gateway is still starting. API routes are not ready yet."
                        break
                    if r.status_code == 401:
                        return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                    if r.status_code == 403:
                        body_text = r.text[:600]
                        if _is_missing_operator_write_error(body_text):
                            # Token-level scope failure - no point trying other paths.
                            return {
                                "ok": False,
                                "error": "Token is valid but missing operator.write scope. Chat requires an operator token.",
                                "error_code": "missing_operator_write",
                                "hint": "Provide an OpenClaw token with operator.write scope.",
                                "path": p,
                            }
                        last_err = f"{p}: 403 {body_text}"
                        break
                    if r.status_code == 502:
                        # Path exists on the proxy but the upstream LLM/backend is down.
                        # No point probing remaining paths - they will 404.
                        saw_502 = saw_502 or p
                        last_err = f"{p}: 502 {r.text[:300]}"
                        break
                    if r.status_code == 405:
                        saw_405 = True
                    if r.status_code == 404:
                        saw_404_paths.append(p)
                    if r.status_code >= 400:
                        last_err = f"{p}: {r.status_code} {r.text[:300]}"
                        # Try alternate model IDs only when this looks like model-resolution failure.
                        if (model_idx + 1) < len(model_hints) and _is_model_resolution_error(r.status_code, r.text):
                            continue
                        break

                    ctype = r.headers.get("content-type", "")
                    if "application/json" in ctype:
                        data: Any = r.json()
                    else:
                        data = {"raw": r.text[:4000]}
                    return {
                        "ok": True,
                        "path": p,
                        "response": data,
                        "text": _extract_chat_text(data),
                        "model_hint": model_hint,
                    }
                except Exception as e:
                    last_err = f"{p}: {str(e)}"
                    break

    # ── Connector fallback: if direct chat failed, try routing through connector ──
    # This MUST be checked before any error returns so it catches all failure modes
    # (502, 405, 404, login page, etc.)
    if user_id:
        try:
            from services.connector_dispatch import get_user_online_connector, connector_chat_sync
            online_connector = get_user_online_connector(user_id)
            if online_connector:
                print(f"[openclaw_chat] Direct chat failed (last_err={last_err}), trying connector fallback via {online_connector['id']}", flush=True)
                connector_res = await connector_chat_sync(
                    connector_id=str(online_connector["id"]),
                    message=message,
                    agent_id=agent_id,
                    session_key=session_key,
                    timeout_sec=max(timeout_sec, 90),
                )
                if connector_res.get("ok"):
                    return connector_res
                # If connector also failed, fall through to direct error below
                print(f"[openclaw_chat] Connector fallback also failed: {connector_res.get('error')}", flush=True)
        except Exception as e:
            print(f"[openclaw_chat] Connector fallback error: {e}", flush=True)

    if saw_502:
        return {
            "ok": False,
            "error": (
                f"OpenClaw chat endpoint ({saw_502}) returned 502 Bad Gateway. "
                "The OpenClaw gateway proxy is running but its upstream LLM provider is unreachable. "
                "Check: (1) OpenClaw provider key is valid and has credits, "
                "(2) the upstream model/provider is reachable from the OpenClaw server, "
                "(3) OpenClaw service logs for upstream connection errors."
            ),
            "hint": "502 means the path exists on the proxy but the backend is down - this is a server-side OpenClaw config issue, not an auth problem.",
            "path": saw_502,
        }
    if saw_405:
        return {
            "ok": False,
            "error": "Chat endpoint returned 405 Method Not Allowed. On OpenClaw, enable gateway.http.endpoints.chatCompletions.enabled=true.",
            "hint": "OpenClaw docs: OpenAI Chat Completions endpoint is disabled by default.",
        }
    if saw_404_paths and len(saw_404_paths) == len(CHAT_PATHS):
        return {
            "ok": False,
            "error": "OpenClaw chat endpoint is not exposed on this base_url (all candidate POST paths returned 404 Not Found).",
            "error_code": "chat_endpoint_not_exposed",
            "tried_paths": saw_404_paths,
            "hint": (
                "Enable OpenClaw HTTP chat routes (for example gateway.http.endpoints.chatCompletions.enabled=true) "
                "and ensure your reverse proxy forwards POST /v1/chat/completions or /v1/responses."
            ),
        }

    hint = "Your OpenClaw may use different chat path(s). Update CHAT_PATHS in core/db.py."
    if last_err and "403" in str(last_err):
        hint = (
            "Got 403 on all chat paths. In gateway.auth.mode='token', a valid bearer token should "
            "automatically receive full operator scopes. Possible causes: (1) endpoint not enabled "
            "(set gateway.http.endpoints.chatCompletions.enabled=true), (2) token is incorrect or "
            "doesn't match gateway.auth.token, (3) gateway.auth.mode is not set to 'token'."
        )
    return {
        "ok": False,
        "error": f"Could not call chat endpoint on common paths. Last error: {last_err}",
        "hint": hint,
    }

def _collect_text_fields(node: Any, out: List[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            lk = key.lower()
            if lk in {"text", "content", "delta", "response", "answer"} and isinstance(value, str):
                text = value.strip()
                if text:
                    out.append(text)
            elif isinstance(value, (dict, list)):
                _collect_text_fields(value, out)
    elif isinstance(node, list):
        for item in node:
            _collect_text_fields(item, out)

def _join_delta_chunks(chunks: List[str]) -> str:
    out = ""
    no_space_before = {".", ",", "!", "?", ";", ":", ")", "]", "}", "%"}
    no_space_after_prev = {"(", "[", "{", "/", "-", "\n"}
    contractions = {"'s", "'re", "'ve", "'m", "'ll", "'d", "n't"}
    for raw in chunks:
        part = raw.strip()
        if not part:
            continue
        if not out:
            out = part
            continue
        if part in no_space_before or part in contractions or part.startswith("'"):
            out += part
            continue
        if out.endswith(tuple(no_space_after_prev)):
            out += part
            continue
        out += " " + part
    return out.strip()

def _derive_ws_session_key(session_key: str, agent_id: Optional[str]) -> str:
    base = (session_key or "main").strip() or "main"
    aid = (agent_id or "").strip()
    if not aid:
        return base
    if base.startswith("agent:"):
        return base
    return f"agent:{aid}:{base}"

async def openclaw_ws_chat(
    base_url: str,
    api_key: str,
    message: str,
    agent_id: Optional[str] = None,
    session_key: str = "main",
    timeout_sec: int = 25,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    # HTTP-only mode: keep function name for backward compatibility with existing callers.
    routed_session_key = _derive_ws_session_key(session_key=session_key, agent_id=agent_id)
    http_res = await openclaw_chat(
        base_url=base_url,
        api_key=api_key,
        message=message,
        agent_id=agent_id,
        session_key=routed_session_key,
        user_id=user_id,
        timeout_sec=timeout_sec,
    )
    if http_res.get("ok"):
        return {
            "ok": True,
            "transport": "http",
            "path": str(http_res.get("path") or "http"),
            "text": http_res.get("text"),
            "response": http_res.get("response"),
            "frames": [],
        }
    return {
        "ok": False,
        "transport": "http",
        "path": str(http_res.get("path") or "http"),
        "error": http_res.get("error") or "HTTP chat failed",
        "details": http_res.get("details"),
        "hint": http_res.get("hint"),
        "error_code": http_res.get("error_code"),
        "tried_paths": http_res.get("tried_paths"),
    }
async def openclaw_ws_list_agents(base_url: str, api_key: str, timeout_sec: int = 12) -> Dict[str, Any]:
    # HTTP-only mode: this helper remains for backward compatibility.
    _ = base_url
    _ = api_key
    _ = timeout_sec
    return {
        "ok": False,
        "error": "Legacy realtime transport is disabled in this build. Use HTTP agent endpoints.",
        "hint": "Enable /agents or /v1/models over HTTP on the OpenClaw gateway.",
    }
async def _ensure_project_info_document(project_id: str, *, force: bool = False) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Project not found"}
    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()
    if not role_rows:
        return {"ok": False, "error": "No invited agents configured"}

    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    if not primary_agent_id:
        return {"ok": False, "error": "Primary agent is not configured"}

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
    except Exception as e:
        return {"ok": False, "error": detail_to_text(e)[:300]}

    _initialize_project_folder(
        project_dir,
        str(row["title"] or ""),
        str(row["brief"] or ""),
        str(row["goal"] or ""),
        setup_details=setup_details,
    )
    info_path = project_dir / PROJECT_INFO_FILE
    existing_info = ""
    if info_path.exists():
        try:
            existing_info = info_path.read_text(encoding="utf-8")
        except Exception:
            existing_info = ""
    if (
        existing_info.strip()
        and (not force)
        and "pending primary agent completion" not in existing_info.lower()
        and len(existing_info.strip()) >= 160
    ):
        return {"ok": True, "text": existing_info.strip(), "source": "existing", "agent_id": primary_agent_id}

    context = _project_context_instruction(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        project_root=str(row["project_root"] or ""),
        plan_status=PLAN_STATUS_PENDING,
    )
    roster = _agent_roster_markdown(role_rows)
    task = (
        f"{context}\n\n"
        f"{roster}\n\n"
        "Task:\n"
        f"1) Read `{SETUP_CHAT_HISTORY_FILE}`, `agents/ROLES.md`, and `{PROJECT_PROTOCOL_FILE}`.\n"
        f"2) Write or replace `{PROJECT_INFO_FILE}` with complete project context.\n"
        "3) Include: project summary, user requirements, constraints, assumptions, role responsibilities, execution prerequisites, and open questions.\n"
        "4) If some information is missing, make reasonable assumptions and clearly mark them under `Assumptions`.\n"
        "5) Return JSON only with `chat_update`, `output_files`, optional `notes`, and pause fields.\n"
        "6) Keep language concise and human-readable.\n"
    )
    info_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            "agents/ROLES.md",
            OVERVIEW_FILE,
            PROJECT_SETUP_FILE,
            PROJECT_PROTOCOL_FILE,
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=str(setup_details.get("setup_chat_summary") or ""),
        max_total_chars=8_500,
        max_files=8,
    )
    if info_context:
        task = f"{task}\n\n{info_context}"

    await emit(project_id, "project.info.generating", {"project_id": project_id})
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=connection_api_key,
        message=task,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:project-info",
        timeout_sec=55,
        user_id=str(row["user_id"] or ""),
    )
    p_tokens, c_tokens, _ = _extract_usage_counts(res)
    if p_tokens <= 0:
        p_tokens = _estimate_tokens_from_text(task)
    if c_tokens <= 0:
        c_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=p_tokens, completion_tokens=c_tokens)

    if not res.get("ok"):
        fallback = _python_project_info_markdown(
            title=str(row["title"] or ""),
            brief=str(row["brief"] or ""),
            goal=str(row["goal"] or ""),
            setup_details=setup_details,
            role_rows=role_rows,
        )
        try:
            info_path.write_text(fallback, encoding="utf-8")
        except Exception:
            return {"ok": False, "error": detail_to_text(res.get("error") or res.get("details"))[:1200]}
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="project.info.fallback",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.info.ready", {"status": "fallback", "preview": fallback[:900]})
        return {"ok": True, "text": fallback, "source": "fallback", "agent_id": primary_agent_id}

    raw_text = str(res.get("text") or "").strip()
    parsed = _extract_agent_report_payload(raw_text)
    writes = parsed.get("output_files") if isinstance(parsed.get("output_files"), list) else []
    has_info_write = False
    for item in writes:
        rel = _clean_relative_project_path(str(item.get("path") or ""))
        if rel and rel.lower() in {PROJECT_INFO_FILE.lower(), "project-info.md"}:
            has_info_write = True
            break
    if not has_info_write:
        fallback_content = raw_text.strip()
        if not fallback_content:
            fallback_content = _python_project_info_markdown(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
            )
        elif not fallback_content.lstrip().startswith("#"):
            fallback_content = (
                _seed_project_info_markdown(
                    title=str(row["title"] or ""),
                    brief=str(row["brief"] or ""),
                    goal=str(row["goal"] or ""),
                ).strip()
                + "\n\n## Primary Agent Notes\n"
                + fallback_content
            )
        writes = [{"path": PROJECT_INFO_FILE, "content": fallback_content, "append": False}, *writes]
    write_result = _apply_project_file_writes(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        writes=writes,
        default_prefix=f"{USER_OUTPUTS_DIRNAME}/project-info",
    )
    saved = write_result.get("saved") or []
    if not any(str(item.get("path") or "").strip().lower() == PROJECT_INFO_FILE.lower() for item in saved):
        info_path.write_text(
            _python_project_info_markdown(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
            ),
            encoding="utf-8",
        )
    try:
        text = info_path.read_text(encoding="utf-8").strip()
    except Exception:
        text = ""
    if not text:
        text = _python_project_info_markdown(
            title=str(row["title"] or ""),
            brief=str(row["brief"] or ""),
            goal=str(row["goal"] or ""),
            setup_details=setup_details,
            role_rows=role_rows,
        )
        info_path.write_text(text, encoding="utf-8")
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="project.info.ready",
        text=(parsed.get("chat_update") or raw_text or "Project info updated.")[:1500],
        payload={"saved_files": saved[:12]},
    )
    await emit(
        project_id,
        "project.info.ready",
        {"status": "ok", "agent_id": primary_agent_id, "preview": text[:900], "saved_files": saved[:12]},
    )
    return {"ok": True, "text": text, "source": "agent", "agent_id": primary_agent_id}

async def _generate_project_plan(project_id: str, *, force: bool = False) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id, p.plan_status,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    if (not force) and _coerce_plan_status(row["plan_status"]) == PLAN_STATUS_APPROVED:
        conn.close()
        return

    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    if not role_rows:
        now = int(time.time())
        msg = "Invite at least one project agent (and select a primary) before generating plan."
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, msg, now, project_id),
        )
        conn.commit()
        conn.close()
        _refresh_project_documents(project_id)
        await emit(project_id, "project.plan.failed", {"error": msg})
        return
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_GENERATING, int(time.time()), project_id),
    )
    conn.commit()
    conn.close()
    await emit(project_id, "project.plan.generating", {"project_id": project_id})
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.generating",
        text="Primary agent is generating project plan.",
    )

    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None

    info_result = await _ensure_project_info_document(project_id, force=force)
    project_info_excerpt = str(info_result.get("text") or "").strip()[:10_000]
    instruction = _plan_prompt_from_project(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        project_root=str(row["project_root"] or ""),
        project_info_excerpt=project_info_excerpt,
    )
    plan_file_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            OVERVIEW_FILE,
            PROJECT_SETUP_FILE,
            PROJECT_PROTOCOL_FILE,
            "agents/ROLES.md",
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=f"{str(row['brief'] or '')}\n{str(row['goal'] or '')}",
        max_total_chars=7_000,
        max_files=8,
    )
    if plan_file_context:
        instruction = f"{instruction}\n\n{plan_file_context}"
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=connection_api_key,
        message=instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:plan",
        timeout_sec=55,
        user_id=str(row["user_id"] or ""),
    )
    prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
    if prompt_tokens <= 0:
        prompt_tokens = _estimate_tokens_from_text(instruction)
    if completion_tokens <= 0:
        completion_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)

    now = int(time.time())
    conn = db()
    if not res.get("ok"):
        error_text = detail_to_text(res.get("error") or res.get("details") or "Failed to generate project plan")
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, error_text[:5000], now, project_id),
        )
        conn.commit()
        conn.close()
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.failed",
            text=error_text[:1200],
        )
        await emit(project_id, "project.plan.failed", {"error": error_text[:1200]})
        return

    plan_text = str(res.get("text") or "").strip()
    if not plan_text:
        plan_text = detail_to_text(res.get("frames") or "Plan generated with empty text")
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_AWAITING_APPROVAL, plan_text[:20000], now, project_id),
    )
    conn.commit()
    conn.close()
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.ready",
        text=(plan_text or "")[:1600],
    )
    await emit(project_id, "project.plan.ready", {"status": PLAN_STATUS_AWAITING_APPROVAL, "preview": plan_text[:1000]})

async def _delegate_project_tasks(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()

    if _coerce_plan_status(row["plan_status"]) != PLAN_STATUS_APPROVED:
        await emit(project_id, "project.delegation.skipped", {"reason": "Plan not approved"})
        return
    if not role_rows:
        _set_project_execution_state(project_id, status=EXEC_STATUS_IDLE, progress_pct=0)
        _refresh_project_documents(project_id)
        await emit(project_id, "project.delegation.skipped", {"reason": "No invited agents yet"})
        return

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=15)
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="delegation.started",
        text="Primary agent started delegation planning after plan approval.",
        payload={"agents": [str(r.get("agent_id") or "") for r in role_rows]},
    )
    await emit(project_id, "project.delegation.started", {"agents": [r.get("agent_id") for r in role_rows]})
    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None

    info_result = await _ensure_project_info_document(project_id, force=False)
    project_info_excerpt = str(info_result.get("text") or "").strip()[:10_000]
    instruction = _delegate_prompt_from_project(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        plan_text=str(row["plan_text"] or ""),
        project_root=str(row["project_root"] or ""),
        project_info_excerpt=project_info_excerpt,
    )
    delegate_file_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            OVERVIEW_FILE,
            PROJECT_PLAN_FILE,
            PROJECT_SETUP_FILE,
            PROJECT_PROTOCOL_FILE,
            "agents/ROLES.md",
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=str(row["plan_text"] or ""),
        max_total_chars=8_000,
        max_files=8,
    )
    if delegate_file_context:
        instruction = f"{instruction}\n\n{delegate_file_context}"
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=connection_api_key,
        message=instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:delegate",
        timeout_sec=55,
        user_id=str(row["user_id"] or ""),
    )
    prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
    if prompt_tokens <= 0:
        prompt_tokens = _estimate_tokens_from_text(instruction)
    if completion_tokens <= 0:
        completion_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)

    if not res.get("ok"):
        _set_project_execution_state(project_id, status=EXEC_STATUS_STOPPED)
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="delegation.failed",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.delegation.failed", {"error": detail_to_text(res.get("error") or res.get("details"))[:1200]})
        return

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=55)
    primary_reply = str(res.get("text") or "").strip()
    payload = _parse_delegation_payload(primary_reply)
    by_id = {str(r.get("agent_id") or "").strip(): r for r in role_rows}
    project_md = str(payload.get("project_delegation_md") or payload.get("project_md") or "").strip()
    if not project_md:
        project_md = str(row["plan_text"] or "").strip() or "Delegation initialized."
    if primary_reply:
        await emit(
            project_id,
            "agent.primary.update",
            {
                "agent_id": primary_agent_id,
                "agent_name": next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(primary_agent_id or "")), ""),
                "text": primary_reply[:1200],
            },
        )
    for note in _summarize_ws_frames(res.get("frames"), limit=10):
        await emit(project_id, "agent.primary.live", {"agent_id": primary_agent_id, "note": note})
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="agent.primary.update",
        text=primary_reply[:1800] if primary_reply else "Primary agent returned delegation payload.",
    )

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
    except Exception:
        await emit(project_id, "project.delegation.failed", {"error": "Project directory unavailable"})
        return
    project_dir.mkdir(parents=True, exist_ok=True)
    agents_dir = project_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)

    (project_dir / PROJECT_DELEGATION_FILE).write_text(project_md + "\n", encoding="utf-8")
    legacy_delegation = (project_dir / "project-delegation.md").resolve()
    if _path_within(legacy_delegation, project_dir) and legacy_delegation.exists():
        try:
            legacy_delegation.unlink()
        except Exception:
            pass
    assigned_count = 0
    raw_tasks = payload.get("agent_tasks")
    task_map: Dict[str, str] = {}
    if isinstance(raw_tasks, list):
        for item in raw_tasks:
            if not isinstance(item, dict):
                continue
            aid = str(item.get("agent_id") or "").strip()
            task_md = str(item.get("task_md") or "").strip()
            if aid and task_md and aid in by_id:
                task_map[aid] = task_md

    agent_order = list(by_id.keys())
    assigned_task_map: Dict[str, str] = {}
    assigned_mentions_map: Dict[str, List[str]] = {}
    for pos, aid in enumerate(agent_order):
        row_item = by_id.get(aid) or {}
        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        default_task = (
            f"Read {PROJECT_INFO_FILE}, {PROJECT_PROTOCOL_FILE}, {OVERVIEW_FILE}, {PROJECT_PLAN_FILE}, and {PROJECT_DELEGATION_FILE}, then execute assigned scope and report progress in chat.\n"
            f"- Follow dependency order from {PROJECT_DELEGATION_FILE}.\n"
            "- If your output unblocks another agent, mention them explicitly as @agent_id in chat_update so handoff happens in chat.\n"
            "- Save concrete artifacts into project files using output_files.\n"
            "- Persist deliverables in Hivee project files; do not keep final-only copies on provider/local runtime server.\n"
            "- If blocked by missing user approval/input (credentials, API key, sign-off, pit stop), set requires_user_input=true with pause_reason and resume_hint.\n"
            "- If user answers SKIP, decide assumptions responsibly and continue.\n"
        )
        next_aid = agent_order[pos + 1] if (pos + 1) < len(agent_order) else None
        task_text = _normalize_task_markdown_for_agent(
            agent_id=aid,
            role=role,
            task_md=task_map.get(aid, default_task),
            next_agent_id=next_aid,
        )
        assigned_task_map[aid] = task_text
        fname = _safe_agent_filename(aid) + ".md"
        (agents_dir / fname).write_text(task_text.strip() + "\n", encoding="utf-8")
        assigned_count += 1
        mention_targets = sorted({m for m in re.findall(r"@([a-zA-Z0-9._-]+)", task_text) if m and m != aid})[:8]
        assigned_mentions_map[aid] = mention_targets
        await emit(
            project_id,
            "agent.task.assigned",
            {
                "agent_id": aid,
                "agent_name": str(row_item.get("agent_name") or aid),
                "role": role,
                "task_file": f"agents/{fname}",
                "task_preview": task_text[:500],
                "mentions": mention_targets,
            },
        )
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agent.task.assigned",
            text=f"{aid}: {task_text[:800]}",
            payload={"task_file": f"agents/{fname}", "mentions": mention_targets},
        )

    _write_project_agent_roles_file(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        agents=role_rows,
    )
    outputs_dir = project_dir / USER_OUTPUTS_DIRNAME
    outputs_dir.mkdir(parents=True, exist_ok=True)
    processed_agents = 0
    failed_agents = 0
    agent_total = max(1, len(agent_order))
    team_roster_text = _agent_roster_markdown(role_rows)
    primary_agent_name = next(
        (
            str(r.get("agent_name") or r.get("agent_id") or "")
            for r in role_rows
            if str(r.get("agent_id") or "").strip() == str(primary_agent_id or "").strip()
        ),
        str(primary_agent_id or "primary"),
    )
    primary_last_chat_update = ""
    primary_last_notes = ""
    primary_last_pause_reason = ""
    primary_last_resume_hint = ""
    primary_pause_resolved = False

    for idx, aid in enumerate(agent_order, start=1):
        row_item = by_id.get(aid) or {}
        while True:
            state, _ = _read_project_execution_state(project_id)
            if state == EXEC_STATUS_PAUSED:
                await asyncio.sleep(0.7)
                continue
            if state == EXEC_STATUS_STOPPED:
                _refresh_project_documents(project_id)
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="delegation.stopped",
                    text="Delegation run stopped by user before all agents reported.",
                )
                await emit(
                    project_id,
                    "project.delegation.stopped",
                    {"processed_agents": processed_agents, "failed_agents": failed_agents, "total_agents": len(agent_order)},
                )
                return
            break

        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        agent_name = str(row_item.get("agent_name") or aid)
        task_text = assigned_task_map.get(aid) or f"# Task for {aid}\n\nRole: {role}\n"
        task_rel = f"agents/{_safe_agent_filename(aid)}.md"
        agent_file_context = _build_project_file_context(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            include_paths=[
                task_rel,
                PROJECT_INFO_FILE,
                PROJECT_DELEGATION_FILE,
                PROJECT_PLAN_FILE,
                OVERVIEW_FILE,
                PROJECT_PROTOCOL_FILE,
                "agents/ROLES.md",
                SETUP_CHAT_HISTORY_FILE,
            ],
            request_text=task_text,
            max_total_chars=7_500,
            max_files=8,
        )
        await emit(
            project_id,
            "agent.task.started",
            {"agent_id": aid, "agent_name": agent_name, "role": role},
        )

        agent_instruction = (
            _project_context_instruction(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
                project_root=str(row["project_root"] or ""),
                plan_status=PLAN_STATUS_APPROVED,
            )
            + "\n\n"
            + f"You are invited agent `{aid}` with role `{role}`.\n"
            + team_roster_text
            + "\n"
            + "Execute your assigned task and return JSON object only:\n"
            + "{\n"
            + "  \"chat_update\": \"Human-friendly update sentence to show in chat\",\n"
            + "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"file content\",\"append\":false}],\n"
            + "  \"notes\": \"optional technical notes\",\n"
            + "  \"requires_user_input\": false,\n"
            + "  \"pause_reason\": \"\",\n"
            + "  \"resume_hint\": \"\"\n"
            + "}\n"
            + "Rules:\n"
            + "- chat_update must read like normal conversation.\n"
            + "- Put every created/updated artifact in output_files.\n"
            + "- Persist deliverables in Hivee project files; do not keep final-only copies on provider/local runtime server.\n"
            + "- Use relative paths inside this project only.\n"
            + "- Use exact IDs from roster when mentioning other agents.\n"
            + "- Mention handoff needs in chat_update with @agent_id if needed.\n"
            + f"- Follow `{PROJECT_PROTOCOL_FILE}` for delegation, mention, and status update rules.\n\n"
            + "- If blocked by user approval/input or planned pit stop, set requires_user_input=true and explain pause_reason.\n"
            + "- If user says SKIP for missing info, proceed with assumptions and state them briefly in chat_update.\n"
            + "Assigned task:\n"
            + task_text.strip()
        )
        if agent_file_context:
            agent_instruction = f"{agent_instruction}\n\n{agent_file_context}"
        agent_res = await openclaw_ws_chat(
            base_url=str(row["base_url"]),
            api_key=connection_api_key,
            message=agent_instruction,
            agent_id=aid,
            session_key=f"{project_id}:agent:{aid}",
            timeout_sec=50,
            user_id=str(row["user_id"] or ""),
        )
        p_tokens, c_tokens, _ = _extract_usage_counts(agent_res)
        if p_tokens <= 0:
            p_tokens = _estimate_tokens_from_text(agent_instruction)
        if c_tokens <= 0:
            c_tokens = _estimate_tokens_from_text(agent_res.get("text"))
        _update_project_usage_metrics(project_id, prompt_tokens=p_tokens, completion_tokens=c_tokens)

        if not agent_res.get("ok"):
            failed_agents += 1
            err_text = detail_to_text(agent_res.get("error") or agent_res.get("details") or "Agent task failed")[:1200]
            await emit(
                project_id,
                "agent.task.failed",
                {"agent_id": aid, "agent_name": agent_name, "error": err_text},
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.task.failed",
                text=f"{aid}: {err_text}",
            )
            continue

        report_text = str(agent_res.get("text") or "").strip()
        if not report_text:
            report_text = detail_to_text(agent_res.get("frames") or "No text response.")
        parsed_report = _extract_agent_report_payload(report_text)
        chat_update = str(parsed_report.get("chat_update") or "").strip() or "I have completed this task step."
        report_notes = str(parsed_report.get("notes") or "").strip()
        requires_user_input = bool(parsed_report.get("requires_user_input"))
        pause_reason = str(parsed_report.get("pause_reason") or "").strip()
        resume_hint = str(parsed_report.get("resume_hint") or "").strip()
        output_files_raw = parsed_report.get("output_files") or []
        agent_output_allow_paths = [
            USER_OUTPUTS_DIRNAME,
            f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
        ]
        write_result = _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files_raw if isinstance(output_files_raw, list) else [],
            default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
            allow_paths=agent_output_allow_paths,
        )
        saved_files = write_result.get("saved") or []
        skipped_files = write_result.get("skipped") or []
        artifact_followup_used = False
        artifact_rescue_used = False
        artifact_like_task = _looks_like_artifact_request(task_text)
        if _should_request_artifact_followup(
            user_message=task_text,
            raw_response=report_text,
            parsed_payload=parsed_report,
            saved_files=saved_files,
        ):
            artifact_followup_used = True
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "No synced files detected yet. Requesting explicit output_files payload.",
                },
            )
            followup_prompt = _build_artifact_followup_prompt(
                user_message=task_text,
                previous_response=report_text,
            )
            followup_res = await openclaw_ws_chat(
                base_url=str(row["base_url"]),
                api_key=connection_api_key,
                message=followup_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=45,
                user_id=str(row["user_id"] or ""),
            )
            if followup_res.get("ok"):
                fp, fc, _ = _extract_usage_counts(followup_res)
                if fp <= 0:
                    fp = _estimate_tokens_from_text(followup_prompt)
                if fc <= 0:
                    fc = _estimate_tokens_from_text(followup_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=fp, completion_tokens=fc)
                followup_text = str(followup_res.get("text") or "").strip()
                parsed_followup = _extract_agent_report_payload(followup_text)
                followup_chat = str(parsed_followup.get("chat_update") or "").strip()
                followup_writes = parsed_followup.get("output_files") or []
                requires_user_input = requires_user_input or bool(parsed_followup.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_followup.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_followup.get("resume_hint") or "").strip()
                if not report_notes:
                    report_notes = str(parsed_followup.get("notes") or "").strip()
                followup_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=followup_writes if isinstance(followup_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                    allow_paths=agent_output_allow_paths,
                )
                followup_saved = followup_write_result.get("saved") or []
                followup_skipped = followup_write_result.get("skipped") or []
                if followup_saved:
                    saved_files.extend(followup_saved)
                if followup_skipped:
                    skipped_files.extend(followup_skipped)
                if followup_chat:
                    chat_update = followup_chat
                if followup_text:
                    report_text = (report_text + "\n\n[ARTIFACT FOLLOW-UP]\n" + followup_text).strip()
                for note in _summarize_ws_frames(followup_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_files.append(
                    "artifact follow-up failed: "
                    + detail_to_text(followup_res.get("error") or followup_res.get("details") or "unknown")
                )

        if not saved_files and not requires_user_input and artifact_like_task:
            artifact_rescue_used = True
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "Still no synced files. Forcing concrete deliverables payload.",
                },
            )
            rescue_prompt = _build_artifact_recovery_prompt(
                agent_id=aid,
                role=role,
                task_text=task_text,
                previous_response=report_text,
            )
            rescue_res = await openclaw_ws_chat(
                base_url=str(row["base_url"]),
                api_key=connection_api_key,
                message=rescue_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=45,
                user_id=str(row["user_id"] or ""),
            )
            if rescue_res.get("ok"):
                rp, rc, _ = _extract_usage_counts(rescue_res)
                if rp <= 0:
                    rp = _estimate_tokens_from_text(rescue_prompt)
                if rc <= 0:
                    rc = _estimate_tokens_from_text(rescue_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=rp, completion_tokens=rc)
                rescue_text = str(rescue_res.get("text") or "").strip()
                parsed_rescue = _extract_agent_report_payload(rescue_text)
                rescue_chat = str(parsed_rescue.get("chat_update") or "").strip()
                rescue_writes = parsed_rescue.get("output_files") or []
                if not rescue_writes:
                    rescue_writes = _extract_artifacts_from_fenced_code(rescue_text)
                rescue_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=rescue_writes if isinstance(rescue_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                    allow_paths=agent_output_allow_paths,
                )
                rescue_saved = rescue_write_result.get("saved") or []
                rescue_skipped = rescue_write_result.get("skipped") or []
                if rescue_saved:
                    saved_files.extend(rescue_saved)
                if rescue_skipped:
                    skipped_files.extend(rescue_skipped)
                requires_user_input = requires_user_input or bool(parsed_rescue.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_rescue.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_rescue.get("resume_hint") or "").strip()
                if not report_notes:
                    report_notes = str(parsed_rescue.get("notes") or "").strip()
                if rescue_chat:
                    chat_update = rescue_chat
                if rescue_text:
                    report_text = (report_text + "\n\n[ARTIFACT RESCUE]\n" + rescue_text).strip()
                for note in _summarize_ws_frames(rescue_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_files.append(
                    "artifact rescue failed: "
                    + detail_to_text(rescue_res.get("error") or rescue_res.get("details") or "unknown")
                )

        if not saved_files and not requires_user_input and artifact_like_task:
            fallback_rel = f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}-deliverable.md"
            fallback_content = (
                f"# Deliverable Snapshot: {agent_name}\n\n"
                f"- agent_id: {aid}\n"
                f"- role: {role}\n"
                f"- generated_at: {format_ts(int(time.time()))}\n\n"
                f"## Chat Update\n{chat_update}\n\n"
                f"## Raw Response\n{report_text.strip()}\n"
            )
            fallback_write_result = _apply_project_file_writes(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                writes=[{"path": fallback_rel, "content": fallback_content, "append": False}],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                allow_paths=agent_output_allow_paths,
            )
            fallback_saved = fallback_write_result.get("saved") or []
            fallback_skipped = fallback_write_result.get("skipped") or []
            if fallback_saved:
                saved_files.extend(fallback_saved)
                skipped_files.append("No explicit output_files from agent; saved fallback markdown deliverable.")
            if fallback_skipped:
                skipped_files.extend(fallback_skipped)

        pause_decision = _infer_pause_request(
            chat_update=chat_update,
            notes=report_notes,
            explicit_requires_user_input=requires_user_input,
            explicit_pause_reason=pause_reason,
            explicit_resume_hint=resume_hint,
        )
        if pause_decision.get("pause"):
            pause_reason = str(pause_decision.get("reason") or pause_reason or chat_update).strip()
            resume_hint = str(pause_decision.get("resume_hint") or resume_hint).strip()
            chat_update = _ensure_owner_mention(chat_update)
        else:
            chat_update = _ensure_chat_handoff_mentions(chat_update, assigned_mentions_map.get(aid) or [])

        if str(aid).strip() == str(primary_agent_id or "").strip():
            primary_last_chat_update = chat_update
            primary_last_notes = report_notes
            primary_last_pause_reason = pause_reason
            primary_last_resume_hint = resume_hint

        report_file = outputs_dir / f"{_safe_agent_filename(aid)}-latest.md"
        report_file.write_text(
            f"# Agent Report: {agent_name}\n\n"
            f"- agent_id: {aid}\n"
            f"- role: {role}\n"
            f"- generated_at: {format_ts(int(time.time()))}\n"
            f"- files_saved: {len(saved_files)}\n\n"
            f"- artifact_followup_used: {'yes' if artifact_followup_used else 'no'}\n\n"
            f"- artifact_rescue_used: {'yes' if artifact_rescue_used else 'no'}\n\n"
            f"- requires_user_input: {'yes' if pause_decision.get('pause') else 'no'}\n"
            f"- pause_reason: {pause_reason or '-'}\n"
            f"- resume_hint: {resume_hint or '-'}\n\n"
            f"## Chat Update\n{chat_update}\n\n"
            f"## Raw Response\n{report_text.strip()}\n",
            encoding="utf-8",
        )
        processed_agents += 1
        pct = min(95, 55 + int((idx / agent_total) * 40))
        _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=pct)
        for note in _summarize_ws_frames(agent_res.get("frames"), limit=8):
            await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
        for item in saved_files:
            await emit(
                project_id,
                "project.file.written",
                {
                    "path": str(item.get("path") or ""),
                    "mode": str(item.get("mode") or "w"),
                    "bytes": int(item.get("bytes") or 0),
                    "actor": f"agent:{aid}",
                },
            )
        await emit(
            project_id,
            "agent.task.reported",
            {
                "agent_id": aid,
                "agent_name": agent_name,
                "text": chat_update[:1200],
                "output_file": f"{USER_OUTPUTS_DIRNAME}/{report_file.name}",
                "saved_files": saved_files[:20],
                "skipped_files": skipped_files[:10],
                "requires_user_input": bool(pause_decision.get("pause")),
                "pause_reason": pause_reason[:500],
                "resume_hint": resume_hint[:300],
            },
        )
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agent.task.reported",
            text=f"{aid}: {chat_update[:1600]}",
            payload={
                "output_file": f"{USER_OUTPUTS_DIRNAME}/{report_file.name}",
                "saved_files": saved_files[:20],
                "skipped_files": skipped_files[:10],
                "requires_user_input": bool(pause_decision.get("pause")),
                "pause_reason": pause_reason[:500],
                "resume_hint": resume_hint[:300],
            },
        )
        _refresh_project_documents(project_id)
        if pause_decision.get("pause"):
            state_now, pct_now = _read_project_execution_state(project_id)
            if state_now not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
                pause_pct = max(5, _clamp_progress(pct_now if pct_now > 0 else pct))
                _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
                _refresh_project_documents(project_id)
                summary = pause_reason or "Execution paused. Waiting for owner input."
                await emit(
                    project_id,
                    "project.execution.auto_paused",
                    {
                        "status": EXEC_STATUS_PAUSED,
                        "progress_pct": pause_pct,
                        "agent_id": aid,
                        "agent_name": agent_name,
                        "reason": summary[:900],
                        "resume_hint": (resume_hint or "Reply with required input, then say CONTINUE or press Resume.")[:300],
                    },
                )
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="execution.auto_paused",
                    text=f"{aid}: {summary[:1200]}",
                    payload={"agent_id": aid, "resume_hint": resume_hint[:300]},
                )
                while True:
                    wait_state, _ = _read_project_execution_state(project_id)
                    if wait_state == EXEC_STATUS_PAUSED:
                        await asyncio.sleep(0.7)
                        continue
                    if wait_state == EXEC_STATUS_STOPPED:
                        _refresh_project_documents(project_id)
                        _append_project_daily_log(
                            owner_user_id=str(row["user_id"]),
                            project_root=str(row["project_root"] or ""),
                            kind="delegation.stopped",
                            text="Delegation run stopped by user while waiting for resume.",
                        )
                        await emit(
                            project_id,
                            "project.delegation.stopped",
                            {"processed_agents": processed_agents, "failed_agents": failed_agents, "total_agents": len(agent_order)},
                        )
                        return
                    break
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="execution.resumed",
                    text=f"Execution resumed after pause request from {aid}.",
                )
                await emit(
                    project_id,
                    "project.execution.resumed_after_pause",
                    {"status": EXEC_STATUS_RUNNING, "agent_id": aid, "agent_name": agent_name},
                )
                if str(aid).strip() == str(primary_agent_id or "").strip():
                    primary_pause_resolved = True

    final_primary_pause = _infer_pause_request(
        chat_update=primary_last_chat_update,
        notes=primary_last_notes,
        explicit_requires_user_input=False,
        explicit_pause_reason=primary_last_pause_reason,
        explicit_resume_hint=primary_last_resume_hint,
    )
    if (
        str(primary_agent_id or "").strip()
        and not primary_pause_resolved
        and final_primary_pause.get("pause")
    ):
        state_now, pct_now = _read_project_execution_state(project_id)
        if state_now not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
            pause_pct = max(5, _clamp_progress(pct_now if pct_now > 0 else 95))
            _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
            _refresh_project_documents(project_id)
            summary = str(
                final_primary_pause.get("reason")
                or "Primary agent still needs owner input before finishing."
            ).strip()
            hint = str(
                final_primary_pause.get("resume_hint")
                or "Reply with required information, then say CONTINUE (or press Resume)."
            ).strip()
            await emit(
                project_id,
                "project.execution.auto_paused",
                {
                    "status": EXEC_STATUS_PAUSED,
                    "progress_pct": pause_pct,
                    "agent_id": primary_agent_id,
                    "agent_name": primary_agent_name,
                    "reason": summary[:900],
                    "resume_hint": hint[:300],
                },
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="execution.auto_paused",
                text=f"{primary_agent_id}: {summary[:1200]}",
                payload={"agent_id": primary_agent_id, "resume_hint": hint[:300]},
            )
            return

    _set_project_execution_state(project_id, status=EXEC_STATUS_COMPLETED, progress_pct=100)
    _refresh_project_documents(project_id)
    quoted_project_id = url_quote(project_id, safe="")
    project_files_api_link = f"/api/projects/{project_id}/files"
    outputs_folder_api_link = f"/api/projects/{project_id}/files?path={url_quote(USER_OUTPUTS_DIRNAME, safe='')}"
    project_files_link = f"/?project={quoted_project_id}&project_pane=folder"
    outputs_folder_link = (
        f"/?project={quoted_project_id}&project_pane=folder&project_path={url_quote(USER_OUTPUTS_DIRNAME, safe='')}"
    )
    latest_output_rel = _latest_file_relative_path(outputs_dir, project_dir)
    latest_preview_api_link = (
        f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(latest_output_rel)}"
        if latest_output_rel
        else ""
    )
    latest_preview_link = ""
    if latest_output_rel:
        preview_rel = _clean_relative_project_path(latest_output_rel)
        preview_parent = USER_OUTPUTS_DIRNAME
        if preview_rel and "/" in preview_rel:
            preview_parent = preview_rel.rsplit("/", 1)[0]
        latest_preview_link = (
            f"/?project={quoted_project_id}"
            f"&project_pane=folder"
            f"&project_path={url_quote(preview_parent, safe='')}"
            f"&project_preview={url_quote(preview_rel, safe='')}"
        )
    owner_notice_parts = [
        f"@owner project `{str(row['title'] or project_id)}` is completed.",
        f"Open project files: {project_files_link}",
        f"Outputs folder: {outputs_folder_link}",
    ]
    if latest_preview_link:
        owner_notice_parts.append(f"Latest file preview: {latest_preview_link}")
    primary_done_update = " ".join(owner_notice_parts).strip()
    await emit(
        project_id,
        "agent.primary.update",
        {
            "agent_id": primary_agent_id or "primary",
            "agent_name": primary_agent_name,
            "text": primary_done_update[:1200],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
            "project_files_api_link": project_files_api_link,
            "outputs_folder_api_link": outputs_folder_api_link,
            "latest_preview_api_link": latest_preview_api_link,
        },
    )
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="delegation.ready",
        text=f"Delegation documents generated for {assigned_count} invited agents. Reports: {processed_agents}, failed: {failed_agents}. {primary_done_update}",
        payload={
            "agents": assigned_count,
            "processed_agents": processed_agents,
            "failed_agents": failed_agents,
            "notes": str(payload.get("notes") or "")[:1000],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
            "project_files_api_link": project_files_api_link,
            "outputs_folder_api_link": outputs_folder_api_link,
            "latest_preview_api_link": latest_preview_api_link,
        },
    )
    await emit(
        project_id,
        "project.delegation.ready",
        {
            "agents": assigned_count,
            "processed_agents": processed_agents,
            "failed_agents": failed_agents,
            "notes": str(payload.get("notes") or "")[:1000],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
            "project_files_api_link": project_files_api_link,
            "outputs_folder_api_link": outputs_folder_api_link,
            "latest_preview_api_link": latest_preview_api_link,
            "owner_message": primary_done_update[:1200],
        },
    )


def _read_project_execution_state(project_id: str) -> Tuple[str, int]:
    conn = db()
    row = conn.execute(
        "SELECT execution_status, progress_pct FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    conn.close()
    if not row:
        return EXEC_STATUS_IDLE, 0
    return _coerce_execution_status(row["execution_status"]), _clamp_progress(row["progress_pct"])

__all__ = [name for name in globals() if not name.startswith('__')]

