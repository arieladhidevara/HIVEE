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
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    seen: set[str] = set()
    candidates = raw_agents if isinstance(raw_agents, list) else []
    for row in candidates:
        if isinstance(row, str):
            aid = str(row).strip()
            nm = aid
            raw = None
        elif isinstance(row, dict):
            aid = str(row.get("id") or row.get("agent_id") or row.get("name") or "").strip()
            nm = str(row.get("name") or row.get("title") or aid).strip()
            raw = row
        else:
            continue
        if not aid:
            continue
        if aid in seen:
            continue
        seen.add(aid)
        cleaned.append({"id": aid[:180], "name": (nm or aid)[:220], "raw": raw})
    fallback_id = str(fallback_agent_id or "").strip()
    if fallback_id and fallback_id not in seen:
        cleaned.append(
            {
                "id": fallback_id[:180],
                "name": (str(fallback_agent_name or fallback_id).strip() or fallback_id)[:220],
                "raw": None,
            }
        )
    return cleaned

def _managed_agent_capability_key(raw_label: Any) -> str:
    label = str(raw_label or "").strip()
    if not label:
        return ""
    label = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", label)
    label = re.sub(r"[^a-zA-Z0-9]+", "_", label).strip("_").lower()
    return label[:80]

def _merge_managed_agent_capabilities(target: Dict[str, Any], source: Any) -> None:
    if not isinstance(target, dict) or source is None:
        return
    if isinstance(source, dict):
        for raw_key, raw_value in source.items():
            key = _managed_agent_capability_key(raw_key)
            if not key:
                continue
            if isinstance(raw_value, bool):
                target[key] = raw_value
            elif isinstance(raw_value, (int, float)):
                target[key] = raw_value
            elif isinstance(raw_value, dict):
                target[key] = dict(raw_value)
            elif isinstance(raw_value, list):
                target[key] = [str(item).strip() for item in raw_value if str(item).strip()]
            else:
                target[key] = bool(str(raw_value).strip())
        return
    if isinstance(source, (list, tuple, set)):
        for item in source:
            if isinstance(item, dict):
                label = item.get("name") or item.get("id") or item.get("title") or item.get("label")
            else:
                label = item
            key = _managed_agent_capability_key(label)
            if key:
                target[key] = True
        return
    key = _managed_agent_capability_key(source)
    if key:
        target[key] = True

def _normalize_managed_agent_skills(*sources: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        if isinstance(source, (list, tuple, set)):
            candidates = list(source)
        elif isinstance(source, (dict, str)):
            candidates = [source]
        else:
            candidates = []
        for item in candidates:
            if isinstance(item, str):
                skill_name = str(item).strip()
                if not skill_name:
                    continue
                skill_id = _managed_agent_capability_key(skill_name) or "skill"
                skill_payload: Dict[str, Any] = {"id": skill_id, "name": skill_name}
            elif isinstance(item, dict):
                skill_name = str(item.get("name") or item.get("title") or item.get("id") or "").strip()
                if not skill_name:
                    continue
                skill_id = str(item.get("id") or _managed_agent_capability_key(skill_name) or "skill").strip()
                skill_payload = {"id": skill_id[:120], "name": skill_name[:220]}
                skill_desc = str(item.get("description") or item.get("summary") or "").strip()
                if skill_desc:
                    skill_payload["description"] = skill_desc[:320]
                tags = item.get("tags")
                if isinstance(tags, list):
                    skill_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
                    if skill_tags:
                        skill_payload["tags"] = skill_tags[:12]
            else:
                continue
            dedupe_key = str(skill_payload.get("name") or skill_payload.get("id") or "").strip().lower()
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            output.append(skill_payload)
    return output[:12]

def _select_managed_agent_description(
    *,
    agent_id: str,
    raw_agent: Optional[Dict[str, Any]],
    existing_card: Optional[Dict[str, Any]],
) -> str:
    raw = raw_agent if isinstance(raw_agent, dict) else {}
    existing = existing_card if isinstance(existing_card, dict) else {}
    candidates = [
        raw.get("description"),
        raw.get("summary"),
        raw.get("role"),
        existing.get("description"),
        f"Hivee managed profile for agent `{agent_id}`.",
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text[:600]
    return f"Hivee managed profile for agent `{agent_id}`."

def _build_managed_agent_card(
    *,
    agent_id: str,
    agent_name: str,
    base_url: str,
    connection_id: str,
    env_id: Optional[str],
    root_path: str,
    raw_agent: Optional[Dict[str, Any]] = None,
    existing_card: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    safe_skill_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-") or "agent"
    raw = raw_agent if isinstance(raw_agent, dict) else {}
    existing = existing_card if isinstance(existing_card, dict) else {}
    provider = dict(existing.get("provider") or {}) if isinstance(existing.get("provider"), dict) else {}
    provider.setdefault("organization", "Hivee")

    capabilities: Dict[str, Any] = {}
    _merge_managed_agent_capabilities(capabilities, existing.get("capabilities"))
    _merge_managed_agent_capabilities(capabilities, raw.get("capabilities"))
    _merge_managed_agent_capabilities(capabilities, raw.get("tools"))
    _merge_managed_agent_capabilities(capabilities, raw.get("tags"))
    capabilities.setdefault("streaming", True)
    capabilities.setdefault("push_notifications", False)
    capabilities.setdefault("state_transition_history", True)

    skills = _normalize_managed_agent_skills(
        raw.get("skills"),
        existing.get("skills"),
    )
    if not skills:
        skills = [
            {
                "id": f"{safe_skill_id}.execute",
                "name": "Project Execution",
                "description": "Executes scoped project tasks and reports progress.",
                "tags": ["execution", "workflow", "collaboration"],
            }
        ]

    metadata = dict(existing.get("metadata") or {}) if isinstance(existing.get("metadata"), dict) else {}
    metadata.update(
        {
            "managedBy": "hivee",
            "connectionId": connection_id,
            "environmentId": env_id,
            "rootPath": root_path,
            "provisionedAt": now,
            "hiveeProjectOps": [
                "write_file",
                "append_file",
                "upload_file",
                "delete_file",
                "move_file",
                "create_dir",
                "delete_dir",
                "create_task",
                "update_task",
                "delete_task",
                "add_task_dependency",
                "remove_task_dependency",
                "apply_task_blueprint",
                "update_execution",
                "post_chat_message",
            ],
            "hiveeRealtime": ["project.chat.message", "project.chat.mention", "project.execution.updated"],
        }
    )
    model_hint = str(
        raw.get("model")
        or raw.get("adapter_type")
        or metadata.get("agentModel")
        or ""
    ).strip()
    if model_hint:
        metadata["agentModel"] = model_hint[:180]
    source_role = str(raw.get("role") or "").strip()
    if source_role:
        metadata["sourceRole"] = source_role[:180]

    card_payload = dict(existing)
    card_payload.update(
        {
        "schemaVersion": MANAGED_AGENT_CARD_VERSION,
        "name": str(agent_name or agent_id),
        "description": _select_managed_agent_description(
            agent_id=agent_id,
            raw_agent=raw,
            existing_card=existing,
        ),
        "version": str(existing.get("version") or "1.0.0"),
        "provider": provider,
        "supportedInterfaces": [
            {
                "url": str(base_url or "").rstrip("/"),
                "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0",
            }
        ],
        "capabilities": capabilities,
        "defaultInputModes": (
            list(existing.get("defaultInputModes"))
            if isinstance(existing.get("defaultInputModes"), list) and existing.get("defaultInputModes")
            else ["text"]
        ),
        "defaultOutputModes": (
            list(existing.get("defaultOutputModes"))
            if isinstance(existing.get("defaultOutputModes"), list) and existing.get("defaultOutputModes")
            else ["text"]
        ),
        "skills": skills,
        "securityRequirements": (
            list(existing.get("securityRequirements"))
            if isinstance(existing.get("securityRequirements"), list) and existing.get("securityRequirements")
            else [{"type": "bearer", "scopes": ["env.read", "project.read", "project.write", "project.chat", "project.state.write"]}]
        ),
        "metadata": metadata,
        }
    )
    return card_payload

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
        raw_agent = agent.get("raw") if isinstance(agent.get("raw"), dict) else None
        if not agent_id:
            failed += 1
            errors.append("Missing agent id in candidate entry")
            continue
        try:
            parts = _agent_component_paths(user_id, connection_id, agent_id)
            try:
                for path in parts.values():
                    path.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass  # Filesystem may be ephemeral (e.g. Railway) — don't block DB provisioning

            root_path = str(parts["root"].resolve()) if parts["root"].exists() else str(parts["root"])
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
                "scopes": ["env.read", "project.read", "project.write", "project.chat", "project.state.write"],
                "tools": ["workspace.read", "workspace.write", "chat.send", "project.control", "project.agent_ops", "project.chat.post"],
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
                "SELECT id, card_json FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id = ?",
                (user_id, connection_id, agent_id),
            ).fetchone()
            existing_card: Dict[str, Any] = {}
            if existing:
                try:
                    existing_card = json.loads(str(existing["card_json"] or "{}"))
                except Exception:
                    existing_card = {}
            card_payload = _build_managed_agent_card(
                agent_id=agent_id,
                agent_name=agent_name,
                base_url=base_url,
                connection_id=connection_id,
                env_id=env_id,
                root_path=root_path,
                raw_agent=raw_agent,
                existing_card=existing_card,
            )
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
            return {
                "ok": False,
                "transport": "rest-models-fallback",
                "path": rest_model_source_path or AGENTS_PATHS[0],
                "error": "Only model endpoints are available. Hivee will wait for real agent listing instead of showing default models.",
                "hint": "Expose /agents on the gateway or let the connector publish an agent snapshot so Hivee can render real agent cards.",
            }

        return {
            "ok": False,
            "error": f"Could not list agents on common paths. Last error: {last_err}",
            "hint": (
                "This OpenClaw likely does not expose REST JSON agent listing on your base_url path. "
                "Expose /agents over HTTP on the gateway or let the hub publish an agent snapshot."
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
        "hint": "Expose /agents over HTTP on the OpenClaw gateway or let the hub publish an agent snapshot.",
    }


async def _project_chat(
    row: Any,
    connection_api_key: str,
    message: str,
    *,
    agent_id: Optional[str] = None,
    session_key: str = "main",
    timeout_sec: Optional[int] = 120,
    user_id: Optional[str] = None,
    from_agent_id: Optional[str] = None,
    from_label: Optional[str] = None,
    context_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Route a project chat through the correct connector for this project/user."""
    from services.connector_dispatch import connector_chat_sync, get_user_online_connector

    backend_mode = ""
    try:
        backend_mode = str(row["backend_mode"] or "").strip().lower()
    except Exception:
        backend_mode = ""

    connector_id = ""
    try:
        connector_id = str(row["connector_id"] or "").strip()
    except Exception:
        connector_id = ""

    if not connector_id and backend_mode == "connector":
        try:
            connector_id = str(row["connection_id"] or "").strip()
        except Exception:
            connector_id = ""

    # Direct OpenClaw projects still need a live Hivee Connector to deliver
    # project-scoped prompts to the runtime agent. Do not treat connection_id
    # from openclaw_connections as a connector id.
    if not connector_id and user_id:
        try:
            online_connector = get_user_online_connector(user_id)
        except Exception:
            online_connector = None
        if online_connector:
            connector_id = str(online_connector.get("id") or "").strip()

    if not connector_id:
        return {
            "ok": False,
            "error": "No live Hivee Hub is available for this project. Pair/start a hub, then retry.",
            "transport": "none",
        }
    try:
        project_id = str(row["id"] or "").strip()
    except Exception:
        project_id = ""
    hivee_api_base = _get_hivee_api_base(project_id) if project_id else ""
    return await connector_chat_sync(
        connector_id=connector_id,
        message=message,
        agent_id=agent_id,
        session_key=session_key,
        timeout_sec=timeout_sec,
        from_agent_id=from_agent_id or "hivee",
        from_label=from_label or "Hivee",
        context_type=context_type or "message",
        project_id=project_id,
        hivee_api_base=hivee_api_base,
    )
async def _ensure_project_info_document(project_id: str, *, force: bool = False) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
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
        project_id=str(row["id"] or ""),
        hivee_api_base=_get_hivee_api_base(str(row["id"] or "")),
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
        "5) Return JSON only with `chat_update`, `output_files`, optional `actions`, optional `notes`, and pause fields.\n"
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
    res = await _project_chat(
        row,
        connection_api_key,
        task,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:project-info",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="control",
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
               p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
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

    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id or "")
    task = (
        f"Read fundamentals.md first, then context.md and setup-chat.md.\n"
        f"Build a complete, detailed project plan IN ENGLISH based on the project brief, goals, and agents roster.\n"
        f"The plan must include: milestones, deliverables, agent responsibilities, handoff triggers, pit-stop approval gates, assumptions, risks, and open questions.\n"
        f"Return a JSON object with:\n"
        f"  - chat_update: brief status message ending with 'WAITING FOR USER APPROVAL'\n"
        f"  - output_files: [{{\"path\": \"plan.md\", \"content\": \"<full markdown plan content here>\"}}]\n"
        f"  - actions: [{{\"type\": \"post_chat_message\", \"text\": \"@owner Plan is ready for review.\", \"mentions\": [\"owner\"]}}]\n"
        f"The plan.md content in output_files must be the full human-readable markdown plan — NOT JSON.\n"
        f"Post your status to chat (@owner) at start and when done."
    )
    instruction = _build_fundamentals_session_prompt(
        task=task,
        project_id=project_id,
        agent_id=primary_agent_id or "",
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )
    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:plan",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="plan_generation",
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

    raw_plan_text = str(res.get("text") or "").strip()
    if not raw_plan_text:
        raw_plan_text = detail_to_text(res.get("frames") or "Plan generated with empty text")

    # Parse the agent's JSON response and extract actual plan content from output_files
    parsed_plan = _extract_agent_report_payload(raw_plan_text)
    plan_text = ""
    plan_writes = parsed_plan.get("output_files") if isinstance(parsed_plan.get("output_files"), list) else []
    for f in plan_writes:
        rel = str(f.get("path") or "").strip().lower().lstrip("/")
        if rel in {"plan.md", "outputs/plan.md"}:
            plan_text = str(f.get("content") or "").strip()
            break
    # Fallback: if no output_files[plan.md], try chat_update, then raw text
    if not plan_text:
        plan_text = str(parsed_plan.get("chat_update") or raw_plan_text).strip()

    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_AWAITING_APPROVAL, plan_text[:20000], now, project_id),
    )
    conn.commit()
    conn.close()

    # Save plan.md to project folder
    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
        plan_md_content = f"# Project Plan\n> Generated by primary agent. Awaiting user approval.\n\n{plan_text}"
        # Apply all file writes from agent (including plan.md itself)
        if plan_writes:
            _apply_project_file_writes(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                writes=plan_writes,
                default_prefix="",
                allow_paths=None,
            )
        else:
            (project_dir / "plan.md").write_text(plan_md_content, encoding="utf-8")
        # Always keep legacy path in sync
        (project_dir / PROJECT_PLAN_FILE).write_text(plan_md_content, encoding="utf-8")
        # Post to chat so user sees plan is ready
        _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=[{
                "type": "post_chat_message",
                "text": f"@owner Plan is ready for your review. Saved to `plan.md`. Please approve or request changes.",
                "mentions": ["owner"],
            }],
            allow_paths=None,
            actor_type="project_agent",
            actor_id=str(primary_agent_id or ""),
            actor_label=f"agent:{primary_agent_id}",
        )
    except Exception:
        pass

    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.ready",
        text=(plan_text or "")[:1600],
    )
    await emit(project_id, "project.plan.ready", {"status": PLAN_STATUS_AWAITING_APPROVAL, "preview": plan_text[:1000]})

async def _run_agent_subplan_phase(
    project_id: str,
    row: Any,
    connection_api_key: str,
    *,
    agent_id: str,
    agent_name: str,
    primary_agent_id: str,
    primary_agent_name: str,
    task_text: str,
) -> Tuple[bool, str]:
    """
    Two-step sub-plan phase for a single agent:
    1. Invoke agent to write a detailed sub-plan and @mention primary for approval.
    2. Invoke primary to review and approve/reject via chat.
    Returns (approved: bool, subplan_text: str).
    """
    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, agent_id)
    primary_token = _issue_agent_session_token(project_id, primary_agent_id)

    # ── Step 1: Agent writes sub-plan ─────────────────────────────────────────
    subplan_instruction = (
        f"hivee_agent_id: {agent_id}\n"
        f"hivee_project_token: {agent_token}\n"
        f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
        f"  Headers: X-Project-Agent-Id: {agent_id}\n"
        f"           X-Project-Agent-Token: {agent_token}\n\n"
        f"All Hivee API requests must include:\n"
        f"  X-Project-Agent-Id: {agent_id}\n"
        f"  X-Project-Agent-Token: {agent_token}\n\n"
        f"You are agent `{agent_id}` ({agent_name}).\n"
        f"You have been assigned this high-level task:\n\n"
        f"{task_text}\n\n"
        f"Before executing, write a DETAILED SUB-PLAN. Save it to "
        f"`agents/{agent_id}-subplan.md` in output_files. Include:\n"
        f"- Approach and methodology\n"
        f"- Step-by-step sub-tasks you will create on the progress map\n"
        f"- Timeline estimate per sub-task\n"
        f"- Deliverable files\n"
        f"- Risks, assumptions, dependencies\n\n"
        f"Then post to chat requesting approval:\n"
        f"  @{primary_agent_id} here is my sub-plan for [task title]. "
        f"Read `agents/{agent_id}-subplan.md`. Please approve or provide feedback.\n\n"
        f"Do NOT start executing yet.\n\n"
        f"Return JSON: {{\"chat_update\": \"...\", "
        f"\"output_files\": [{{\"path\": \"agents/{agent_id}-subplan.md\", \"content\": \"...\"}}], "
        f"\"actions\": [{{\"type\": \"post_chat_message\", "
        f"\"text\": \"@{primary_agent_id} sub-plan ready for review...\", "
        f"\"mentions\": [\"{primary_agent_id}\"]}}], "
        f"\"requires_user_input\": true, "
        f"\"pause_reason\": \"Waiting for @{primary_agent_id} sub-plan approval.\"}}"
    )
    subplan_res = await _project_chat(
        row,
        connection_api_key,
        subplan_instruction,
        agent_id=agent_id,
        session_key=f"{project_id}:subplan:{agent_id}",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="subplan",
    )
    _update_project_usage_metrics(
        project_id,
        prompt_tokens=_estimate_tokens_from_text(subplan_instruction),
        completion_tokens=_estimate_tokens_from_text(subplan_res.get("text")),
    )

    subplan_text = str(subplan_res.get("text") or "").strip()
    parsed_subplan = _extract_agent_report_payload(subplan_text)

    # Persist sub-plan file + chat message
    subplan_writes = parsed_subplan.get("output_files") or []
    if isinstance(subplan_writes, list) and subplan_writes:
        _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=subplan_writes,
            default_prefix=USER_OUTPUTS_DIRNAME,
            allow_paths=None,
        )
    _apply_project_actions(
        owner_user_id=str(row["user_id"]),
        project_id=project_id,
        project_root=str(row["project_root"] or ""),
        actions=parsed_subplan.get("actions") or [],
        allow_paths=None,
        actor_type="project_agent",
        actor_id=agent_id,
        actor_label=f"agent:{agent_id}",
    )

    # Extract the actual sub-plan content for primary to read
    actual_subplan = ""
    for f in subplan_writes if isinstance(subplan_writes, list) else []:
        if str(f.get("path") or "").strip().endswith("subplan.md"):
            actual_subplan = str(f.get("content") or "").strip()
            break
    if not actual_subplan:
        actual_subplan = str(parsed_subplan.get("chat_update") or subplan_text)[:3000]

    await emit(project_id, "agent.subplan.submitted", {
        "agent_id": agent_id, "agent_name": agent_name, "preview": actual_subplan[:400],
    })

    if not subplan_res.get("ok"):
        # Sub-plan call failed — default approve so execution isn't blocked
        return True, actual_subplan

    # ── Step 2: Primary reviews sub-plan ──────────────────────────────────────
    review_instruction = (
        f"hivee_agent_id: {primary_agent_id}\n"
        f"hivee_project_token: {primary_token}\n"
        f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
        f"  Headers: X-Project-Agent-Id: {primary_agent_id}\n"
        f"           X-Project-Agent-Token: {primary_token}\n\n"
        f"All Hivee API requests must include:\n"
        f"  X-Project-Agent-Id: {primary_agent_id}\n"
        f"  X-Project-Agent-Token: {primary_token}\n\n"
        f"You are the primary agent `{primary_agent_id}` ({primary_agent_name}).\n"
        f"Agent `{agent_id}` ({agent_name}) submitted their sub-plan for review:\n\n"
        f"---\n{actual_subplan[:3000]}\n---\n\n"
        f"Review this sub-plan against the overall project plan, goals, and constraints "
        f"(read plan.md if needed). Consider: scope alignment, realistic timeline, correct "
        f"dependencies, resource fit.\n\n"
        f"Respond with your decision. Include `\"approved\": true` or `\"approved\": false`.\n"
        f"Post your decision to chat @{agent_id}.\n\n"
        f"Return JSON: {{\"chat_update\": \"...\", \"approved\": true, \"feedback\": \"...\", "
        f"\"actions\": [{{\"type\": \"post_chat_message\", "
        f"\"text\": \"@{agent_id} Sub-plan approved. Proceed.\", "
        f"\"mentions\": [\"{agent_id}\"]}}]}}"
    )
    review_res = await _project_chat(
        row,
        connection_api_key,
        review_instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:review:{agent_id}",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="subplan_review",
    )
    _update_project_usage_metrics(
        project_id,
        prompt_tokens=_estimate_tokens_from_text(review_instruction),
        completion_tokens=_estimate_tokens_from_text(review_res.get("text")),
    )

    if not review_res.get("ok"):
        return True, actual_subplan  # fallback: approve

    review_text = str(review_res.get("text") or "").strip()
    parsed_review = _extract_agent_report_payload(review_text)

    # Post primary's decision to chat
    _apply_project_actions(
        owner_user_id=str(row["user_id"]),
        project_id=project_id,
        project_root=str(row["project_root"] or ""),
        actions=parsed_review.get("actions") or [],
        allow_paths=None,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=f"agent:{primary_agent_id}",
    )

    # Determine approval — check explicit field first, then infer from text
    approved_flag = parsed_review.get("approved")
    if approved_flag is None:
        review_chat = str(parsed_review.get("chat_update") or "").lower()
        approved_flag = any(kw in review_chat for kw in ("approved", "proceed", "looks good", "good to go", "approve"))
    approved = bool(approved_flag)

    await emit(project_id, "agent.subplan.reviewed", {
        "agent_id": agent_id, "agent_name": agent_name,
        "approved": approved,
        "feedback": str(parsed_review.get("feedback") or "")[:400],
    })
    return approved, actual_subplan


def _parse_parallel_groups(
    parsed_delegation: Dict[str, Any],
    by_id: Dict[str, Any],
) -> List[List[str]]:
    """
    Extract parallel_groups from the primary agent's delegation response.
    Returns a list of groups; each group is a list of agent_ids to run concurrently.
    Falls back to one-agent-per-group (fully sequential) if not present or invalid.
    """
    raw = parsed_delegation.get("parallel_groups")
    if isinstance(raw, list) and raw:
        groups: List[List[str]] = []
        seen: set = set()
        for g in raw:
            if not isinstance(g, list):
                continue
            valid = [str(a).strip() for a in g if str(a).strip() in by_id and str(a).strip() not in seen]
            if valid:
                groups.append(valid)
                seen.update(valid)
        # Any agents not covered by primary's groups get appended as individual groups
        for aid in by_id:
            if aid not in seen:
                groups.append([aid])
        return groups if groups else [[aid] for aid in by_id]
    # Fallback: each agent in its own sequential group
    return [[aid] for aid in by_id]


async def _delegate_project_tasks(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
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
    permissions_by_agent: Dict[str, Dict[str, Any]] = {}
    for role_row in role_rows:
        agent_key = str(role_row.get("agent_id") or "").strip()
        if not agent_key:
            continue
        permissions_by_agent[agent_key] = _get_project_agent_permissions(
            conn,
            project_id=project_id,
            agent_id=agent_key,
            source_type=str(role_row.get("source_type") or "owner"),
        )
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

    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id or "")
    instruction = _delegate_prompt_from_project(
        project_id=project_id,
        agent_id=primary_agent_id or "",
        role_rows=role_rows,
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )
    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:delegate",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="delegation",
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
    parsed = _extract_agent_report_payload(primary_reply)
    by_id = {str(r.get("agent_id") or "").strip(): r for r in role_rows}

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
    (project_dir / USER_OUTPUTS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / USER_OUTPUTS_DIRNAME / HANDOFFS_DIRNAME).mkdir(parents=True, exist_ok=True)

    # Save delegation.md (new) + legacy PROJECT-DELEGATION.MD
    output_files = parsed.get("output_files") if isinstance(parsed.get("output_files"), list) else []
    delegation_content = ""
    for f in output_files:
        rel = str(f.get("path") or "").strip().lower()
        if "delegation" in rel:
            delegation_content = str(f.get("content") or "").strip()
            break
    if not delegation_content:
        # Fallback: use chat_update as delegation summary
        delegation_content = str(parsed.get("chat_update") or primary_reply or "Delegation initialized.").strip()
    (project_dir / "delegation.md").write_text(f"# Delegation\n\n{delegation_content}\n", encoding="utf-8")
    (project_dir / PROJECT_DELEGATION_FILE).write_text(f"# Delegation\n\n{delegation_content}\n", encoding="utf-8")

    # Apply all actions from agent response: creates tasks, posts chat @mentions, writes files
    actions_raw = parsed.get("actions") if isinstance(parsed.get("actions"), list) else []
    if actions_raw:
        action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=actions_raw,
            allow_paths=None,  # primary agent has full access
            actor_type="project_agent",
            actor_id=str(primary_agent_id or ""),
            actor_label=f"agent:{primary_agent_id}",
        )
        for item in (action_result.get("applied") or []):
            event_name = str(item.get("event") or "").strip()
            event_payload = item.get("event_payload") if isinstance(item.get("event_payload"), dict) else {}
            if event_name:
                await emit(project_id, event_name, event_payload)

    # Apply output_files writes
    if output_files:
        _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files,
            default_prefix=USER_OUTPUTS_DIRNAME,
            allow_paths=None,
        )

    # Fallback: if agent didn't create task cards via actions, create them ourselves per agent
    existing_tasks_count = 0
    for act in actions_raw:
        if _normalize_agent_action_kind(act.get("type")) == "create_task":
            existing_tasks_count += 1

    # Legacy task_map from old payload format (backward compat)
    payload_legacy = _parse_delegation_payload(primary_reply)
    raw_tasks = payload_legacy.get("agent_tasks")
    task_map: Dict[str, str] = {}
    if isinstance(raw_tasks, list):
        for item in raw_tasks:
            if not isinstance(item, dict):
                continue
            aid = str(item.get("agent_id") or "").strip()
            task_md = str(item.get("task_md") or "").strip()
            if aid and task_md and aid in by_id:
                task_map[aid] = task_md

    # Parse parallel groups from primary's delegation response
    parallel_groups = _parse_parallel_groups(parsed, by_id)

    agent_order = list(by_id.keys())
    assigned_task_map: Dict[str, str] = {}
    assigned_mentions_map: Dict[str, List[str]] = {}
    agents_dir = project_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    assigned_count = 0
    for pos, aid in enumerate(agent_order):
        row_item = by_id.get(aid) or {}
        agent_name = str(row_item.get("agent_name") or aid)
        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        next_aid = agent_order[pos + 1] if (pos + 1) < len(agent_order) else None
        task_text = _normalize_task_markdown_for_agent(
            agent_id=aid,
            role=role,
            task_md=task_map.get(aid, f"Read delegation.md and execute your assigned scope. Report progress via chat with @mentions."),
            next_agent_id=next_aid,
        )
        assigned_task_map[aid] = task_text
        fname = _safe_agent_filename(aid) + ".md"
        (agents_dir / fname).write_text(task_text.strip() + "\n", encoding="utf-8")
        assigned_count += 1
        mention_targets = sorted({m for m in re.findall(r"@([a-zA-Z0-9._-]+)", task_text) if m and m != aid})[:8]
        assigned_mentions_map[aid] = mention_targets

        # Fallback: if agent didn't post a chat @mention for this agent, do it now
        if existing_tasks_count == 0:
            _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "post_chat_message",
                    "text": f"@{aid} your tasks are assigned. Check `delegation.md` for your scope and start trigger.",
                    "mentions": [aid],
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(primary_agent_id or ""),
                actor_label=f"agent:{primary_agent_id}",
            )
            # Fallback task card creation
            _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "create_task",
                    "title": f"{role} — {agent_name}",
                    "description": task_text[:TASK_DESCRIPTION_MAX_CHARS],
                    "assignee_agent_id": aid,
                    "status": "todo",
                    "priority": "high",
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(primary_agent_id or ""),
                actor_label=f"agent:{primary_agent_id}",
            )

        await emit(
            project_id,
            "agent.task.assigned",
            {
                "agent_id": aid,
                "agent_name": agent_name,
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

    # ── SUB-PLAN PHASE ────────────────────────────────────────────────────────
    # Each non-primary agent writes a detailed sub-plan; primary reviews it.
    # Agents within the same parallel group submit sub-plans concurrently.
    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=20)
    await emit(project_id, "project.subplan.phase_started", {
        "groups": [[a for a in g] for g in parallel_groups],
    })
    subplan_map: Dict[str, str] = {}  # agent_id -> approved sub-plan text

    for grp in parallel_groups:
        non_primary_in_group = [a for a in grp if a != primary_agent_id]
        if not non_primary_in_group:
            continue
        state, _ = _read_project_execution_state(project_id)
        if state == EXEC_STATUS_STOPPED:
            break

        async def _collect_subplan(aid: str) -> Tuple[str, bool, str]:
            a_name = str((by_id.get(aid) or {}).get("agent_name") or aid)
            approved, sp_text = await _run_agent_subplan_phase(
                project_id, row, connection_api_key,
                agent_id=aid,
                agent_name=a_name,
                primary_agent_id=primary_agent_id or aid,
                primary_agent_name=primary_agent_name,
                task_text=assigned_task_map.get(aid) or "",
            )
            return aid, approved, sp_text

        results = await asyncio.gather(*[_collect_subplan(a) for a in non_primary_in_group])
        for aid, approved, sp_text in results:
            subplan_map[aid] = sp_text
            a_name = str((by_id.get(aid) or {}).get("agent_name") or aid)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.subplan.reviewed",
                text=f"{aid}: {'approved' if approved else 'needs revision'} — {sp_text[:400]}",
            )

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=40)
    await emit(project_id, "project.subplan.phase_complete", {
        "agents_with_subplans": list(subplan_map.keys()),
    })

    # ── EXECUTION PHASE (parallel groups) ────────────────────────────────────
    # Inner async function so we can run agents within a group concurrently.
    async def _run_one_agent(aid: str, grp_idx: int) -> None:
        nonlocal processed_agents, failed_agents
        nonlocal primary_last_chat_update, primary_last_notes
        nonlocal primary_last_pause_reason, primary_last_resume_hint, primary_pause_resolved

        row_item = by_id.get(aid) or {}
        # Wait while paused; bail on stop
        while True:
            state, _ = _read_project_execution_state(project_id)
            if state == EXEC_STATUS_PAUSED:
                await asyncio.sleep(0.7)
                continue
            if state == EXEC_STATUS_STOPPED:
                return
            break

        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        agent_name = str(row_item.get("agent_name") or aid)
        task_text = assigned_task_map.get(aid) or f"# Task for {aid}\n\nRole: {role}\n"
        approved_subplan = subplan_map.get(aid, "")
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
        await emit(project_id, "agent.task.started", {"agent_id": aid, "agent_name": agent_name, "role": role})

        subplan_section = (
            f"\n\n## Your Approved Sub-Plan\n{approved_subplan[:2000]}\n\n"
            f"Follow this sub-plan. Your FIRST action must be to create detailed sub-task cards "
            f"(via `create_task` actions) matching the steps in your sub-plan, then begin executing.\n"
        ) if approved_subplan else (
            f"\n\nFirst, create detailed sub-task cards (via `create_task` actions) for each step "
            f"of your work, then begin executing.\n"
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
            + subplan_section
            + "\n\n"
            + _build_project_task_snapshot(project_id)
            + "\n"
            + _build_project_chat_snapshot(project_id)
            + "\n"
            + "Execute your assigned task and return JSON object only:\n"
            + "{\n"
            + "  \"chat_update\": \"Human-friendly update sentence to show in chat\",\n"
            + "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"file content\",\"append\":false}],\n"
            + "  \"actions\": [{\"type\":\"create_task\",\"title\":\"...\",\"description\":\"...\"},{\"type\":\"post_chat_message\",\"text\":\"handoff to @agent_id\"},{\"type\":\"update_execution\",\"progress_pct\":45}],\n"
            + "  \"notes\": \"optional technical notes\",\n"
            + "  \"requires_user_input\": false,\n"
            + "  \"pause_reason\": \"\",\n"
            + "  \"resume_hint\": \"\"\n"
            + "}\n"
            + "Rules:\n"
            + "- chat_update must read like normal conversation.\n"
            + "- Put every created/updated artifact in output_files.\n"
            + "- Use `actions` when you need to change real project files, group chat state, or task/progress state.\n"
            + "- FIRST action: create sub-task cards for each step of your sub-plan.\n"
            + "- Use exact IDs from roster when mentioning other agents.\n"
            + "- Mention handoff needs in chat_update with @agent_id.\n"
            + f"- Follow `{PROJECT_PROTOCOL_FILE}` for delegation, mention, and status update rules.\n\n"
            + "- If blocked by user approval/input or planned pit stop, set requires_user_input=true and explain pause_reason.\n"
            + "- If user says SKIP for missing info, proceed with assumptions and state them briefly in chat_update.\n"
            + "Assigned task:\n"
            + task_text.strip()
        )
        if agent_file_context:
            agent_instruction = f"{agent_instruction}\n\n{agent_file_context}"

        agent_res = await _project_chat(
            row,
            connection_api_key,
            agent_instruction,
            agent_id=aid,
            session_key=f"{project_id}:agent:{aid}",
            timeout_sec=None,
            user_id=str(row["user_id"] or ""),
            from_agent_id="hivee",
            from_label="Hivee System",
            context_type="task_execution",
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
            await emit(project_id, "agent.task.failed", {"agent_id": aid, "agent_name": agent_name, "error": err_text})
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.task.failed",
                text=f"{aid}: {err_text}",
            )
            return

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
        action_items_raw = parsed_report.get("actions") or []
        agent_perms = permissions_by_agent.get(str(aid).strip()) or {}
        if bool(agent_perms.get("can_write_files")):
            agent_output_allow_paths = _normalize_permission_write_paths(
                agent_perms.get("write_paths") or [],
                fallback=[USER_OUTPUTS_DIRNAME, f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}"],
            )
        else:
            agent_output_allow_paths = []
        write_result = _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files_raw if isinstance(output_files_raw, list) else [],
            default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
            allow_paths=agent_output_allow_paths,
        )
        saved_files = write_result.get("saved") or []
        skipped_files = write_result.get("skipped") or []
        action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=action_items_raw if isinstance(action_items_raw, list) else [],
            allow_paths=agent_output_allow_paths,
            actor_type="project_agent",
            actor_id=aid,
            actor_label=f"agent:{aid}",
        )
        applied_actions = action_result.get("applied") or []
        skipped_actions = action_result.get("skipped") or []
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
            followup_res = await _project_chat(
                row,
                connection_api_key,
                followup_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=120,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution",
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
                followup_actions = parsed_followup.get("actions") or []
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
                followup_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=followup_actions if isinstance(followup_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                followup_applied_actions = followup_action_result.get("applied") or []
                followup_skipped_actions = followup_action_result.get("skipped") or []
                if followup_saved:
                    saved_files.extend(followup_saved)
                if followup_skipped:
                    skipped_files.extend(followup_skipped)
                if followup_applied_actions:
                    applied_actions.extend(followup_applied_actions)
                if followup_skipped_actions:
                    skipped_actions.extend(followup_skipped_actions)
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
            rescue_res = await _project_chat(
                row,
                connection_api_key,
                rescue_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=120,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution",
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
                rescue_actions = parsed_rescue.get("actions") or []
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
                rescue_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=rescue_actions if isinstance(rescue_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                rescue_applied_actions = rescue_action_result.get("applied") or []
                rescue_skipped_actions = rescue_action_result.get("skipped") or []
                if rescue_saved:
                    saved_files.extend(rescue_saved)
                if rescue_skipped:
                    skipped_files.extend(rescue_skipped)
                if rescue_applied_actions:
                    applied_actions.extend(rescue_applied_actions)
                if rescue_skipped_actions:
                    skipped_actions.extend(rescue_skipped_actions)
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
            # Create a blocked task card so user sees the issue clearly
            issue_title = f"[BLOCKED] {agent_name} — {pause_reason[:120]}"
            issue_desc = (
                f"Agent `{aid}` ({role}) is blocked and needs user input.\n\n"
                f"**Blocker:** {pause_reason}\n\n"
                f"**Resume hint:** {resume_hint or 'No hint provided.'}\n\n"
                f"**Last chat update:** {chat_update[:600]}"
            )
            _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "create_task",
                    "title": issue_title,
                    "description": issue_desc[:TASK_DESCRIPTION_MAX_CHARS],
                    "assignee_agent_id": aid,
                    "status": "blocked",
                    "priority": "urgent",
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(aid),
                actor_label=f"agent:{aid}",
            )
        else:
            chat_update = _ensure_chat_handoff_mentions(chat_update, assigned_mentions_map.get(aid) or [])

        auto_chat_message = None
        has_explicit_chat_action = any(
            _normalize_agent_action_kind(
                (item or {}).get("type")
                or (item or {}).get("method")
                or (item or {}).get("action")
                or (item or {}).get("name")
            ) == "post_chat_message"
            for item in applied_actions
            if isinstance(item, dict)
        )
        if not has_explicit_chat_action:
            chat_conn = db()
            try:
                auto_chat_message = _create_project_chat_message(
                    chat_conn,
                    project_id=project_id,
                    author_type="project_agent",
                    author_id=aid,
                    author_label=agent_name,
                    text=chat_update,
                    metadata={
                        "source": "delegation.agent_task",
                        "output_file": f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}-latest.md",
                        "requires_user_input": bool(pause_decision.get("pause")),
                    },
                )
                chat_conn.commit()
            finally:
                chat_conn.close()
        if isinstance(auto_chat_message, dict):
            await emit(project_id, "project.chat.message", auto_chat_message)
            for target in (auto_chat_message.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]:
                await emit(
                    project_id,
                    "project.chat.mention",
                    {
                        "message_id": str(auto_chat_message.get("id") or ""),
                        "project_id": project_id,
                        "target": target,
                        "author_type": str(auto_chat_message.get("author_type") or ""),
                        "author_id": auto_chat_message.get("author_id"),
                        "author_label": auto_chat_message.get("author_label"),
                        "text": str(auto_chat_message.get("text") or "")[:500],
                        "created_at": int(auto_chat_message.get("created_at") or 0),
                    },
                )

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
        pct = min(95, 40 + int(((grp_idx + 1) / max(1, len(parallel_groups))) * 53))
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
        for item in applied_actions:
            event_name = str(item.get("event") or "").strip()
            event_payload = item.get("event_payload") if isinstance(item.get("event_payload"), dict) else {}
            if event_name:
                await emit(project_id, event_name, event_payload)
            for extra in (item.get("extra_events") or []):
                if not isinstance(extra, dict):
                    continue
                extra_event_name = str(extra.get("event") or "").strip()
                extra_event_payload = extra.get("event_payload") if isinstance(extra.get("event_payload"), dict) else {}
                if extra_event_name:
                    await emit(project_id, extra_event_name, extra_event_payload)
                # Route @mentions to the mentioned agent's connector
                if extra_event_name == "project.chat.mention" and isinstance(extra_event_payload, dict):
                    import asyncio
                    asyncio.ensure_future(_dispatch_chat_mention_to_connector(
                        project_id=project_id,
                        mention_target=str(extra_event_payload.get("target") or ""),
                        message_text=str(extra_event_payload.get("text") or ""),
                        from_agent_id=str(extra_event_payload.get("author_id") or extra_event_payload.get("author_label") or "agent"),
                        from_label=str(extra_event_payload.get("author_label") or "Agent"),
                    ))
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
                "applied_actions": applied_actions[:20],
                "skipped_actions": skipped_actions[:10],
                "requires_user_input": bool(pause_decision.get("pause")),
                "pause_reason": pause_reason[:500],
                "resume_hint": resume_hint[:300],
            },
        )
        if applied_actions:
            await emit(
                project_id,
                "agent.task.actions_applied",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "applied_actions": applied_actions[:20],
                    "skipped_actions": skipped_actions[:10],
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
                "applied_actions": applied_actions[:20],
                "skipped_actions": skipped_actions[:10],
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

    # ── Parallel group runner ─────────────────────────────────────────────────
    for grp_idx, grp in enumerate(parallel_groups):
        state, _ = _read_project_execution_state(project_id)
        if state == EXEC_STATUS_STOPPED:
            _refresh_project_documents(project_id)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="delegation.stopped",
                text="Delegation run stopped by user.",
            )
            await emit(project_id, "project.delegation.stopped", {
                "processed_agents": processed_agents,
                "failed_agents": failed_agents,
                "total_agents": len(agent_order),
            })
            return
        await asyncio.gather(*[_run_one_agent(aid, grp_idx) for aid in grp])

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


async def _onboard_agents_into_project(
    project_id: str,
    added_agent_ids: List[str],
    all_agent_ids: List[str],
) -> None:
    """Called when agents are added/changed on an already-approved project.
    Primary agent reviews the roster + current progress and assigns tasks to new agents."""
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, p.execution_status, p.progress_pct,
               p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
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

    if not role_rows:
        return

    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    if not primary_agent_id:
        return

    exec_status = str(row["execution_status"] or "idle")
    progress_pct = int(row["progress_pct"] or 0)
    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id)

    added_lines = "\n".join(
        f"- {aid}" + (
            " — " + str(next((r.get("role") or "" for r in role_rows if str(r.get("agent_id") or "") == aid), "")).strip()
            if next((r.get("role") or "" for r in role_rows if str(r.get("agent_id") or "") == aid), "").strip()
            else ""
        )
        for aid in added_agent_ids
    ) or "- (config update on existing agents)"

    task = (
        f"New or updated agents have been added to the project:\n{added_lines}\n\n"
        f"Current execution progress: {exec_status} at {progress_pct}%.\n\n"
        f"Your task:\n"
        f"1. Read plan.md and state.md to understand current progress.\n"
        f"2. Determine what work remains and what tasks fit the new agent(s).\n"
        f"3. Create task cards for them using create_task actions.\n"
        f"4. Notify each new agent via post_chat_message with @mention so they receive their assignment.\n"
        f"5. Update execution progress with update_execution if needed.\n"
        f"6. Post a summary to @owner via post_chat_message.\n"
    )
    instruction = _build_fundamentals_session_prompt(
        task=task,
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )

    await emit(project_id, "project.agents.onboarding_started", {
        "added": added_agent_ids,
        "primary_agent_id": primary_agent_id,
        "progress_pct": progress_pct,
    })
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="agents.onboarding",
        text=f"Primary agent onboarding new agents: {', '.join(added_agent_ids) or 'config update'}",
    )

    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:onboard",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="delegation",
    )

    if not res.get("ok"):
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agents.onboarding_failed",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.agents.onboarding_failed", {
            "error": detail_to_text(res.get("error") or res.get("details"))[:500],
        })
        return

    raw_text = str(res.get("text") or "").strip()
    parsed = _extract_agent_report_payload(raw_text)
    write_payload = parsed.get("output_files") or []
    action_payload = parsed.get("actions") or []

    owner_user_id = str(row["user_id"] or "")
    project_root = str(row["project_root"] or "")

    _apply_project_file_writes(
        owner_user_id=owner_user_id,
        project_root=project_root,
        writes=write_payload if isinstance(write_payload, list) else [],
        default_prefix=f"{USER_OUTPUTS_DIRNAME}/onboarding",
        allow_paths=["*"],
    )
    _apply_project_actions(
        owner_user_id=owner_user_id,
        project_id=project_id,
        project_root=project_root,
        actions=action_payload if isinstance(action_payload, list) else [],
        allow_paths=["*"],
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=f"agent:{primary_agent_id}",
    )

    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=owner_user_id,
        project_root=project_root,
        kind="agents.onboarding_done",
        text=f"Primary agent completed onboarding for: {', '.join(added_agent_ids) or 'config update'}",
    )
    await emit(project_id, "project.agents.onboarding_done", {
        "added": added_agent_ids,
        "primary_agent_id": primary_agent_id,
    })


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

