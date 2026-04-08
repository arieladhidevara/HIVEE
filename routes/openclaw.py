from hivee_shared import *

def register_routes(app: FastAPI) -> None:
    @app.post("/api/openclaw/connect")
    async def connect_openclaw(request: Request, payload: ConnectIn):
        user_id = get_session_user(request)
        primary_env = _ensure_primary_environment_for_user(user_id)
        env_id = str(primary_env.get("id") or "").strip() or None
        if not (payload.base_url.startswith("http://") or payload.base_url.startswith("https://")):
            raise HTTPException(400, "base_url must start with http:// or https://")
    
        health = await openclaw_health(payload.base_url, payload.api_key)
        if not health.get("ok"):
            raise HTTPException(400, {"message": "Could not verify OpenClaw health", "details": health})

        # Health passed — save the connection before attempting bootstrap so a
        # temporarily-unavailable WS/agent endpoint doesn't block the save.
        conn = db()
        conn_id = new_id("oc")
        conn.execute(
            "INSERT INTO openclaw_connections (id, user_id, env_id, base_url, api_key, name, created_at) VALUES (?,?,?,?,?,?,?)",
            (conn_id, user_id, env_id, payload.base_url.rstrip("/"), payload.api_key, payload.name, int(time.time())),
        )
        conn.commit()
        conn.close()

        bootstrap = await _bootstrap_connection_workspace(user_id, payload.base_url.rstrip("/"), payload.api_key)
        bootstrap_ok = bool(bootstrap.get("ok"))
        bootstrap_error_code = bootstrap.get("error_code") or ""
        if bootstrap_ok:
            bs_status = "ok"
        elif bootstrap_error_code == "missing_operator_write":
            bs_status = "token_missing_operator_write"
        else:
            bs_status = "failed"
        _upsert_connection_policy(
            conn_id,
            user_id,
            main_agent_id=bootstrap.get("main_agent_id"),
            main_agent_name=bootstrap.get("main_agent_name"),
            bootstrap_status=bs_status,
            bootstrap_error=None if bootstrap_ok else detail_to_text(bootstrap.get("error") or bootstrap.get("ws_result")),
            workspace_tree=bootstrap.get("workspace_tree"),
            workspace_root=str(bootstrap.get("workspace_root") or HIVEE_ROOT),
            templates_root=str(bootstrap.get("templates_root") or HIVEE_TEMPLATES_ROOT),
        )
        provision = None
        if bootstrap_ok:
            provision = _provision_managed_agents_for_connection(
                user_id=user_id,
                env_id=env_id,
                connection_id=conn_id,
                base_url=payload.base_url.rstrip("/"),
                raw_agents=bootstrap.get("agents") or [],
                fallback_agent_id=bootstrap.get("main_agent_id"),
                fallback_agent_name=bootstrap.get("main_agent_name"),
            )

        if bootstrap_ok:
            connection_state = "healthy_connection"
        elif bootstrap_error_code == "missing_operator_write":
            connection_state = "token_missing_operator_write"
        else:
            connection_state = "bootstrap_failed"

        response: Dict[str, Any] = {
            "ok": True,
            "connection": {"id": conn_id, "base_url": payload.base_url.rstrip("/"), "name": payload.name},
            "health": health,
            "bootstrap": bootstrap,
            "bootstrap_status": bs_status,
            "connection_state": connection_state,
            "agent_provision": provision,
        }
        if not bootstrap_ok:
            if bootstrap_error_code == "missing_operator_write":
                response["warning"] = (
                    "Connection saved and health OK, but token is missing operator.write scope. "
                    "Chat and agent listing will not work until an operator token is provided."
                )
                response["hint"] = (
                    bootstrap.get("hint")
                    or "In OpenClaw: set gateway.auth.mode=token and use a token with operator.write scope."
                )
            else:
                response["warning"] = (
                    "Connected, but bootstrap failed. Fix OpenClaw WS/HTTP config and retry bootstrap "
                    f"via POST /api/openclaw/{conn_id}/bootstrap."
                )
                response["hint"] = (
                    bootstrap.get("hint")
                    or "Enable gateway.http.endpoints.chatCompletions.enabled=true in OpenClaw to allow "
                       "HTTP agent listing, or ensure the WS gateway is accessible without device-identity pairing."
                )
        return response
    
    @app.post("/api/openclaw/{connection_id}/bootstrap")
    async def bootstrap_openclaw_connection(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        bootstrap = await _bootstrap_connection_workspace(user_id, row["base_url"], connection_api_key)
        _bs_ok = bool(bootstrap.get("ok"))
        _bs_err_code = bootstrap.get("error_code") or ""
        if _bs_ok:
            _bs_status = "ok"
        elif _bs_err_code == "missing_operator_write":
            _bs_status = "token_missing_operator_write"
        else:
            _bs_status = "failed"
        _upsert_connection_policy(
            connection_id,
            user_id,
            main_agent_id=bootstrap.get("main_agent_id"),
            main_agent_name=bootstrap.get("main_agent_name"),
            bootstrap_status=_bs_status,
            bootstrap_error=None if _bs_ok else detail_to_text(bootstrap.get("error") or bootstrap.get("ws_result")),
            workspace_tree=bootstrap.get("workspace_tree"),
            workspace_root=str(bootstrap.get("workspace_root") or HIVEE_ROOT),
            templates_root=str(bootstrap.get("templates_root") or HIVEE_TEMPLATES_ROOT),
        )
        if not bootstrap.get("ok"):
            raise HTTPException(400, bootstrap)
        bootstrap["agent_provision"] = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=str(row["env_id"] or "").strip() or None,
            connection_id=connection_id,
            base_url=str(row["base_url"]),
            raw_agents=bootstrap.get("agents") or [],
            fallback_agent_id=bootstrap.get("main_agent_id"),
            fallback_agent_name=bootstrap.get("main_agent_name"),
        )
        return bootstrap
    
    @app.get("/api/openclaw/connections", response_model=List[ConnectionOut])
    async def list_connections(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            "SELECT id, base_url, name FROM openclaw_connections WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        conn.close()
        return [ConnectionOut(id=r["id"], base_url=r["base_url"], name=r["name"]) for r in rows]
    
    @app.get("/api/openclaw/{connection_id}/agents")
    async def list_agents(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        # Load previously provisioned agents from DB as fallback for when live discovery fails.
        saved_rows = conn.execute(
            "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND status = ? ORDER BY agent_name ASC",
            (user_id, connection_id, "active"),
        ).fetchall()
        conn.close()
        saved_agents = [{"id": r["agent_id"], "name": r["agent_name"], "source": "saved"} for r in saved_rows]
        saved_ids = {r["agent_id"] for r in saved_rows}
        if not row:
            raise HTTPException(404, "Connection not found")

        res = await openclaw_list_agents(row["base_url"], connection_api_key)
        if not res.get("ok"):
            error_code = res.get("error_code") or ""
            return {
                "ok": True,
                "agents": saved_agents,
                "agents_source": "saved" if saved_agents else "none",
                "transport": "none",
                "connection_state": (
                    "token_missing_operator_write" if error_code == "missing_operator_write"
                    else "agent_discovery_failed"
                ),
                "warning": res.get("error") or "Agent listing unavailable; WS requires device identity and REST agent endpoints are not exposed.",
                "hint": res.get("hint") or "Enable gateway.http.endpoints.chatCompletions.enabled=true in OpenClaw to allow HTTP agent listing.",
            }

        # Provision any newly discovered targets into the DB so they appear in managed agents view.
        # This includes model-fallback targets returned from /v1/models when dedicated agent endpoints
        # are not exposed by the gateway.
        live_agents = [a for a in (res.get("agents") or []) if isinstance(a, dict)]
        new_agents = [a for a in live_agents if str(a.get("id") or "").strip() not in saved_ids]
        if new_agents:
            env_id = str(row["env_id"] or "").strip() or None
            _provision_managed_agents_for_connection(
                user_id=user_id,
                env_id=env_id,
                connection_id=connection_id,
                base_url=row["base_url"],
                raw_agents=new_agents,
            )

        return res
    
    @app.get("/api/openclaw/{connection_id}/policy", response_model=ConnectionPolicyOut)
    async def get_connection_policy(request: Request, connection_id: str):
        user_id = get_session_user(request)
        workspace = _ensure_user_workspace(user_id)
        conn = db()
        exists = conn.execute(
            "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if not exists:
            conn.close()
            raise HTTPException(404, "Connection not found")
    
        policy = conn.execute(
            """
            SELECT connection_id, workspace_root, templates_root, main_agent_id, main_agent_name, bootstrap_status, bootstrap_error, workspace_tree
            FROM connection_policies
            WHERE connection_id = ? AND user_id = ?
            """,
            (connection_id, user_id),
        ).fetchone()
        conn.close()
        if not policy:
            return ConnectionPolicyOut(
                connection_id=connection_id,
                workspace_root=workspace["workspace_root"],
                templates_root=workspace["templates_root"],
                main_agent_id=None,
                main_agent_name=None,
                bootstrap_status="unknown",
                bootstrap_error=None,
                workspace_tree=workspace["workspace_tree"],
            )
        payload = dict(policy)
        if not payload.get("workspace_tree"):
            payload["workspace_tree"] = workspace["workspace_tree"]
        if not payload.get("workspace_root"):
            payload["workspace_root"] = workspace["workspace_root"]
        if not payload.get("templates_root"):
            payload["templates_root"] = workspace["templates_root"]
        return ConnectionPolicyOut(**payload)
    

    @app.post("/api/openclaw/{connection_id}/chat")
    async def chat_openclaw(request: Request, connection_id: str, payload: OpenClawChatIn):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        policy = conn.execute(
            "SELECT main_agent_id, workspace_root FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
        main_agent_id = str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else ""
        main_agent_id = main_agent_id.strip() or None
        if payload.agent_id:
            if not main_agent_id:
                raise HTTPException(400, "Main workspace agent is not configured. Re-run OpenClaw bootstrap.")
            if payload.agent_id != main_agent_id:
                raise HTTPException(403, "Workspace chat can only target your main user agent")
        effective_agent_id = main_agent_id
        if not effective_agent_id:
            raise HTTPException(400, "Main workspace agent is not configured. Re-run OpenClaw bootstrap.")
        scoped_message = _compose_guardrailed_message(payload.message.strip(), workspace_root=workspace_root)
        res = await openclaw_chat(
            row["base_url"],
            connection_api_key,
            scoped_message,
            effective_agent_id,
            max_output_tokens=SAFE_PROVIDER_MAX_OUTPUT_TOKENS,
        )
        if not res.get("ok"):
            raise HTTPException(400, res)
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        return res
    
    @app.post("/api/openclaw/{connection_id}/ws-chat")
    async def chat_openclaw_ws(request: Request, connection_id: str, payload: OpenClawWsChatIn):
        session_user: Optional[str] = None
        try:
            session_user = get_optional_session_user(request)
        except HTTPException:
            if not str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip():
                raise
            session_user = None
        a2a_access = _resolve_optional_a2a_agent_session(request, required_scope="env.read")
        user_id = str(session_user or (a2a_access.get("user_id") if a2a_access else "") or "").strip()
        if not user_id:
            raise HTTPException(401, "Missing authorization. Login first or use A2A agent session headers.")

        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        policy = conn.execute(
            "SELECT main_agent_id, workspace_root FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        context_mode = str(payload.context_mode or "auto").strip().lower() or "auto"
        if context_mode not in {"auto", "workspace", "project"}:
            conn.close()
            raise HTTPException(400, "Invalid context_mode. Use auto, workspace, or project.")
        session_key = (payload.session_key or "main").strip() or "main"
        wants_project_context = context_mode == "project" or (context_mode == "auto" and session_key.startswith("prj_"))
        if context_mode == "project" and not session_key.startswith("prj_"):
            conn.close()
            raise HTTPException(400, "Project context requires a project session_key.")
        if a2a_access and not wants_project_context:
            conn.close()
            raise HTTPException(403, "A2A agent session can only use project context chat")
        project_scope = None
        project_owner_user_id = user_id
        project_access_mode = "owner"
        role_rows: List[Dict[str, Any]] = []
        project_primary_agent_id: Optional[str] = None
        member_allowed_agent_ids: set[str] = set()
        selected_agent_permissions: Dict[str, Any] = {
            "can_chat_project": True,
            "can_read_files": True,
            "can_write_files": True,
            "write_paths": [USER_OUTPUTS_DIRNAME, PROJECT_INFO_DIRNAME, "agents", "logs"],
            "has_custom": False,
        }
        if wants_project_context:
            project_scope = conn.execute(
                """
                SELECT user_id, project_root, title, brief, goal, setup_json, plan_status, execution_status, progress_pct
                FROM projects
                WHERE id = ?
                LIMIT 1
                """,
                (session_key,),
            ).fetchone()
            if not project_scope:
                conn.close()
                raise HTTPException(404, "Project not found for project chat context")
            project_owner_user_id = str(project_scope["user_id"] or "").strip() or user_id
            raw_roles = conn.execute(
                """
                SELECT agent_id, agent_name, is_primary, role, COALESCE(source_type, 'owner') AS source_type
                FROM project_agents
                WHERE project_id = ?
                ORDER BY is_primary DESC, agent_name ASC
                """,
                (session_key,),
            ).fetchall()
            role_rows = [dict(r) for r in raw_roles]
            if not role_rows:
                conn.close()
                raise HTTPException(400, "Invite at least one agent before using project chat")
            first_primary = next((r for r in role_rows if bool(r.get("is_primary"))), None)
            if first_primary:
                project_primary_agent_id = str(first_primary.get("agent_id") or "").strip() or None
            elif role_rows:
                project_primary_agent_id = str(role_rows[0].get("agent_id") or "").strip() or None

            if project_owner_user_id != user_id:
                project_access_mode = "member"
                member_rows = conn.execute(
                    """
                    SELECT agent_id
                    FROM project_external_agent_memberships
                    WHERE project_id = ? AND member_user_id = ? AND member_connection_id = ? AND status = 'active'
                    ORDER BY updated_at DESC, created_at DESC
                    """,
                    (session_key, user_id, connection_id),
                ).fetchall()
                member_allowed_agent_ids = {
                    str(r["agent_id"] or "").strip()
                    for r in member_rows
                    if str(r["agent_id"] or "").strip()
                }
                if not member_allowed_agent_ids:
                    conn.close()
                    raise HTTPException(403, "This connection is not an active external member for the selected project")

                if a2a_access:
                    a2a_agent_id = str(a2a_access.get("agent_id") or "").strip()
                    if not a2a_agent_id or a2a_agent_id not in member_allowed_agent_ids:
                        conn.close()
                        raise HTTPException(403, "A2A agent session is not an active member for this project")
                    if payload.agent_id and str(payload.agent_id).strip() != a2a_agent_id:
                        conn.close()
                        raise HTTPException(403, "A2A agent session can only chat as its own agent_id")

            if payload.agent_id:
                allowed = (
                    member_allowed_agent_ids
                    if project_access_mode == "member"
                    else {str(r.get("agent_id") or "").strip() for r in role_rows}
                )
                if payload.agent_id not in allowed:
                    conn.close()
                    raise HTTPException(403, "Only allowed project agents can be targeted in this project chat")
        if not row:
            conn.close()
            raise HTTPException(404, "Connection not found")
        if a2a_access:
            connection_env_id = str(row["env_id"] or "").strip()
            if not connection_env_id or connection_env_id != str(a2a_access.get("env_id") or "").strip():
                conn.close()
                raise HTTPException(403, "A2A agent session is not linked to this connection")

        workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
        main_agent_id = str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else ""
        main_agent_id = main_agent_id.strip() or None
        workspace_agent_rows = conn.execute(
            """
            SELECT agent_id
            FROM managed_agents
            WHERE user_id = ? AND connection_id = ? AND status = 'active'
            ORDER BY updated_at DESC, provisioned_at DESC, agent_name ASC
            """,
            (user_id, connection_id),
        ).fetchall()
        workspace_agent_ids: List[str] = []
        for _agent_row in workspace_agent_rows:
            _aid = str(_agent_row["agent_id"] or "").strip()
            if _aid and _aid not in workspace_agent_ids:
                workspace_agent_ids.append(_aid)
        if (not project_scope) and payload.agent_id:
            requested_workspace_agent_id = str(payload.agent_id or "").strip()
            allowed_workspace_ids = set(workspace_agent_ids)
            if main_agent_id:
                allowed_workspace_ids.add(main_agent_id)
            if requested_workspace_agent_id not in allowed_workspace_ids:
                live = await openclaw_list_agents(row["base_url"], connection_api_key)
                live_agents = live.get("agents") or []
                live_ids = {
                    str(a.get("id") or "").strip()
                    for a in live_agents
                    if isinstance(a, dict) and str(a.get("id") or "").strip()
                }

                if requested_workspace_agent_id in live_ids:
                    env_id = str(row["env_id"] or "").strip() or None
                    _provision_managed_agents_for_connection(
                        user_id=user_id,
                        env_id=env_id,
                        connection_id=connection_id,
                        base_url=row["base_url"],
                        raw_agents=live_agents,
                    )
                    allowed_workspace_ids.update(live_ids)
                else:
                    conn.close()
                    raise HTTPException(403, "Workspace chat can only target agents available on this connection")
        project_root = str(project_scope["project_root"]) if (project_scope and project_scope["project_root"]) else None
        project_instruction = None
        write_allow_paths = None
        if project_scope:
            if project_access_mode == "member":
                if a2a_access:
                    effective_agent_id = str(a2a_access.get("agent_id") or "").strip() or None
                else:
                    member_ordered = [
                        str(r.get("agent_id") or "").strip()
                        for r in role_rows
                        if str(r.get("agent_id") or "").strip() in member_allowed_agent_ids
                    ]
                    default_member_agent_id = None
                    if project_primary_agent_id and project_primary_agent_id in member_allowed_agent_ids:
                        default_member_agent_id = project_primary_agent_id
                    elif member_ordered:
                        default_member_agent_id = member_ordered[0]
                    elif member_allowed_agent_ids:
                        default_member_agent_id = sorted(member_allowed_agent_ids)[0]
                    effective_agent_id = payload.agent_id or default_member_agent_id
            else:
                is_paused_scope = _coerce_execution_status(project_scope["execution_status"]) == EXEC_STATUS_PAUSED
                if is_paused_scope:
                    effective_agent_id = project_primary_agent_id or payload.agent_id
                else:
                    effective_agent_id = payload.agent_id or project_primary_agent_id
            if not effective_agent_id:
                conn.close()
                raise HTTPException(400, "No project agent configured for this chat")

            selected_source_type = next(
                (
                    str(r.get("source_type") or "owner").strip() or "owner"
                    for r in role_rows
                    if str(r.get("agent_id") or "").strip() == str(effective_agent_id)
                ),
                "owner",
            )
            selected_agent_permissions = _get_project_agent_permissions(
                conn,
                project_id=session_key,
                agent_id=str(effective_agent_id),
                source_type=selected_source_type,
            )
            if not bool(selected_agent_permissions.get("can_chat_project")):
                conn.close()
                raise HTTPException(403, "This project agent is not allowed to use project chat")

            project_instruction = _project_context_instruction(
                title=str(project_scope["title"] or ""),
                brief=str(project_scope["brief"] or ""),
                goal=str(project_scope["goal"] or ""),
                setup_details=_parse_setup_json(project_scope["setup_json"]),
                role_rows=role_rows,
                project_root=str(project_scope["project_root"] or ""),
                plan_status=_coerce_plan_status(project_scope["plan_status"]),
            )
            roster_text = _agent_roster_markdown(role_rows)
            sections = [project_instruction, roster_text]
            project_exec_status = _coerce_execution_status(project_scope["execution_status"])
            if project_exec_status == EXEC_STATUS_PAUSED:
                sections.append(
                    "Execution state note:\n"
                    "- Project is currently paused waiting for user input.\n"
                    "- Evaluate the latest user reply now.\n"
                    "- If information is sufficient, continue and set requires_user_input=false.\n"
                    "- If information is still missing, ask clearly and include @owner in chat_update with requires_user_input=true.\n"
                    "- If user explicitly says SKIP, continue with reasonable assumptions and set requires_user_input=false."
                )
            if bool(selected_agent_permissions.get("can_read_files")):
                project_file_context = _build_project_file_context(
                    owner_user_id=project_owner_user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    include_paths=[
                        PROJECT_INFO_FILE,
                        PROJECT_DELEGATION_FILE,
                        OVERVIEW_FILE,
                        PROJECT_PLAN_FILE,
                        PROJECT_PROTOCOL_FILE,
                        "agents/ROLES.md",
                        SETUP_CHAT_HISTORY_FILE,
                        SETUP_CHAT_HISTORY_COMPAT_FILE,
                    ],
                    request_text=payload.message,
                    max_total_chars=8_000,
                    max_files=8,
                )
                if project_file_context:
                    sections.append(project_file_context)
            else:
                sections.append(
                    "File access note:\n"
                    "- You currently do not have permission to read project files.\n"
                    "- Ask @owner to grant file-read permission if deeper context is required."
                )
            project_instruction = "\n\n".join([s for s in sections if str(s or "").strip()])
            if bool(selected_agent_permissions.get("can_write_files")):
                write_allow_paths = _normalize_permission_write_paths(
                    selected_agent_permissions.get("write_paths") or [],
                    fallback=[],
                )
            else:
                write_allow_paths = []
        else:
            effective_agent_id = str(payload.agent_id or "").strip() or None
            if not effective_agent_id:
                if main_agent_id:
                    effective_agent_id = main_agent_id
                elif workspace_agent_ids:
                    effective_agent_id = workspace_agent_ids[0]
            session_key = "main"
            if not effective_agent_id:
                effective_agent_id = None

        conn.close()
        scoped_message = _compose_guardrailed_message(
            payload.message.strip(),
            workspace_root=workspace_root,
            project_root=project_root,
            task_instruction=project_instruction,
        )
        res = await openclaw_ws_chat(
            base_url=row["base_url"],
            api_key=connection_api_key,
            message=scoped_message,
            agent_id=effective_agent_id,
            session_key=session_key,
            timeout_sec=payload.timeout_sec,
        )
        if not res.get("ok"):
            if project_scope:
                _append_project_daily_log(
                    owner_user_id=project_owner_user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    kind="chat.error",
                    text=detail_to_text(res.get("error") or res.get("details"))[:1200],
                )
            raise HTTPException(400, res)
        if project_scope:
            raw_agent_text = str(res.get("text") or "")
            parsed_payload = _extract_agent_report_payload(raw_agent_text)
            chat_update = str(parsed_payload.get("chat_update") or "").strip()
            parsed_notes = str(parsed_payload.get("notes") or "").strip()
            requires_user_input = bool(parsed_payload.get("requires_user_input"))
            pause_reason = str(parsed_payload.get("pause_reason") or "").strip()
            resume_hint = str(parsed_payload.get("resume_hint") or "").strip()
            write_payload = parsed_payload.get("output_files") or []
            write_result = _apply_project_file_writes(
                owner_user_id=project_owner_user_id,
                project_root=str(project_scope["project_root"] or ""),
                writes=write_payload if isinstance(write_payload, list) else [],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
            allow_paths=write_allow_paths,
            )
            saved_writes = write_result.get("saved") or []
            skipped_writes = write_result.get("skipped") or []
            artifact_followup_used = False
            artifact_rescue_used = False
            artifact_like_request = _looks_like_artifact_request(payload.message)
            effective_role = next(
                (
                    str(r.get("role") or "").strip()
                    for r in role_rows
                    if str(r.get("agent_id") or "") == str(effective_agent_id or "")
                ),
                "",
            ) or "Collaborator"
            if _should_request_artifact_followup(
                user_message=payload.message,
                raw_response=raw_agent_text,
                parsed_payload=parsed_payload,
                saved_files=saved_writes,
            ):
                artifact_followup_used = True
                await emit(
                    session_key,
                    "agent.chat.live",
                    {
                        "agent_id": effective_agent_id,
                        "note": "No synced files detected yet. Requesting explicit output_files payload.",
                    },
                )
                followup_prompt = _build_artifact_followup_prompt(
                    user_message=payload.message,
                    previous_response=raw_agent_text,
                )
                followup_res = await openclaw_ws_chat(
                    base_url=row["base_url"],
                    api_key=connection_api_key,
                    message=followup_prompt,
                    agent_id=effective_agent_id,
                    session_key=session_key,
                    timeout_sec=max(10, min(payload.timeout_sec, 60)),
                )
                if followup_res.get("ok"):
                    followup_text = str(followup_res.get("text") or "").strip()
                    followup_parsed = _extract_agent_report_payload(followup_text)
                    followup_chat_update = str(followup_parsed.get("chat_update") or "").strip()
                    followup_writes_raw = followup_parsed.get("output_files") or []
                    requires_user_input = requires_user_input or bool(followup_parsed.get("requires_user_input"))
                    if not pause_reason:
                        pause_reason = str(followup_parsed.get("pause_reason") or "").strip()
                    if not resume_hint:
                        resume_hint = str(followup_parsed.get("resume_hint") or "").strip()
                    if not parsed_notes:
                        parsed_notes = str(followup_parsed.get("notes") or "").strip()
                    followup_write_result = _apply_project_file_writes(
                        owner_user_id=project_owner_user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        writes=followup_writes_raw if isinstance(followup_writes_raw, list) else [],
                        default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                    allow_paths=write_allow_paths,
                    )
                    followup_saved = followup_write_result.get("saved") or []
                    followup_skipped = followup_write_result.get("skipped") or []
                    if followup_saved:
                        saved_writes.extend(followup_saved)
                    if followup_skipped:
                        skipped_writes.extend(followup_skipped)
                    if followup_chat_update:
                        chat_update = followup_chat_update
                        res["text"] = followup_chat_update
                    for note in _summarize_ws_frames(followup_res.get("frames"), limit=6):
                        await emit(session_key, "agent.chat.live", {"agent_id": effective_agent_id, "note": note})
                    fp, fc, _ = _extract_usage_counts(followup_res)
                    if fp <= 0:
                        fp = _estimate_tokens_from_text(followup_prompt)
                    if fc <= 0:
                        fc = _estimate_tokens_from_text(followup_res.get("text"))
                    _update_project_usage_metrics(session_key, prompt_tokens=fp, completion_tokens=fc)
                else:
                    skipped_writes.append(
                        "artifact follow-up failed: "
                        + detail_to_text(followup_res.get("error") or followup_res.get("details") or "unknown")
                    )
            if not saved_writes and not requires_user_input and artifact_like_request:
                artifact_rescue_used = True
                await emit(
                    session_key,
                    "agent.chat.live",
                    {
                        "agent_id": effective_agent_id,
                        "note": "Still no synced files. Forcing concrete deliverables payload.",
                    },
                )
                rescue_prompt = _build_artifact_recovery_prompt(
                    agent_id=str(effective_agent_id or "agent"),
                    role=effective_role,
                    task_text=payload.message,
                    previous_response=raw_agent_text,
                )
                rescue_res = await openclaw_ws_chat(
                    base_url=row["base_url"],
                    api_key=connection_api_key,
                    message=rescue_prompt,
                    agent_id=effective_agent_id,
                    session_key=session_key,
                    timeout_sec=max(10, min(payload.timeout_sec, 60)),
                )
                if rescue_res.get("ok"):
                    rescue_text = str(rescue_res.get("text") or "").strip()
                    rescue_parsed = _extract_agent_report_payload(rescue_text)
                    rescue_chat_update = str(rescue_parsed.get("chat_update") or "").strip()
                    rescue_writes_raw = rescue_parsed.get("output_files") or []
                    if not rescue_writes_raw:
                        rescue_writes_raw = _extract_artifacts_from_fenced_code(rescue_text)
                    rescue_write_result = _apply_project_file_writes(
                        owner_user_id=project_owner_user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        writes=rescue_writes_raw if isinstance(rescue_writes_raw, list) else [],
                        default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                    allow_paths=write_allow_paths,
                    )
                    rescue_saved = rescue_write_result.get("saved") or []
                    rescue_skipped = rescue_write_result.get("skipped") or []
                    if rescue_saved:
                        saved_writes.extend(rescue_saved)
                    if rescue_skipped:
                        skipped_writes.extend(rescue_skipped)
                    requires_user_input = requires_user_input or bool(rescue_parsed.get("requires_user_input"))
                    if not pause_reason:
                        pause_reason = str(rescue_parsed.get("pause_reason") or "").strip()
                    if not resume_hint:
                        resume_hint = str(rescue_parsed.get("resume_hint") or "").strip()
                    if not parsed_notes:
                        parsed_notes = str(rescue_parsed.get("notes") or "").strip()
                    if rescue_chat_update:
                        chat_update = rescue_chat_update
                        res["text"] = rescue_chat_update
                    for note in _summarize_ws_frames(rescue_res.get("frames"), limit=6):
                        await emit(session_key, "agent.chat.live", {"agent_id": effective_agent_id, "note": note})
                    rp, rc, _ = _extract_usage_counts(rescue_res)
                    if rp <= 0:
                        rp = _estimate_tokens_from_text(rescue_prompt)
                    if rc <= 0:
                        rc = _estimate_tokens_from_text(rescue_res.get("text"))
                    _update_project_usage_metrics(session_key, prompt_tokens=rp, completion_tokens=rc)
                else:
                    skipped_writes.append(
                        "artifact rescue failed: "
                        + detail_to_text(rescue_res.get("error") or rescue_res.get("details") or "unknown")
                    )
            if not saved_writes and not requires_user_input and artifact_like_request:
                fallback_rel = f"{USER_OUTPUTS_DIRNAME}/chat-generated-deliverable.md"
                fallback_content = (
                    f"# Chat Deliverable Snapshot\n\n"
                    f"- agent_id: {effective_agent_id or '-'}\n"
                    f"- role: {effective_role}\n"
                    f"- generated_at: {format_ts(int(time.time()))}\n\n"
                    f"## User Request\n{payload.message.strip()}\n\n"
                    f"## Agent Response\n{str(res.get('text') or raw_agent_text).strip()}\n"
                )
                fallback_write_result = _apply_project_file_writes(
                    owner_user_id=project_owner_user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    writes=[{"path": fallback_rel, "content": fallback_content, "append": False}],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                allow_paths=write_allow_paths,
                )
                fallback_saved = fallback_write_result.get("saved") or []
                fallback_skipped = fallback_write_result.get("skipped") or []
                if fallback_saved:
                    saved_writes.extend(fallback_saved)
                    skipped_writes.append("No explicit output_files from chat response; saved fallback markdown deliverable.")
                if fallback_skipped:
                    skipped_writes.extend(fallback_skipped)
            pause_decision = _infer_pause_request(
                chat_update=chat_update,
                notes=parsed_notes,
                explicit_requires_user_input=requires_user_input,
                explicit_pause_reason=pause_reason,
                explicit_resume_hint=resume_hint,
            )
            if pause_decision.get("pause"):
                chat_update = _ensure_owner_mention(chat_update)
            if chat_update:
                res["text"] = chat_update
    
            prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
            if prompt_tokens <= 0:
                prompt_tokens = _estimate_tokens_from_text(scoped_message)
            if completion_tokens <= 0:
                completion_tokens = _estimate_tokens_from_text(raw_agent_text or res.get("text"))
            _update_project_usage_metrics(session_key, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)
            state_now, pct_now = _read_project_execution_state(session_key)
            if pause_decision.get("pause"):
                if state_now not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
                    pause_pct = max(5, _clamp_progress(pct_now))
                    _set_project_execution_state(session_key, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
                    reason_text = str(pause_decision.get("reason") or pause_reason or chat_update or "Execution paused.").strip()
                    hint_text = str(
                        pause_decision.get("resume_hint")
                        or resume_hint
                        or "Reply with required input, then say CONTINUE or press Resume."
                    ).strip()
                    res["requires_user_input"] = True
                    res["pause_reason"] = reason_text[:900]
                    res["resume_hint"] = hint_text[:300]
                    await emit(
                        session_key,
                        "project.execution.auto_paused",
                        {
                            "status": EXEC_STATUS_PAUSED,
                            "progress_pct": pause_pct,
                            "agent_id": effective_agent_id,
                            "agent_name": next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(effective_agent_id or "")), ""),
                            "reason": reason_text[:900],
                            "resume_hint": hint_text[:300],
                        },
                    )
                    _append_project_daily_log(
                        owner_user_id=project_owner_user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        kind="execution.auto_paused",
                        text=f"{effective_agent_id or 'agent'}: {reason_text[:1200]}",
                        payload={"resume_hint": hint_text[:300]},
                    )
            else:
                if state_now == EXEC_STATUS_PAUSED:
                    resume_pct = max(5, _clamp_progress(pct_now))
                    _set_project_execution_state(session_key, status=EXEC_STATUS_RUNNING, progress_pct=resume_pct)
                    resume_summary = (
                        "Resumed after user continue message in chat."
                        if _is_resume_command_message(payload.message)
                        else "Resumed after primary agent accepted latest user input."
                    )
                    await emit(
                        session_key,
                        "project.execution.resumed_after_pause",
                        {
                            "status": EXEC_STATUS_RUNNING,
                            "progress_pct": resume_pct,
                            "summary": resume_summary,
                            "agent_id": effective_agent_id,
                        },
                    )
                    _append_project_daily_log(
                        owner_user_id=project_owner_user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        kind="execution.resume",
                        text=resume_summary,
                    )
                elif _is_resume_command_message(payload.message) and state_now != EXEC_STATUS_COMPLETED:
                    resume_pct = max(5, _clamp_progress(pct_now))
                    _set_project_execution_state(session_key, status=EXEC_STATUS_RUNNING, progress_pct=resume_pct)
                    await emit(
                        session_key,
                        "project.execution.resume",
                        {
                            "status": EXEC_STATUS_RUNNING,
                            "progress_pct": resume_pct,
                            "summary": "Resumed after user continue message in chat.",
                        },
                    )
                    _append_project_daily_log(
                        owner_user_id=project_owner_user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        kind="execution.resume",
                        text="Execution resumed after user continue message in chat.",
                    )
            _refresh_project_documents(session_key)
            await emit(
                session_key,
                "agent.chat.update",
                {
                    "agent_id": effective_agent_id,
                    "agent_name": next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(effective_agent_id or "")), ""),
                    "text": str(res.get("text") or "")[:1200],
                },
            )
            for note in _summarize_ws_frames(res.get("frames"), limit=8):
                await emit(session_key, "agent.chat.live", {"agent_id": effective_agent_id, "note": note})
            for item in saved_writes:
                await emit(
                    session_key,
                    "project.file.written",
                    {
                        "path": str(item.get("path") or ""),
                        "mode": str(item.get("mode") or "w"),
                        "bytes": int(item.get("bytes") or 0),
                        "actor": f"agent:{effective_agent_id or 'unknown'}",
                    },
                )
            if saved_writes:
                await emit(
                    session_key,
                    "agent.chat.files_saved",
                    {
                        "agent_id": effective_agent_id,
                        "saved_files": saved_writes,
                        "skipped": skipped_writes[:10],
                    },
                )
            _append_project_daily_log(
                owner_user_id=project_owner_user_id,
                project_root=str(project_scope["project_root"] or ""),
                kind="chat.hivee",
                text=(
                    f"USER: {payload.message.strip()}\n"
                    f"AGENT({effective_agent_id or 'auto'}): {str(res.get('text') or '').strip()}\n"
                    f"FILES_SAVED: {len(saved_writes)}\n"
                    f"ARTIFACT_FOLLOWUP_USED: {'yes' if artifact_followup_used else 'no'}\n"
                    f"ARTIFACT_RESCUE_USED: {'yes' if artifact_rescue_used else 'no'}"
                ),
                payload={
                    "saved_files": saved_writes,
                    "skipped_files": skipped_writes[:10],
                    "requires_user_input": bool(res.get("requires_user_input")),
                    "pause_reason": str(res.get("pause_reason") or "")[:500],
                    "resume_hint": str(res.get("resume_hint") or "")[:300],
                },
            )
            res["saved_files"] = saved_writes
            res["skipped_files"] = skipped_writes[:20]
            res["artifact_followup_used"] = artifact_followup_used
            res["artifact_rescue_used"] = artifact_rescue_used
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        res["context_mode"] = "project" if project_root else "workspace"
        res["session_key"] = session_key
        res["auth_mode"] = "a2a_session" if a2a_access else "user_session"
        if a2a_access:
            res["a2a_agent_id"] = str(a2a_access.get("agent_id") or "")
            res["a2a_env_id"] = str(a2a_access.get("env_id") or "")
        if project_root:
            res["project_root"] = project_root
        if project_scope:
            res["project_access_mode"] = project_access_mode
            res["project_permissions"] = {
                "can_chat_project": bool(selected_agent_permissions.get("can_chat_project")),
                "can_read_files": bool(selected_agent_permissions.get("can_read_files")),
                "can_write_files": bool(selected_agent_permissions.get("can_write_files")),
                "write_paths": selected_agent_permissions.get("write_paths") or [],
            }
        return res
