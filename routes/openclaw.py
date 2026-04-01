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
    
        bootstrap = await _bootstrap_connection_workspace(user_id, payload.base_url.rstrip("/"), payload.api_key)
        if not bootstrap.get("ok"):
            raise HTTPException(
                400,
                {
                    "message": "OpenClaw connected, but Hivee workspace bootstrap failed.",
                    "details": bootstrap,
                },
            )
    
        conn = db()
        conn_id = new_id("oc")
        conn.execute(
            "INSERT INTO openclaw_connections (id, user_id, env_id, base_url, api_key, name, created_at) VALUES (?,?,?,?,?,?,?)",
            (conn_id, user_id, env_id, payload.base_url.rstrip("/"), payload.api_key, payload.name, int(time.time())),
        )
        conn.commit()
        conn.close()
    
        _upsert_connection_policy(
            conn_id,
            user_id,
            main_agent_id=bootstrap.get("main_agent_id"),
            main_agent_name=bootstrap.get("main_agent_name"),
            bootstrap_status="ok",
            bootstrap_error=None,
            workspace_tree=bootstrap.get("workspace_tree"),
            workspace_root=str(bootstrap.get("workspace_root") or HIVEE_ROOT),
            templates_root=str(bootstrap.get("templates_root") or HIVEE_TEMPLATES_ROOT),
        )
        provision = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=env_id,
            connection_id=conn_id,
            base_url=payload.base_url.rstrip("/"),
            raw_agents=bootstrap.get("agents") or [],
            fallback_agent_id=bootstrap.get("main_agent_id"),
            fallback_agent_name=bootstrap.get("main_agent_name"),
        )
    
        return {
            "ok": True,
            "connection": {"id": conn_id, "base_url": payload.base_url.rstrip("/"), "name": payload.name},
            "health": health,
            "bootstrap": bootstrap,
            "agent_provision": provision,
        }
    
    @app.post("/api/openclaw/{connection_id}/bootstrap")
    async def bootstrap_openclaw_connection(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        bootstrap = await _bootstrap_connection_workspace(user_id, row["base_url"], row["api_key"])
        _upsert_connection_policy(
            connection_id,
            user_id,
            main_agent_id=bootstrap.get("main_agent_id"),
            main_agent_name=bootstrap.get("main_agent_name"),
            bootstrap_status="ok" if bootstrap.get("ok") else "failed",
            bootstrap_error=None if bootstrap.get("ok") else detail_to_text(bootstrap.get("ws_result")),
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
            "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        res = await openclaw_list_agents(row["base_url"], row["api_key"])
        if not res.get("ok"):
            raise HTTPException(400, res)
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
            "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
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
            row["api_key"],
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
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
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
        project_scope = None
        role_rows: List[Dict[str, Any]] = []
        project_primary_agent_id: Optional[str] = None
        if wants_project_context:
            project_scope = conn.execute(
                "SELECT project_root, title, brief, goal, setup_json, plan_status, execution_status, progress_pct FROM projects WHERE id = ? AND user_id = ?",
                (session_key, user_id),
            ).fetchone()
            if not project_scope:
                conn.close()
                raise HTTPException(404, "Project not found for project chat context")
            raw_roles = conn.execute(
                "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
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
            if payload.agent_id:
                allowed = {str(r.get("agent_id") or "").strip() for r in role_rows}
                if payload.agent_id not in allowed:
                    conn.close()
                    raise HTTPException(403, "Only invited project agents can be targeted in this project chat")
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
        main_agent_id = str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else ""
        main_agent_id = main_agent_id.strip() or None
        if (not project_scope) and payload.agent_id:
            if not main_agent_id:
                raise HTTPException(400, "Main workspace agent is not configured. Re-run OpenClaw bootstrap.")
            if payload.agent_id != main_agent_id:
                raise HTTPException(403, "Workspace chat can only target your main user agent")
        project_root = str(project_scope["project_root"]) if (project_scope and project_scope["project_root"]) else None
        project_instruction = None
        if project_scope:
            project_instruction = _project_context_instruction(
                title=str(project_scope["title"] or ""),
                brief=str(project_scope["brief"] or ""),
                goal=str(project_scope["goal"] or ""),
                setup_details=_parse_setup_json(project_scope["setup_json"]),
                role_rows=role_rows,
                plan_status=_coerce_plan_status(project_scope["plan_status"]),
            )
            roster_text = _agent_roster_markdown(role_rows)
            project_file_context = _build_project_file_context(
                owner_user_id=user_id,
                project_root=str(project_scope["project_root"] or ""),
                include_paths=[
                    PROJECT_INFO_FILE,
                    PROJECT_DELEGATION_FILE,
                    OVERVIEW_FILE,
                    PROJECT_PLAN_FILE,
                    "agents/ROLES.md",
                    SETUP_CHAT_HISTORY_FILE,
                    SETUP_CHAT_HISTORY_COMPAT_FILE,
                ],
                request_text=payload.message,
                max_total_chars=8_000,
                max_files=8,
            )
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
            if project_file_context:
                sections.append(project_file_context)
            project_instruction = "\n\n".join([s for s in sections if str(s or "").strip()])
        if project_scope:
            is_paused_scope = _coerce_execution_status(project_scope["execution_status"]) == EXEC_STATUS_PAUSED
            if is_paused_scope:
                effective_agent_id = project_primary_agent_id or payload.agent_id
            else:
                effective_agent_id = payload.agent_id or project_primary_agent_id
            if not effective_agent_id:
                raise HTTPException(400, "No primary project agent configured")
        else:
            effective_agent_id = main_agent_id
            session_key = "main"
            if not effective_agent_id:
                raise HTTPException(400, "Main workspace agent is not configured. Re-run OpenClaw bootstrap.")
        scoped_message = _compose_guardrailed_message(
            payload.message.strip(),
            workspace_root=workspace_root,
            project_root=project_root,
            task_instruction=project_instruction,
        )
        res = await openclaw_ws_chat(
            base_url=row["base_url"],
            api_key=row["api_key"],
            message=scoped_message,
            agent_id=effective_agent_id,
            session_key=session_key,
            timeout_sec=payload.timeout_sec,
        )
        if not res.get("ok"):
            if project_scope:
                _append_project_daily_log(
                    owner_user_id=user_id,
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
                owner_user_id=user_id,
                project_root=str(project_scope["project_root"] or ""),
                writes=write_payload if isinstance(write_payload, list) else [],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
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
                    api_key=row["api_key"],
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
                        owner_user_id=user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        writes=followup_writes_raw if isinstance(followup_writes_raw, list) else [],
                        default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
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
                    api_key=row["api_key"],
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
                        owner_user_id=user_id,
                        project_root=str(project_scope["project_root"] or ""),
                        writes=rescue_writes_raw if isinstance(rescue_writes_raw, list) else [],
                        default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
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
                    owner_user_id=user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    writes=[{"path": fallback_rel, "content": fallback_content, "append": False}],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
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
                        owner_user_id=user_id,
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
                        owner_user_id=user_id,
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
                        owner_user_id=user_id,
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
                owner_user_id=user_id,
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
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        res["context_mode"] = "project" if project_root else "workspace"
        res["session_key"] = session_key
        if project_root:
            res["project_root"] = project_root
        return res
    
