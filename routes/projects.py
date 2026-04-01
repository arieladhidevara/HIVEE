from hivee_shared import *
from services.project_runtime import simulate_run

def register_routes(app: FastAPI) -> None:
    @app.post("/api/projects/setup-chat")
    async def project_setup_chat(request: Request, payload: ProjectSetupChatIn):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        policy = conn.execute(
            "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        workspace = _ensure_user_workspace(user_id)
        workspace_root = str(workspace["workspace_root"])
        templates_root = str(workspace["templates_root"])
        setup_template = _read_project_setup_template(user_id)
        effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
        instruction = _build_new_project_setup_instruction(
            payload.message or "Start new project setup and ask the first question.",
            template_content=setup_template,
            workspace_root=workspace_root,
            start_mode=bool(payload.start),
        )
        session_key = (payload.session_key or "new-project").strip() or "new-project"
        res = await openclaw_ws_chat(
            base_url=row["base_url"],
            api_key=row["api_key"],
            message=instruction,
            agent_id=effective_agent_id,
            session_key=f"project-setup:{session_key}",
            timeout_sec=max(10, min(payload.timeout_sec, 45 if payload.optimize_tokens else 90)),
        )
        if not res.get("ok"):
            raise HTTPException(400, res)
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        res["templates_root"] = templates_root
        return res
    
    @app.post("/api/projects/setup-draft")
    async def project_setup_draft(request: Request, payload: ProjectSetupDraftIn):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        policy = conn.execute(
            "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Connection not found")
    
        workspace = _ensure_user_workspace(user_id)
        workspace_root = str(workspace["workspace_root"])
        transcript = payload.transcript or []
        local_draft = _local_setup_draft(transcript)
        local_details = _normalize_setup_details(local_draft.get("setup_details") or {})
        local_details["autofill_used"] = True
        local_details["draft_source"] = "local_optimized"
    
        if payload.optimize_tokens:
            return {
                "ok": True,
                "title": str(local_draft.get("title") or "")[:160],
                "brief": str(local_draft.get("brief") or "")[:5000],
                "goal": str(local_draft.get("goal") or "")[:5000],
                "setup_details": local_details,
                "raw": "LOCAL_DRAFT_OPTIMIZED",
            }
    
        setup_template = _read_project_setup_template(user_id)
        effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
        instruction = _build_setup_draft_instruction(
            template_content=setup_template,
            transcript=transcript,
            workspace_root=workspace_root,
        )
        session_key = (payload.session_key or "new-project").strip() or "new-project"
        res = await openclaw_ws_chat(
            base_url=row["base_url"],
            api_key=row["api_key"],
            message=instruction,
            agent_id=effective_agent_id,
            session_key=f"project-setup-draft:{session_key}",
            timeout_sec=max(10, min(payload.timeout_sec, 60)),
        )
    
        text = ""
        parsed: Dict[str, Any] = {}
        if res.get("ok"):
            text = str(res.get("text") or "").strip()
            parsed = _extract_json_object(text) or {}
        else:
            text = "LOCAL_DRAFT_FALLBACK"
    
        details = _normalize_setup_details(parsed)
        merged_details: Dict[str, Any] = dict(local_details)
        if details:
            merged_details.update(details)
    
        title = str(parsed.get("title") or local_draft.get("title") or "").strip()[:160]
        brief = str(parsed.get("brief") or local_draft.get("brief") or "").strip()[:5000]
        goal = str(parsed.get("goal") or local_draft.get("goal") or "").strip()[:5000]
        if not title:
            title = _fallback_project_title(transcript)[:160]
        if not brief:
            user_lines = _first_user_lines(transcript)
            brief = (" ".join(user_lines) if user_lines else "Project brief drafted from setup conversation.")[:5000]
        if not goal:
            fallback_goal = str(merged_details.get("first_output") or merged_details.get("milestones") or "").strip()
            goal = (fallback_goal or "Produce a practical first deliverable and execution plan for this project.")[:5000]
    
        merged_details["autofill_used"] = (not bool(parsed.get("title"))) or (not bool(parsed.get("brief"))) or (not bool(parsed.get("goal")))
        if res.get("ok"):
            merged_details["draft_source"] = "agent"
        else:
            merged_details["draft_source"] = "local_fallback"
        merged_details.pop("title", None)
        merged_details.pop("brief", None)
        merged_details.pop("goal", None)
    
        return {
            "ok": True,
            "title": title,
            "brief": brief,
            "goal": goal,
            "setup_details": merged_details,
            "raw": text[:2000] if text else "LOCAL_DRAFT_FALLBACK",
        }
    
    @app.post("/api/projects", response_model=ProjectOut)
    async def create_project(request: Request, payload: ProjectCreateIn):
        user_id = get_session_user(request)
        primary_env = _ensure_primary_environment_for_user(user_id)
        env_id = str(primary_env.get("id") or "").strip() or None
    
        conn = db()
        c = conn.execute(
            "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        if not c:
            conn.close()
            raise HTTPException(400, "Invalid connection_id (not found for this user)")
    
        pid = new_id("prj")
        now = int(time.time())
        setup_details = _normalize_setup_details(payload.setup_details or {})
        setup_chat_history_text = str(payload.setup_chat_history or "").replace("\r", "").strip()[:120_000]
        workspace = _ensure_user_workspace(user_id)
        workspace_root = str(workspace["workspace_root"])
        project_root = _build_project_root(pid, payload.title, workspace_root=workspace_root)
        project_dir = Path(project_root)
        if not _path_within(project_dir, Path(workspace_root)):
            conn.close()
            raise HTTPException(400, "Invalid project root derived from workspace")
        _initialize_project_folder(
            project_dir,
            payload.title,
            payload.brief,
            payload.goal,
            setup_details=setup_details,
            setup_chat_history_text=setup_chat_history_text,
        )
    
        conn.execute(
            """
            INSERT INTO projects (
                id, user_id, env_id, title, brief, goal, setup_json, plan_text, plan_status, plan_updated_at, plan_approved_at,
                execution_status, progress_pct, execution_updated_at,
                usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
                connection_id, workspace_root, project_root, scope_requires_owner_approval, created_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pid,
                user_id,
                env_id,
                payload.title,
                payload.brief,
                payload.goal,
                json.dumps(setup_details, ensure_ascii=False),
                "",
                PLAN_STATUS_PENDING,
                int(time.time()),
                None,
                EXEC_STATUS_IDLE,
                0,
                int(time.time()),
                0,
                0,
                0,
                int(time.time()),
                payload.connection_id,
                workspace_root,
                project_root,
                1,
                now,
            ),
        )
        conn.commit()
        conn.close()
    
        _refresh_project_documents(pid)
    
        await emit(pid, "project.created", {"title": payload.title})
        await emit(pid, "project.scope_initialized", {"project_root": project_root})
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=project_root,
            kind="project.created",
            text=f"Project created: {payload.title}",
            payload={"brief": payload.brief[:500], "goal": payload.goal[:500]},
        )
        return ProjectOut(
            id=pid,
            title=payload.title,
            brief=payload.brief,
            goal=payload.goal,
            connection_id=payload.connection_id,
            created_at=now,
            workspace_root=workspace_root,
            project_root=project_root,
            setup_details=setup_details,
            plan_status=PLAN_STATUS_PENDING,
            plan_text="",
            plan_updated_at=int(time.time()),
            plan_approved_at=None,
            execution_status=EXEC_STATUS_IDLE,
            progress_pct=0,
            execution_updated_at=int(time.time()),
            usage_prompt_tokens=0,
            usage_completion_tokens=0,
            usage_total_tokens=0,
            usage_updated_at=int(time.time()),
        )
    
    @app.get("/api/projects", response_model=List[ProjectOut])
    async def list_projects(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            """
            SELECT id, title, brief, goal, connection_id, created_at, workspace_root, project_root, setup_json,
                   plan_text, plan_status, plan_updated_at, plan_approved_at,
                   execution_status, progress_pct, execution_updated_at,
                   usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
            FROM projects
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        return [_project_out_from_row(r) for r in rows]
    
    @app.get("/api/projects/{project_id}", response_model=ProjectOut)
    async def get_project(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            """
            SELECT id, title, brief, goal, connection_id, created_at, workspace_root, project_root, setup_json,
                   plan_text, plan_status, plan_updated_at, plan_approved_at,
                   execution_status, progress_pct, execution_updated_at,
                   usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
            FROM projects
            WHERE id = ? AND user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        return _project_out_from_row(row)
    
    @app.get("/api/projects/{project_id}/card")
    async def get_project_card(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT id, user_id, project_root FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        _refresh_project_documents(project_id)
        meta = _load_project_meta_snapshot(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            history_limit=20,
        )
        if not meta.get("ok"):
            raise HTTPException(400, meta.get("error") or "Project meta is not available")
        return {
            "ok": True,
            "project_id": project_id,
            "card": meta.get("card") or {},
        }
    
    @app.get("/api/projects/{project_id}/meta")
    async def get_project_meta(request: Request, project_id: str, limit: int = 40):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT id, user_id, project_root FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        _refresh_project_documents(project_id)
        meta = _load_project_meta_snapshot(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            history_limit=max(1, min(int(limit or 40), 300)),
        )
        if not meta.get("ok"):
            raise HTTPException(400, meta.get("error") or "Project meta is not available")
        meta["project_id"] = project_id
        return meta
    
    @app.delete("/api/projects/{project_id}")
    async def delete_project(request: Request, project_id: str):
        user_id = get_session_user(request)
        deleted = _delete_project_with_resources(owner_user_id=user_id, project_id=project_id)
        if not deleted.get("ok"):
            raise HTTPException(404, "Project not found")
        return deleted
    
    @app.get("/api/projects/{project_id}/plan", response_model=ProjectPlanOut)
    async def get_project_plan(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT id, plan_status, plan_text, plan_updated_at, plan_approved_at FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        return ProjectPlanOut(
            project_id=project_id,
            status=_coerce_plan_status(row["plan_status"]),
            text=str(row["plan_text"] or ""),
            updated_at=row["plan_updated_at"],
            approved_at=row["plan_approved_at"],
        )
    
    @app.post("/api/projects/{project_id}/plan/regenerate", response_model=ProjectPlanOut)
    async def regenerate_project_plan(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT id, plan_text FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Project not found")
        now = int(time.time())
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_GENERATING, now, project_id),
        )
        conn.commit()
        conn.close()
        asyncio.create_task(_generate_project_plan(project_id, force=True))
        await emit(project_id, "project.plan.regenerate_requested", {"project_id": project_id})
        return ProjectPlanOut(
            project_id=project_id,
            status=PLAN_STATUS_GENERATING,
            text=str(row["plan_text"] or ""),
            updated_at=now,
            approved_at=None,
        )
    
    @app.post("/api/projects/{project_id}/plan/approve", response_model=ProjectPlanOut)
    async def approve_project_plan(request: Request, project_id: str, payload: ProjectPlanApproveIn):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            """
            SELECT id, user_id, title, brief, goal, project_root, setup_json, plan_text, plan_status, plan_updated_at, plan_approved_at
            FROM projects
            WHERE id = ? AND user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Project not found")
    
        now = int(time.time())
        if payload.approve:
            new_status = PLAN_STATUS_APPROVED
            approved_at = now
            conn.execute(
                "UPDATE projects SET plan_status = ?, plan_approved_at = ?, plan_updated_at = ? WHERE id = ?",
                (new_status, approved_at, now, project_id),
            )
            conn.execute(
                "UPDATE projects SET execution_status = ?, progress_pct = ?, execution_updated_at = ? WHERE id = ?",
                (EXEC_STATUS_RUNNING, 5, now, project_id),
            )
        else:
            new_status = PLAN_STATUS_AWAITING_APPROVAL
            approved_at = row["plan_approved_at"]
            conn.execute(
                "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
                (new_status, now, project_id),
            )
            conn.execute(
                "UPDATE projects SET execution_status = ?, execution_updated_at = ? WHERE id = ?",
                (EXEC_STATUS_IDLE, now, project_id),
            )
        conn.commit()
        conn.close()
    
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.approve" if payload.approve else "plan.revert",
            text="User approved project plan." if payload.approve else "User reverted plan to waiting approval.",
        )
    
        if payload.approve:
            await emit(project_id, "project.plan.approved", {"project_id": project_id})
            asyncio.create_task(_delegate_project_tasks(project_id))
        else:
            await emit(project_id, "project.plan.awaiting_approval", {"project_id": project_id})
    
        return ProjectPlanOut(
            project_id=project_id,
            status=new_status,
            text=str(row["plan_text"] or ""),
            updated_at=now,
            approved_at=approved_at,
        )
    
    @app.get("/api/projects/{project_id}/workspace/tree", response_model=ProjectWorkspaceTreeOut)
    async def get_project_workspace_tree(request: Request, project_id: str):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        _ensure_user_workspace(owner_user_id)
        project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or ""))
        project_dir.mkdir(parents=True, exist_ok=True)
        return ProjectWorkspaceTreeOut(
            project_id=project_id,
            project_root=project_dir.as_posix(),
            tree=_render_tree(project_dir),
            access_mode=access["mode"],
        )
    
    @app.get("/api/projects/{project_id}/files", response_model=ProjectFilesOut)
    async def list_project_files(request: Request, project_id: str, path: str = ""):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or "")).resolve()
        target = _resolve_project_relative_path(
            owner_user_id,
            str(project.get("project_root") or ""),
            path,
            require_exists=True,
            require_dir=True,
        ).resolve()
        current_rel = ""
        if target != project_dir:
            current_rel = target.relative_to(project_dir).as_posix()
        parent_rel: Optional[str] = None
        if current_rel:
            parent_rel = str(Path(current_rel).parent).replace("\\", "/")
            if parent_rel == ".":
                parent_rel = ""
    
        entries: List[ProjectFileEntryOut] = []
        for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            rel = child.relative_to(project_dir).as_posix()
            stat = child.stat()
            entries.append(
                ProjectFileEntryOut(
                    name=child.name,
                    path=rel,
                    kind="dir" if child.is_dir() else "file",
                    size=None if child.is_dir() else int(stat.st_size),
                    modified_at=int(stat.st_mtime),
                )
            )
    
        return ProjectFilesOut(
            project_id=project_id,
            project_root=project_dir.as_posix(),
            current_path=current_rel,
            parent_path=parent_rel,
            access_mode=access["mode"],
            entries=entries,
        )
    
    @app.get("/api/projects/{project_id}/files/content", response_model=ProjectFileContentOut)
    async def read_project_file(request: Request, project_id: str, path: str):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or "")).resolve()
        target = _resolve_project_relative_path(
            owner_user_id,
            str(project.get("project_root") or ""),
            path,
            require_exists=True,
            require_dir=False,
        ).resolve()
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        data = target.read_bytes()
        size = len(data)
        truncated = size > MAX_FILE_PREVIEW_BYTES
        if truncated:
            data = data[:MAX_FILE_PREVIEW_BYTES]
        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError:
            content = data.decode("utf-8", errors="replace")
        rel = target.relative_to(project_dir).as_posix()
        return ProjectFileContentOut(
            project_id=project_id,
            path=rel,
            size=size,
            truncated=truncated,
            content=content,
        )
    
    @app.get("/api/projects/{project_id}/files/raw")
    async def read_project_file_raw(request: Request, project_id: str, path: str):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        project_root = str(project.get("project_root") or "")
        target = _resolve_project_relative_path(
            owner_user_id,
            project_root,
            path,
            require_exists=True,
            require_dir=False,
        ).resolve()
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        guessed, _ = mimetypes.guess_type(target.name)
        media_type = guessed or "application/octet-stream"
        return FileResponse(str(target), media_type=media_type)
    
    @app.get("/api/projects/{project_id}/preview/{path:path}")
    async def preview_project_file(request: Request, project_id: str, path: str):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        project_root = str(project.get("project_root") or "")
        target = _resolve_project_relative_path(
            owner_user_id,
            project_root,
            path,
            require_exists=True,
            require_dir=False,
        ).resolve()
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        guessed, _ = mimetypes.guess_type(target.name)
        media_type = guessed or "application/octet-stream"
        return FileResponse(str(target), media_type=media_type)
    
    @app.post("/api/projects/{project_id}/files/write")
    async def write_project_file(request: Request, project_id: str, payload: ProjectFileWriteIn):
        access = _resolve_project_workspace_access(request, project_id)
        project = access["project"]
        owner_user_id = str(project["user_id"])
        project_root = str(project.get("project_root") or "")
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
        target = _resolve_project_relative_path(
            owner_user_id,
            project_root,
            payload.path,
            require_exists=False,
            require_dir=False,
        ).resolve()
        if target.exists() and target.is_dir():
            raise HTTPException(400, "Target path is a directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if bool(payload.append) else "w"
        content = str(payload.content or "")
        with target.open(mode, encoding="utf-8") as f:
            f.write(content)
        rel = target.relative_to(project_dir).as_posix()
        actor = f"owner:{access.get('user_id')}" if access.get("mode") == "owner" else f"agent:{access.get('agent_id')}"
        _append_project_daily_log(
            owner_user_id=owner_user_id,
            project_root=project_root,
            kind="file.write",
            text=f"{actor} wrote {rel}",
            payload={"mode": mode, "bytes": len(content.encode('utf-8'))},
        )
        await emit(
            project_id,
            "project.file.written",
            {"path": rel, "mode": mode, "bytes": len(content.encode("utf-8")), "actor": actor},
        )
        return {"ok": True, "path": rel, "mode": mode, "bytes": len(content.encode("utf-8"))}
    
    @app.get("/api/projects/{project_id}/usage", response_model=ProjectUsageOut)
    async def get_project_usage(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            """
            SELECT usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
            FROM projects
            WHERE id = ? AND user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        return ProjectUsageOut(
            project_id=project_id,
            prompt_tokens=max(0, _to_int(row["usage_prompt_tokens"])),
            completion_tokens=max(0, _to_int(row["usage_completion_tokens"])),
            total_tokens=max(0, _to_int(row["usage_total_tokens"])),
            updated_at=row["usage_updated_at"],
        )
    
    @app.get("/api/projects/{project_id}/execution", response_model=ProjectExecutionOut)
    async def get_project_execution(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT execution_status, progress_pct, execution_updated_at FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Project not found")
        return ProjectExecutionOut(
            project_id=project_id,
            status=_coerce_execution_status(row["execution_status"]),
            progress_pct=_clamp_progress(row["progress_pct"]),
            updated_at=row["execution_updated_at"],
        )
    
    @app.post("/api/projects/{project_id}/execution/control", response_model=ProjectExecutionOut)
    async def control_project_execution(request: Request, project_id: str, payload: ProjectExecutionControlIn):
        user_id = get_session_user(request)
        action = str(payload.action or "").strip().lower()
        if action not in {"pause", "resume", "stop"}:
            raise HTTPException(400, "Invalid action. Use pause, resume, or stop.")
    
        conn = db()
        row = conn.execute(
            """
            SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.workspace_root,
                   p.plan_status, p.execution_status, p.progress_pct, p.connection_id,
                   c.base_url, c.api_key, cp.main_agent_id
            FROM projects p
            JOIN openclaw_connections c ON c.id = p.connection_id
            LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
            WHERE p.id = ? AND p.user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Project not found")
        role_rows = _project_agent_rows(conn, project_id)
        conn.close()
    
        current_status = _coerce_execution_status(row["execution_status"])
        current_progress = _clamp_progress(row["progress_pct"])
        new_status = current_status
        new_progress = current_progress
        if action == "pause" and current_status not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
            new_status = EXEC_STATUS_PAUSED
        elif action == "resume" and current_status != EXEC_STATUS_COMPLETED:
            new_status = EXEC_STATUS_RUNNING
            if new_progress <= 0:
                new_progress = 5
        elif action == "stop":
            new_status = EXEC_STATUS_STOPPED
    
        state = _set_project_execution_state(project_id, status=new_status, progress_pct=new_progress)
        _refresh_project_documents(project_id)
    
        primary_agent_id = None
        for r in role_rows:
            if bool(r.get("is_primary")):
                primary_agent_id = str(r.get("agent_id") or "").strip() or None
                break
        if not primary_agent_id:
            primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    
        control_summary = f"Execution action `{action}` accepted."
        if primary_agent_id:
            command_text = {
                "pause": "Owner pressed PAUSE. Pause current execution and wait for resume instruction.",
                "resume": "Owner pressed RESUME. Continue execution from latest checkpoint.",
                "stop": "Owner pressed STOP. Stop all ongoing execution immediately and report final status.",
            }[action]
            instruction = _project_context_instruction(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=_parse_setup_json(row["setup_json"]),
                role_rows=role_rows,
                plan_status=_coerce_plan_status(row["plan_status"]),
            )
            scoped_message = _compose_guardrailed_message(
                command_text,
                workspace_root=str(row["workspace_root"] or _user_workspace_root_dir(user_id).as_posix()),
                project_root=str(row["project_root"] or ""),
                task_instruction=instruction,
            )
            ctrl_res = await openclaw_ws_chat(
                base_url=str(row["base_url"]),
                api_key=str(row["api_key"]),
                message=scoped_message,
                agent_id=primary_agent_id,
                session_key=f"{project_id}:control",
                timeout_sec=25,
            )
            if ctrl_res.get("ok"):
                control_summary = str(ctrl_res.get("text") or control_summary)[:1000]
                ptk, ctk, _ = _extract_usage_counts(ctrl_res)
                if ptk <= 0:
                    ptk = _estimate_tokens_from_text(scoped_message)
                if ctk <= 0:
                    ctk = _estimate_tokens_from_text(ctrl_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=ptk, completion_tokens=ctk)
                _refresh_project_documents(project_id)
            else:
                control_summary = f"Action saved, but agent ack failed: {detail_to_text(ctrl_res.get('error') or ctrl_res.get('details'))[:700]}"
    
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=str(row["project_root"] or ""),
            kind=f"execution.{action}",
            text=control_summary,
            payload={"status": new_status, "progress_pct": new_progress},
        )
        await emit(
            project_id,
            f"project.execution.{action}",
            {"status": new_status, "progress_pct": new_progress, "summary": control_summary},
        )
        final = state or {
            "status": new_status,
            "progress_pct": new_progress,
            "updated_at": int(time.time()),
        }
        return ProjectExecutionOut(
            project_id=project_id,
            status=_coerce_execution_status(final.get("status")),
            progress_pct=_clamp_progress(final.get("progress_pct")),
            updated_at=final.get("updated_at"),
        )
    
    @app.post("/api/projects/{project_id}/agents")
    async def set_project_agents(request: Request, project_id: str, payload: ProjectAgentsIn):
        user_id = get_session_user(request)
        if len(payload.agent_ids) != len(payload.agent_names):
            raise HTTPException(400, "agent_ids and agent_names must have same length")
        role_values = payload.agent_roles or []
        if role_values and len(role_values) != len(payload.agent_ids):
            raise HTTPException(400, "agent_roles must have same length as agent_ids when provided")
        if not payload.agent_ids:
            raise HTTPException(400, "Select at least one agent")
        if len(set(payload.agent_ids)) != len(payload.agent_ids):
            raise HTTPException(400, "agent_ids must be unique")
        if payload.primary_agent_id and payload.primary_agent_id not in payload.agent_ids:
            raise HTTPException(400, "primary_agent_id must be one of selected agent_ids")
    
        conn = db()
        proj = conn.execute(
            "SELECT id, project_root, title, brief, goal, setup_json, plan_text, plan_status FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")
    
        conn.execute("DELETE FROM project_agents WHERE project_id = ?", (project_id,))
        conn.execute("DELETE FROM project_agent_access_tokens WHERE project_id = ?", (project_id,))
        primary_id = payload.primary_agent_id or payload.agent_ids[0]
        now = int(time.time())
        issued_tokens: List[Dict[str, Any]] = []
        role_map: Dict[str, str] = {}
        for idx, (aid, name) in enumerate(zip(payload.agent_ids, payload.agent_names)):
            role = str(role_values[idx]).strip()[:500] if idx < len(role_values) else ""
            role_map[aid] = role
            conn.execute(
                "INSERT INTO project_agents (project_id, agent_id, agent_name, is_primary, role) VALUES (?,?,?,?,?)",
                (project_id, aid, name, 1 if aid == primary_id else 0, role),
            )
            raw_token = _new_agent_access_token()
            conn.execute(
                "INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, created_at) VALUES (?,?,?,?)",
                (project_id, aid, _hash_access_token(raw_token), now),
            )
            issued_tokens.append(
                {
                    "agent_id": aid,
                    "agent_name": name,
                    "token": raw_token,
                    "is_primary": aid == primary_id,
                    "role": role,
                }
            )
        conn.commit()
        conn.close()
    
        _write_project_agent_roles_file(
            owner_user_id=user_id,
            project_root=str(proj["project_root"] or ""),
            agents=[
                {
                    "agent_id": aid,
                    "agent_name": name,
                    "role": role_map.get(aid, ""),
                    "is_primary": aid == primary_id,
                }
                for aid, name in zip(payload.agent_ids, payload.agent_names)
            ],
        )
        _refresh_project_documents(project_id)
    
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=str(proj["project_root"] or ""),
            kind="agents.updated",
            text=f"Invited agents updated. Primary agent: {primary_id}.",
            payload={"count": len(payload.agent_ids)},
        )
        await emit(project_id, "project.agents_set", {"count": len(payload.agent_ids), "primary_agent_id": primary_id})
        asyncio.create_task(_generate_project_plan(project_id, force=True))
        await emit(project_id, "project.plan.regenerate_requested", {"project_id": project_id, "source": "agents_set"})
        return {"ok": True, "primary_agent_id": primary_id, "agent_access_tokens": issued_tokens}
    
    @app.get("/api/projects/{project_id}/agents")
    async def get_project_agents(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")
        rows = conn.execute(
            "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.close()
        agents = [
            {
                "id": r["agent_id"],
                "name": r["agent_name"],
                "is_primary": bool(r["is_primary"]),
                "role": str(r["role"] or ""),
            }
            for r in rows
        ]
        primary = next((a for a in agents if a["is_primary"]), None)
        return {"ok": True, "agents": agents, "primary_agent": primary}

    @app.get("/api/projects/{project_id}/readiness", response_model=ProjectReadinessOut)
    async def get_project_readiness(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id, user_id, project_root, plan_status FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not proj:
            raise HTTPException(404, "Project not found")

        readiness = _project_readiness_snapshot(
            owner_user_id=str(proj["user_id"]),
            project_id=project_id,
            project_root=str(proj["project_root"] or ""),
            plan_status=proj["plan_status"],
        )
        return ProjectReadinessOut(**readiness)
    

    @app.post("/api/projects/{project_id}/run")
    async def run_project(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            """
            SELECT id, user_id, title, brief, goal, plan_status, project_root, execution_status, progress_pct
            FROM projects WHERE id = ? AND user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        agents = conn.execute(
            "SELECT agent_id, agent_name, is_primary FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.close()
        if not proj:
            raise HTTPException(404, "Project not found")

        role_rows = [dict(a) for a in agents]
        readiness = _project_readiness_snapshot(
            owner_user_id=str(proj["user_id"]),
            project_id=project_id,
            project_root=str(proj["project_root"] or ""),
            plan_status=proj["plan_status"],
            role_rows=role_rows,
        )
        if not bool(readiness.get("can_run")):
            raise HTTPException(
                400,
                {
                    "message": str(readiness.get("summary") or "Project is not ready to run."),
                    "readiness": readiness,
                },
            )
    
        current_progress = _clamp_progress(proj["progress_pct"])
        start_progress = max(10, current_progress)
        _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=start_progress)
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(proj["user_id"]),
            project_root=str(proj["project_root"] or ""),
            kind="run.started",
            text="User started project execution run.",
            payload={"agents": len(agents)},
        )
        asyncio.create_task(simulate_run(project_id, dict(proj), [dict(a) for a in agents]))
        return {"ok": True}
    

    @app.get("/api/projects/{project_id}/events")
    async def project_events(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        conn.close()
        if not proj:
            raise HTTPException(404, "Project not found")
    
        q = get_queue(project_id)
    
        async def event_generator():
            yield "event: hello\ndata: {}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=10)
                    payload = {"ts": ev.ts, "kind": ev.kind, "data": ev.data}
                    yield f"event: {ev.kind}\ndata: {json.dumps(payload)}\n\n"
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
    
        return StreamingResponse(event_generator(), media_type="text/event-stream")
