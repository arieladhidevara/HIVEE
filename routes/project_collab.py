from hivee_shared import *


def _normalize_channel_name(raw_value: Any) -> str:
    text = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(raw_value or "").strip().lower())
    text = text.strip("-_")
    return text[:80]


def _require_project_row_with_membership(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    user_id: str,
) -> Tuple[sqlite3.Row, str]:
    row = conn.execute(
        """
        SELECT id, user_id, owner_user_id, title, goal, project_root, workspace_root, connection_id,
               plan_status, execution_status, status, created_at, updated_at, setup_json,
               plan_text, plan_updated_at, plan_approved_at,
               progress_pct, execution_updated_at,
               usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
               brief, created_via, project_api_key_hash
        FROM projects
        WHERE id = ?
        LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Project not found")

    owner_user_id = str(row["owner_user_id"] or row["user_id"] or "").strip()
    if owner_user_id == user_id:
        return row, "owner"

    membership = conn.execute(
        """
        SELECT role
        FROM project_memberships
        WHERE project_id = ? AND user_id = ?
        LIMIT 1
        """,
        (project_id, user_id),
    ).fetchone()
    if membership:
        role = str(membership["role"] or "member").strip().lower() or "member"
        return row, role

    raise HTTPException(404, "Project not found")


def _ensure_default_project_channels(conn: sqlite3.Connection, project_id: str) -> None:
    now = int(time.time())
    for channel_name, channel_desc in DEFAULT_PROJECT_CHANNELS:
        conn.execute(
            """
            INSERT OR IGNORE INTO project_channels (id, project_id, name, description, created_at)
            VALUES (?,?,?,?,?)
            """,
            (new_id("pch"), project_id, channel_name, channel_desc, now),
        )
    conn.execute(
        """
        INSERT OR IGNORE INTO channel_memory (channel_id, summary_md, state_json, updated_at)
        SELECT pc.id, '', '{}', COALESCE(pc.created_at, ?)
        FROM project_channels pc
        WHERE pc.project_id = ?
        """,
        (now, project_id),
    )


def _require_project_channel(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    channel_id: Optional[str] = None,
    channel_name: Optional[str] = None,
) -> sqlite3.Row:
    if channel_id:
        row = conn.execute(
            """
            SELECT id, project_id, name, description, created_at
            FROM project_channels
            WHERE id = ? AND project_id = ?
            LIMIT 1
            """,
            (channel_id, project_id),
        ).fetchone()
        if row:
            return row
        raise HTTPException(404, "Channel not found")

    desired = _normalize_channel_name(channel_name or PROJECT_CHANNEL_MAIN)
    if not desired:
        desired = PROJECT_CHANNEL_MAIN
    row = conn.execute(
        """
        SELECT id, project_id, name, description, created_at
        FROM project_channels
        WHERE project_id = ? AND name = ?
        LIMIT 1
        """,
        (project_id, desired),
    ).fetchone()
    if row:
        return row

    now = int(time.time())
    cid = new_id("pch")
    conn.execute(
        """
        INSERT INTO project_channels (id, project_id, name, description, created_at)
        VALUES (?,?,?,?,?)
        """,
        (cid, project_id, desired, "", now),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO channel_memory (channel_id, summary_md, state_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (cid, "", "{}", now),
    )
    row = conn.execute(
        """
        SELECT id, project_id, name, description, created_at
        FROM project_channels
        WHERE id = ?
        LIMIT 1
        """,
        (cid,),
    ).fetchone()
    if not row:
        raise HTTPException(500, "Channel creation failed")
    return row


def _ensure_runtime_session_lane(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    channel_id: Optional[str],
    channel_name: str,
    task_id: Optional[str],
    managed_agent_id: str,
    connection_id: Optional[str],
) -> sqlite3.Row:
    if task_id:
        runtime_session_key = f"project:{project_id}:task:{task_id}:agent:{managed_agent_id}"
    else:
        runtime_session_key = f"project:{project_id}:channel:{channel_name}:agent:{managed_agent_id}"

    row = conn.execute(
        """
        SELECT id, project_id, channel_id, task_id, managed_agent_id, connection_id,
               runtime_session_key, summary_md, last_message_id, updated_at
        FROM runtime_sessions
        WHERE runtime_session_key = ?
        LIMIT 1
        """,
        (runtime_session_key,),
    ).fetchone()
    if row:
        return row

    now = int(time.time())
    rid = new_id("rs")
    conn.execute(
        """
        INSERT INTO runtime_sessions (
            id, project_id, channel_id, task_id, managed_agent_id, connection_id,
            runtime_session_key, summary_md, last_message_id, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rid,
            project_id,
            channel_id,
            task_id,
            managed_agent_id,
            connection_id,
            runtime_session_key,
            "",
            None,
            now,
        ),
    )
    row = conn.execute(
        """
        SELECT id, project_id, channel_id, task_id, managed_agent_id, connection_id,
               runtime_session_key, summary_md, last_message_id, updated_at
        FROM runtime_sessions
        WHERE id = ?
        LIMIT 1
        """,
        (rid,),
    ).fetchone()
    if not row:
        raise HTTPException(500, "Runtime session lane creation failed")
    return row


def _project_message_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "project_id": str(row["project_id"]),
        "channel_id": str(row["channel_id"]),
        "sender_type": str(row["sender_type"]),
        "sender_user_id": str(row["sender_user_id"] or "") or None,
        "sender_agent_membership_id": str(row["sender_agent_membership_id"] or "") or None,
        "message_kind": str(row["message_kind"] or PROJECT_MESSAGE_KIND_CHAT),
        "body": str(row["body"] or ""),
        "task_id": str(row["task_id"] or "") or None,
        "metadata": _parse_setup_json(row["metadata_json"]),
        "created_at": _to_int(row["created_at"]),
    }


def _update_project_memory_roster(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    managed_agent_id: str,
    runtime_agent_id: str,
    agent_name: str,
    user_id: str,
    role: str,
) -> None:
    now = int(time.time())
    mem = conn.execute(
        """
        SELECT project_id, summary_md, state_json, task_map_json, policy_json, updated_at
        FROM project_memory
        WHERE project_id = ?
        LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    summary_md = str((mem["summary_md"] if mem else "") or "")
    state_json = _parse_setup_json(mem["state_json"] if mem else "{}")
    task_map_json = _parse_setup_json(mem["task_map_json"] if mem else "{}")
    policy_json = _parse_setup_json(mem["policy_json"] if mem else "{}")

    roster = state_json.get("agent_roster") if isinstance(state_json.get("agent_roster"), list) else []
    existing = {
        str(item.get("managed_agent_id") or "").strip(): dict(item)
        for item in roster
        if isinstance(item, dict) and str(item.get("managed_agent_id") or "").strip()
    }
    existing[managed_agent_id] = {
        "managed_agent_id": managed_agent_id,
        "runtime_agent_id": runtime_agent_id,
        "agent_name": agent_name,
        "joined_by_user_id": user_id,
        "role": role,
        "updated_at": now,
    }
    state_json["agent_roster"] = list(existing.values())

    if "## Agent Roster" not in summary_md:
        summary_md = (summary_md.strip() + "\n\n## Agent Roster\n\n") if summary_md.strip() else "## Agent Roster\n\n"
    roster_line = f"- {agent_name} (`{runtime_agent_id}`) joined as `{role}`"
    if roster_line not in summary_md:
        summary_md = (summary_md.rstrip() + "\n" + roster_line).strip() + "\n"

    conn.execute(
        """
        INSERT OR REPLACE INTO project_memory (
            project_id, summary_md, state_json, task_map_json, policy_json, updated_at
        ) VALUES (?,?,?,?,?,?)
        """,
        (
            project_id,
            summary_md,
            json.dumps(state_json, ensure_ascii=False),
            json.dumps(task_map_json, ensure_ascii=False),
            json.dumps(policy_json, ensure_ascii=False),
            now,
        ),
    )


def _hydrate_runtime_prompt(
    *,
    project_row: sqlite3.Row,
    channel_row: sqlite3.Row,
    managed_agent_row: sqlite3.Row,
    project_memory_row: Optional[sqlite3.Row],
    channel_memory_row: Optional[sqlite3.Row],
    recent_messages: List[sqlite3.Row],
    user_message: str,
    task_id: Optional[str],
) -> str:
    title = str(project_row["title"] or "")
    goal = str(project_row["goal"] or "")
    brief = str(project_row["brief"] or "")
    channel_name = str(channel_row["name"] or PROJECT_CHANNEL_MAIN)
    agent_name = str(managed_agent_row["agent_name"] or managed_agent_row["runtime_agent_id"] or "agent")
    runtime_agent_id = str(managed_agent_row["runtime_agent_id"] or managed_agent_row["agent_id"] or "agent")

    card_json = _parse_setup_json(managed_agent_row["agent_card_json"])
    project_summary = str((project_memory_row["summary_md"] if project_memory_row else "") or "").strip()
    channel_summary = str((channel_memory_row["summary_md"] if channel_memory_row else "") or "").strip()

    recent_lines: List[str] = []
    for row in recent_messages[-8:]:
        sender = str(row["sender_type"] or "user")
        body = str(row["body"] or "").strip()
        if not body:
            continue
        recent_lines.append(f"- {sender}: {body[:500]}")

    parts: List[str] = [
        "You are operating as a Hivee managed runtime agent inside a project channel.",
        f"Project: {title}",
        f"Goal: {goal}",
    ]
    if brief:
        parts.append(f"Brief: {brief}")
    parts.extend(
        [
            f"Channel: {channel_name}",
            f"Managed Agent: {agent_name} ({runtime_agent_id})",
            f"Task Context: {task_id or 'none'}",
        ]
    )

    if project_summary:
        parts.append("Project Memory Summary:\n" + project_summary[:4000])
    if channel_summary:
        parts.append("Channel Memory Summary:\n" + channel_summary[:3000])
    if card_json:
        parts.append("Agent Card JSON:\n" + json.dumps(card_json, ensure_ascii=False)[:5000])
    if recent_lines:
        parts.append("Recent Channel Messages:\n" + "\n".join(recent_lines))

    parts.append("User Message:\n" + user_message.strip())
    parts.append(
        "Respond directly to the user request. Keep output concise and actionable. "
        "If artifacts are needed, explain what you would create next."
    )

    return "\n\n".join([p for p in parts if str(p or "").strip()])


async def _dispatch_project_channel_message(
    *,
    conn: sqlite3.Connection,
    project_row: sqlite3.Row,
    channel_row: sqlite3.Row,
    user_id: str,
    payload: ProjectChannelMessageCreateIn,
) -> Dict[str, Any]:
    now = int(time.time())
    body = str(payload.body or "").strip()
    if not body:
        raise HTTPException(400, "body is required")

    message_id = new_id("msg")
    metadata_json = json.dumps(payload.metadata or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO project_messages (
            id, project_id, channel_id, sender_type, sender_user_id,
            sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message_id,
            str(project_row["id"]),
            str(channel_row["id"]),
            "user",
            user_id,
            None,
            PROJECT_MESSAGE_KIND_CHAT,
            body,
            str(payload.task_id or "").strip() or None,
            metadata_json,
            now,
        ),
    )

    target_managed_agent_id = str(payload.target_managed_agent_id or "").strip()
    if target_managed_agent_id:
        pam_row = conn.execute(
            """
            SELECT pam.id AS project_agent_membership_id,
                   pam.project_id,
                   pam.managed_agent_id,
                   pam.connection_id,
                   pam.user_id,
                   pam.role,
                   pam.is_primary,
                   ma.user_id AS managed_user_id,
                   ma.connection_id AS managed_connection_id,
                   COALESCE(NULLIF(ma.runtime_agent_id, ''), ma.agent_id) AS runtime_agent_id,
                   ma.agent_id,
                   ma.agent_name,
                   ma.agent_card_json,
                   ma.status
            FROM project_agent_memberships pam
            JOIN managed_agents ma ON ma.id = pam.managed_agent_id
            WHERE pam.project_id = ?
              AND pam.managed_agent_id = ?
              AND pam.status = 'active'
            LIMIT 1
            """,
            (str(project_row["id"]), target_managed_agent_id),
        ).fetchone()
    else:
        pam_row = conn.execute(
            """
            SELECT pam.id AS project_agent_membership_id,
                   pam.project_id,
                   pam.managed_agent_id,
                   pam.connection_id,
                   pam.user_id,
                   pam.role,
                   pam.is_primary,
                   ma.user_id AS managed_user_id,
                   ma.connection_id AS managed_connection_id,
                   COALESCE(NULLIF(ma.runtime_agent_id, ''), ma.agent_id) AS runtime_agent_id,
                   ma.agent_id,
                   ma.agent_name,
                   ma.agent_card_json,
                   ma.status
            FROM project_agent_memberships pam
            JOIN managed_agents ma ON ma.id = pam.managed_agent_id
            WHERE pam.project_id = ?
              AND pam.status = 'active'
            ORDER BY pam.is_primary DESC, pam.updated_at DESC, pam.joined_at DESC
            LIMIT 1
            """,
            (str(project_row["id"]),),
        ).fetchone()
    if not pam_row:
        conn.execute(
            """
            INSERT INTO project_messages (
                id, project_id, channel_id, sender_type, sender_user_id,
                sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                new_id("msg"),
                str(project_row["id"]),
                str(channel_row["id"]),
                "system",
                user_id,
                None,
                PROJECT_MESSAGE_KIND_EVENT,
                "No attached project agent is currently available. Attach a managed agent to continue.",
                str(payload.task_id or "").strip() or None,
                json.dumps({"event": "project.agent.required"}, ensure_ascii=False),
                now,
            ),
        )
        conn.execute(
            "UPDATE channel_memory SET updated_at = ? WHERE channel_id = ?",
            (now, str(channel_row["id"])),
        )
        return {
            "ok": True,
            "project_id": str(project_row["id"]),
            "channel_id": str(channel_row["id"]),
            "dispatched": False,
            "message": "No project agent attached",
        }

    managed_agent_id = str(pam_row["managed_agent_id"])
    runtime_agent_id = str(pam_row["runtime_agent_id"] or pam_row["agent_id"] or "").strip()
    if not runtime_agent_id:
        runtime_agent_id = managed_agent_id
    connection_id = str(pam_row["connection_id"] or pam_row["managed_connection_id"] or "").strip() or None

    lane = _ensure_runtime_session_lane(
        conn,
        project_id=str(project_row["id"]),
        channel_id=str(channel_row["id"]),
        channel_name=str(channel_row["name"] or PROJECT_CHANNEL_MAIN),
        task_id=(str(payload.task_id or "").strip() or None),
        managed_agent_id=managed_agent_id,
        connection_id=connection_id,
    )

    project_memory_row = conn.execute(
        """
        SELECT project_id, summary_md, state_json, task_map_json, policy_json, updated_at
        FROM project_memory
        WHERE project_id = ?
        LIMIT 1
        """,
        (str(project_row["id"]),),
    ).fetchone()
    channel_memory_row = conn.execute(
        """
        SELECT channel_id, summary_md, state_json, updated_at
        FROM channel_memory
        WHERE channel_id = ?
        LIMIT 1
        """,
        (str(channel_row["id"]),),
    ).fetchone()
    recent_messages = conn.execute(
        """
        SELECT id, sender_type, body, created_at
        FROM project_messages
        WHERE project_id = ? AND channel_id = ?
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (str(project_row["id"]), str(channel_row["id"])),
    ).fetchall()

    hydrated_prompt = _hydrate_runtime_prompt(
        project_row=project_row,
        channel_row=channel_row,
        managed_agent_row=pam_row,
        project_memory_row=project_memory_row,
        channel_memory_row=channel_memory_row,
        recent_messages=recent_messages,
        user_message=body,
        task_id=(str(payload.task_id or "").strip() or None),
    )

    dispatch_ok = False
    dispatch_queued = False
    dispatch_error: Optional[str] = None
    dispatch_job_id: Optional[str] = None
    reply_text = ""

    if connection_id:
        c_row = conn.execute(
            """
            SELECT id, user_id, legacy_openclaw_connection_id, install_token_hash, hub_status
            FROM connections
            WHERE id = ?
            LIMIT 1
            """,
            (connection_id,),
        ).fetchone()
        if not c_row:
            dispatch_error = "Connection not found for selected managed agent"
        else:
            has_hub_install_token = bool(str(c_row["install_token_hash"] or "").strip())
            hub_status = str(c_row["hub_status"] or "").strip().lower()
            if has_hub_install_token:
                now_dispatch = int(time.time())
                dispatch_job_id = new_id("rjob")
                conn.execute(
                    """
                    INSERT INTO runtime_dispatch_jobs (
                        id, connection_id, project_id, channel_id, task_id,
                        managed_agent_id, project_agent_membership_id, runtime_agent_id,
                        runtime_session_key, prompt_text, status,
                        created_at, claimed_at, completed_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        dispatch_job_id,
                        connection_id,
                        str(project_row["id"]),
                        str(channel_row["id"]),
                        str(payload.task_id or "").strip() or None,
                        managed_agent_id,
                        str(pam_row["project_agent_membership_id"]),
                        runtime_agent_id,
                        str(lane["runtime_session_key"]),
                        hydrated_prompt,
                        RUNTIME_DISPATCH_STATUS_PENDING,
                        now_dispatch,
                        None,
                        None,
                        now_dispatch,
                    ),
                )
                conn.execute(
                    "UPDATE runtime_sessions SET updated_at = ? WHERE id = ?",
                    (now_dispatch, str(lane["id"])),
                )
                dispatch_queued = True
                dispatch_error = (
                    "Queued to Hivee Hub runtime lane."
                    if hub_status == HUB_STATUS_ONLINE
                    else "Queued to Hivee Hub. Waiting for hub heartbeat."
                )
            else:
                legacy_connection_id = str(c_row["legacy_openclaw_connection_id"] or "").strip() or connection_id
                oc_row = conn.execute(
                    """
                    SELECT id, user_id, base_url, api_key, api_key_secret_id
                    FROM openclaw_connections
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (legacy_connection_id,),
                ).fetchone()
                if oc_row:
                    adapter_user_id = str(oc_row["user_id"] or pam_row["managed_user_id"] or user_id).strip()
                    api_key = _resolve_connection_api_key_from_row(conn, user_id=adapter_user_id, row=oc_row)
                    res = await openclaw_ws_chat(
                        base_url=str(oc_row["base_url"]),
                        api_key=api_key,
                        message=hydrated_prompt,
                        agent_id=runtime_agent_id,
                        session_key=str(lane["runtime_session_key"]),
                        timeout_sec=25,
                    )
                    if res.get("ok"):
                        dispatch_ok = True
                        reply_text = str(res.get("text") or "").strip()
                        if not reply_text:
                            reply_text = "Acknowledged."
                    else:
                        dispatch_error = detail_to_text(res.get("error") or res.get("details") or "runtime dispatch failed")[:1000]
                else:
                    dispatch_error = "No OpenClaw adapter row available yet for this connection"
    else:
        dispatch_error = "No connection is attached for the selected managed agent"

    if dispatch_ok:
        reply_message_id = new_id("msg")
        conn.execute(
            """
            INSERT INTO project_messages (
                id, project_id, channel_id, sender_type, sender_user_id,
                sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                reply_message_id,
                str(project_row["id"]),
                str(channel_row["id"]),
                "agent",
                None,
                str(pam_row["project_agent_membership_id"]),
                PROJECT_MESSAGE_KIND_CHAT,
                reply_text,
                str(payload.task_id or "").strip() or None,
                json.dumps(
                    {
                        "managed_agent_id": managed_agent_id,
                        "runtime_agent_id": runtime_agent_id,
                        "runtime_session_key": str(lane["runtime_session_key"]),
                    },
                    ensure_ascii=False,
                ),
                int(time.time()),
            ),
        )
        conn.execute(
            """
            UPDATE runtime_sessions
            SET last_message_id = ?, updated_at = ?, summary_md = COALESCE(NULLIF(summary_md, ''), ?)
            WHERE id = ?
            """,
            (
                reply_message_id,
                int(time.time()),
                f"Active lane for {runtime_agent_id} in #{str(channel_row['name'] or PROJECT_CHANNEL_MAIN)}",
                str(lane["id"]),
            ),
        )
    elif (not dispatch_queued):
        conn.execute(
            """
            INSERT INTO project_messages (
                id, project_id, channel_id, sender_type, sender_user_id,
                sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                new_id("msg"),
                str(project_row["id"]),
                str(channel_row["id"]),
                "system",
                user_id,
                None,
                PROJECT_MESSAGE_KIND_EVENT,
                "Message recorded. Runtime dispatch is pending or unavailable.",
                str(payload.task_id or "").strip() or None,
                json.dumps(
                    {
                        "event": "runtime.dispatch.pending",
                        "managed_agent_id": managed_agent_id,
                        "runtime_agent_id": runtime_agent_id,
                        "runtime_session_key": str(lane["runtime_session_key"]),
                        "reason": dispatch_error,
                    },
                    ensure_ascii=False,
                ),
                int(time.time()),
            ),
        )

    conn.execute(
        "UPDATE channel_memory SET updated_at = ? WHERE channel_id = ?",
        (int(time.time()), str(channel_row["id"])),
    )

    return {
        "ok": True,
        "project_id": str(project_row["id"]),
        "channel_id": str(channel_row["id"]),
        "runtime_session_key": str(lane["runtime_session_key"]),
        "target_managed_agent_id": managed_agent_id,
        "target_runtime_agent_id": runtime_agent_id,
        "dispatched": dispatch_ok,
        "queued": dispatch_queued,
        "dispatch_job_id": dispatch_job_id,
        "dispatch_error": dispatch_error,
    }


def register_routes(app: FastAPI) -> None:
    @app.post("/api/projects/join", response_model=ProjectOut)
    async def join_project_by_api_key(request: Request, payload: ProjectJoinByKeyIn):
        user_id = get_session_user(request)
        raw_key = str(payload.project_api_key or "").strip()
        if not raw_key:
            raise HTTPException(400, "project_api_key is required")

        key_hash = _hash_access_token(raw_key)
        conn = db()
        row = conn.execute(
            """
            SELECT id, title, brief, goal, connection_id, created_at, workspace_root, project_root, setup_json,
                   plan_text, plan_status, plan_updated_at, plan_approved_at,
                   execution_status, progress_pct, execution_updated_at,
                   usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
                   status, created_via, owner_user_id, user_id, updated_at, project_api_key_hash
            FROM projects
            WHERE project_api_key_hash = ?
            LIMIT 1
            """,
            (key_hash,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Project not found for provided API key")

        now = int(time.time())
        conn.execute(
            """
            INSERT OR IGNORE INTO project_memberships (id, project_id, user_id, role, joined_at)
            VALUES (?,?,?,?,?)
            """,
            (new_id("pm"), str(row["id"]), user_id, "member", now),
        )
        conn.commit()
        conn.close()

        await emit(str(row["id"]), "project.member.joined", {"user_id": user_id, "role": "member"})
        return _project_out_from_row(row)

    @app.get("/api/projects/{project_id}/members")
    async def list_project_members(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        _project, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        rows = conn.execute(
            """
            SELECT id, project_id, user_id, role, joined_at
            FROM project_memberships
            WHERE project_id = ?
            ORDER BY CASE WHEN role = 'owner' THEN 0 ELSE 1 END, joined_at ASC
            """,
            (project_id,),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "project_id": project_id,
            "members": [
                {
                    "id": str(r["id"]),
                    "project_id": str(r["project_id"]),
                    "user_id": str(r["user_id"]),
                    "role": str(r["role"] or "member"),
                    "joined_at": _to_int(r["joined_at"]),
                }
                for r in rows
            ],
        }

    @app.post("/api/projects/{project_id}/agents/attach")
    async def attach_managed_agent_to_project(request: Request, project_id: str, payload: ProjectAttachManagedAgentIn):
        user_id = get_session_user(request)
        managed_agent_id = str(payload.managed_agent_id or "").strip()
        if not managed_agent_id:
            raise HTTPException(400, "managed_agent_id is required")

        conn = db()
        project_row, membership_role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        owner_user_id = str(project_row["owner_user_id"] or project_row["user_id"] or "").strip() or user_id
        ma = conn.execute(
            """
            SELECT id, user_id, connection_id,
                   COALESCE(NULLIF(runtime_agent_id, ''), agent_id) AS runtime_agent_id,
                   agent_id, agent_name, status
            FROM managed_agents
            WHERE id = ? AND user_id = ?
            LIMIT 1
            """,
            (managed_agent_id, user_id),
        ).fetchone()
        if not ma:
            conn.close()
            raise HTTPException(404, "Managed agent not found")

        now = int(time.time())
        role_value = str(payload.role or "member").strip()[:120] or "member"
        is_primary = 1 if bool(payload.is_primary) else 0

        if is_primary:
            conn.execute(
                "UPDATE project_agent_memberships SET is_primary = 0, updated_at = ? WHERE project_id = ?",
                (now, project_id),
            )
            conn.execute(
                "UPDATE project_agents SET is_primary = 0 WHERE project_id = ?",
                (project_id,),
            )

        conn.execute(
            """
            INSERT INTO project_agent_memberships (
                id, project_id, managed_agent_id, connection_id, user_id, role,
                is_primary, status, joined_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(project_id, managed_agent_id) DO UPDATE SET
                connection_id = excluded.connection_id,
                user_id = excluded.user_id,
                role = excluded.role,
                is_primary = excluded.is_primary,
                status = 'active',
                updated_at = excluded.updated_at
            """,
            (
                new_id("pam"),
                project_id,
                managed_agent_id,
                str(ma["connection_id"] or "") or None,
                user_id,
                role_value,
                is_primary,
                "active",
                now,
                now,
            ),
        )

        runtime_agent_id = str(ma["runtime_agent_id"] or ma["agent_id"] or managed_agent_id).strip() or managed_agent_id
        agent_name = str(ma["agent_name"] or runtime_agent_id).strip() or runtime_agent_id
        source_type = "owner" if membership_role == "owner" else "external"

        conn.execute(
            """
            INSERT INTO project_agents (
                project_id, agent_id, agent_name, is_primary, role,
                source_type, source_user_id, source_connection_id, joined_via_invite_id, added_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(project_id, agent_id) DO UPDATE SET
                agent_name = excluded.agent_name,
                is_primary = excluded.is_primary,
                role = excluded.role,
                source_type = excluded.source_type,
                source_user_id = excluded.source_user_id,
                source_connection_id = excluded.source_connection_id,
                added_at = excluded.added_at
            """,
            (
                project_id,
                runtime_agent_id,
                agent_name,
                is_primary,
                role_value,
                source_type,
                user_id,
                str(ma["connection_id"] or "") or None,
                None,
                now,
            ),
        )

        if membership_role != "owner":
            member_connection_id = str(ma["connection_id"] or "").strip()
            if not member_connection_id:
                conn.close()
                raise HTTPException(400, "Managed agent must be backed by a connection before joining as member")
            conn.execute(
                """
                INSERT INTO project_external_agent_memberships (
                    id, project_id, owner_user_id, member_user_id, member_connection_id,
                    agent_id, agent_name, role, invite_id, status, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(project_id, member_user_id, member_connection_id, agent_id) DO UPDATE SET
                    owner_user_id = excluded.owner_user_id,
                    agent_name = excluded.agent_name,
                    role = excluded.role,
                    invite_id = excluded.invite_id,
                    status = 'active',
                    updated_at = excluded.updated_at
                """,
                (
                    new_id("pem"),
                    project_id,
                    owner_user_id,
                    user_id,
                    member_connection_id,
                    runtime_agent_id,
                    agent_name,
                    role_value,
                    None,
                    "active",
                    now,
                    now,
                ),
            )

        _update_project_memory_roster(
            conn,
            project_id=project_id,
            managed_agent_id=managed_agent_id,
            runtime_agent_id=runtime_agent_id,
            agent_name=agent_name,
            user_id=user_id,
            role=role_value,
        )

        system_channel = _require_project_channel(conn, project_id=project_id, channel_name=PROJECT_CHANNEL_SYSTEM)
        conn.execute(
            """
            INSERT INTO project_messages (
                id, project_id, channel_id, sender_type, sender_user_id,
                sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                new_id("msg"),
                project_id,
                str(system_channel["id"]),
                "system",
                user_id,
                None,
                PROJECT_MESSAGE_KIND_EVENT,
                f"Managed agent attached: {agent_name} ({runtime_agent_id})",
                None,
                json.dumps({"event": "project.agent.joined", "managed_agent_id": managed_agent_id}, ensure_ascii=False),
                now,
            ),
        )
        conn.execute("UPDATE projects SET updated_at = ? WHERE id = ?", (now, project_id))
        conn.commit()
        conn.close()

        await emit(project_id, "project.agent.joined", {
            "managed_agent_id": managed_agent_id,
            "runtime_agent_id": runtime_agent_id,
            "agent_name": agent_name,
            "user_id": user_id,
            "role": role_value,
            "is_primary": bool(is_primary),
        })
        await emit(project_id, "project.plan.refresh.requested", {
            "project_id": project_id,
            "reason": "agent_joined",
            "managed_agent_id": managed_agent_id,
        })

        return {
            "ok": True,
            "project_id": project_id,
            "managed_agent_id": managed_agent_id,
            "runtime_agent_id": runtime_agent_id,
            "agent_name": agent_name,
            "role": role_value,
            "is_primary": bool(is_primary),
        }

    @app.get("/api/projects/{project_id}/channels")
    async def list_project_channels(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        _project, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        _ensure_default_project_channels(conn, project_id)
        conn.commit()
        rows = conn.execute(
            """
            SELECT pc.id, pc.project_id, pc.name, pc.description, pc.created_at,
                   COALESCE(msg.cnt, 0) AS message_count
            FROM project_channels pc
            LEFT JOIN (
                SELECT channel_id, COUNT(1) AS cnt
                FROM project_messages
                WHERE project_id = ?
                GROUP BY channel_id
            ) msg ON msg.channel_id = pc.id
            WHERE pc.project_id = ?
            ORDER BY pc.created_at ASC
            """,
            (project_id, project_id),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "project_id": project_id,
            "channels": [
                {
                    "id": str(r["id"]),
                    "project_id": str(r["project_id"]),
                    "name": str(r["name"]),
                    "description": str(r["description"] or ""),
                    "created_at": _to_int(r["created_at"]),
                    "message_count": _to_int(r["message_count"]),
                }
                for r in rows
            ],
        }

    @app.post("/api/projects/{project_id}/channels")
    async def create_project_channel(request: Request, project_id: str, payload: ProjectChannelCreateIn):
        user_id = get_session_user(request)
        channel_name = _normalize_channel_name(payload.name)
        if not channel_name:
            raise HTTPException(400, "Channel name is required")

        conn = db()
        _project, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        now = int(time.time())
        channel_id = new_id("pch")
        conn.execute(
            """
            INSERT INTO project_channels (id, project_id, name, description, created_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(project_id, name) DO NOTHING
            """,
            (channel_id, project_id, channel_name, str(payload.description or "")[:240], now),
        )
        channel = _require_project_channel(conn, project_id=project_id, channel_name=channel_name)
        conn.execute(
            """
            INSERT OR IGNORE INTO channel_memory (channel_id, summary_md, state_json, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (str(channel["id"]), "", "{}", now),
        )
        conn.commit()
        conn.close()

        await emit(project_id, "project.channel.created", {
            "channel_id": str(channel["id"]),
            "name": str(channel["name"]),
            "created_by": user_id,
        })

        return {
            "ok": True,
            "channel": {
                "id": str(channel["id"]),
                "project_id": str(channel["project_id"]),
                "name": str(channel["name"]),
                "description": str(channel["description"] or ""),
                "created_at": _to_int(channel["created_at"]),
            },
        }

    @app.get("/api/projects/{project_id}/messages")
    async def list_project_messages(request: Request, project_id: str, channel: str = PROJECT_CHANNEL_MAIN, limit: int = 80):
        user_id = get_session_user(request)
        safe_limit = max(1, min(int(limit or 80), 250))
        conn = db()
        _project, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        channel_row = _require_project_channel(conn, project_id=project_id, channel_name=channel)
        rows = conn.execute(
            """
            SELECT id, project_id, channel_id, sender_type, sender_user_id,
                   sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            FROM project_messages
            WHERE project_id = ? AND channel_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (project_id, str(channel_row["id"]), safe_limit),
        ).fetchall()
        conn.close()
        messages = [_project_message_payload(r) for r in reversed(rows)]
        return {
            "ok": True,
            "project_id": project_id,
            "channel": {
                "id": str(channel_row["id"]),
                "name": str(channel_row["name"]),
                "description": str(channel_row["description"] or ""),
            },
            "messages": messages,
        }

    @app.post("/api/projects/{project_id}/messages")
    async def post_project_message(request: Request, project_id: str, payload: ProjectChannelMessageCreateIn, channel: str = PROJECT_CHANNEL_MAIN):
        user_id = get_session_user(request)
        conn = db()
        project_row, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        channel_row = _require_project_channel(conn, project_id=project_id, channel_name=channel)
        result = await _dispatch_project_channel_message(
            conn=conn,
            project_row=project_row,
            channel_row=channel_row,
            user_id=user_id,
            payload=payload,
        )
        conn.commit()
        conn.close()

        await emit(project_id, "project.channel.message", {
            "channel_id": str(channel_row["id"]),
            "channel": str(channel_row["name"]),
            "sender_type": "user",
            "user_id": user_id,
        })

        return result

    @app.get("/api/projects/{project_id}/channels/{channel_id}/messages")
    async def list_project_channel_messages(request: Request, project_id: str, channel_id: str, limit: int = 80):
        user_id = get_session_user(request)
        safe_limit = max(1, min(int(limit or 80), 250))
        conn = db()
        _project, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        channel_row = _require_project_channel(conn, project_id=project_id, channel_id=channel_id)
        rows = conn.execute(
            """
            SELECT id, project_id, channel_id, sender_type, sender_user_id,
                   sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
            FROM project_messages
            WHERE project_id = ? AND channel_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (project_id, channel_id, safe_limit),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "project_id": project_id,
            "channel": {
                "id": str(channel_row["id"]),
                "name": str(channel_row["name"]),
                "description": str(channel_row["description"] or ""),
            },
            "messages": [_project_message_payload(r) for r in reversed(rows)],
        }

    @app.post("/api/projects/{project_id}/channels/{channel_id}/messages")
    async def post_project_channel_message(request: Request, project_id: str, channel_id: str, payload: ProjectChannelMessageCreateIn):
        user_id = get_session_user(request)
        conn = db()
        project_row, _role = _require_project_row_with_membership(conn, project_id=project_id, user_id=user_id)
        channel_row = _require_project_channel(conn, project_id=project_id, channel_id=channel_id)
        result = await _dispatch_project_channel_message(
            conn=conn,
            project_row=project_row,
            channel_row=channel_row,
            user_id=user_id,
            payload=payload,
        )
        conn.commit()
        conn.close()
        await emit(project_id, "project.channel.message", {
            "channel_id": str(channel_row["id"]),
            "channel": str(channel_row["name"]),
            "sender_type": "user",
            "user_id": user_id,
        })
        return result


