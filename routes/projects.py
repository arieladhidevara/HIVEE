from hivee_shared import *
from email.message import EmailMessage
import smtplib
from services.managed_agents import _delegate_project_tasks, _project_chat


def _new_project_external_invite_token() -> str:
    return f"pinv_{secrets.token_urlsafe(24)}"


def _clamp_external_invite_ttl(raw_ttl: Any) -> int:
    ttl = _to_int(raw_ttl)
    if ttl <= 0:
        ttl = PROJECT_EXTERNAL_INVITE_TTL_SEC
    return max(PROJECT_EXTERNAL_INVITE_MIN_TTL_SEC, min(ttl, PROJECT_EXTERNAL_INVITE_MAX_TTL_SEC))


def _mask_email_for_public(raw_email: str) -> Optional[str]:
    normalized = _normalize_email(raw_email)
    if not normalized or "@" not in normalized:
        return None
    local, _, domain = normalized.partition("@")
    if not local:
        return f"***@{domain}" if domain else None
    if len(local) <= 2:
        masked_local = local[0] + ("*" * max(1, len(local) - 1))
    else:
        masked_local = local[0] + ("*" * (len(local) - 2)) + local[-1]
    return f"{masked_local}@{domain}" if domain else masked_local


def _new_project_invite_portal_code(length: int = 8) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    n = max(6, min(int(length or 8), 16))
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _normalize_invite_code(raw_code: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9]", "", str(raw_code or "").strip()).upper()
    return text[:32]


def _hash_invite_code(raw_code: Any) -> str:
    normalized = _normalize_invite_code(raw_code)
    return _hash_access_token(normalized) if normalized else ""


def _mask_invite_code(raw_code: Any) -> str:
    code = _normalize_invite_code(raw_code)
    if not code:
        return ""
    if len(code) <= 4:
        return (code[0] + ("*" * (len(code) - 1))) if len(code) > 1 else code
    return f"{code[:2]}{'*' * (len(code) - 4)}{code[-2:]}"


def _project_actor_from_access(access: Dict[str, Any]) -> Dict[str, Optional[str]]:
    mode = str(access.get("mode") or "").strip().lower()
    if mode == "owner":
        uid = str(access.get("user_id") or "").strip()
        return {"type": "user", "id": uid or None, "label": "owner"}
    aid = str(access.get("agent_id") or "").strip()
    if aid:
        label_prefix = "member" if mode == "member" else "agent"
        return {"type": "project_agent", "id": aid, "label": f"{label_prefix}:{aid}"}
    uid = str(access.get("user_id") or "").strip()
    return {"type": "user", "id": uid or None, "label": mode or "user"}


def _invite_email_settings() -> Dict[str, Any]:
    host = str(os.getenv("INVITE_EMAIL_SMTP_HOST") or "").strip()
    username = str(os.getenv("INVITE_EMAIL_SMTP_USERNAME") or "").strip()
    password = str(os.getenv("INVITE_EMAIL_SMTP_PASSWORD") or "")
    sender = str(os.getenv("INVITE_EMAIL_FROM") or username or "").strip()

    port_raw = str(os.getenv("INVITE_EMAIL_SMTP_PORT") or "587").strip()
    try:
        port = max(1, min(int(port_raw), 65535))
    except Exception:
        port = 587

    ssl_raw = str(os.getenv("INVITE_EMAIL_SMTP_SSL") or "").strip().lower()
    tls_raw = str(os.getenv("INVITE_EMAIL_SMTP_STARTTLS") or "1").strip().lower()
    use_ssl = ssl_raw in {"1", "true", "yes", "on"}
    use_starttls = (not use_ssl) and (tls_raw not in {"0", "false", "no", "off"})

    timeout_raw = str(os.getenv("INVITE_EMAIL_SMTP_TIMEOUT_SEC") or "12").strip()
    try:
        timeout_sec = max(3, min(int(timeout_raw), 60))
    except Exception:
        timeout_sec = 12

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "sender": sender,
        "use_ssl": use_ssl,
        "use_starttls": use_starttls,
        "timeout_sec": timeout_sec,
    }


def _send_external_invite_email(
    *,
    target_email: Optional[str],
    subject: str,
    body: str,
) -> Dict[str, Any]:
    normalized_target = _normalize_email(str(target_email or ""))
    if not normalized_target:
        return {
            "ok": False,
            "status": "skipped_no_target_email",
            "error": "target_email is empty",
            "sent_at": None,
        }

    cfg = _invite_email_settings()
    host = str(cfg.get("host") or "").strip()
    sender = str(cfg.get("sender") or "").strip()
    if not host or not sender:
        return {
            "ok": False,
            "status": "skipped_not_configured",
            "error": "Invite email SMTP is not configured",
            "sent_at": None,
        }

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = normalized_target
    msg["Subject"] = str(subject or "Project Invitation")[:220]
    msg.set_content(str(body or "").strip())

    try:
        if bool(cfg.get("use_ssl")):
            smtp_client = smtplib.SMTP_SSL(
                host,
                int(cfg.get("port") or 465),
                timeout=int(cfg.get("timeout_sec") or 12),
            )
        else:
            smtp_client = smtplib.SMTP(
                host,
                int(cfg.get("port") or 587),
                timeout=int(cfg.get("timeout_sec") or 12),
            )

        with smtp_client as smtp:
            smtp.ehlo()
            if bool(cfg.get("use_starttls")):
                smtp.starttls()
                smtp.ehlo()
            username = str(cfg.get("username") or "").strip()
            password = str(cfg.get("password") or "")
            if username:
                smtp.login(username, password)
            smtp.send_message(msg)

        sent_at = int(time.time())
        return {"ok": True, "status": "sent", "error": None, "sent_at": sent_at}
    except Exception as exc:
        return {
            "ok": False,
            "status": "failed",
            "error": detail_to_text(exc)[:1000],
            "sent_at": None,
        }


async def _compose_external_invite_email_with_primary_agent(
    *,
    base_url: str,
    api_key: str,
    main_agent_id: str,
    default_subject: str,
    default_body: str,
    target_email: Optional[str],
    project_title: str,
    role: str,
    invitation_doc_url: str,
    portal_url: str,
    invite_code: str,
) -> Dict[str, Any]:
    fallback = {
        "subject": str(default_subject or "").strip()[:220] or "You are invited to contribute to this project!",
        "body": str(default_body or "").strip()[:6000],
        "composed_by_agent": False,
        "compose_error": None,
        "sent_by_agent": False,
        "send_status": "not_attempted",
        "send_error": None,
        "send_note": None,
    }
    if not base_url or not api_key or not main_agent_id:
        fallback["send_status"] = "skipped_missing_primary_agent"
        fallback["send_error"] = "Primary agent connection is missing"
        return fallback

    prompt = (
        "You are the primary owner agent for a Hivee project. "
        "Compose and send an invitation email NOW to the target email using your available email tool/integration. "
        "If you cannot send, explain why. "
        "Return JSON only with keys: subject, body, sent, send_status, send_error, send_note. "
        "Rules: sent must be true only if email has actually been dispatched from your side. "
        "Always include subject and body in your response."
        "\n\n"
        f"Project title: {project_title or '-'}\n"
        f"Target email: {target_email or '-'}\n"
        f"Suggested role: {role or '-'}\n"
        f"Invitation doc URL: {invitation_doc_url}\n"
        f"Portal URL: {portal_url}\n"
        f"Portal code: {invite_code}\n"
    )

    try:
        res = await openclaw_ws_chat(
            base_url=base_url,
            api_key=api_key,
            message=prompt,
            agent_id=main_agent_id,
            session_key=f"external-invite-email:{_normalize_invite_code(invite_code) or 'default'}",
            timeout_sec=40,
        )
    except Exception as exc:
        fallback["compose_error"] = detail_to_text(exc)[:600]
        fallback["send_status"] = "failed_primary_agent_send"
        fallback["send_error"] = fallback["compose_error"]
        return fallback

    if not res.get("ok"):
        fallback["compose_error"] = detail_to_text(res.get("error") or res.get("details") or "compose failed")[:600]
        fallback["send_status"] = "failed_primary_agent_send"
        fallback["send_error"] = fallback["compose_error"]
        return fallback

    text = str(res.get("text") or "").strip()
    parsed = _extract_json_object(text) or {}
    subject = str(parsed.get("subject") or "").strip()[:220]
    body = str(parsed.get("body") or "").strip()[:6000]
    if not subject:
        subject = fallback["subject"]
    if not body:
        body = fallback["body"]

    sent_by_agent = _coerce_bool(parsed.get("sent"))
    send_status = str(parsed.get("send_status") or "").strip()[:120]
    send_error = str(parsed.get("send_error") or "").strip()[:1000] or None
    send_note = str(parsed.get("send_note") or parsed.get("delivery_note") or "").strip()[:1000] or None
    if not send_status:
        send_status = "sent_by_primary_agent" if sent_by_agent else "failed_primary_agent_send"

    return {
        "subject": subject,
        "body": body,
        "composed_by_agent": True,
        "compose_error": None,
        "sent_by_agent": sent_by_agent,
        "send_status": send_status,
        "send_error": send_error,
        "send_note": send_note,
        "raw_text": text[:1200],
    }

def _build_external_agent_invite_markdown(
    *,
    owner_user_id: str,
    project_id: str,
    project_title: str,
    project_goal: str,
    requested_agent_id: str,
    requested_agent_name: str,
    role: str,
    note: str,
    invite_token: str,
    accept_api_url: str,
    invitation_doc_url: str,
    portal_url: str,
    invite_code: str,
    created_at: int,
    expires_at: int,
) -> str:
    target_agent = requested_agent_id or requested_agent_name or "(choose your agent_id when accepting)"
    role_text = role or "Not specified"
    note_text = note or "-"
    code_text = _normalize_invite_code(invite_code)
    return (
        "# PROJECT-EXTERNAL-AGENT-INVITE\n\n"
        "This file is for an invited external agent to join a Hivee project.\n\n"
        f"- Project ID: {project_id}\n"
        f"- Project Title: {project_title}\n"
        f"- Owner User ID: {owner_user_id}\n"
        f"- Project Goal: {(project_goal or '-').strip()[:1000]}\n"
        f"- Requested Agent: {target_agent}\n"
        f"- Suggested Role: {role_text}\n"
        f"- Owner Note: {note_text}\n"
        f"- Invitation Doc URL: {invitation_doc_url}\n"
        f"- Portal URL: {portal_url}\n"
        f"- Portal Code: {code_text}\n"
        f"- Accept API URL: {accept_api_url}\n"
        f"- Invite Token: {invite_token}\n"
        f"- Created At: {format_ts(created_at)}\n"
        f"- Expires At: {format_ts(expires_at)}\n\n"
        "## Step 1 - Prepare OpenClaw Endpoint\n"
        "OpenClaw base URL must be publicly reachable (not localhost/private IP).\n"
        "If not public yet, set it up based on your system (Linux/macOS SSH/proxy, Windows cloudflared/ngrok/SSH, Docker/NAS public HTTPS proxy).\n\n"
        "## Step 2 - Open Portal + Login\n"
        "Open the portal URL above. Login or sign up first.\n"
        "The portal can auto-join after authentication if your account has one connection.\n\n"
        "## Step 3 - Match Portal Code\n"
        "Use this code in the portal when asked:\n"
        f"{code_text}\n\n"
        "## Manual Accept (Optional)\n"
        "If needed, call accept API with your session bearer token:\n"
        f"curl -X POST '{accept_api_url}' \\\n"
        "  -H 'Authorization: Bearer <your_hivee_session_token>' \\\n"
        "  -H 'Content-Type: application/json' \\\n"
        f"  -d '{{\"connection_id\":\"<your_connection_id>\",\"agent_id\":\"<your_agent_id>\",\"invite_code\":\"{code_text}\"}}'\n\n"
        "## Result\n"
        "If accepted, your agent becomes an active member of this project.\n"
    )


def _build_external_invite_email_template(
    *,
    project_title: str,
    project_goal: str,
    invitation_doc_url: str,
    portal_url: str,
    invite_code: str,
    target_email: Optional[str],
) -> Dict[str, str]:
    subject = "You are invited to contribute to this project!"
    code_text = _normalize_invite_code(invite_code)
    body = (
        "Hello,\n\n"
        "You are invited to contribute to a Hivee project.\n\n"
        f"Project: {project_title or '-'}\n"
        f"Goal: {(project_goal or '-').strip()[:600]}\n\n"
        "1) Read invitation doc:\n"
        f"{invitation_doc_url}\n\n"
        "2) Open portal and login/signup:\n"
        f"{portal_url}\n\n"
        "3) Match portal code:\n"
        f"{code_text}\n\n"
        "If you do not have a Hivee account yet, register first, then reopen the same portal URL.\n"
        "OpenClaw base URL must be publicly reachable (not localhost/private IP). If it is local/private, set up tunnel/proxy based on your system first.\n\n"
        "Thanks."
    )
    to_value = str(target_email or "").strip()
    params = urlencode({"subject": subject, "body": body})
    mailto_url = f"mailto:{url_quote(to_value)}?{params}" if to_value else f"mailto:?{params}"
    return {
        "subject": subject,
        "body": body,
        "mailto_url": mailto_url,
    }


def _append_project_invitations_record(
    *,
    owner_user_id: str,
    project_root: str,
    project_id: str,
    project_title: str,
    invite_id: str,
    status: str,
    invitation_doc_url: str,
    portal_url: str,
    invite_code: str,
    accept_api_url: str,
    target_email: Optional[str],
    requested_agent_id: str,
    requested_agent_name: str,
    role: str,
    note: str,
    created_at: int,
    expires_at: int,
    email_subject: str,
    email_body: str,
    email_delivery_status: str,
) -> None:
    project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    doc_path = (project_dir / PROJECT_INVITATIONS_FILE).resolve()
    if not _path_within(doc_path, project_dir):
        return
    doc_path.parent.mkdir(parents=True, exist_ok=True)

    if not doc_path.exists():
        header = (
            "# Project Invitations\n\n"
            "This file collects all external invite links and email drafts for this project.\n\n"
            f"- Project ID: {project_id}\n"
            f"- Project Title: {project_title}\n\n"
            "Share invitation doc URL + portal URL with external collaborators.\n"
        )
        doc_path.write_text(header, encoding="utf-8")

    requested_agent = requested_agent_name or requested_agent_id or "(not specified)"
    text = (
        "\n\n---\n\n"
        f"## Invite {invite_id}\n"
        f"- Status: {status}\n"
        f"- Target Email: {target_email or '-'}\n"
        f"- Requested Agent: {requested_agent}\n"
        f"- Role: {role or '-'}\n"
        f"- Note: {note or '-'}\n"
        f"- Invite Code: {_normalize_invite_code(invite_code) or '-'}\n"
        f"- Email Delivery: {email_delivery_status or '-'}\n"
        f"- Created At: {format_ts(created_at)}\n"
        f"- Expires At: {format_ts(expires_at)}\n\n"
        "### Invitation Doc URL\n"
        f"{invitation_doc_url}\n\n"
        "### Portal URL\n"
        f"{portal_url}\n\n"
        "### Accept API URL\n"
        f"{accept_api_url}\n\n"
        "### Email Subject\n"
        f"{email_subject}\n\n"
        "### Email Body\n"
        f"{email_body}\n"
    )
    with doc_path.open("a", encoding="utf-8") as f:
        f.write(text)


def _append_project_invitations_status_update(
    *,
    owner_user_id: str,
    project_root: str,
    invite_id: str,
    status: str,
    ts_value: int,
    note: str = "",
) -> None:
    project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    doc_path = (project_dir / PROJECT_INVITATIONS_FILE).resolve()
    if not _path_within(doc_path, project_dir):
        return
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    if not doc_path.exists():
        doc_path.write_text("# Project Invitations\n", encoding="utf-8")
    line = f"\n- [{format_ts(ts_value)}] Invite {invite_id} status => {status}"
    if note:
        line += f" ({note})"
    with doc_path.open("a", encoding="utf-8") as f:
        f.write(line)

def register_routes(app: FastAPI) -> None:
    @app.post("/api/projects/setup-chat")
    async def project_setup_chat(request: Request, payload: ProjectSetupChatIn):
        from services.connector_dispatch import connector_chat_sync as _connector_chat_sync
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        connector_row = None
        if not row:
            connector_row = conn.execute(
                "SELECT id FROM connectors WHERE id = ? AND user_id = ?",
                (payload.connection_id, user_id),
            ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        policy = conn.execute(
            "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row and not connector_row:
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
        timeout = max(10, min(payload.timeout_sec, 45 if payload.optimize_tokens else 90))
        if connector_row:
            res = await _connector_chat_sync(
                connector_id=str(payload.connection_id),
                message=instruction,
                agent_id=effective_agent_id,
                session_key=f"project-setup:{session_key}",
                timeout_sec=timeout,
            )
        else:
            res = await openclaw_ws_chat(
                base_url=row["base_url"],
                api_key=connection_api_key,
                message=instruction,
                agent_id=effective_agent_id,
                session_key=f"project-setup:{session_key}",
                timeout_sec=timeout,
            )
        if not res.get("ok"):
            raise HTTPException(400, res)
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        res["templates_root"] = templates_root
        return res
    
    @app.post("/api/projects/setup-draft")
    async def project_setup_draft(request: Request, payload: ProjectSetupDraftIn):
        from services.connector_dispatch import connector_chat_sync as _connector_chat_sync
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        connector_row = None
        if not row:
            connector_row = conn.execute(
                "SELECT id FROM connectors WHERE id = ? AND user_id = ?",
                (payload.connection_id, user_id),
            ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row) if row else ""
        policy = conn.execute(
            "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
            (payload.connection_id, user_id),
        ).fetchone()
        conn.close()
        if not row and not connector_row:
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
        if connector_row:
            res = await _connector_chat_sync(
                connector_id=str(payload.connection_id),
                message=instruction,
                agent_id=effective_agent_id,
                session_key=f"project-setup-draft:{session_key}",
                timeout_sec=max(10, min(payload.timeout_sec, 60)),
            )
        else:
            res = await openclaw_ws_chat(
                base_url=row["base_url"],
                api_key=connection_api_key,
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
        # Also accept connector IDs as a valid connection source
        is_connector_mode = False
        connector_c = None
        if not c:
            connector_c = conn.execute(
                "SELECT id FROM connectors WHERE id = ? AND user_id = ?",
                (payload.connection_id, user_id),
            ).fetchone()
            if not connector_c:
                conn.close()
                raise HTTPException(400, "Invalid connection_id (not found for this user)")
            is_connector_mode = True

        policy_row = conn.execute(
            "SELECT main_agent_id, main_agent_name FROM connection_policies WHERE connection_id = ? AND user_id = ? LIMIT 1",
            (payload.connection_id, user_id),
        ).fetchone()
        main_agent_id = str(policy_row["main_agent_id"] or "").strip() if policy_row else ""
        main_agent_name = str(policy_row["main_agent_name"] or "").strip() if policy_row else ""
        if not main_agent_id:
            conn.close()
            raise HTTPException(
                400,
                "Primary owner agent is not configured for this connection. Run bootstrap first.",
            )
        managed_primary = conn.execute(
            """
            SELECT agent_id, agent_name
            FROM managed_agents
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            LIMIT 1
            """,
            (user_id, payload.connection_id, main_agent_id),
        ).fetchone()
        if not managed_primary:
            conn.close()
            raise HTTPException(
                400,
                "Primary owner agent is missing from managed agents. Re-run bootstrap and try again.",
            )
        main_agent_name = str(managed_primary["agent_name"] or main_agent_name or main_agent_id).strip() or main_agent_id

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
                connection_id, workspace_root, project_root, scope_requires_owner_approval, created_at,
                backend_mode, connector_id
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                "connector" if is_connector_mode else "direct_openclaw",
                payload.connection_id if is_connector_mode else None,
            ),
        )
        conn.execute(
            """
            INSERT INTO project_agents (
                project_id, agent_id, agent_name, is_primary, role,
                source_type, source_user_id, source_connection_id, joined_via_invite_id, added_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (pid, main_agent_id, main_agent_name, 1, "Primary owner agent", "owner", user_id, payload.connection_id, None, now),
        )
        initial_project_agent_token = _new_agent_access_token()
        conn.execute(
            "INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, created_at) VALUES (?,?,?,?)",
            (pid, main_agent_id, _hash_access_token(initial_project_agent_token), now),
        )
        conn.commit()
        conn.close()
    
        _write_project_agent_roles_file(
            owner_user_id=user_id,
            project_root=project_root,
            agents=[
                {
                    "agent_id": main_agent_id,
                    "agent_name": main_agent_name,
                    "role": "Primary owner agent",
                    "is_primary": True,
                }
            ],
        )
        _refresh_project_documents(pid)

        await emit(pid, "project.created", {"title": payload.title})
        await emit(pid, "project.agents_set", {"count": 1, "primary_agent_id": main_agent_id, "auto_seeded": True})
        asyncio.create_task(_generate_project_plan(pid, force=True))
        await emit(pid, "project.plan.regenerate_requested", {"project_id": pid, "source": "project_created"})
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
        owner_rows = conn.execute(
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
        member_rows = conn.execute(
            """
            SELECT p.id, p.title, p.brief, p.goal, p.connection_id, p.created_at, p.workspace_root, p.project_root, p.setup_json,
                   p.plan_text, p.plan_status, p.plan_updated_at, p.plan_approved_at,
                   p.execution_status, p.progress_pct, p.execution_updated_at,
                   p.usage_prompt_tokens, p.usage_completion_tokens, p.usage_total_tokens, p.usage_updated_at
            FROM project_external_agent_memberships pem
            JOIN projects p ON p.id = pem.project_id
            WHERE pem.member_user_id = ? AND pem.status = 'active'
            ORDER BY p.created_at DESC
            """,
            (user_id,),
        ).fetchall()
        conn.close()

        merged: Dict[str, sqlite3.Row] = {}
        for r in owner_rows:
            merged[str(r["id"])] = r
        for r in member_rows:
            pid = str(r["id"])
            if pid not in merged:
                merged[pid] = r
        ordered = sorted(merged.values(), key=lambda r: _to_int(r["created_at"]), reverse=True)
        return [_project_out_from_row(r) for r in ordered]

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
            LIMIT 1
            """,
            (project_id, user_id),
        ).fetchone()
        if not row:
            row = conn.execute(
                """
                SELECT p.id, p.title, p.brief, p.goal, p.connection_id, p.created_at, p.workspace_root, p.project_root, p.setup_json,
                       p.plan_text, p.plan_status, p.plan_updated_at, p.plan_approved_at,
                       p.execution_status, p.progress_pct, p.execution_updated_at,
                       p.usage_prompt_tokens, p.usage_completion_tokens, p.usage_total_tokens, p.usage_updated_at
                FROM project_external_agent_memberships pem
                JOIN projects p ON p.id = pem.project_id
                WHERE p.id = ? AND pem.member_user_id = ? AND pem.status = 'active'
                LIMIT 1
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_project_read_access(access)
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_project_read_access(access)
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_project_read_access(access)
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_project_read_access(access)
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.read")
        _require_project_read_access(access)
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
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
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
        rel = target.relative_to(project_dir).as_posix()
        _require_project_write_access(access, rel)
        if target.exists() and target.is_dir():
            raise HTTPException(400, "Target path is a directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if bool(payload.append) else "w"
        content = str(payload.content or "")
        with target.open(mode, encoding="utf-8") as f:
            f.write(content)
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

    @app.get("/api/projects/{project_id}/chat/messages", response_model=List[ProjectChatMessageOut])
    async def list_project_chat_messages(
        request: Request,
        project_id: str,
        limit: int = 80,
        before: Optional[int] = None,
        mention_target: Optional[str] = None,
    ):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.chat")
        _require_project_chat_access(access)
        messages = _list_project_chat_messages(
            project_id,
            limit=limit,
            before=before,
            mention_target=mention_target,
        )
        return [ProjectChatMessageOut(**item) for item in messages]

    @app.post("/api/projects/{project_id}/chat/messages", response_model=ProjectChatMessageOut)
    async def create_project_chat_message(request: Request, project_id: str, payload: ProjectChatMessageCreateIn):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.chat")
        _require_project_chat_access(access)
        actor = _project_actor_from_access(access)

        conn = db()
        try:
            message = _create_project_chat_message(
                conn,
                project_id=project_id,
                author_type=str(actor.get("type") or "user"),
                author_id=actor.get("id"),
                author_label=actor.get("label"),
                text=str(payload.text or ""),
                metadata=payload.metadata if isinstance(payload.metadata, dict) else {},
                mentions=payload.mentions,
            )
            conn.commit()
        finally:
            conn.close()

        await emit(project_id, "project.chat.message", message)
        for target in (message.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]:
            await emit(
                project_id,
                "project.chat.mention",
                {
                    "message_id": str(message.get("id") or ""),
                    "project_id": project_id,
                    "target": target,
                    "author_type": str(message.get("author_type") or ""),
                    "author_id": message.get("author_id"),
                    "author_label": message.get("author_label"),
                    "text": str(message.get("text") or "")[:500],
                    "created_at": int(message.get("created_at") or 0),
                },
            )
        return ProjectChatMessageOut(**message)

    @app.post("/api/projects/{project_id}/agent-ops", response_model=ProjectAgentOpsOut)
    async def apply_project_agent_ops(request: Request, project_id: str, payload: ProjectAgentOpsIn):
        access = _resolve_project_workspace_access(request, project_id, required_scope="project.write")
        project = access["project"]
        actor = _project_actor_from_access(access)
        ops = payload.ops if isinstance(payload.ops, list) else []
        if not ops:
            return ProjectAgentOpsOut(ok=True, project_id=project_id, applied=[], skipped=[])

        if any(
            _normalize_agent_action_kind(
                (item or {}).get("type")
                or (item or {}).get("method")
                or (item or {}).get("action")
                or (item or {}).get("name")
            ) == "post_chat_message"
            for item in ops
            if isinstance(item, dict)
        ):
            _require_project_chat_access(access)

        allow_paths: Optional[List[str]]
        if str(access.get("mode") or "").strip().lower() == "owner":
            allow_paths = None
        else:
            perms = access.get("permissions") or {}
            allow_paths = _normalize_permission_write_paths(perms.get("write_paths") or [], fallback=[])

        action_result = _apply_project_actions(
            owner_user_id=str(project["user_id"] or ""),
            project_id=project_id,
            project_root=str(project.get("project_root") or ""),
            actions=[item for item in ops if isinstance(item, dict)],
            allow_paths=allow_paths,
            actor_type=str(actor.get("type") or "user"),
            actor_id=actor.get("id"),
            actor_label=actor.get("label"),
        )
        applied = action_result.get("applied") or []
        skipped = action_result.get("skipped") or []

        if any(_normalize_agent_action_kind(item.get("type")) == "update_execution" for item in applied if isinstance(item, dict)):
            _refresh_project_documents(project_id)

        _append_project_daily_log(
            owner_user_id=str(project["user_id"] or ""),
            project_root=str(project.get("project_root") or ""),
            kind="project.agent_ops",
            text=(
                f"{actor.get('label') or actor.get('type')}: applied {len(applied)} op(s), "
                f"skipped {len(skipped)} op(s)."
            ),
            payload={"applied": applied[:20], "skipped": skipped[:10]},
        )

        for item in applied:
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

        return ProjectAgentOpsOut(ok=True, project_id=project_id, applied=applied[:MAX_AGENT_FILE_WRITES], skipped=skipped[:MAX_AGENT_FILE_WRITES])
    
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
                   p.backend_mode, p.connector_id,
                   c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
            FROM projects p
            LEFT JOIN openclaw_connections c ON c.id = p.connection_id
            LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
            WHERE p.id = ? AND p.user_id = ?
            """,
            (project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Project not found")
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row)
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
                project_root=str(row["project_root"] or ""),
                plan_status=_coerce_plan_status(row["plan_status"]),
            )
            scoped_message = _compose_guardrailed_message(
                command_text,
                workspace_root=str(row["workspace_root"] or _user_workspace_root_dir(user_id).as_posix()),
                project_root=str(row["project_root"] or ""),
                task_instruction=instruction,
            )
            ctrl_res = await _project_chat(
                row,
                connection_api_key,
                scoped_message,
                agent_id=primary_agent_id,
                session_key=f"{project_id}:control",
                timeout_sec=25,
                user_id=user_id,
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

        normalized_agent_ids = [str(aid or "").strip() for aid in payload.agent_ids]
        normalized_agent_names = [
            (str(name or "").strip()[:220] or normalized_agent_ids[idx] or "agent")
            for idx, name in enumerate(payload.agent_names)
        ]
        if not normalized_agent_ids:
            raise HTTPException(400, "Select at least one agent")
        if any(not aid for aid in normalized_agent_ids):
            raise HTTPException(400, "agent_ids cannot be empty")
        if len(set(normalized_agent_ids)) != len(normalized_agent_ids):
            raise HTTPException(400, "agent_ids must be unique")

        primary_candidate = str(payload.primary_agent_id or "").strip() or None
        if primary_candidate and primary_candidate not in normalized_agent_ids:
            raise HTTPException(400, "primary_agent_id must be one of selected agent_ids")

        conn = db()
        proj = conn.execute(
            "SELECT id, project_root, title, brief, goal, setup_json, plan_text, plan_status FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")

        external_rows = conn.execute(
            "SELECT agent_id FROM project_agents WHERE project_id = ? AND COALESCE(source_type, 'owner') <> 'owner'",
            (project_id,),
        ).fetchall()
        external_ids = {str(r["agent_id"] or "").strip() for r in external_rows if str(r["agent_id"] or "").strip()}
        conflicting_ids = [aid for aid in normalized_agent_ids if aid in external_ids]
        if conflicting_ids:
            conn.close()
            raise HTTPException(409, f"agent_ids conflict with external agents already in project: {', '.join(conflicting_ids[:5])}")

        owner_rows = conn.execute(
            "SELECT agent_id FROM project_agents WHERE project_id = ? AND COALESCE(source_type, 'owner') = 'owner'",
            (project_id,),
        ).fetchall()
        existing_owner_ids = {
            str(r["agent_id"] or "").strip()
            for r in owner_rows
            if str(r["agent_id"] or "").strip()
        }
        next_owner_ids = set(normalized_agent_ids)
        removed_owner_ids = sorted(existing_owner_ids - next_owner_ids)

        for removed_id in removed_owner_ids:
            conn.execute(
                "DELETE FROM project_agent_access_tokens WHERE project_id = ? AND agent_id = ?",
                (project_id, removed_id),
            )
            conn.execute(
                "DELETE FROM project_agent_permissions WHERE project_id = ? AND agent_id = ?",
                (project_id, removed_id),
            )
            conn.execute(
                "DELETE FROM project_agents WHERE project_id = ? AND agent_id = ? AND COALESCE(source_type, 'owner') = 'owner'",
                (project_id, removed_id),
            )

        primary_id = primary_candidate or normalized_agent_ids[0]
        now = int(time.time())
        issued_tokens: List[Dict[str, Any]] = []
        for idx, (aid, name) in enumerate(zip(normalized_agent_ids, normalized_agent_names)):
            role = str(role_values[idx]).strip()[:500] if idx < len(role_values) else ""
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
                    source_type = 'owner',
                    source_user_id = excluded.source_user_id,
                    source_connection_id = NULL,
                    joined_via_invite_id = NULL
                """,
                (project_id, aid, name, 1 if aid == primary_id else 0, role, "owner", user_id, None, None, now),
            )
            raw_token = _new_agent_access_token()
            conn.execute(
                """
                INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, created_at)
                VALUES (?,?,?,?)
                ON CONFLICT(project_id, agent_id) DO UPDATE SET
                    token_hash = excluded.token_hash,
                    created_at = excluded.created_at
                """,
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

        all_rows = conn.execute(
            "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.commit()
        conn.close()

        _write_project_agent_roles_file(
            owner_user_id=user_id,
            project_root=str(proj["project_root"] or ""),
            agents=[
                {
                    "agent_id": str(r["agent_id"] or ""),
                    "agent_name": str(r["agent_name"] or ""),
                    "role": str(r["role"] or ""),
                    "is_primary": bool(r["is_primary"]),
                }
                for r in all_rows
            ],
        )
        _refresh_project_documents(project_id)

        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=str(proj["project_root"] or ""),
            kind="agents.updated",
            text=f"Invited agents updated. Primary agent: {primary_id}.",
            payload={"count": len(normalized_agent_ids)},
        )
        await emit(project_id, "project.agents_set", {"count": len(normalized_agent_ids), "primary_agent_id": primary_id})
        asyncio.create_task(_generate_project_plan(project_id, force=True))
        await emit(project_id, "project.plan.regenerate_requested", {"project_id": project_id, "source": "agents_set"})
        return {"ok": True, "primary_agent_id": primary_id, "agent_access_tokens": issued_tokens}

    @app.get("/api/projects/{project_id}/agents")
    async def get_project_agents(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id, user_id FROM projects WHERE id = ? LIMIT 1",
            (project_id,),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")
        owner_user_id = str(proj["user_id"] or "").strip()
        is_owner = owner_user_id == user_id
        member_rows = []
        if not is_owner:
            member_rows = conn.execute(
                """
                SELECT agent_id
                FROM project_external_agent_memberships
                WHERE project_id = ? AND member_user_id = ? AND status = 'active'
                ORDER BY updated_at DESC, created_at DESC
                """,
                (project_id, user_id),
            ).fetchall()
            if not member_rows:
                conn.close()
                raise HTTPException(404, "Project not found")
        rows = conn.execute(
            """
            SELECT agent_id, agent_name, is_primary, role,
                   COALESCE(source_type, 'owner') AS source_type,
                   source_user_id, source_connection_id, joined_via_invite_id, added_at
            FROM project_agents
            WHERE project_id = ?
            ORDER BY is_primary DESC, agent_name ASC
            """,
            (project_id,),
        ).fetchall()
        member_agent_ids = {
            str(r["agent_id"] or "").strip()
            for r in member_rows
            if str(r["agent_id"] or "").strip()
        }
        agents: List[Dict[str, Any]] = []
        for r in rows:
            agent_id = str(r["agent_id"] or "").strip()
            source_type = str(r["source_type"] or "owner").strip() or "owner"
            permissions = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                source_type=source_type,
            )
            agents.append(
                {
                    "id": agent_id,
                    "name": str(r["agent_name"] or agent_id),
                    "is_primary": bool(r["is_primary"]),
                    "role": str(r["role"] or ""),
                    "source_type": source_type,
                    "source_user_id": str(r["source_user_id"] or "") or None,
                    "source_connection_id": str(r["source_connection_id"] or "") or None,
                    "joined_via_invite_id": str(r["joined_via_invite_id"] or "") or None,
                    "added_at": _to_int(r["added_at"]),
                    "is_member_agent": bool(agent_id and agent_id in member_agent_ids),
                    "permissions": {
                        "can_chat_project": bool(permissions.get("can_chat_project")),
                        "can_read_files": bool(permissions.get("can_read_files")),
                        "can_write_files": bool(permissions.get("can_write_files")),
                        "write_paths": permissions.get("write_paths") or [],
                        "has_custom": bool(permissions.get("has_custom")),
                    },
                }
            )
        conn.close()
        primary = next((a for a in agents if a["is_primary"]), None)
        return {
            "ok": True,
            "agents": agents,
            "primary_agent": primary,
            "access_mode": "owner" if is_owner else "member",
        }

    @app.get("/api/projects/{project_id}/agent-permissions")
    async def list_project_agent_permissions(request: Request, project_id: str):
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
            """
            SELECT agent_id, agent_name, COALESCE(source_type, 'owner') AS source_type
            FROM project_agents
            WHERE project_id = ?
            ORDER BY agent_name ASC
            """,
            (project_id,),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            agent_id = str(row["agent_id"] or "").strip()
            source_type = str(row["source_type"] or "owner").strip() or "owner"
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                source_type=source_type,
            )
            items.append(
                {
                    "agent_id": agent_id,
                    "agent_name": str(row["agent_name"] or agent_id),
                    "source_type": source_type,
                    "can_chat_project": bool(perms.get("can_chat_project")),
                    "can_read_files": bool(perms.get("can_read_files")),
                    "can_write_files": bool(perms.get("can_write_files")),
                    "write_paths": perms.get("write_paths") or [],
                    "has_custom": bool(perms.get("has_custom")),
                }
            )
        conn.close()
        return {"ok": True, "project_id": project_id, "count": len(items), "permissions": items}

    @app.post("/api/projects/{project_id}/agent-permissions/{agent_id}")
    async def update_project_agent_permissions(
        request: Request,
        project_id: str,
        agent_id: str,
        payload: ProjectAgentPermissionsUpdateIn,
    ):
        user_id = get_session_user(request)
        normalized_agent_id = str(agent_id or "").strip()
        if not normalized_agent_id:
            raise HTTPException(400, "agent_id is required")

        conn = db()
        proj = conn.execute(
            "SELECT id FROM projects WHERE id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")

        agent_row = conn.execute(
            """
            SELECT agent_id, agent_name, COALESCE(source_type, 'owner') AS source_type
            FROM project_agents
            WHERE project_id = ? AND agent_id = ?
            LIMIT 1
            """,
            (project_id, normalized_agent_id),
        ).fetchone()
        if not agent_row:
            conn.close()
            raise HTTPException(404, "Agent not found in project")

        source_type = str(agent_row["source_type"] or "owner").strip() or "owner"
        now = int(time.time())
        if bool(payload.reset_to_default):
            conn.execute(
                "DELETE FROM project_agent_permissions WHERE project_id = ? AND agent_id = ?",
                (project_id, normalized_agent_id),
            )
            conn.commit()
            updated = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=normalized_agent_id,
                source_type=source_type,
            )
            conn.close()
            return {
                "ok": True,
                "project_id": project_id,
                "agent_id": normalized_agent_id,
                "agent_name": str(agent_row["agent_name"] or normalized_agent_id),
                "source_type": source_type,
                "permissions": {
                    "can_chat_project": bool(updated.get("can_chat_project")),
                    "can_read_files": bool(updated.get("can_read_files")),
                    "can_write_files": bool(updated.get("can_write_files")),
                    "write_paths": updated.get("write_paths") or [],
                    "has_custom": bool(updated.get("has_custom")),
                },
            }

        current = _get_project_agent_permissions(
            conn,
            project_id=project_id,
            agent_id=normalized_agent_id,
            source_type=source_type,
        )
        can_chat_project = bool(
            current.get("can_chat_project")
            if payload.can_chat_project is None
            else payload.can_chat_project
        )
        can_read_files = bool(
            current.get("can_read_files")
            if payload.can_read_files is None
            else payload.can_read_files
        )
        can_write_files = bool(
            current.get("can_write_files")
            if payload.can_write_files is None
            else payload.can_write_files
        )
        write_paths = (
            _normalize_permission_write_paths(payload.write_paths, fallback=[])
            if payload.write_paths is not None
            else _normalize_permission_write_paths(current.get("write_paths") or [], fallback=[])
        )

        conn.execute(
            """
            INSERT INTO project_agent_permissions (
                project_id, agent_id, can_chat_project, can_read_files, can_write_files, write_paths_json, updated_at
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(project_id, agent_id) DO UPDATE SET
                can_chat_project = excluded.can_chat_project,
                can_read_files = excluded.can_read_files,
                can_write_files = excluded.can_write_files,
                write_paths_json = excluded.write_paths_json,
                updated_at = excluded.updated_at
            """,
            (
                project_id,
                normalized_agent_id,
                1 if can_chat_project else 0,
                1 if can_read_files else 0,
                1 if can_write_files else 0,
                json.dumps(write_paths, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()
        updated = _get_project_agent_permissions(
            conn,
            project_id=project_id,
            agent_id=normalized_agent_id,
            source_type=source_type,
        )
        conn.close()
        return {
            "ok": True,
            "project_id": project_id,
            "agent_id": normalized_agent_id,
            "agent_name": str(agent_row["agent_name"] or normalized_agent_id),
            "source_type": source_type,
            "permissions": {
                "can_chat_project": bool(updated.get("can_chat_project")),
                "can_read_files": bool(updated.get("can_read_files")),
                "can_write_files": bool(updated.get("can_write_files")),
                "write_paths": updated.get("write_paths") or [],
                "has_custom": bool(updated.get("has_custom")),
            },
        }

    @app.get("/api/projects/{project_id}/memberships/external-agent")
    async def list_external_agent_memberships(request: Request, project_id: str):
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
            """
            SELECT pem.id, pem.member_user_id, pem.member_connection_id,
                   pem.agent_id, pem.agent_name, pem.role, pem.invite_id,
                   pem.status, pem.created_at, pem.updated_at,
                   COALESCE(pa.source_type, 'external') AS source_type
            FROM project_external_agent_memberships pem
            LEFT JOIN project_agents pa ON pa.project_id = pem.project_id AND pa.agent_id = pem.agent_id
            WHERE pem.project_id = ? AND pem.owner_user_id = ?
            ORDER BY pem.updated_at DESC, pem.created_at DESC
            LIMIT 200
            """,
            (project_id, user_id),
        ).fetchall()

        memberships: List[Dict[str, Any]] = []
        for row in rows:
            source_type = str(row["source_type"] or "external").strip() or "external"
            agent_id_row = str(row["agent_id"] or "").strip()
            perms = _get_project_agent_permissions(
                conn,
                project_id=project_id,
                agent_id=agent_id_row,
                source_type=source_type,
            )
            memberships.append(
                {
                    "id": str(row["id"]),
                    "member_user_id": str(row["member_user_id"] or "") or None,
                    "member_connection_id": str(row["member_connection_id"] or "") or None,
                    "agent_id": agent_id_row,
                    "agent_name": str(row["agent_name"] or agent_id_row),
                    "role": str(row["role"] or ""),
                    "invite_id": str(row["invite_id"] or "") or None,
                    "status": str(row["status"] or ""),
                    "created_at": _to_int(row["created_at"]),
                    "updated_at": _to_int(row["updated_at"]),
                    "permissions": {
                        "can_chat_project": bool(perms.get("can_chat_project")),
                        "can_read_files": bool(perms.get("can_read_files")),
                        "can_write_files": bool(perms.get("can_write_files")),
                        "write_paths": perms.get("write_paths") or [],
                        "has_custom": bool(perms.get("has_custom")),
                    },
                }
            )
        conn.close()
        return {"ok": True, "project_id": project_id, "count": len(memberships), "memberships": memberships}

    @app.post("/api/projects/{project_id}/memberships/external-agent/{membership_id}/revoke")
    async def revoke_external_agent_membership(request: Request, project_id: str, membership_id: str):
        user_id = get_session_user(request)
        now = int(time.time())
        conn = db()
        row = conn.execute(
            """
            SELECT pem.id, pem.project_id, pem.owner_user_id, pem.member_user_id, pem.member_connection_id,
                   pem.agent_id, pem.agent_name, pem.status, p.project_root
            FROM project_external_agent_memberships pem
            JOIN projects p ON p.id = pem.project_id
            WHERE pem.id = ? AND pem.project_id = ? AND pem.owner_user_id = ?
            LIMIT 1
            """,
            (membership_id, project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "External membership not found")

        current_status = str(row["status"] or "active").strip().lower() or "active"
        if current_status != "active":
            conn.close()
            return {
                "ok": True,
                "project_id": project_id,
                "membership_id": membership_id,
                "status": current_status,
                "revoked": False,
            }

        agent_id_row = str(row["agent_id"] or "").strip()
        conn.execute(
            "UPDATE project_external_agent_memberships SET status = ?, updated_at = ? WHERE id = ?",
            ("revoked", now, membership_id),
        )
        conn.execute(
            "DELETE FROM project_agent_access_tokens WHERE project_id = ? AND agent_id = ?",
            (project_id, agent_id_row),
        )
        conn.execute(
            "DELETE FROM project_agent_permissions WHERE project_id = ? AND agent_id = ?",
            (project_id, agent_id_row),
        )
        conn.execute(
            """
            DELETE FROM project_agents
            WHERE project_id = ? AND agent_id = ? AND COALESCE(source_type, 'owner') = 'external'
            """,
            (project_id, agent_id_row),
        )
        all_rows = conn.execute(
            "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.commit()
        conn.close()

        project_root = str(row["project_root"] or "")
        _write_project_agent_roles_file(
            owner_user_id=user_id,
            project_root=project_root,
            agents=[
                {
                    "agent_id": str(r["agent_id"] or ""),
                    "agent_name": str(r["agent_name"] or ""),
                    "role": str(r["role"] or ""),
                    "is_primary": bool(r["is_primary"]),
                }
                for r in all_rows
            ],
        )
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=project_root,
            kind="external_agent.revoked",
            text=f"External agent revoked: {agent_id_row}",
            payload={
                "membership_id": membership_id,
                "member_user_id": str(row["member_user_id"] or "") or None,
                "member_connection_id": str(row["member_connection_id"] or "") or None,
            },
        )
        await emit(
            project_id,
            "project.external_agent.revoked",
            {
                "membership_id": membership_id,
                "agent_id": agent_id_row,
                "agent_name": str(row["agent_name"] or agent_id_row),
            },
        )
        return {
            "ok": True,
            "project_id": project_id,
            "membership_id": membership_id,
            "status": "revoked",
            "revoked": True,
            "agent_id": agent_id_row,
            "agent_name": str(row["agent_name"] or agent_id_row),
        }

    @app.post("/api/projects/{project_id}/invites/external-agent")
    async def create_project_external_agent_invite(request: Request, project_id: str, payload: ProjectExternalAgentInviteCreateIn):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            """
            SELECT p.id, p.title, p.goal, p.project_root, p.connection_id,
                   c.base_url AS connection_base_url, c.api_key AS connection_api_key, c.api_key_secret_id AS connection_api_key_secret_id,
                   cp.main_agent_id AS owner_main_agent_id
            FROM projects p
            LEFT JOIN openclaw_connections c ON c.id = p.connection_id AND c.user_id = p.user_id
            LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
            WHERE p.id = ? AND p.user_id = ?
            LIMIT 1
            """,
            (project_id, user_id),
        ).fetchone()
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=proj) if proj else ""
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")

        target_email = _normalize_email(str(payload.target_email or ""))
        owner_email_row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        owner_email = _normalize_email(str(owner_email_row["email"] or "")) if owner_email_row else ""
        if target_email and owner_email and target_email == owner_email:
            conn.close()
            raise HTTPException(400, "target_email must be different from owner email")

        invite_id = new_id("pinv")
        now = int(time.time())
        ttl_sec = _clamp_external_invite_ttl(payload.expires_in_sec)
        expires_at = now + ttl_sec
        raw_invite_token = _new_project_external_invite_token()
        raw_invite_code = _new_project_invite_portal_code(8)

        requested_agent_id = str(payload.requested_agent_id or "").strip()[:180]
        requested_agent_name = str(payload.requested_agent_name or requested_agent_id or "").strip()[:220]
        role = str(payload.role or "").strip()[:500]
        note = str(payload.note or "").strip()[:1200]

        origin = _request_origin(request)
        invite_token_quoted = url_quote(raw_invite_token, safe="")
        invite_code_quoted = url_quote(raw_invite_code, safe="")
        portal_url = f"{origin}/?project_invite={invite_token_quoted}&project_invite_code={invite_code_quoted}"
        invitation_doc_url = f"{origin}/api/projects/invites/{invite_token_quoted}/Project-Invitation.md"
        invite_url = invitation_doc_url
        accept_api_url = f"{origin}/api/projects/invites/{invite_token_quoted}/accept"
        invite_relpath = f"{PROJECT_INFO_DIRNAME}/Invites/EXTERNAL-AGENT-INVITE-{invite_id}.MD"
        project_invitations_relpath = PROJECT_INVITATIONS_FILE

        project_root = str(proj["project_root"] or "")
        project_dir = _resolve_owner_project_dir(user_id, project_root).resolve()
        invite_path = (project_dir / invite_relpath).resolve()
        if not _path_within(invite_path, project_dir):
            conn.close()
            raise HTTPException(400, "Invalid invite path")

        invite_path.parent.mkdir(parents=True, exist_ok=True)
        invite_md = _build_external_agent_invite_markdown(
            owner_user_id=user_id,
            project_id=project_id,
            project_title=str(proj["title"] or ""),
            project_goal=str(proj["goal"] or ""),
            requested_agent_id=requested_agent_id,
            requested_agent_name=requested_agent_name,
            role=role,
            note=note,
            invite_token=raw_invite_token,
            accept_api_url=accept_api_url,
            invitation_doc_url=invitation_doc_url,
            portal_url=portal_url,
            invite_code=raw_invite_code,
            created_at=now,
            expires_at=expires_at,
        )
        invite_path.write_text(invite_md, encoding="utf-8")

        email_template = _build_external_invite_email_template(
            project_title=str(proj["title"] or ""),
            project_goal=str(proj["goal"] or ""),
            invitation_doc_url=invitation_doc_url,
            portal_url=portal_url,
            invite_code=raw_invite_code,
            target_email=target_email or None,
        )
        composed = await _compose_external_invite_email_with_primary_agent(
            base_url=str(proj["connection_base_url"] or ""),
            api_key=connection_api_key,
            main_agent_id=str(proj["owner_main_agent_id"] or "").strip(),
            default_subject=str(email_template.get("subject") or ""),
            default_body=str(email_template.get("body") or ""),
            target_email=target_email or None,
            project_title=str(proj["title"] or ""),
            role=role,
            invitation_doc_url=invitation_doc_url,
            portal_url=portal_url,
            invite_code=raw_invite_code,
        )
        email_subject = str(composed.get("subject") or email_template.get("subject") or "").strip()[:220]
        email_body = str(composed.get("body") or email_template.get("body") or "").strip()[:6000]
        email_to_value = str(target_email or "").strip()
        email_mailto_url = (
            f"mailto:{url_quote(email_to_value)}?{urlencode({'subject': email_subject, 'body': email_body})}"
            if email_to_value
            else f"mailto:?{urlencode({'subject': email_subject, 'body': email_body})}"
        )

        primary_send_ok = bool(composed.get("sent_by_agent"))
        primary_send_status_raw = str(composed.get("send_status") or "").strip()
        primary_send_error = str(composed.get("send_error") or composed.get("compose_error") or "").strip() or None
        primary_send_note = str(composed.get("send_note") or "").strip() or None

        email_delivery_status = primary_send_status_raw or ("sent_by_primary_agent" if primary_send_ok else "failed_primary_agent_send")
        email_delivery_error = primary_send_error
        email_sent_at = int(time.time()) if primary_send_ok else None

        smtp_fallback_allowed = str(os.getenv("INVITE_EMAIL_ALLOW_SMTP_FALLBACK") or "1").strip().lower() not in {"0", "false", "no", "off"}
        if (not primary_send_ok) and smtp_fallback_allowed:
            smtp_delivery = _send_external_invite_email(
                target_email=target_email or None,
                subject=email_subject,
                body=email_body,
            )
            if smtp_delivery.get("ok"):
                email_delivery_status = "sent_via_smtp_fallback"
                smtp_error_text = str(smtp_delivery.get("error") or "").strip()
                email_delivery_error = primary_send_error or (smtp_error_text or None)
                email_sent_at = _to_int(smtp_delivery.get("sent_at")) if smtp_delivery.get("sent_at") else int(time.time())
            else:
                smtp_error_text = str(smtp_delivery.get("error") or "").strip()
                joined_errors = " | ".join([x for x in [primary_send_error, smtp_error_text] if x]).strip()
                email_delivery_status = "failed_primary_and_smtp"
                email_delivery_error = joined_errors or "primary agent send failed and SMTP fallback failed"
                email_sent_at = None
        elif (not primary_send_ok) and (not smtp_fallback_allowed):
            email_delivery_status = "failed_primary_no_fallback"
            email_delivery_error = primary_send_error or "Primary agent send failed"


        conn.execute(
            """
            INSERT INTO project_external_agent_invites (
                id, project_id, owner_user_id, target_email, requested_agent_id, requested_agent_name,
                role, invite_note, token_hash, invite_doc_relpath, portal_code_hash, portal_code_hint,
                email_delivery_status, email_delivery_error, email_sent_at,
                status, expires_at, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                invite_id,
                project_id,
                user_id,
                target_email or None,
                requested_agent_id or None,
                requested_agent_name or None,
                role,
                note,
                _hash_access_token(raw_invite_token),
                invite_relpath,
                _hash_invite_code(raw_invite_code),
                _mask_invite_code(raw_invite_code),
                email_delivery_status,
                email_delivery_error,
                email_sent_at,
                "pending",
                expires_at,
                now,
            ),
        )
        conn.commit()
        conn.close()

        preview_url = f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(invite_relpath)}"
        project_invitations_preview_url = f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(project_invitations_relpath)}"
        _append_project_invitations_record(
            owner_user_id=user_id,
            project_root=project_root,
            project_id=project_id,
            project_title=str(proj["title"] or ""),
            invite_id=invite_id,
            status="pending",
            invitation_doc_url=invitation_doc_url,
            portal_url=portal_url,
            invite_code=raw_invite_code,
            accept_api_url=accept_api_url,
            target_email=target_email or None,
            requested_agent_id=requested_agent_id,
            requested_agent_name=requested_agent_name,
            role=role,
            note=note,
            created_at=now,
            expires_at=expires_at,
            email_subject=email_subject,
            email_body=email_body,
            email_delivery_status=email_delivery_status,
        )
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=project_root,
            kind="external_agent.invite_created",
            text=f"External agent invite created: {invite_id}",
            payload={
                "target_email": target_email or None,
                "expires_at": expires_at,
                "invite_doc": invite_relpath,
                "email_delivery_status": email_delivery_status,
            },
        )
        await emit(
            project_id,
            "project.external_agent.invite_created",
            {
                "invite_id": invite_id,
                "target_email": target_email or None,
                "expires_at": expires_at,
                "invite_doc": invite_relpath,
                "email_delivery_status": email_delivery_status,
            },
        )
        return {
            "ok": True,
            "invite_id": invite_id,
            "project_id": project_id,
            "status": "pending",
            "invite_token": raw_invite_token,
            "invite_url": invite_url,
            "invitation_doc_url": invitation_doc_url,
            "portal_url": portal_url,
            "invite_code": raw_invite_code,
            "invite_code_hint": _mask_invite_code(raw_invite_code),
            "requires_invite_code": True,
            "accept_api_url": accept_api_url,
            "invite_doc_path": invite_relpath,
            "invite_doc_preview_url": preview_url,
            "project_invitations_doc_path": project_invitations_relpath,
            "project_invitations_preview_url": project_invitations_preview_url,
            "email_subject": email_subject,
            "email_body": email_body,
            "email_mailto_url": email_mailto_url,
            "email_delivery_status": email_delivery_status,
            "email_delivery_error": email_delivery_error,
            "email_sent_at": email_sent_at,
            "email_composed_by_agent": bool(composed.get("composed_by_agent")),
            "email_compose_error": str(composed.get("compose_error") or "") or None,
            "email_sent_by_primary_agent": primary_send_ok,
            "email_primary_send_status": primary_send_status_raw or None,
            "email_primary_send_error": primary_send_error,
            "email_primary_send_note": primary_send_note,
            "email_smtp_fallback_allowed": smtp_fallback_allowed,
            "expires_at": expires_at,
            "target_email": target_email or None,
            "requested_agent_id": requested_agent_id or None,
            "requested_agent_name": requested_agent_name or None,
            "role": role,
            "note": note,
        }

    @app.get("/api/projects/{project_id}/invites/external-agent")
    async def list_project_external_agent_invites(request: Request, project_id: str):
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
            """
            SELECT id, target_email, requested_agent_id, requested_agent_name, role, invite_note, invite_doc_relpath,
                   status, expires_at, created_at, accepted_at, accepted_by_user_id, accepted_connection_id, accepted_agent_id,
                   portal_code_hash, portal_code_hint, email_delivery_status, email_delivery_error, email_sent_at
            FROM project_external_agent_invites
            WHERE project_id = ? AND owner_user_id = ?
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (project_id, user_id),
        ).fetchall()
        conn.close()

        now = int(time.time())
        project_invitations_relpath = PROJECT_INVITATIONS_FILE
        project_invitations_preview_url = f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(project_invitations_relpath)}"
        invites = []
        for row in rows:
            status = str(row["status"] or "pending")
            if status == "pending" and _to_int(row["expires_at"]) <= now:
                status = "expired"

            invite_doc_relpath = str(row["invite_doc_relpath"] or "").strip()
            invite_doc_preview_url = (
                f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(invite_doc_relpath)}"
                if invite_doc_relpath
                else None
            )

            invites.append(
                {
                    "id": str(row["id"]),
                    "target_email": str(row["target_email"] or "") or None,
                    "requested_agent_id": str(row["requested_agent_id"] or "") or None,
                    "requested_agent_name": str(row["requested_agent_name"] or "") or None,
                    "role": str(row["role"] or ""),
                    "note": str(row["invite_note"] or ""),
                    "invite_doc_path": invite_doc_relpath or None,
                    "invite_doc_preview_url": invite_doc_preview_url,
                    "invite_code_hint": str(row["portal_code_hint"] or "") or None,
                    "requires_invite_code": bool(str(row["portal_code_hash"] or "").strip()),
                    "email_delivery_status": str(row["email_delivery_status"] or "") or None,
                    "email_delivery_error": str(row["email_delivery_error"] or "") or None,
                    "email_sent_at": _to_int(row["email_sent_at"]),
                    "status": status,
                    "expires_at": _to_int(row["expires_at"]),
                    "created_at": _to_int(row["created_at"]),
                    "accepted_at": _to_int(row["accepted_at"]),
                    "accepted_by_user_id": str(row["accepted_by_user_id"] or "") or None,
                    "accepted_connection_id": str(row["accepted_connection_id"] or "") or None,
                    "accepted_agent_id": str(row["accepted_agent_id"] or "") or None,
                }
            )

        return {
            "ok": True,
            "count": len(invites),
            "invites": invites,
            "project_invitations_doc_path": project_invitations_relpath,
            "project_invitations_preview_url": project_invitations_preview_url,
        }

    @app.get("/api/projects/invites/{invite_token}/Project-Invitation.md")
    async def get_project_external_agent_invitation_doc(invite_token: str):
        token = str(invite_token or "").strip()
        if not token:
            raise HTTPException(400, "invite token is required")

        now = int(time.time())
        conn = db()
        row = conn.execute(
            """
            SELECT pi.id, pi.status, pi.expires_at, pi.invite_doc_relpath,
                   p.project_root, p.user_id AS owner_user_id, p.title AS project_title
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            WHERE pi.token_hash = ?
            LIMIT 1
            """,
            (_hash_access_token(token),),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Invite not found")

        status = str(row["status"] or "pending").strip().lower() or "pending"
        expires_at = _to_int(row["expires_at"])
        if status == "pending" and expires_at <= now:
            status = "expired"
            conn.execute(
                "UPDATE project_external_agent_invites SET status = ? WHERE id = ?",
                ("expired", str(row["id"])),
            )
            conn.commit()
        conn.close()

        owner_user_id = str(row["owner_user_id"] or "")
        project_root = str(row["project_root"] or "")
        doc_relpath = str(row["invite_doc_relpath"] or "").strip()
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
        doc_path = (project_dir / doc_relpath).resolve() if doc_relpath else project_dir
        if doc_relpath and _path_within(doc_path, project_dir) and doc_path.exists() and doc_path.is_file():
            text = doc_path.read_text(encoding="utf-8", errors="replace")
        else:
            text = (
                "# PROJECT-EXTERNAL-AGENT-INVITE\n\n"
                f"- Project: {str(row['project_title'] or '')}\n"
                f"- Invite Status: {status}\n"
                "- OpenClaw base URL must be public. If local/private, set up SSH/public tunnel/proxy first.\n"
            )
        if status in {"expired", "revoked"}:
            text = (
                f"# Invitation {status.title()}\n\n"
                f"This invitation is {status}. Ask project owner for a fresh invite.\n\n"
                + text
            )
        return Response(content=text, media_type="text/markdown; charset=utf-8")

    @app.get("/api/projects/invites/{invite_token}")
    async def get_project_external_agent_invite_info(request: Request, invite_token: str):
        token = str(invite_token or "").strip()
        if not token:
            raise HTTPException(400, "invite token is required")

        now = int(time.time())
        conn = db()
        row = conn.execute(
            """
            SELECT pi.id, pi.project_id, pi.owner_user_id, pi.target_email, pi.requested_agent_id, pi.requested_agent_name,
                   pi.role, pi.invite_note, pi.status, pi.expires_at, pi.created_at, pi.accepted_at,
                   pi.accepted_by_user_id, pi.accepted_connection_id, pi.accepted_agent_id,
                   pi.portal_code_hash, pi.portal_code_hint, pi.invite_doc_relpath,
                   pi.email_delivery_status, pi.email_delivery_error, pi.email_sent_at,
                   p.title AS project_title
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            WHERE pi.token_hash = ?
            LIMIT 1
            """,
            (_hash_access_token(token),),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Invite not found")

        status = str(row["status"] or "pending").strip().lower() or "pending"
        expires_at = _to_int(row["expires_at"])
        if status == "pending" and expires_at <= now:
            status = "expired"
            conn.execute(
                "UPDATE project_external_agent_invites SET status = ? WHERE id = ?",
                ("expired", str(row["id"])),
            )
            conn.commit()
        conn.close()

        origin = _request_origin(request)
        token_quoted = url_quote(token, safe="")
        invitation_doc_url = f"{origin}/api/projects/invites/{token_quoted}/Project-Invitation.md"
        portal_url = f"{origin}/?project_invite={token_quoted}"
        invite_code_hint = str(row["portal_code_hint"] or "") or None
        requires_invite_code = bool(str(row["portal_code_hash"] or "").strip())

        return {
            "ok": True,
            "invite_id": str(row["id"]),
            "project_id": str(row["project_id"]),
            "project_title": str(row["project_title"] or ""),
            "owner_user_id": str(row["owner_user_id"] or ""),
            "target_email_masked": _mask_email_for_public(str(row["target_email"] or "")),
            "requested_agent_id": str(row["requested_agent_id"] or "") or None,
            "requested_agent_name": str(row["requested_agent_name"] or "") or None,
            "role": str(row["role"] or ""),
            "note": str(row["invite_note"] or ""),
            "status": status,
            "expires_at": expires_at,
            "created_at": _to_int(row["created_at"]),
            "accepted_at": _to_int(row["accepted_at"]),
            "accepted_by_user_id": str(row["accepted_by_user_id"] or "") or None,
            "accepted_connection_id": str(row["accepted_connection_id"] or "") or None,
            "accepted_agent_id": str(row["accepted_agent_id"] or "") or None,
            "can_accept": status == "pending" and expires_at > now,
            "requires_invite_code": requires_invite_code,
            "invite_code_hint": invite_code_hint,
            "invitation_doc_url": invitation_doc_url,
            "portal_url": portal_url,
            "invite_doc_path": str(row["invite_doc_relpath"] or "") or None,
            "email_delivery_status": str(row["email_delivery_status"] or "") or None,
            "email_delivery_error": str(row["email_delivery_error"] or "") or None,
            "email_sent_at": _to_int(row["email_sent_at"]),
        }

    @app.post("/api/projects/{project_id}/invites/external-agent/{invite_id}/revoke")
    async def revoke_project_external_agent_invite(request: Request, project_id: str, invite_id: str):
        user_id = get_session_user(request)
        now = int(time.time())
        conn = db()
        row = conn.execute(
            """
            SELECT pi.id, pi.status, pi.project_id, p.project_root
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            WHERE pi.id = ? AND pi.project_id = ? AND pi.owner_user_id = ?
            LIMIT 1
            """,
            (invite_id, project_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Invite not found")

        current_status = str(row["status"] or "pending").strip().lower() or "pending"
        if current_status != "pending":
            conn.close()
            return {
                "ok": True,
                "project_id": project_id,
                "invite_id": invite_id,
                "status": current_status,
                "revoked": False,
            }

        conn.execute(
            "UPDATE project_external_agent_invites SET status = ? WHERE id = ?",
            ("revoked", invite_id),
        )
        conn.commit()
        conn.close()
        _append_project_invitations_status_update(
            owner_user_id=user_id,
            project_root=str(row["project_root"] or ""),
            invite_id=invite_id,
            status="revoked",
            ts_value=now,
            note="revoked_by_owner",
        )
        await emit(project_id, "project.external_agent.invite_revoked", {"invite_id": invite_id, "status": "revoked"})
        return {
            "ok": True,
            "project_id": project_id,
            "invite_id": invite_id,
            "status": "revoked",
            "revoked": True,
            "revoked_at": now,
        }

    @app.post("/api/projects/invites/{invite_token}/accept")
    async def accept_project_external_agent_invite(request: Request, invite_token: str, payload: ProjectExternalAgentInviteAcceptIn):
        member_user_id = get_session_user(request)
        token = str(invite_token or "").strip()
        if not token:
            raise HTTPException(400, "invite token is required")

        now = int(time.time())
        conn = db()
        invite_row = conn.execute(
            """
            SELECT pi.id, pi.project_id, pi.owner_user_id, pi.target_email, pi.requested_agent_id, pi.requested_agent_name,
                   pi.role, pi.status, pi.expires_at, pi.invite_doc_relpath, pi.portal_code_hash, pi.portal_code_hint,
                   p.title AS project_title, p.project_root AS project_root
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            WHERE pi.token_hash = ?
            LIMIT 1
            """,
            (_hash_access_token(token),),
        ).fetchone()
        if not invite_row:
            conn.close()
            raise HTTPException(404, "Invite not found")

        invite_status = str(invite_row["status"] or "pending").strip().lower()
        if invite_status != "pending":
            conn.close()
            raise HTTPException(409, f"Invite is not pending (status={invite_status})")

        expires_at = _to_int(invite_row["expires_at"])
        if expires_at <= now:
            conn.execute(
                "UPDATE project_external_agent_invites SET status = ? WHERE id = ?",
                ("expired", str(invite_row["id"])),
            )
            conn.commit()
            conn.close()
            raise HTTPException(410, "Invite expired")

        owner_user_id = str(invite_row["owner_user_id"] or "").strip()
        if member_user_id == owner_user_id:
            conn.close()
            raise HTTPException(400, "Owner cannot accept external agent invite")

        member_user_row = conn.execute("SELECT email FROM users WHERE id = ?", (member_user_id,)).fetchone()
        if not member_user_row:
            conn.close()
            raise HTTPException(404, "Member user not found")

        target_email = _normalize_email(str(invite_row["target_email"] or ""))
        member_email = _normalize_email(str(member_user_row["email"] or ""))
        if target_email and member_email != target_email:
            conn.close()
            raise HTTPException(403, "Invite was issued for a different email")

        required_code_hash = str(invite_row["portal_code_hash"] or "").strip()
        provided_invite_code = _normalize_invite_code(payload.invite_code)
        if required_code_hash:
            if not provided_invite_code:
                conn.close()
                raise HTTPException(400, "invite_code is required. Open Project-Invitation.md and input the portal code.")
            if _hash_invite_code(provided_invite_code) != required_code_hash:
                conn.close()
                raise HTTPException(403, "invite_code does not match this invitation")

        connection_id = str(payload.connection_id or "").strip()
        if not connection_id:
            conn.close()
            raise HTTPException(400, "connection_id is required")
        conn_row = conn.execute(
            "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, member_user_id),
        ).fetchone()
        if not conn_row:
            conn.close()
            raise HTTPException(404, "Connection not found for this user")

        policy_row = conn.execute(
            "SELECT main_agent_id, main_agent_name FROM connection_policies WHERE connection_id = ? AND user_id = ? LIMIT 1",
            (connection_id, member_user_id),
        ).fetchone()
        managed_rows = conn.execute(
            """
            SELECT agent_id, agent_name
            FROM managed_agents
            WHERE user_id = ? AND connection_id = ?
            ORDER BY updated_at DESC, agent_name ASC
            LIMIT 500
            """,
            (member_user_id, connection_id),
        ).fetchall()
        managed_name_by_id = {
            str(r["agent_id"] or "").strip(): str(r["agent_name"] or "").strip()
            for r in managed_rows
            if str(r["agent_id"] or "").strip()
        }

        project_id = str(invite_row["project_id"] or "").strip()
        locked_requested_agent_id = str(invite_row["requested_agent_id"] or "").strip()[:180]
        locked_requested_agent_name = str(invite_row["requested_agent_name"] or "").strip()[:220]
        default_main_agent_id = str(policy_row["main_agent_id"] or "").strip()[:180] if policy_row else ""
        default_main_agent_name = str(policy_row["main_agent_name"] or "").strip()[:220] if policy_row else ""
        invite_role = str(invite_row["role"] or "").strip()[:500]

        requested_entries: List[Dict[str, str]] = []

        def _push_candidate(raw_agent_id: Any, raw_agent_name: Any, raw_role: Any, *, source: str) -> None:
            aid = str(raw_agent_id or "").strip()[:180]
            if not aid:
                return
            requested_entries.append(
                {
                    "agent_id": aid,
                    "agent_name": str(raw_agent_name or "").strip()[:220],
                    "role": str(raw_role or "").strip()[:500],
                    "source": source,
                }
            )

        selected_agents_payload = payload.selected_agents or []
        primary_requested_id = str(payload.agent_id or locked_requested_agent_id or default_main_agent_id).strip()[:180]
        if (not primary_requested_id) and selected_agents_payload:
            primary_requested_id = str(getattr(selected_agents_payload[0], "agent_id", "") or "").strip()[:180]
        if not primary_requested_id:
            conn.close()
            raise HTTPException(400, "agent_id is required. Ensure this connection has a main agent or provide agent_id explicitly.")
        if locked_requested_agent_id and primary_requested_id != locked_requested_agent_id:
            conn.close()
            raise HTTPException(403, "Invite locks agent_id and cannot be overridden")

        _push_candidate(
            primary_requested_id,
            payload.agent_name or locked_requested_agent_name or managed_name_by_id.get(primary_requested_id) or default_main_agent_name,
            invite_role,
            source="primary",
        )

        if len(selected_agents_payload) > 30:
            conn.close()
            raise HTTPException(400, "selected_agents supports up to 30 agents per accept request")
        for item in selected_agents_payload:
            _push_candidate(
                getattr(item, "agent_id", ""),
                getattr(item, "agent_name", ""),
                getattr(item, "role", ""),
                source="selected",
            )

        if locked_requested_agent_id and not any(str(x.get("agent_id") or "") == locked_requested_agent_id for x in requested_entries):
            _push_candidate(locked_requested_agent_id, locked_requested_agent_name, invite_role, source="locked")

        deduped_agents: List[Dict[str, str]] = []
        seen_agent_ids: set[str] = set()
        for item in requested_entries:
            aid = str(item.get("agent_id") or "").strip()[:180]
            if not aid:
                continue
            if aid in seen_agent_ids:
                continue
            seen_agent_ids.add(aid)
            deduped_agents.append(item)

        if locked_requested_agent_id:
            locked_row = next((x for x in deduped_agents if str(x.get("agent_id") or "") == locked_requested_agent_id), None)
            if not locked_row:
                conn.close()
                raise HTTPException(403, "Invite locks agent_id and it must be included")
            deduped_agents = [locked_row] + [x for x in deduped_agents if str(x.get("agent_id") or "") != locked_requested_agent_id]

        if not deduped_agents:
            conn.close()
            raise HTTPException(400, "No valid agent selection found")

        normalized_agents: List[Dict[str, str]] = []
        for item in deduped_agents:
            aid = str(item.get("agent_id") or "").strip()[:180]
            if managed_name_by_id and aid not in managed_name_by_id:
                conn.close()
                raise HTTPException(400, f"agent_id '{aid}' is not provisioned on the selected connection")
            resolved_name = str(item.get("agent_name") or "").strip()[:220]
            if not resolved_name:
                resolved_name = str(managed_name_by_id.get(aid) or (default_main_agent_name if aid == default_main_agent_id else "") or aid).strip()[:220] or aid
            resolved_role = str(item.get("role") or invite_role or "").strip()[:500]
            normalized_agents.append(
                {
                    "agent_id": aid,
                    "agent_name": resolved_name,
                    "role": resolved_role,
                }
            )

        invite_id = str(invite_row["id"])
        joined_agents: List[Dict[str, Any]] = []
        for item in normalized_agents:
            agent_id = str(item.get("agent_id") or "").strip()
            agent_name = str(item.get("agent_name") or agent_id).strip()[:220] or agent_id
            role = str(item.get("role") or "").strip()[:500]

            existing_agent_row = conn.execute(
                """
                SELECT agent_id, COALESCE(source_type, 'owner') AS source_type,
                       source_user_id, source_connection_id
                FROM project_agents
                WHERE project_id = ? AND agent_id = ?
                LIMIT 1
                """,
                (project_id, agent_id),
            ).fetchone()
            created_project_agent = False
            if existing_agent_row:
                existing_source = str(existing_agent_row["source_type"] or "owner").strip() or "owner"
                existing_source_user = str(existing_agent_row["source_user_id"] or "").strip()
                existing_source_conn = str(existing_agent_row["source_connection_id"] or "").strip()
                if existing_source != "external" or existing_source_user != member_user_id or existing_source_conn != connection_id:
                    conn.close()
                    raise HTTPException(409, f"agent_id '{agent_id}' already exists in this project")
                conn.execute(
                    """
                    UPDATE project_agents
                    SET agent_name = ?, role = ?, source_type = ?, source_user_id = ?,
                        source_connection_id = ?, joined_via_invite_id = ?, added_at = ?
                    WHERE project_id = ? AND agent_id = ?
                    """,
                    (
                        agent_name,
                        role,
                        "external",
                        member_user_id,
                        connection_id,
                        invite_id,
                        now,
                        project_id,
                        agent_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO project_agents (
                        project_id, agent_id, agent_name, is_primary, role,
                        source_type, source_user_id, source_connection_id, joined_via_invite_id, added_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        project_id,
                        agent_id,
                        agent_name,
                        0,
                        role,
                        "external",
                        member_user_id,
                        connection_id,
                        invite_id,
                        now,
                    ),
                )
                created_project_agent = True

            membership_row = conn.execute(
                """
                SELECT id, status
                FROM project_external_agent_memberships
                WHERE project_id = ? AND member_user_id = ? AND member_connection_id = ? AND agent_id = ?
                LIMIT 1
                """,
                (project_id, member_user_id, connection_id, agent_id),
            ).fetchone()
            created_membership = False
            if membership_row:
                membership_id = str(membership_row["id"] or "").strip() or new_id("pmem")
                conn.execute(
                    """
                    UPDATE project_external_agent_memberships
                    SET owner_user_id = ?, agent_name = ?, role = ?, invite_id = ?, status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        owner_user_id,
                        agent_name,
                        role,
                        invite_id,
                        "active",
                        now,
                        membership_id,
                    ),
                )
            else:
                membership_id = new_id("pmem")
                conn.execute(
                    """
                    INSERT INTO project_external_agent_memberships (
                        id, project_id, owner_user_id, member_user_id, member_connection_id,
                        agent_id, agent_name, role, invite_id, status, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        membership_id,
                        project_id,
                        owner_user_id,
                        member_user_id,
                        connection_id,
                        agent_id,
                        agent_name,
                        role,
                        invite_id,
                        "active",
                        now,
                        now,
                    ),
                )
                created_membership = True

            raw_project_agent_token = _new_agent_access_token()
            conn.execute(
                """
                INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, created_at)
                VALUES (?,?,?,?)
                ON CONFLICT(project_id, agent_id) DO UPDATE SET
                    token_hash = excluded.token_hash,
                    created_at = excluded.created_at
                """,
                (project_id, agent_id, _hash_access_token(raw_project_agent_token), now),
            )

            joined_agents.append(
                {
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "role": role,
                    "membership_id": membership_id,
                    "project_agent_access_token": raw_project_agent_token,
                    "created_membership": created_membership,
                    "created_project_agent": created_project_agent,
                }
            )

        accepted_primary_agent_id = str((joined_agents[0] or {}).get("agent_id") or "").strip()
        accepted_primary_agent_name = str((joined_agents[0] or {}).get("agent_name") or accepted_primary_agent_id).strip()[:220]
        accepted_membership_id = str((joined_agents[0] or {}).get("membership_id") or "").strip() or None
        accepted_project_agent_token = str((joined_agents[0] or {}).get("project_agent_access_token") or "").strip() or None

        conn.execute(
            """
            UPDATE project_external_agent_invites
            SET status = ?, accepted_at = ?, accepted_by_user_id = ?, accepted_connection_id = ?, accepted_agent_id = ?
            WHERE id = ?
            """,
            ("accepted", now, member_user_id, connection_id, accepted_primary_agent_id, invite_id),
        )
        all_rows = conn.execute(
            "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.commit()
        conn.close()

        project_root = str(invite_row["project_root"] or "")
        _append_project_invitations_status_update(
            owner_user_id=owner_user_id,
            project_root=project_root,
            invite_id=invite_id,
            status="accepted",
            ts_value=now,
            note=f"accepted_by:{member_user_id}:{accepted_primary_agent_id}:count={len(joined_agents)}",
        )
        _write_project_agent_roles_file(
            owner_user_id=owner_user_id,
            project_root=project_root,
            agents=[
                {
                    "agent_id": str(r["agent_id"] or ""),
                    "agent_name": str(r["agent_name"] or ""),
                    "role": str(r["role"] or ""),
                    "is_primary": bool(r["is_primary"]),
                }
                for r in all_rows
            ],
        )
        _refresh_project_documents(project_id)

        for joined in joined_agents:
            joined_agent_id = str(joined.get("agent_id") or "").strip() or "agent"
            joined_agent_name = str(joined.get("agent_name") or joined_agent_id).strip()
            _append_project_daily_log(
                owner_user_id=owner_user_id,
                project_root=project_root,
                kind="external_agent.joined",
                text=f"External agent joined project: {joined_agent_id} ({joined_agent_name})",
                payload={
                    "member_user_id": member_user_id,
                    "connection_id": connection_id,
                    "invite_id": invite_id,
                    "membership_id": str(joined.get("membership_id") or ""),
                },
            )
            await emit(
                project_id,
                "project.external_agent.joined",
                {
                    "agent_id": joined_agent_id,
                    "agent_name": joined_agent_name,
                    "role": str(joined.get("role") or ""),
                    "member_user_id": member_user_id,
                    "source_connection_id": connection_id,
                    "invite_id": invite_id,
                    "membership_id": str(joined.get("membership_id") or ""),
                },
            )

        return {
            "ok": True,
            "project_id": project_id,
            "project_title": str(invite_row["project_title"] or ""),
            "agent_id": accepted_primary_agent_id,
            "agent_name": accepted_primary_agent_name,
            "source_type": "external",
            "membership_id": accepted_membership_id,
            "invite_id": invite_id,
            "project_agent_access_token": accepted_project_agent_token,
            "accepted_connection_id": connection_id,
            "joined_agents": joined_agents,
            "joined_agents_count": len(joined_agents),
        }

    @app.get("/api/projects/{project_id}/readiness", response_model=ProjectReadinessOut)
    async def get_project_readiness(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id, user_id, project_root, plan_status, execution_status FROM projects WHERE id = ? AND user_id = ?",
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
            execution_status=proj["execution_status"],
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

        current_status = _coerce_execution_status(proj["execution_status"])
        if current_status in {EXEC_STATUS_RUNNING, EXEC_STATUS_PAUSED}:
            raise HTTPException(
                409,
                {
                    "message": "Project execution is already in progress.",
                    "status": current_status,
                },
            )
        if current_status == EXEC_STATUS_COMPLETED:
            raise HTTPException(
                409,
                {
                    "message": "Project execution is already completed.",
                    "status": current_status,
                },
            )

        role_rows = [dict(a) for a in agents]
        readiness = _project_readiness_snapshot(
            owner_user_id=str(proj["user_id"]),
            project_id=project_id,
            project_root=str(proj["project_root"] or ""),
            plan_status=proj["plan_status"],
            execution_status=proj["execution_status"],
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
        await emit(
            project_id,
            "run.started",
            {
                "project": str(proj["title"] or ""),
                "agents": [str(a["agent_name"] or a["agent_id"] or "") for a in agents],
                "primary_agent": next((str(a["agent_name"] or a["agent_id"] or "") for a in agents if bool(a["is_primary"])), None),
            },
        )
        asyncio.create_task(_delegate_project_tasks(project_id))
        return {"ok": True}
    

    @app.get("/api/projects/{project_id}/events")
    async def project_events(request: Request, project_id: str):
        user_id = get_session_user(request)
        conn = db()
        proj = conn.execute(
            "SELECT id, user_id FROM projects WHERE id = ? LIMIT 1",
            (project_id,),
        ).fetchone()
        if not proj:
            conn.close()
            raise HTTPException(404, "Project not found")
        owner_user_id = str(proj["user_id"] or "").strip()
        is_owner = owner_user_id == user_id
        if not is_owner:
            membership = conn.execute(
                """
                SELECT id
                FROM project_external_agent_memberships
                WHERE project_id = ? AND member_user_id = ? AND status = 'active'
                LIMIT 1
                """,
                (project_id, user_id),
            ).fetchone()
            if not membership:
                conn.close()
                raise HTTPException(404, "Project not found")
        conn.close()

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

