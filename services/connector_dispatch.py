from hivee_shared import *


def connector_auth(request: Request, connector_id: str) -> Dict[str, Any]:
    """Validate Bearer connectorSecret and return connector row dict.
    Raises HTTPException 401/404 on failure."""
    auth = str(request.headers.get("Authorization") or "").strip()
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Authorization: Bearer <connectorSecret>")
    provided_secret = auth[len("Bearer "):].strip()
    if not provided_secret:
        raise HTTPException(401, "Empty connector secret")

    conn = db()
    row = conn.execute(
        "SELECT * FROM connectors WHERE id = ?",
        (connector_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connector not found")
    if not secrets.compare_digest(str(row["secret"]), provided_secret):
        raise HTTPException(401, "Invalid connector secret")
    return dict(row)


def enqueue_connector_command(
    connector_id: str,
    command_type: str,
    payload: Dict[str, Any],
    *,
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> str:
    """Insert a command into connector_commands queue. Returns command_id."""
    now = int(time.time())
    command_id = new_id("ccmd")
    conn = db()
    conn.execute(
        """
        INSERT INTO connector_commands (id, connector_id, project_id, task_id, command_type, payload_json, status, created_at)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (command_id, connector_id, project_id, task_id, command_type, json.dumps(payload, ensure_ascii=False), "queued", now),
    )
    conn.commit()
    conn.close()
    return command_id


def get_project_connector(project_id: str) -> Optional[Dict[str, Any]]:
    """Return the best connector row for a project, with legacy fallback."""
    conn = db()
    project = conn.execute(
        "SELECT user_id, backend_mode, connector_id FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not project:
        conn.close()
        return None
    connector_id = str(project["connector_id"] or "").strip()
    row = None
    if connector_id:
        row = conn.execute(
            "SELECT * FROM connectors WHERE id = ?",
            (connector_id,),
        ).fetchone()
    user_id = str(project["user_id"] or "").strip()
    conn.close()
    if row:
        return dict(row)
    if not user_id:
        return None
    return get_user_online_connector(user_id)


def latest_connector_result(command_id: str) -> Optional[Dict[str, Any]]:
    """Return the latest result for a command_id, or None."""
    conn = db()
    row = conn.execute(
        """
        SELECT id, command_id, connector_id, ok, result_json, created_at
        FROM connector_command_results
        WHERE command_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (command_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    result = dict(row)
    raw = str(result.get("result_json") or "").strip()
    if raw:
        try:
            result["result"] = json.loads(raw)
        except Exception:
            result["result"] = {}
    else:
        result["result"] = {}
    return result


def get_user_online_connector(user_id: str) -> Optional[Dict[str, Any]]:
    """Return the best live connector for a user, preferring ones with
    internal/Docker OpenClaw URLs over public HTTPS URLs (which often can't chat)."""
    conn = db()
    rows = conn.execute(
        """
        SELECT * FROM connectors
        WHERE user_id = ? AND status IN ('online', 'active')
        ORDER BY CASE
            WHEN status = 'online' THEN 0
            WHEN status = 'active' THEN 1
            ELSE 2
        END, last_seen_at DESC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    if not rows:
        return None

    # Prefer connectors with internal (http://) OpenClaw URLs — they can chat.
    # Public HTTPS URLs often can't chat due to gateway config.
    best = None
    for row in rows:
        r = dict(row)
        oc_url = str(r.get("openclaw_base_url") or "").lower()
        # Internal Docker URLs like http://openclaw:18790
        if oc_url.startswith("http://") and not any(pub in oc_url for pub in [".hstgr.cloud", ".ngrok", ".loca.lt", ".trycloudflare"]):
            return r  # Best: internal Docker network
        if best is None:
            best = r
    return best


def _connector_is_alive(connector_id: str, stale_threshold_sec: int = 45) -> bool:
    """Return True if the connector has sent a heartbeat within stale_threshold_sec."""
    conn = db()
    row = conn.execute(
        "SELECT last_seen_at FROM connectors WHERE id = ?",
        (connector_id,),
    ).fetchone()
    conn.close()
    if not row or not row["last_seen_at"]:
        return False
    return (time.time() - int(row["last_seen_at"])) < stale_threshold_sec


async def connector_chat_sync(
    connector_id: str,
    message: str,
    agent_id: Optional[str] = None,
    session_key: Optional[str] = None,
    timeout_sec: Optional[int] = 45,
    *,
    from_agent_id: Optional[str] = None,
    from_label: Optional[str] = None,
    context_type: Optional[str] = None,
    project_id: Optional[str] = None,
    hivee_api_base: Optional[str] = None,
    project_agent_id: Optional[str] = None,
    project_agent_token: Optional[str] = None,
    project_root: Optional[str] = None,
    workspace_root: Optional[str] = None,
) -> Dict[str, Any]:
    """Enqueue an openclaw.chat command to a connector and poll for the result.

    If timeout_sec is None, polls indefinitely — stopping only if the connector
    goes offline (no heartbeat for >45s). Use this for long-running background
    tasks like plan generation.

    If timeout_sec is set, also enforces a hard deadline as a safety net.

    from_agent_id: who is sending this message (agent_id or 'hivee' for system messages)
    from_label: human-readable label for the sender
    context_type: what kind of message (plan_generation, delegation, task_execution, mention, control)
    project_id: which project this message belongs to
    hivee_api_base: base URL for agent to call back to Hivee
    """
    import asyncio

    # Never silently substitute openclaw/default — projects must dispatch to a real
    # user-selected agent. If agent_id is empty, the caller has a bug; surface that
    # by passing the empty value through so the connector returns an actionable error.
    payload = {
        "message": message,
        "agentId": str(agent_id or "").strip(),
        "hivee": {
            "from": from_agent_id or "hivee",
            "fromLabel": from_label or "Hivee System",
            "contextType": context_type or "message",
            "projectId": project_id or "",
            "hiveeApiBase": hivee_api_base or "",
            "fundamentalsUrl": f"{hivee_api_base.rstrip('/')}/files/fundamentals.md" if hivee_api_base else "",
            "projectAgentId": project_agent_id or "",
            "projectAgentToken": project_agent_token or "",
            "projectRoot": project_root or "",
            "workspaceRoot": workspace_root or "",
            "projectAuthHeaders": {
                "X-Project-Agent-Id": project_agent_id or "",
                "X-Project-Agent-Token": project_agent_token or "",
            },
        },
    }
    if session_key:
        payload["sessionKey"] = session_key

    command_id = enqueue_connector_command(
        connector_id=connector_id,
        command_type="openclaw.chat",
        payload=payload,
    )

    liveness_only = timeout_sec is None
    deadline = None if liveness_only else (time.time() + timeout_sec)
    poll_interval = 0.5  # start fast
    polls_since_liveness_check = 0

    while True:
        # Hard deadline check (only when timeout_sec is set)
        if deadline is not None and time.time() >= deadline:
            break

        await asyncio.sleep(poll_interval)
        polls_since_liveness_check += 1

        # Check connector liveness every ~15s of wall time
        if polls_since_liveness_check >= 8:
            polls_since_liveness_check = 0
            if not _connector_is_alive(connector_id):
                conn = db()
                conn.execute(
                    "UPDATE connector_commands SET status = 'timeout' WHERE id = ? AND status = 'queued'",
                    (command_id,),
                )
                conn.commit()
                conn.close()
                return {
                    "ok": False,
                    "error": "Connector went offline while waiting for agent response.",
                    "error_code": "connector_offline",
                    "transport": "connector",
                    "connector_id": connector_id,
                    "command_id": command_id,
                    "project_id": project_id or "",
                    "agent_id": str(agent_id or "").strip(),
                }

        result = latest_connector_result(command_id)
        if result is not None:
            inner = result.get("result") or {}
            ok = bool(result.get("ok"))
            # The connector returns: { ok, output: { ok, text, transport, raw } }
            output = inner.get("output") or {}
            text = str(
                output.get("text")
                or output.get("response")
                or inner.get("text")
                or inner.get("response")
                or ""
            ).strip()
            transport_used = output.get("transport") or "connector"
            print(f"[connector_chat_sync] ok={ok} text_len={len(text)} transport={transport_used}", flush=True)
            if ok and text:
                return {
                    "ok": True,
                    "text": text,
                    "transport": "http via connector",
                    "path": "connector",
                    "response": output.get("raw") or output or inner,
                    "connector_id": connector_id,
                    "command_id": command_id,
                }
            elif ok:
                # Got result but no text — might be an error in the output
                err_msg = str(output.get("error") or inner.get("error") or "(empty response)")
                return {
                    "ok": False,
                    "error": err_msg,
                    "transport": "connector",
                    "connector_id": connector_id,
                    "command_id": command_id,
                }
            else:
                return {
                    "ok": False,
                    "error": str(inner.get("error") or output.get("error") or "Connector command failed"),
                    "transport": "connector",
                    "connector_id": connector_id,
                    "command_id": command_id,
                }
        # Gradually slow down polling
        poll_interval = min(poll_interval * 1.3, 2.0)

    # Hard deadline reached
    conn = db()
    conn.execute(
        "UPDATE connector_commands SET status = 'timeout' WHERE id = ? AND status = 'queued'",
        (command_id,),
    )
    conn.commit()
    conn.close()

    return {
        "ok": False,
        "error": f"Connector chat timed out after {timeout_sec}s. The connector may be busy or offline.",
        "error_code": "delivery_timeout",
        "transport": "connector",
        "connector_id": connector_id,
        "command_id": command_id,
        "project_id": project_id or "",
        "agent_id": str(agent_id or "").strip(),
    }


__all__ = [name for name in globals() if not name.startswith('__')]
