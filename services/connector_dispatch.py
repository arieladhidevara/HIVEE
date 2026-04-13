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
    """Return connector row if project uses connector backend mode, else None."""
    conn = db()
    project = conn.execute(
        "SELECT backend_mode, connector_id FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not project or str(project["backend_mode"] or "direct_openclaw") != "connector":
        conn.close()
        return None
    connector_id = str(project["connector_id"] or "").strip()
    if not connector_id:
        conn.close()
        return None
    row = conn.execute(
        "SELECT * FROM connectors WHERE id = ?",
        (connector_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


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
    """Return the best online connector for a user, preferring ones with
    internal/Docker OpenClaw URLs over public HTTPS URLs (which often can't chat)."""
    conn = db()
    rows = conn.execute(
        """
        SELECT * FROM connectors
        WHERE user_id = ? AND status = 'online'
        ORDER BY last_seen_at DESC
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


async def connector_chat_sync(
    connector_id: str,
    message: str,
    agent_id: Optional[str] = None,
    session_key: Optional[str] = None,
    timeout_sec: int = 45,
    *,
    from_agent_id: Optional[str] = None,
    from_label: Optional[str] = None,
    context_type: Optional[str] = None,
    project_id: Optional[str] = None,
    hivee_api_base: Optional[str] = None,
) -> Dict[str, Any]:
    """Enqueue an openclaw.chat command to a connector and poll for the result.
    Blocks up to `timeout_sec` seconds. Returns a dict compatible with openclaw_chat output.

    from_agent_id: who is sending this message (agent_id or 'hivee' for system messages)
    from_label: human-readable label for the sender
    context_type: what kind of message (plan_generation, delegation, task_execution, mention, control)
    project_id: which project this message belongs to
    hivee_api_base: base URL for agent to call back to Hivee
    """
    import asyncio

    payload = {
        "message": message,
        "agentId": agent_id or "openclaw/default",
        "hivee": {
            "from": from_agent_id or "hivee",
            "fromLabel": from_label or "Hivee System",
            "contextType": context_type or "message",
            "projectId": project_id or "",
            "hiveeApiBase": hivee_api_base or "",
            "fundamentalsUrl": f"{hivee_api_base.rstrip('/')}/files/fundamentals.md" if hivee_api_base else "",
        },
    }
    if session_key:
        payload["sessionKey"] = session_key

    command_id = enqueue_connector_command(
        connector_id=connector_id,
        command_type="openclaw.chat",
        payload=payload,
    )

    # Poll for result
    deadline = time.time() + timeout_sec
    poll_interval = 0.5  # start fast
    while time.time() < deadline:
        await asyncio.sleep(poll_interval)
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
                    "transport": f"http via connector",
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

    # Timeout
    # Mark command as timed out
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
        "transport": "connector",
        "connector_id": connector_id,
        "command_id": command_id,
    }


__all__ = [name for name in globals() if not name.startswith('__')]
