from hivee_shared import *
from services.connector_dispatch import connector_chat_sync as _connector_chat_sync
from services.project_utils import _project_planning_session_key


def _get_connector_row(connection_id: str, user_id: str, conn) -> Optional[Dict]:
    """Return connector row if connection_id maps to a connector owned by user, else None."""
    row = conn.execute(
        "SELECT * FROM connectors WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    return dict(row) if row else None


def _is_retired_direct_connection(connection_id: str, user_id: str, conn) -> bool:
    row = conn.execute(
        "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ? LIMIT 1",
        (connection_id, user_id),
    ).fetchone()
    return bool(row)


def _raise_direct_connection_retired() -> None:
    raise HTTPException(
        410,
        "Direct OpenClaw connections are retired. Pair/start a Hivee Hub instead.",
    )


def _get_connector_agents(connector_id: str, conn) -> List[Dict]:
    """Return agents list from the latest connector snapshot."""
    snap = conn.execute(
        "SELECT snapshot_json FROM connector_agent_snapshots WHERE connector_id = ? ORDER BY updated_at DESC LIMIT 1",
        (connector_id,),
    ).fetchone()
    if not snap:
        return []
    try:
        data = json.loads(str(snap["snapshot_json"] or "{}"))
        return [
            {"id": str(a.get("id") or ""), "name": str(a.get("name") or a.get("id") or ""), "source": "connector"}
            for a in (data.get("agents") or [])
            if str(a.get("id") or "").strip()
        ]
    except Exception:
        return []

def _list_connection_delete_blockers(conn, *, user_id: str, connection_id: str) -> Dict[str, Any]:
    owner_projects = conn.execute(
        """
        SELECT id, title
        FROM projects
        WHERE user_id = ? AND connection_id = ?
        ORDER BY created_at DESC
        LIMIT 6
        """,
        (user_id, connection_id),
    ).fetchall()
    active_memberships = conn.execute(
        """
        SELECT pem.project_id, pem.agent_id, p.title AS project_title
        FROM project_external_agent_memberships pem
        LEFT JOIN projects p ON p.id = pem.project_id
        WHERE pem.member_user_id = ? AND pem.member_connection_id = ? AND pem.status = 'active'
        ORDER BY pem.updated_at DESC, pem.created_at DESC
        LIMIT 6
        """,
        (user_id, connection_id),
    ).fetchall()
    return {
        "projects": [
            {"id": str(row["id"] or ""), "title": str(row["title"] or row["id"] or "").strip() or "Project"}
            for row in owner_projects
        ],
        "active_memberships": [
            {
                "project_id": str(row["project_id"] or ""),
                "project_title": str(row["project_title"] or row["project_id"] or "").strip() or "Project",
                "agent_id": str(row["agent_id"] or ""),
            }
            for row in active_memberships
        ],
    }

def _cleanup_connection_runtime_state(conn, *, user_id: str, connection_id: str) -> None:
    conn.execute("DELETE FROM managed_agent_approval_rules WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agent_metrics WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agent_permissions WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agent_checkpoints WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agent_history WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agent_memory WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM managed_agents WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))
    conn.execute("DELETE FROM connection_policies WHERE user_id = ? AND connection_id = ?", (user_id, connection_id))

def _delete_connection_secret_rows(conn, *, user_id: str, secret_id: Optional[str]) -> None:
    sid = str(secret_id or "").strip()
    if not sid:
        return
    row = conn.execute(
        "SELECT id FROM user_secrets WHERE id = ? AND user_id = ? LIMIT 1",
        (sid, user_id),
    ).fetchone()
    if not row:
        return
    conn.execute("DELETE FROM user_secret_versions WHERE secret_id = ?", (sid,))
    conn.execute("DELETE FROM user_secrets WHERE id = ?", (sid,))


def register_routes(app: FastAPI) -> None:
    @app.post("/api/openclaw/connect")
    async def connect_openclaw(request: Request, payload: ConnectIn):
        raise HTTPException(
            410,
            "Direct OpenClaw connections are retired. Pair a Hivee Hub instead.",
        )
        user_id = get_session_user(request)
        primary_env = _ensure_primary_environment_for_user(user_id)
        env_id = str(primary_env.get("id") or "").strip() or None
        if not (payload.base_url.startswith("http://") or payload.base_url.startswith("https://")):
            raise HTTPException(400, "base_url must start with http:// or https://")
    
        health = await openclaw_health(payload.base_url, payload.api_key)
        if not health.get("ok"):
            raise HTTPException(400, {"message": "Could not verify OpenClaw health", "details": health})

        # Health passed — save the connection before attempting bootstrap so a
        # temporarily-unavailable agent endpoint doesn't block the save.
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
            bootstrap_error=None if bootstrap_ok else detail_to_text(bootstrap.get("error")),
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
                    "Connected, but bootstrap failed. Fix OpenClaw HTTP endpoint config and retry bootstrap "
                    f"via POST /api/openclaw/{conn_id}/bootstrap."
                )
                response["hint"] = (
                    bootstrap.get("hint")
                    or "Enable gateway.http.endpoints.chatCompletions.enabled=true in OpenClaw to allow HTTP chat and agent listing."
                )
        return response
    
    @app.post("/api/openclaw/{connection_id}/bootstrap")
    async def bootstrap_openclaw_connection(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        connector_row = _get_connector_row(connection_id, user_id, conn)
        if connector_row:
            agents = _get_connector_agents(connection_id, conn)
            first = agents[0] if agents else None
            main_agent_id = first["id"] if first else None
            main_agent_name = first["name"] if first else None
            workspace = _ensure_user_workspace(user_id)
            if agents:
                _provision_managed_agents_for_connection(
                    user_id=user_id,
                    env_id=None,
                    connection_id=connection_id,
                    base_url=str(connector_row.get("openclaw_base_url") or ""),
                    raw_agents=agents,
                    fallback_agent_id=main_agent_id,
                    fallback_agent_name=main_agent_name,
                )
            _upsert_connection_policy(
                connection_id,
                user_id,
                main_agent_id=main_agent_id,
                main_agent_name=main_agent_name,
                bootstrap_status="ok" if agents else "no_agents",
                bootstrap_error=None if agents else "No agents found in connector snapshot",
                workspace_tree=workspace.get("workspace_tree"),
                workspace_root=workspace["workspace_root"],
                templates_root=workspace["templates_root"],
            )
            conn.close()
            return {
                "ok": True,
                "agents": agents,
                "main_agent_id": main_agent_id,
                "main_agent_name": main_agent_name,
                "workspace_root": workspace["workspace_root"],
                "transport": "connector",
                "mode": "connector",
            }
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if row:
            conn.close()
            _raise_direct_connection_retired()

        # ── Connector mode ────────────────────────────────────────────────
        if not row:
            connector_row = _get_connector_row(connection_id, user_id, conn)
            if not connector_row:
                conn.close()
                raise HTTPException(404, "Connection not found")
            agents = _get_connector_agents(connection_id, conn)
            first = agents[0] if agents else None
            main_agent_id = first["id"] if first else None
            main_agent_name = first["name"] if first else None
            workspace = _ensure_user_workspace(user_id)
            if agents:
                _provision_managed_agents_for_connection(
                    user_id=user_id,
                    env_id=None,
                    connection_id=connection_id,
                    base_url=str(connector_row.get("openclaw_base_url") or ""),
                    raw_agents=agents,
                    fallback_agent_id=main_agent_id,
                    fallback_agent_name=main_agent_name,
                )
            _upsert_connection_policy(
                connection_id,
                user_id,
                main_agent_id=main_agent_id,
                main_agent_name=main_agent_name,
                bootstrap_status="ok" if agents else "no_agents",
                bootstrap_error=None if agents else "No agents found in connector snapshot",
                workspace_tree=workspace.get("workspace_tree"),
                workspace_root=workspace["workspace_root"],
                templates_root=workspace["templates_root"],
            )
            conn.close()
            return {
                "ok": True,
                "agents": agents,
                "main_agent_id": main_agent_id,
                "main_agent_name": main_agent_name,
                "workspace_root": workspace["workspace_root"],
                "transport": "connector",
                "mode": "connector",
            }
        # ── Direct OpenClaw mode ──────────────────────────────────────────
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row)
        conn.close()
    
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
            bootstrap_error=None if _bs_ok else detail_to_text(bootstrap.get("error")),
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
        connector_rows = conn.execute(
            """
            SELECT id, name, openclaw_base_url, status
            FROM connectors
            WHERE user_id = ?
            ORDER BY CASE WHEN status = 'online' THEN 0 ELSE 1 END, last_seen_at DESC
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        result: List[ConnectionOut] = []
        for r in connector_rows:
            result.append(ConnectionOut(
                id=r["id"],
                base_url=str(r["openclaw_base_url"] or ""),
                name=str(r["name"] or "Hub"),
                mode="connector",
                connector_id=r["id"],
            ))
        return result

    @app.delete("/api/openclaw/{connection_id}")
    async def delete_connection(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        direct_row = conn.execute(
            "SELECT id, name, base_url, api_key_secret_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        connector_row = None if direct_row else _get_connector_row(connection_id, user_id, conn)
        if not direct_row and not connector_row:
            conn.close()
            raise HTTPException(404, "Connection not found")

        blockers = _list_connection_delete_blockers(conn, user_id=user_id, connection_id=connection_id)
        if blockers["projects"] or blockers["active_memberships"]:
            conn.close()
            raise HTTPException(
                409,
                {
                    "message": "Connection is still in use",
                    "references": blockers,
                    "hint": "Reassign or remove the listed project/membership references first, then delete this connection.",
                },
            )

        mode = "connector" if connector_row else "direct"
        name = str((connector_row or {}).get("name") or (direct_row["name"] if direct_row else "") or connection_id)
        base_url = str((connector_row or {}).get("openclaw_base_url") or (direct_row["base_url"] if direct_row else "") or "")
        try:
            _cleanup_connection_runtime_state(conn, user_id=user_id, connection_id=connection_id)
            if connector_row:
                conn.execute("DELETE FROM connector_command_results WHERE connector_id = ?", (connection_id,))
                conn.execute("DELETE FROM connector_commands WHERE connector_id = ?", (connection_id,))
                conn.execute("DELETE FROM connector_agent_snapshots WHERE connector_id = ?", (connection_id,))
                conn.execute(
                    "UPDATE connector_pairing_tokens SET used_by_connector_id = NULL WHERE user_id = ? AND used_by_connector_id = ?",
                    (user_id, connection_id),
                )
                conn.execute("DELETE FROM connectors WHERE id = ? AND user_id = ?", (connection_id, user_id))
            else:
                _delete_connection_secret_rows(
                    conn,
                    user_id=user_id,
                    secret_id=str(direct_row["api_key_secret_id"] or "").strip() or None,
                )
                conn.execute("DELETE FROM openclaw_connections WHERE id = ? AND user_id = ?", (connection_id, user_id))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            conn.close()
            raise HTTPException(500, f"Failed to delete connection: {detail_to_text(exc)}")
        conn.close()
        return {
            "ok": True,
            "deleted": {
                "id": connection_id,
                "mode": mode,
                "name": name,
                "base_url": base_url,
            },
        }

    @app.get("/api/openclaw/{connection_id}/agents")
    async def list_agents(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        connector_row = _get_connector_row(connection_id, user_id, conn)
        if connector_row:
            agents = _get_connector_agents(connection_id, conn)
            if agents:
                _provision_managed_agents_for_connection(
                    user_id=user_id,
                    env_id=None,
                    connection_id=connection_id,
                    base_url=str(connector_row.get("openclaw_base_url") or ""),
                    raw_agents=agents,
                    fallback_agent_id=str(agents[0].get("id") or "").strip() or None,
                    fallback_agent_name=str(agents[0].get("name") or "").strip() or None,
                )
            saved_rows = conn.execute(
                "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND status = 'active' ORDER BY agent_name ASC",
                (user_id, connection_id),
            ).fetchall()
            conn.close()
            snap_ids = {a["id"] for a in agents}
            for r in saved_rows:
                if str(r["agent_id"]) not in snap_ids:
                    agents.append({"id": str(r["agent_id"]), "name": str(r["agent_name"]), "source": "saved"})
            connector_status = str(connector_row.get("status") or "offline")
            conn_state = "healthy_connection" if connector_status == "online" else "agent_discovery_failed"
            return {
                "ok": True,
                "agents": agents,
                "agents_source": "connector" if agents else ("saved" if saved_rows else "none"),
                "transport": "connector",
                "connection_state": conn_state,
            }
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if row:
            conn.close()
            _raise_direct_connection_retired()

        # ── Connector mode ────────────────────────────────────────────────
        if not row:
            connector_row = _get_connector_row(connection_id, user_id, conn)
            if not connector_row:
                conn.close()
                raise HTTPException(404, "Connection not found")
            agents = _get_connector_agents(connection_id, conn)
            if agents:
                _provision_managed_agents_for_connection(
                    user_id=user_id,
                    env_id=None,
                    connection_id=connection_id,
                    base_url=str(connector_row.get("openclaw_base_url") or ""),
                    raw_agents=agents,
                    fallback_agent_id=str(agents[0].get("id") or "").strip() or None,
                    fallback_agent_name=str(agents[0].get("name") or "").strip() or None,
                )
            # Also merge any previously provisioned managed_agents for this connector
            saved_rows = conn.execute(
                "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND status = 'active' ORDER BY agent_name ASC",
                (user_id, connection_id),
            ).fetchall()
            conn.close()
            snap_ids = {a["id"] for a in agents}
            for r in saved_rows:
                if str(r["agent_id"]) not in snap_ids:
                    agents.append({"id": str(r["agent_id"]), "name": str(r["agent_name"]), "source": "saved"})
            connector_status = str(connector_row.get("status") or "offline")
            conn_state = "healthy_connection" if connector_status == "online" else "agent_discovery_failed"
            return {
                "ok": True,
                "agents": agents,
                "agents_source": "connector" if agents else "none",
                "transport": "connector",
                "connection_state": conn_state,
            }

        # ── Direct OpenClaw mode ──────────────────────────────────────────
        connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=row)
        # Load previously provisioned agents from DB as fallback for when live discovery fails.
        saved_rows = conn.execute(
            "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND status = ? ORDER BY agent_name ASC",
            (user_id, connection_id, "active"),
        ).fetchall()
        conn.close()
        saved_agents = [{"id": r["agent_id"], "name": r["agent_name"], "source": "saved"} for r in saved_rows]

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
                "warning": res.get("error") or "Agent listing unavailable; REST agent endpoints are not exposed.",
                "hint": res.get("hint") or "Enable gateway.http.endpoints.chatCompletions.enabled=true in OpenClaw to allow HTTP agent listing.",
            }

        # Refresh the managed-agent index from the active connection so card
        # metadata stays in sync even when an agent id is unchanged.
        live_agents = [a for a in (res.get("agents") or []) if isinstance(a, dict)]
        if live_agents:
            env_id = str(row["env_id"] or "").strip() or None
            _provision_managed_agents_for_connection(
                user_id=user_id,
                env_id=env_id,
                connection_id=connection_id,
                base_url=row["base_url"],
                raw_agents=live_agents,
            )

        return res
    
    @app.get("/api/openclaw/{connection_id}/policy", response_model=ConnectionPolicyOut)
    async def get_connection_policy(request: Request, connection_id: str):
        user_id = get_session_user(request)
        workspace = _ensure_user_workspace(user_id)
        conn = db()
        connector_exists = conn.execute(
            "SELECT id FROM connectors WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if not connector_exists:
            if _is_retired_direct_connection(connection_id, user_id, conn):
                conn.close()
                _raise_direct_connection_retired()
            conn.close()
            raise HTTPException(404, "Connection not found")
        exists = conn.execute(
            "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if not exists:
            # Accept connector IDs as virtual connections
            connector_exists = conn.execute(
                "SELECT id FROM connectors WHERE id = ? AND user_id = ?",
                (connection_id, user_id),
            ).fetchone()
            if not connector_exists:
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
        connector_row = _get_connector_row(connection_id, user_id, conn)
        if connector_row:
            policy = conn.execute(
                "SELECT main_agent_id, workspace_root FROM connection_policies WHERE connection_id = ? AND user_id = ?",
                (connection_id, user_id),
            ).fetchone()
            conn.close()

            workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
            main_agent_id = str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else ""
            main_agent_id = main_agent_id.strip() or None
            if payload.agent_id:
                if not main_agent_id:
                    raise HTTPException(400, "Main workspace agent is not configured. Re-run hub bootstrap.")
                if payload.agent_id != main_agent_id:
                    raise HTTPException(403, "Workspace chat can only target your main user agent")
            effective_agent_id = main_agent_id
            if not effective_agent_id:
                raise HTTPException(400, "Main workspace agent is not configured. Re-run hub bootstrap.")
            scoped_message = _compose_guardrailed_message(payload.message.strip(), workspace_root=workspace_root)
            res = await _connector_chat_sync(
                connector_id=connection_id,
                message=scoped_message,
                agent_id=effective_agent_id,
                session_key="workspace-chat",
                timeout_sec=90,
                from_agent_id="hivee",
                from_label="Hivee Workspace",
                context_type="message",
            )
            if not res.get("ok"):
                raise HTTPException(400, res)
            res["resolved_agent_id"] = effective_agent_id
            res["workspace_root"] = workspace_root
            return res
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if row:
            conn.close()
            _raise_direct_connection_retired()
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
        from services.connector_dispatch import connector_chat_sync as _connector_chat_sync_ws, get_user_online_connector as _get_oc_ws
        online_ws_connector = _get_oc_ws(user_id)
        if online_ws_connector:
            res = await _connector_chat_sync_ws(
                connector_id=str(online_ws_connector["id"]),
                message=scoped_message,
                agent_id=effective_agent_id,
                session_key="workspace-chat",
                timeout_sec=90,
                from_agent_id="hivee",
                from_label="Hivee Workspace",
                context_type="message",
            )
        else:
            raise HTTPException(400, "No hub available. Connect a Hivee Hub to use workspace chat.")
        if not res.get("ok"):
            raise HTTPException(400, res)
        res["resolved_agent_id"] = effective_agent_id
        res["workspace_root"] = workspace_root
        return res
    
    @app.post("/api/openclaw/{connection_id}/chat-runtime")
    async def chat_openclaw_runtime(request: Request, connection_id: str, payload: OpenClawWsChatIn):
        session_user: Optional[str] = None
        try:
            session_user = get_optional_session_user(request)
        except HTTPException:
            if not str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip():
                raise
            session_user = None
        a2a_access = _resolve_optional_a2a_agent_session(request, required_scope="project.write")
        user_id = str(session_user or (a2a_access.get("user_id") if a2a_access else "") or "").strip()
        if not user_id:
            raise HTTPException(401, "Missing authorization. Login first or use A2A agent session headers.")

        conn = db()
        row = conn.execute(
            "SELECT base_url, api_key, api_key_secret_id, env_id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if row:
            conn.close()
            _raise_direct_connection_retired()
        connector_row: Optional[Dict] = None
        if not row:
            connector_row = _get_connector_row(connection_id, user_id, conn)
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
        if not row and not connector_row:
            conn.close()
            raise HTTPException(404, "Connection not found")
        if a2a_access and row:
            connection_env_id = str(row["env_id"] or "").strip()
            if not connection_env_id or connection_env_id != str(a2a_access.get("env_id") or "").strip():
                conn.close()
                raise HTTPException(403, "A2A agent session is not linked to this connection")
        if a2a_access and connector_row:
            conn.close()
            raise HTTPException(403, "A2A agent session is not supported in connector mode")

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
                conn.close()
                raise HTTPException(403, "Workspace chat can only target agents available on this connection")
        project_root = str(project_scope["project_root"]) if (project_scope and project_scope["project_root"]) else None
        project_instruction = None
        write_allow_paths = None
        connector_session_key = session_key
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
            task_snapshot = _build_project_task_snapshot(session_key)
            if task_snapshot:
                sections.append(task_snapshot)
            chat_snapshot = _build_project_chat_snapshot(session_key)
            if chat_snapshot:
                sections.append(chat_snapshot)
            project_instruction = "\n\n".join([s for s in sections if str(s or "").strip()])
            if bool(selected_agent_permissions.get("can_write_files")):
                write_allow_paths = _normalize_permission_write_paths(
                    selected_agent_permissions.get("write_paths") or [],
                    fallback=[],
                )
            else:
                write_allow_paths = []
            connector_session_key = _project_planning_session_key(
                session_key,
                plan_status=project_scope["plan_status"],
                agent_id=effective_agent_id,
                primary_agent_id=project_primary_agent_id,
                default_session_key=session_key,
            )
        else:
            effective_agent_id = str(payload.agent_id or "").strip() or None
            if not effective_agent_id:
                if main_agent_id:
                    effective_agent_id = main_agent_id
                elif workspace_agent_ids:
                    effective_agent_id = workspace_agent_ids[0]
            session_key = "main"
            connector_session_key = session_key
            if not effective_agent_id:
                effective_agent_id = None

        conn.close()
        scoped_message = _compose_guardrailed_message(
            payload.message.strip(),
            workspace_root=workspace_root,
            project_root=project_root,
            task_instruction=project_instruction,
        )
        if connector_row:
            resolved_rt_connector_id = connection_id
        else:
            from services.connector_dispatch import get_user_online_connector as _get_rt_oc
            online_rt = _get_rt_oc(user_id)
            if not online_rt:
                raise HTTPException(400, "No hub available. Connect a Hivee Hub to use agent chat.")
            resolved_rt_connector_id = str(online_rt["id"])
        res = await _connector_chat_sync(
            connector_id=resolved_rt_connector_id,
            message=scoped_message,
            agent_id=effective_agent_id,
            session_key=connector_session_key,
            timeout_sec=payload.timeout_sec,
            from_agent_id="hivee",
            from_label="Hivee Runtime",
            context_type="message",
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
            action_payload = parsed_payload.get("actions") or []
            write_result = _apply_project_file_writes(
                owner_user_id=project_owner_user_id,
                project_root=str(project_scope["project_root"] or ""),
                writes=write_payload if isinstance(write_payload, list) else [],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                allow_paths=write_allow_paths,
            )
            saved_writes = write_result.get("saved") or []
            skipped_writes = write_result.get("skipped") or []
            action_result = _apply_project_actions(
                owner_user_id=project_owner_user_id,
                project_id=session_key,
                project_root=str(project_scope["project_root"] or ""),
                actions=action_payload if isinstance(action_payload, list) else [],
                allow_paths=write_allow_paths,
                actor_type="project_agent",
                actor_id=str(effective_agent_id or "").strip() or None,
                actor_label=f"agent:{effective_agent_id or 'unknown'}",
            )
            applied_actions = action_result.get("applied") or []
            skipped_actions = action_result.get("skipped") or []
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
                followup_res = await _connector_chat_sync(
                    connector_id=resolved_rt_connector_id,
                    message=followup_prompt,
                    agent_id=effective_agent_id,
                    session_key=connector_session_key,
                    timeout_sec=max(10, min(payload.timeout_sec, 60)),
                    from_agent_id="hivee",
                    from_label="Hivee Runtime",
                    context_type="message",
                )
                if followup_res.get("ok"):
                    followup_text = str(followup_res.get("text") or "").strip()
                    followup_parsed = _extract_agent_report_payload(followup_text)
                    followup_chat_update = str(followup_parsed.get("chat_update") or "").strip()
                    followup_writes_raw = followup_parsed.get("output_files") or []
                    followup_actions_raw = followup_parsed.get("actions") or []
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
                    followup_action_result = _apply_project_actions(
                        owner_user_id=project_owner_user_id,
                        project_id=session_key,
                        project_root=str(project_scope["project_root"] or ""),
                        actions=followup_actions_raw if isinstance(followup_actions_raw, list) else [],
                        allow_paths=write_allow_paths,
                        actor_type="project_agent",
                        actor_id=str(effective_agent_id or "").strip() or None,
                        actor_label=f"agent:{effective_agent_id or 'unknown'}",
                    )
                    followup_applied_actions = followup_action_result.get("applied") or []
                    followup_skipped_actions = followup_action_result.get("skipped") or []
                    if followup_saved:
                        saved_writes.extend(followup_saved)
                    if followup_skipped:
                        skipped_writes.extend(followup_skipped)
                    if followup_applied_actions:
                        applied_actions.extend(followup_applied_actions)
                    if followup_skipped_actions:
                        skipped_actions.extend(followup_skipped_actions)
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
                rescue_res = await _connector_chat_sync(
                    connector_id=resolved_rt_connector_id,
                    message=rescue_prompt,
                    agent_id=effective_agent_id,
                    session_key=connector_session_key,
                    timeout_sec=max(10, min(payload.timeout_sec, 60)),
                    from_agent_id="hivee",
                    from_label="Hivee Runtime",
                    context_type="message",
                )
                if rescue_res.get("ok"):
                    rescue_text = str(rescue_res.get("text") or "").strip()
                    rescue_parsed = _extract_agent_report_payload(rescue_text)
                    rescue_chat_update = str(rescue_parsed.get("chat_update") or "").strip()
                    rescue_writes_raw = rescue_parsed.get("output_files") or []
                    rescue_actions_raw = rescue_parsed.get("actions") or []
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
                    rescue_action_result = _apply_project_actions(
                        owner_user_id=project_owner_user_id,
                        project_id=session_key,
                        project_root=str(project_scope["project_root"] or ""),
                        actions=rescue_actions_raw if isinstance(rescue_actions_raw, list) else [],
                        allow_paths=write_allow_paths,
                        actor_type="project_agent",
                        actor_id=str(effective_agent_id or "").strip() or None,
                        actor_label=f"agent:{effective_agent_id or 'unknown'}",
                    )
                    rescue_applied_actions = rescue_action_result.get("applied") or []
                    rescue_skipped_actions = rescue_action_result.get("skipped") or []
                    if rescue_saved:
                        saved_writes.extend(rescue_saved)
                    if rescue_skipped:
                        skipped_writes.extend(rescue_skipped)
                    if rescue_applied_actions:
                        applied_actions.extend(rescue_applied_actions)
                    if rescue_skipped_actions:
                        skipped_actions.extend(rescue_skipped_actions)
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
            if str(res.get("text") or "").strip() and not has_explicit_chat_action:
                chat_conn = db()
                try:
                    auto_chat_message = _create_project_chat_message(
                        chat_conn,
                        project_id=session_key,
                        author_type="project_agent",
                        author_id=str(effective_agent_id or "").strip() or None,
                        author_label=next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(effective_agent_id or "")), "") or f"agent:{effective_agent_id or 'unknown'}",
                        text=str(res.get("text") or "").strip(),
                        metadata={
                            "source": "openclaw.chat_runtime",
                            "requires_user_input": bool(res.get("requires_user_input")),
                            "saved_files": len(saved_writes),
                            "applied_actions": len(applied_actions),
                        },
                    )
                    chat_conn.commit()
                finally:
                    chat_conn.close()
            if isinstance(auto_chat_message, dict):
                await emit(session_key, "project.chat.message", auto_chat_message)
                for target in (auto_chat_message.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]:
                    await emit(
                        session_key,
                        "project.chat.mention",
                        {
                            "message_id": str(auto_chat_message.get("id") or ""),
                            "project_id": session_key,
                            "target": target,
                            "author_type": str(auto_chat_message.get("author_type") or ""),
                            "author_id": auto_chat_message.get("author_id"),
                            "author_label": auto_chat_message.get("author_label"),
                            "text": str(auto_chat_message.get("text") or "")[:500],
                            "created_at": int(auto_chat_message.get("created_at") or 0),
                        },
                    )
                    # Route @mention to the mentioned agent's connector
                    import asyncio
                    asyncio.ensure_future(_dispatch_chat_mention_to_connector(
                        project_id=session_key,
                        mention_target=target,
                        message_text=str(auto_chat_message.get("text") or "")[:500],
                        from_agent_id=str(auto_chat_message.get("author_id") or "agent"),
                        from_label=str(auto_chat_message.get("author_label") or "Agent"),
                    ))
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
            for item in applied_actions:
                event_name = str(item.get("event") or "").strip()
                event_payload = item.get("event_payload") if isinstance(item.get("event_payload"), dict) else {}
                if event_name:
                    await emit(session_key, event_name, event_payload)
                for extra in (item.get("extra_events") or []):
                    if not isinstance(extra, dict):
                        continue
                    extra_event_name = str(extra.get("event") or "").strip()
                    extra_event_payload = extra.get("event_payload") if isinstance(extra.get("event_payload"), dict) else {}
                    if extra_event_name:
                        await emit(session_key, extra_event_name, extra_event_payload)
                    # Route @mentions to the mentioned agent's connector
                    if extra_event_name == "project.chat.mention" and isinstance(extra_event_payload, dict):
                        import asyncio
                        asyncio.ensure_future(_dispatch_chat_mention_to_connector(
                            project_id=session_key,
                            mention_target=str(extra_event_payload.get("target") or ""),
                            message_text=str(extra_event_payload.get("text") or ""),
                            from_agent_id=str(extra_event_payload.get("author_id") or extra_event_payload.get("author_label") or "agent"),
                            from_label=str(extra_event_payload.get("author_label") or "Agent"),
                        ))
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
            if applied_actions:
                await emit(
                    session_key,
                    "agent.chat.actions_applied",
                    {
                        "agent_id": effective_agent_id,
                        "applied_actions": applied_actions[:20],
                        "skipped_actions": skipped_actions[:10],
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
                    f"ACTIONS_APPLIED: {len(applied_actions)}\n"
                    f"ARTIFACT_FOLLOWUP_USED: {'yes' if artifact_followup_used else 'no'}\n"
                    f"ARTIFACT_RESCUE_USED: {'yes' if artifact_rescue_used else 'no'}"
                ),
                payload={
                    "saved_files": saved_writes,
                    "skipped_files": skipped_writes[:10],
                    "applied_actions": applied_actions[:20],
                    "skipped_actions": skipped_actions[:10],
                    "requires_user_input": bool(res.get("requires_user_input")),
                    "pause_reason": str(res.get("pause_reason") or "")[:500],
                    "resume_hint": str(res.get("resume_hint") or "")[:300],
                },
            )
            res["saved_files"] = saved_writes
            res["skipped_files"] = skipped_writes[:20]
            res["applied_actions"] = applied_actions[:20]
            res["skipped_actions"] = skipped_actions[:20]
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
