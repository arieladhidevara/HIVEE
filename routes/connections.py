from hivee_shared import *


def _sanitize_runtime_type(raw_value: Any) -> str:
    runtime = str(raw_value or CONNECTION_RUNTIME_OPENCLAW).strip().lower()
    if not runtime:
        return CONNECTION_RUNTIME_OPENCLAW
    if runtime != CONNECTION_RUNTIME_OPENCLAW:
        raise HTTPException(400, "Unsupported runtime_type. openclaw is supported in this version.")
    return runtime


def _normalize_hub_status(raw_value: Any, default: str = HUB_STATUS_ONLINE) -> str:
    status = str(raw_value or default).strip().lower()
    if status in {HUB_STATUS_PENDING_INSTALL, HUB_STATUS_ONLINE, HUB_STATUS_OFFLINE, HUB_STATUS_ERROR}:
        return status
    return default


def _new_install_token() -> str:
    return f"hivee_install_{secrets.token_urlsafe(24)}"


def _issue_connection_install_token(
    conn: sqlite3.Connection,
    *,
    connection_id: str,
    ttl_sec: int = PROJECT_INSTALL_TOKEN_DEFAULT_TTL_SEC,
) -> Dict[str, Any]:
    now = int(time.time())
    ttl = max(60 * 15, min(int(ttl_sec or PROJECT_INSTALL_TOKEN_DEFAULT_TTL_SEC), 60 * 60 * 24 * 30))
    token = _new_install_token()
    token_hash = _hash_access_token(token)
    expires_at = now + ttl
    conn.execute(
        """
        UPDATE connections
        SET install_token_hash = ?, install_token_expires_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (token_hash, expires_at, now, connection_id),
    )
    return {
        "token": token,
        "token_hash": token_hash,
        "expires_at": expires_at,
    }


def _platform_install_instructions(origin: str, connection_id: str, install_token: str) -> Dict[str, str]:
    safe_origin = str(origin or "").strip().rstrip("/")
    if not safe_origin:
        safe_origin = "https://hivee.cloud"

    # TODO(hivee-hub-daemon): replace these textual snippets with signed installer manifests.
    linux_cmd = (
        "python -m pip install hivee-hub && "
        f"hivee-hub connect --cloud-url \"{safe_origin}\" --connection-id \"{connection_id}\" "
        f"--install-token \"{install_token}\" --runtime {CONNECTION_RUNTIME_OPENCLAW}"
    )
    mac_cmd = linux_cmd
    windows_cmd = (
        "py -m pip install hivee-hub; "
        f"hivee-hub connect --cloud-url \"{safe_origin}\" --connection-id \"{connection_id}\" "
        f"--install-token \"{install_token}\" --runtime {CONNECTION_RUNTIME_OPENCLAW}"
    )
    docker_cmd = (
        "docker run --rm "
        f"-e HIVEE_CLOUD_URL=\"{safe_origin}\" "
        f"-e HIVEE_CONNECTION_ID=\"{connection_id}\" "
        f"-e HIVEE_INSTALL_TOKEN=\"{install_token}\" "
        "-e HIVEE_RUNTIME_TYPE=\"openclaw\" "
        "ghcr.io/hiveecloud/hivee-hub:latest"
    )
    concept = (
        "Install Hivee Hub on the machine where your runtime is available. "
        "Hub connects outward to Hivee Cloud and discovers local runtime agents. "
        "Then it syncs managed agent cards and heartbeats back to this connection."
    )
    return {
        "concept": concept,
        "ubuntu_linux": linux_cmd,
        "macos": mac_cmd,
        "windows": windows_cmd,
        "docker": docker_cmd,
    }

def _connection_payload(row: sqlite3.Row, *, agent_count: int = 0) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "user_id": str(row["user_id"]),
        "label": str(row["label"] or "Connection"),
        "runtime_type": str(row["runtime_type"] or CONNECTION_RUNTIME_OPENCLAW),
        "hub_status": _normalize_hub_status(row["hub_status"], default=HUB_STATUS_PENDING_INSTALL),
        "os_type": str(row["os_type"] or "") or None,
        "machine_name": str(row["machine_name"] or "") or None,
        "hub_version": str(row["hub_version"] or "") or None,
        "last_heartbeat_at": _to_int(row["last_heartbeat_at"]),
        "install_token_expires_at": _to_int(row["install_token_expires_at"]),
        "created_at": _to_int(row["created_at"]),
        "updated_at": _to_int(row["updated_at"]),
        "legacy_openclaw_connection_id": str(row["legacy_openclaw_connection_id"] or "") or None,
        "managed_agents_count": int(agent_count or 0),
    }


def _require_connection_row(conn: sqlite3.Connection, *, connection_id: str, user_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, user_id, label, runtime_type, install_token_hash, install_token_expires_at,
               hub_status, os_type, machine_name, hub_version, last_heartbeat_at,
               legacy_openclaw_connection_id, created_at, updated_at
        FROM connections
        WHERE id = ? AND user_id = ?
        LIMIT 1
        """,
        (connection_id, user_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Connection not found")
    return row


def _require_hub_connection_auth(
    conn: sqlite3.Connection,
    *,
    connection_id: str,
    install_token: str,
) -> sqlite3.Row:
    token_hash = _hash_access_token(str(install_token or ""))
    now = int(time.time())
    row = conn.execute(
        """
        SELECT id, user_id, label, runtime_type, install_token_hash, install_token_expires_at,
               hub_status, os_type, machine_name, hub_version, last_heartbeat_at,
               legacy_openclaw_connection_id, created_at, updated_at
        FROM connections
        WHERE id = ? AND install_token_hash = ?
        LIMIT 1
        """,
        (connection_id, token_hash),
    ).fetchone()
    if not row:
        raise HTTPException(401, "Invalid hub install token")
    expires_at = _to_int(row["install_token_expires_at"])
    if expires_at and expires_at < now:
        raise HTTPException(401, "Hub install token expired")
    return row


def register_routes(app: FastAPI) -> None:
    @app.post("/api/connections")
    async def create_connection(request: Request, payload: ConnectionCreateIn):
        user_id = get_session_user(request)
        runtime_type = _sanitize_runtime_type(payload.runtime_type)
        label = str(payload.label or "").strip()[:160]
        if not label:
            raise HTTPException(400, "label is required")

        connection_id = new_id("conn")
        now = int(time.time())
        conn = db()
        conn.execute(
            """
            INSERT INTO connections (
                id, user_id, label, runtime_type, install_token_hash, install_token_expires_at,
                hub_status, os_type, machine_name, hub_version, last_heartbeat_at,
                legacy_openclaw_connection_id, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                connection_id,
                user_id,
                label,
                runtime_type,
                None,
                None,
                HUB_STATUS_PENDING_INSTALL,
                None,
                None,
                None,
                None,
                None,
                now,
                now,
            ),
        )
        token_data = _issue_connection_install_token(
            conn,
            connection_id=connection_id,
            ttl_sec=payload.token_ttl_sec,
        )
        conn.commit()
        row = _require_connection_row(conn, connection_id=connection_id, user_id=user_id)
        conn.close()

        instructions = _platform_install_instructions(
            _request_origin(request),
            connection_id,
            token_data["token"],
        )
        return {
            "ok": True,
            "connection": _connection_payload(row),
            "install_token": token_data["token"],
            "install_token_expires_at": token_data["expires_at"],
            "install_instructions": instructions,
        }

    @app.get("/api/connections")
    async def list_connections(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            """
            SELECT c.id, c.user_id, c.label, c.runtime_type, c.hub_status,
                   c.os_type, c.machine_name, c.hub_version, c.last_heartbeat_at,
                   c.install_token_expires_at, c.legacy_openclaw_connection_id,
                   c.created_at, c.updated_at,
                   COALESCE(ma.cnt, 0) AS managed_agents_count
            FROM connections c
            LEFT JOIN (
                SELECT connection_id, COUNT(1) AS cnt
                FROM managed_agents
                WHERE user_id = ?
                GROUP BY connection_id
            ) ma ON ma.connection_id = c.id
            WHERE c.user_id = ?
            ORDER BY c.updated_at DESC, c.created_at DESC
            """,
            (user_id, user_id),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "connections": [
                _connection_payload(r, agent_count=_to_int(r["managed_agents_count"]))
                for r in rows
            ],
        }

    @app.get("/api/connections/{connection_id}")
    async def get_connection(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = _require_connection_row(conn, connection_id=connection_id, user_id=user_id)
        count_row = conn.execute(
            "SELECT COUNT(1) AS c FROM managed_agents WHERE user_id = ? AND connection_id = ?",
            (user_id, connection_id),
        ).fetchone()
        conn.close()
        return {
            "ok": True,
            "connection": _connection_payload(row, agent_count=_to_int((count_row or {"c": 0})["c"])),
        }

    @app.post("/api/connections/{connection_id}/install-token/regenerate")
    async def regenerate_connection_install_token(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        _require_connection_row(conn, connection_id=connection_id, user_id=user_id)
        token_data = _issue_connection_install_token(conn, connection_id=connection_id)
        conn.execute(
            "UPDATE connections SET updated_at = ? WHERE id = ?",
            (int(time.time()), connection_id),
        )
        conn.commit()
        conn.close()
        return {
            "ok": True,
            "connection_id": connection_id,
            "install_token": token_data["token"],
            "install_token_expires_at": token_data["expires_at"],
            "install_instructions": _platform_install_instructions(
                _request_origin(request),
                connection_id,
                token_data["token"],
            ),
        }

    @app.get("/api/connections/{connection_id}/install-instructions")
    async def get_connection_install_instructions(request: Request, connection_id: str, rotate_token: bool = False):
        user_id = get_session_user(request)
        conn = db()
        row = _require_connection_row(conn, connection_id=connection_id, user_id=user_id)

        token = None
        now = int(time.time())
        expires_at = _to_int(row["install_token_expires_at"])
        has_active_token = bool(str(row["install_token_hash"] or "").strip()) and (not expires_at or expires_at > now)
        if rotate_token or (not has_active_token):
            token_data = _issue_connection_install_token(conn, connection_id=connection_id)
            conn.execute("UPDATE connections SET updated_at = ? WHERE id = ?", (now, connection_id))
            conn.commit()
            token = token_data["token"]
            expires_at = token_data["expires_at"]
        conn.close()

        return {
            "ok": True,
            "connection_id": connection_id,
            "install_token": token,
            "install_token_expires_at": expires_at,
            "install_instructions": _platform_install_instructions(
                _request_origin(request),
                connection_id,
                token or "<regenerate-token>",
            ),
            "token_note": None if token else "Install token is hidden. Set rotate_token=true or regenerate token to get a fresh value.",
        }

    @app.get("/api/connections/{connection_id}/agents")
    async def list_connection_discovered_agents(request: Request, connection_id: str, include_cards: bool = False):
        user_id = get_session_user(request)
        conn = db()
        _require_connection_row(conn, connection_id=connection_id, user_id=user_id)
        rows = conn.execute(
            """
            SELECT id, connection_id, user_id,
                   COALESCE(NULLIF(runtime_agent_id, ''), agent_id) AS runtime_agent_id,
                   agent_id, agent_name, status,
                   COALESCE(NULLIF(agent_card_version, ''), card_version, '1.0') AS agent_card_version,
                   COALESCE(NULLIF(agent_card_json, ''), card_json, '{}') AS agent_card_json,
                   discovered_at, updated_at
            FROM managed_agents
            WHERE user_id = ? AND connection_id = ?
            ORDER BY updated_at DESC, agent_name ASC
            """,
            (user_id, connection_id),
        ).fetchall()
        conn.close()

        agents: List[Dict[str, Any]] = []
        for row in rows:
            item = {
                "managed_agent_id": str(row["id"]),
                "connection_id": str(row["connection_id"]),
                "runtime_agent_id": str(row["runtime_agent_id"] or row["agent_id"] or ""),
                "agent_name": str(row["agent_name"] or row["runtime_agent_id"] or row["agent_id"] or "agent"),
                "status": str(row["status"] or "active"),
                "agent_card_version": str(row["agent_card_version"] or "1.0"),
                "discovered_at": _to_int(row["discovered_at"]),
                "updated_at": _to_int(row["updated_at"]),
            }
            if include_cards:
                item["agent_card_json"] = _parse_setup_json(row["agent_card_json"])
            agents.append(item)

        return {
            "ok": True,
            "connection_id": connection_id,
            "count": len(agents),
            "agents": agents,
        }

    @app.post("/api/connections/{connection_id}/agents/refresh")
    async def refresh_connection_discovered_agents(request: Request, connection_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = _require_connection_row(conn, connection_id=connection_id, user_id=user_id)
        legacy_id = str(row["legacy_openclaw_connection_id"] or "").strip() or connection_id

        legacy_conn = conn.execute(
            """
            SELECT id, base_url, api_key, api_key_secret_id
            FROM openclaw_connections
            WHERE id = ? AND user_id = ?
            LIMIT 1
            """,
            (legacy_id, user_id),
        ).fetchone()
        if not legacy_conn:
            conn.execute(
                "UPDATE connections SET updated_at = ? WHERE id = ?",
                (int(time.time()), connection_id),
            )
            conn.commit()
            conn.close()
            return {
                "ok": True,
                "connection_id": connection_id,
                "refresh_dispatched": True,
                "message": "Connection is hub-managed. Hivee Hub will push discovered agents on next heartbeat.",
            }

        api_key = _resolve_connection_api_key_from_row(conn, user_id=user_id, row=legacy_conn)
        conn.close()

        listed = await openclaw_list_agents(str(legacy_conn["base_url"]), api_key)
        if not listed.get("ok"):
            raise HTTPException(400, listed)

        provision = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=None,
            connection_id=connection_id,
            base_url=str(legacy_conn["base_url"]),
            raw_agents=listed.get("agents") or [],
        )

        conn = db()
        conn.execute(
            "UPDATE connections SET hub_status = ?, updated_at = ? WHERE id = ?",
            (HUB_STATUS_ONLINE, int(time.time()), connection_id),
        )
        conn.commit()
        conn.close()

        return {
            "ok": True,
            "connection_id": connection_id,
            "source": "openclaw_runtime_refresh",
            "listed": listed,
            "agent_provision": provision,
        }

    @app.post("/api/hub/install/complete")
    async def hub_install_complete(payload: HubInstallCompleteIn):
        token_hash = _hash_access_token(str(payload.install_token or ""))
        now = int(time.time())
        conn = db()
        row = conn.execute(
            """
            SELECT id, user_id, install_token_expires_at
            FROM connections
            WHERE install_token_hash = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(401, "Invalid install token")
        expires_at = _to_int(row["install_token_expires_at"])
        if expires_at and expires_at < now:
            conn.close()
            raise HTTPException(401, "Install token expired")

        conn.execute(
            """
            UPDATE connections
            SET hub_status = ?, machine_name = COALESCE(?, machine_name),
                os_type = COALESCE(?, os_type), hub_version = COALESCE(?, hub_version),
                last_heartbeat_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                HUB_STATUS_ONLINE,
                str(payload.machine_name or "").strip() or None,
                str(payload.os_type or "").strip() or None,
                str(payload.hub_version or "").strip() or None,
                now,
                now,
                str(row["id"]),
            ),
        )
        conn.commit()
        conn.close()
        return {
            "ok": True,
            "connection_id": str(row["id"]),
            "user_id": str(row["user_id"]),
            "hub_status": HUB_STATUS_ONLINE,
            "connected_at": now,
        }

    @app.post("/api/hub/heartbeat")
    async def hub_heartbeat(payload: HubHeartbeatIn):
        now = int(time.time())
        conn = db()
        _require_hub_connection_auth(
            conn,
            connection_id=str(payload.connection_id),
            install_token=str(payload.install_token),
        )
        next_status = _normalize_hub_status(payload.hub_status, default=HUB_STATUS_ONLINE)
        conn.execute(
            """
            UPDATE connections
            SET hub_status = ?, machine_name = COALESCE(?, machine_name),
                os_type = COALESCE(?, os_type), hub_version = COALESCE(?, hub_version),
                last_heartbeat_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                next_status,
                str(payload.machine_name or "").strip() or None,
                str(payload.os_type or "").strip() or None,
                str(payload.hub_version or "").strip() or None,
                now,
                now,
                str(payload.connection_id),
            ),
        )
        conn.commit()
        conn.close()
        return {
            "ok": True,
            "connection_id": str(payload.connection_id),
            "hub_status": next_status,
            "last_heartbeat_at": now,
        }

    @app.post("/api/hub/agents/discovered")
    async def hub_agents_discovered(payload: HubAgentsDiscoveredIn):
        connection_id = str(payload.connection_id or "").strip()
        if not connection_id:
            raise HTTPException(400, "connection_id is required")

        conn = db()
        c_row = _require_hub_connection_auth(
            conn,
            connection_id=connection_id,
            install_token=str(payload.install_token),
        )
        user_id = str(c_row["user_id"])
        conn.close()

        raw_agents = [
            {
                "id": str(item.runtime_agent_id or "").strip(),
                "name": str(item.agent_name or item.runtime_agent_id or "").strip(),
            }
            for item in (payload.agents or [])
            if str(item.runtime_agent_id or "").strip()
        ]
        provision = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=None,
            connection_id=connection_id,
            base_url=f"hivee://{connection_id}",
            raw_agents=raw_agents,
        )

        now = int(time.time())
        conn = db()
        updates: List[Dict[str, Any]] = []
        for item in payload.agents:
            runtime_agent_id = str(item.runtime_agent_id or "").strip()
            if not runtime_agent_id:
                continue
            agent_name = str(item.agent_name or runtime_agent_id).strip() or runtime_agent_id
            status = str(item.status or "online").strip() or "online"
            card_payload = item.agent_card_json if isinstance(item.agent_card_json, dict) else None
            card_json = json.dumps(card_payload, ensure_ascii=False) if card_payload is not None else None

            ma_row = conn.execute(
                """
                SELECT id
                FROM managed_agents
                WHERE user_id = ? AND connection_id = ?
                  AND (runtime_agent_id = ? OR agent_id = ?)
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (user_id, connection_id, runtime_agent_id, runtime_agent_id),
            ).fetchone()
            if not ma_row:
                continue

            conn.execute(
                """
                UPDATE managed_agents
                SET runtime_agent_id = ?,
                    agent_id = ?,
                    agent_name = ?,
                    status = ?,
                    discovered_at = COALESCE(discovered_at, ?),
                    updated_at = ?,
                    agent_card_json = COALESCE(?, agent_card_json),
                    card_json = COALESCE(?, card_json),
                    agent_card_version = CASE WHEN ? IS NOT NULL THEN COALESCE(agent_card_version, '1.0') ELSE agent_card_version END
                WHERE id = ?
                """,
                (
                    runtime_agent_id,
                    runtime_agent_id,
                    agent_name,
                    status,
                    now,
                    now,
                    card_json,
                    card_json,
                    card_json,
                    str(ma_row["id"]),
                ),
            )
            updates.append(
                {
                    "managed_agent_id": str(ma_row["id"]),
                    "runtime_agent_id": runtime_agent_id,
                    "agent_name": agent_name,
                    "status": status,
                }
            )

        conn.execute(
            "UPDATE connections SET hub_status = ?, last_heartbeat_at = ?, updated_at = ? WHERE id = ?",
            (HUB_STATUS_ONLINE, now, now, connection_id),
        )
        conn.commit()
        conn.close()
        _refresh_managed_agents_index(user_id)

        return {
            "ok": True,
            "connection_id": connection_id,
            "discovered_count": len(updates),
            "agent_provision": provision,
            "agents": updates,
        }

    @app.post("/api/hub/agents/{managed_agent_id}/card")
    async def hub_agent_card_upsert(managed_agent_id: str, payload: HubAgentCardIn):
        connection_id = str(payload.connection_id or "").strip()
        conn = db()
        c_row = _require_hub_connection_auth(
            conn,
            connection_id=connection_id,
            install_token=str(payload.install_token),
        )
        user_id = str(c_row["user_id"])

        row = conn.execute(
            """
            SELECT id
            FROM managed_agents
            WHERE id = ? AND user_id = ? AND connection_id = ?
            LIMIT 1
            """,
            (managed_agent_id, user_id, connection_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Managed agent not found for this connection")

        now = int(time.time())
        card_json = json.dumps(payload.agent_card_json or {}, ensure_ascii=False)
        card_version = str(payload.agent_card_version or "").strip() or "1.0"
        conn.execute(
            """
            UPDATE managed_agents
            SET agent_card_json = ?,
                agent_card_version = ?,
                card_json = ?,
                card_version = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (card_json, card_version, card_json, card_version, now, managed_agent_id),
        )
        conn.commit()
        conn.close()

        return {
            "ok": True,
            "managed_agent_id": managed_agent_id,
            "agent_card_version": card_version,
            "updated_at": now,
        }

