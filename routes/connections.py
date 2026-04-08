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

    install_repo = str(os.getenv("HIVEE_HUB_INSTALL_REPO") or "https://github.com/arieladhidevara/HIVEE-HUB.git").strip()
    install_ref = str(os.getenv("HIVEE_HUB_INSTALL_REF") or "").strip()
    install_subdir = str(os.getenv("HIVEE_HUB_INSTALL_SUBDIR") or "").strip().strip("/")
    install_target = install_repo if install_repo.startswith("git+") else f"git+{install_repo}"
    if install_ref:
        install_target = f"{install_target}@{install_ref}"
    if install_subdir and "#subdirectory=" not in install_target:
        install_target = f"{install_target}#subdirectory={install_subdir}"

    # TODO(hivee-hub-daemon): replace these snippets with signed installer manifests.
    linux_cmd = (
        f"python3 -m pip install --upgrade \"{install_target}\" && "
        f"hivee-hub connect --cloud-url \"{safe_origin}\" --connection-id \"{connection_id}\" "
        f"--install-token \"{install_token}\" --runtime {CONNECTION_RUNTIME_OPENCLAW} "
        "--openclaw-base-url \"<openclaw-base-url>\" --openclaw-api-key \"<openclaw-api-key>\""
    )
    mac_cmd = linux_cmd
    windows_cmd = (
        f"py -m pip install --upgrade \"{install_target}\"; "
        f"hivee-hub connect --cloud-url \"{safe_origin}\" --connection-id \"{connection_id}\" "
        f"--install-token \"{install_token}\" --runtime {CONNECTION_RUNTIME_OPENCLAW} "
        "--openclaw-base-url \"<openclaw-base-url>\" --openclaw-api-key \"<openclaw-api-key>\""
    )
    docker_cmd = (
        "docker run -d --name hivee-hub --restart unless-stopped "
        f"-e HIVEE_CLOUD_URL=\"{safe_origin}\" "
        f"-e HIVEE_CONNECTION_ID=\"{connection_id}\" "
        f"-e HIVEE_INSTALL_TOKEN=\"{install_token}\" "
        "-e HIVEE_RUNTIME_TYPE=\"openclaw\" "
        "-e OPENCLAW_BASE_URL=\"<openclaw-base-url>\" "
        "-e OPENCLAW_API_KEY=\"<openclaw-api-key>\" "
        "python:3.11-slim "
        f"sh -lc \"pip install --no-cache-dir '{install_target}' && hivee-hub run\""
    )
    concept = (
        "Install Hivee Hub on the machine where your runtime is available. "
        "Hub connects outward to Hivee Cloud and discovers local runtime agents. "
        "Set OpenClaw URL/API key for discovery, then Hub syncs managed agent cards and heartbeats back to this connection."
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



def _claim_runtime_dispatch_job(
    conn: sqlite3.Connection,
    *,
    connection_id: str,
    stale_after_sec: int = 120,
) -> Optional[sqlite3.Row]:
    now = int(time.time())
    stale_before = now - max(30, min(int(stale_after_sec or 120), 3600))
    row = conn.execute(
        """
        SELECT id, connection_id, project_id, channel_id, task_id,
               managed_agent_id, project_agent_membership_id, runtime_agent_id,
               runtime_session_key, prompt_text, status,
               created_at, claimed_at, completed_at, updated_at
        FROM runtime_dispatch_jobs
        WHERE connection_id = ?
          AND (
                status = ?
                OR (status = ? AND COALESCE(claimed_at, 0) < ?)
              )
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (
            connection_id,
            RUNTIME_DISPATCH_STATUS_PENDING,
            RUNTIME_DISPATCH_STATUS_CLAIMED,
            stale_before,
        ),
    ).fetchone()
    if not row:
        return None

    conn.execute(
        """
        UPDATE runtime_dispatch_jobs
        SET status = ?, claimed_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            RUNTIME_DISPATCH_STATUS_CLAIMED,
            now,
            now,
            str(row["id"]),
        ),
    )
    return conn.execute(
        """
        SELECT id, connection_id, project_id, channel_id, task_id,
               managed_agent_id, project_agent_membership_id, runtime_agent_id,
               runtime_session_key, prompt_text, status,
               created_at, claimed_at, completed_at, updated_at
        FROM runtime_dispatch_jobs
        WHERE id = ?
        LIMIT 1
        """,
        (str(row["id"]),),
    ).fetchone()
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

    @app.post("/api/hub/runtime/jobs/claim")
    async def hub_runtime_job_claim(payload: HubRuntimeJobClaimIn):
        connection_id = str(payload.connection_id or "").strip()
        if not connection_id:
            raise HTTPException(400, "connection_id is required")
        max_wait_sec = max(0, min(int(payload.max_wait_sec or 0), 20))
        deadline = time.time() + max_wait_sec

        job_row: Optional[sqlite3.Row] = None
        while True:
            conn = db()
            _require_hub_connection_auth(
                conn,
                connection_id=connection_id,
                install_token=str(payload.install_token),
            )
            now = int(time.time())
            conn.execute(
                "UPDATE connections SET hub_status = ?, last_heartbeat_at = ?, updated_at = ? WHERE id = ?",
                (HUB_STATUS_ONLINE, now, now, connection_id),
            )
            job_row = _claim_runtime_dispatch_job(conn, connection_id=connection_id)
            conn.commit()
            conn.close()

            if job_row:
                break
            if max_wait_sec <= 0 or time.time() >= deadline:
                break
            time.sleep(1.0)

        if not job_row:
            return {"ok": True, "connection_id": connection_id, "job": None}

        return {
            "ok": True,
            "connection_id": connection_id,
            "job": {
                "id": str(job_row["id"]),
                "connection_id": str(job_row["connection_id"]),
                "project_id": str(job_row["project_id"]),
                "channel_id": str(job_row["channel_id"] or "") or None,
                "task_id": str(job_row["task_id"] or "") or None,
                "managed_agent_id": str(job_row["managed_agent_id"] or "") or None,
                "project_agent_membership_id": str(job_row["project_agent_membership_id"] or "") or None,
                "runtime_agent_id": str(job_row["runtime_agent_id"] or "") or None,
                "runtime_session_key": str(job_row["runtime_session_key"] or ""),
                "prompt_text": str(job_row["prompt_text"] or ""),
                "status": str(job_row["status"] or RUNTIME_DISPATCH_STATUS_CLAIMED),
                "created_at": _to_int(job_row["created_at"]),
                "claimed_at": _to_int(job_row["claimed_at"]),
            },
        }

    @app.post("/api/hub/runtime/jobs/{job_id}/complete")
    async def hub_runtime_job_complete(job_id: str, payload: HubRuntimeJobCompleteIn):
        connection_id = str(payload.connection_id or "").strip()
        if not connection_id:
            raise HTTPException(400, "connection_id is required")
        next_status = str(payload.status or RUNTIME_DISPATCH_STATUS_COMPLETED).strip().lower()
        if next_status not in {RUNTIME_DISPATCH_STATUS_COMPLETED, RUNTIME_DISPATCH_STATUS_FAILED}:
            raise HTTPException(400, "status must be completed or failed")

        conn = db()
        _require_hub_connection_auth(
            conn,
            connection_id=connection_id,
            install_token=str(payload.install_token),
        )
        row = conn.execute(
            """
            SELECT id, connection_id, project_id, channel_id, task_id,
                   managed_agent_id, project_agent_membership_id, runtime_agent_id,
                   runtime_session_key, status, response_message_id
            FROM runtime_dispatch_jobs
            WHERE id = ? AND connection_id = ?
            LIMIT 1
            """,
            (job_id, connection_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Runtime dispatch job not found")

        current_status = str(row["status"] or "").strip().lower()
        if current_status in {RUNTIME_DISPATCH_STATUS_COMPLETED, RUNTIME_DISPATCH_STATUS_FAILED}:
            now = int(time.time())
            conn.execute(
                "UPDATE connections SET hub_status = ?, last_heartbeat_at = ?, updated_at = ? WHERE id = ?",
                (HUB_STATUS_ONLINE, now, now, connection_id),
            )
            conn.commit()
            conn.close()
            return {
                "ok": True,
                "job_id": job_id,
                "status": current_status,
                "response_message_id": str(row["response_message_id"] or "") or None,
                "idempotent": True,
            }

        now = int(time.time())
        response_message_id: Optional[str] = None
        project_id = str(row["project_id"])
        task_id = str(row["task_id"] or "").strip() or None
        channel_id = str(row["channel_id"] or "").strip() or None
        runtime_agent_id = str(row["runtime_agent_id"] or "").strip() or None
        managed_agent_id = str(row["managed_agent_id"] or "").strip() or None
        project_agent_membership_id = str(row["project_agent_membership_id"] or "").strip() or None
        runtime_session_key = str(row["runtime_session_key"] or "").strip()
        is_planning_channel = False
        if channel_id:
            channel_row = conn.execute(
                """
                SELECT id, name
                FROM project_channels
                WHERE id = ? AND project_id = ?
                LIMIT 1
                """,
                (channel_id, project_id),
            ).fetchone()
            is_planning_channel = bool(
                channel_row and str(channel_row["name"] or "").strip().lower() == PROJECT_CHANNEL_PLANNING
            )

        if next_status == RUNTIME_DISPATCH_STATUS_COMPLETED:
            reply_text = str(payload.result_text or "").strip()
            if not reply_text:
                reply_text = "Acknowledged."
            response_message_id = new_id("msg")
            conn.execute(
                """
                INSERT INTO project_messages (
                    id, project_id, channel_id, sender_type, sender_user_id,
                    sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    response_message_id,
                    project_id,
                    channel_id,
                    "agent",
                    None,
                    project_agent_membership_id,
                    PROJECT_MESSAGE_KIND_CHAT,
                    reply_text,
                    task_id,
                    json.dumps(
                        {
                            "dispatch_job_id": str(row["id"]),
                            "managed_agent_id": managed_agent_id,
                            "runtime_agent_id": runtime_agent_id,
                            "runtime_session_key": runtime_session_key,
                            "source": "hivee_hub",
                        },
                        ensure_ascii=False,
                    ),
                    now,
                ),
            )
            conn.execute(
                """
                UPDATE runtime_sessions
                SET last_message_id = ?, updated_at = ?,
                    summary_md = COALESCE(NULLIF(summary_md, ''), ?)
                WHERE runtime_session_key = ?
                """,
                (
                    response_message_id,
                    now,
                    f"Active lane for {runtime_agent_id or managed_agent_id or 'agent'}",
                    runtime_session_key,
                ),
            )
            conn.execute(
                """
                UPDATE runtime_dispatch_jobs
                SET status = ?, result_text = ?, error_text = NULL,
                    response_message_id = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    RUNTIME_DISPATCH_STATUS_COMPLETED,
                    reply_text,
                    response_message_id,
                    now,
                    now,
                    str(row["id"]),
                ),
            )
            if is_planning_channel:
                conn.execute(
                    """
                    UPDATE projects
                    SET plan_status = ?, plan_text = ?, plan_updated_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        PLAN_STATUS_AWAITING_APPROVAL,
                        reply_text[:20000],
                        now,
                        now,
                        project_id,
                    ),
                )
        else:
            err_text = str(payload.error_text or payload.result_text or "Runtime dispatch failed").strip()[:2000]
            response_message_id = new_id("msg")
            if channel_id:
                conn.execute(
                    """
                    INSERT INTO project_messages (
                        id, project_id, channel_id, sender_type, sender_user_id,
                        sender_agent_membership_id, message_kind, body, task_id, metadata_json, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        response_message_id,
                        project_id,
                        channel_id,
                        "system",
                        None,
                        None,
                        PROJECT_MESSAGE_KIND_EVENT,
                        "Runtime dispatch failed on Hivee Hub.",
                        task_id,
                        json.dumps(
                            {
                                "event": "runtime.dispatch.failed",
                                "dispatch_job_id": str(row["id"]),
                                "managed_agent_id": managed_agent_id,
                                "runtime_agent_id": runtime_agent_id,
                                "runtime_session_key": runtime_session_key,
                                "error": err_text,
                                "source": "hivee_hub",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )
            conn.execute(
                """
                UPDATE runtime_dispatch_jobs
                SET status = ?, result_text = NULL, error_text = ?,
                    response_message_id = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    RUNTIME_DISPATCH_STATUS_FAILED,
                    err_text,
                    response_message_id if channel_id else None,
                    now,
                    now,
                    str(row["id"]),
                ),
            )
            if is_planning_channel:
                conn.execute(
                    """
                    UPDATE projects
                    SET plan_status = ?, plan_text = ?, plan_updated_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        PLAN_STATUS_FAILED,
                        err_text[:20000],
                        now,
                        now,
                        project_id,
                    ),
                )

        if channel_id:
            conn.execute(
                "UPDATE channel_memory SET updated_at = ? WHERE channel_id = ?",
                (now, channel_id),
            )
        conn.execute(
            "UPDATE connections SET hub_status = ?, last_heartbeat_at = ?, updated_at = ? WHERE id = ?",
            (HUB_STATUS_ONLINE, now, now, connection_id),
        )
        conn.commit()
        conn.close()

        await emit(
            project_id,
            "project.runtime.job.completed",
            {
                "job_id": str(row["id"]),
                "status": next_status,
                "connection_id": connection_id,
                "managed_agent_id": managed_agent_id,
                "runtime_agent_id": runtime_agent_id,
                "response_message_id": response_message_id,
            },
        )
        if channel_id:
            await emit(
                project_id,
                "project.channel.message",
                {
                    "channel_id": channel_id,
                    "sender_type": "agent" if next_status == RUNTIME_DISPATCH_STATUS_COMPLETED else "system",
                    "runtime_agent_id": runtime_agent_id,
                    "managed_agent_id": managed_agent_id,
                },
            )
        if is_planning_channel:
            if next_status == RUNTIME_DISPATCH_STATUS_COMPLETED:
                await emit(
                    project_id,
                    "project.plan.ready",
                    {
                        "project_id": project_id,
                        "status": PLAN_STATUS_AWAITING_APPROVAL,
                        "dispatch_job_id": str(row["id"]),
                        "source": "hivee_hub",
                    },
                )
            else:
                await emit(
                    project_id,
                    "project.plan.failed",
                    {
                        "project_id": project_id,
                        "dispatch_job_id": str(row["id"]),
                        "source": "hivee_hub",
                    },
                )

        return {
            "ok": True,
            "job_id": str(row["id"]),
            "status": next_status,
            "response_message_id": response_message_id,
            "completed_at": now,
        }




