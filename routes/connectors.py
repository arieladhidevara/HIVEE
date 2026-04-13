from hivee_shared import *
from services.connector_dispatch import connector_auth, enqueue_connector_command

# ── Connector pairing / registry / heartbeat / command-queue routes ─────
# These endpoints implement the cloud-side contract that the hivee-connector
# edge bridge expects (see hivee-connector/src/services/cloudApi.ts).
# Direct OpenClaw mode is completely untouched.

CONNECTOR_PAIRING_TOKEN_TTL_MIN = 300
CONNECTOR_PAIRING_TOKEN_TTL_MAX = 86400
CONNECTOR_HEARTBEAT_INTERVAL_SEC = 15
CONNECTOR_COMMAND_POLL_INTERVAL_SEC = 5
CONNECTOR_COMMANDS_BATCH_SIZE = 20


def register_routes(app: FastAPI) -> None:

    # ── 1. Create pairing token (user session required) ─────────────────
    @app.post("/api/connectors/pairing-tokens", response_model=ConnectorPairingTokenCreateOut)
    async def create_pairing_token(request: Request, payload: ConnectorPairingTokenCreateIn):
        user_id = get_session_user(request)
        ttl = max(CONNECTOR_PAIRING_TOKEN_TTL_MIN, min(int(payload.expires_in_sec or 3600), CONNECTOR_PAIRING_TOKEN_TTL_MAX))
        now = int(time.time())
        expires_at = now + ttl
        token = f"pair_{secrets.token_urlsafe(24)}"
        token_id = new_id("cpt")

        conn = db()
        conn.execute(
            """
            INSERT INTO connector_pairing_tokens (id, user_id, token, label, status, expires_at, created_at)
            VALUES (?,?,?,?,?,?,?)
            """,
            (token_id, user_id, token, payload.label, "active", expires_at, now),
        )
        conn.commit()
        conn.close()
        return ConnectorPairingTokenCreateOut(token=token, expires_at=expires_at)

    # ── 2. Register connector (pairing token auth, no session) ──────────
    @app.post("/api/connectors/register", response_model=ConnectorRegisterOut)
    async def register_connector(payload: ConnectorRegisterIn):
        now = int(time.time())
        pairing_token = str(payload.pairingToken or "").strip()
        if not pairing_token:
            raise HTTPException(400, "Missing pairingToken")

        conn = db()
        row = conn.execute(
            """
            SELECT id, user_id, status, expires_at, used_by_connector_id
            FROM connector_pairing_tokens
            WHERE token = ?
            LIMIT 1
            """,
            (pairing_token,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Pairing token not found")
        if str(row["status"] or "") != "active":
            conn.close()
            raise HTTPException(400, "Pairing token already used or revoked")
        if int(row["expires_at"] or 0) <= now:
            conn.close()
            raise HTTPException(400, "Pairing token expired")

        user_id = str(row["user_id"])
        token_id = str(row["id"])

        # Create connector
        connector_id = new_id("conn")
        connector_secret = f"csec_{secrets.token_urlsafe(32)}"

        conn.execute(
            """
            INSERT INTO connectors
            (id, user_id, name, secret, status, host_hostname, host_platform, host_arch,
             openclaw_base_url, openclaw_transport, heartbeat_interval_sec, command_poll_interval_sec,
             last_seen_at, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                connector_id, user_id, str(payload.connectorName or "connector"),
                connector_secret, "online",
                str(payload.host.hostname or ""), str(payload.host.platform or ""), str(payload.host.arch or ""),
                str(payload.openclaw.baseUrl or ""), str(payload.openclaw.transport or ""),
                CONNECTOR_HEARTBEAT_INTERVAL_SEC, CONNECTOR_COMMAND_POLL_INTERVAL_SEC,
                now, now, now,
            ),
        )

        # Save initial agent snapshot
        snapshot_data = {
            "agents": [{"id": a.id, "name": a.name} for a in (payload.openclaw.agents or [])],
            "models": list(payload.openclaw.models or []),
            "baseUrl": str(payload.openclaw.baseUrl or ""),
            "transport": str(payload.openclaw.transport or ""),
        }
        snapshot_id = new_id("csnap")
        conn.execute(
            """
            INSERT INTO connector_agent_snapshots (id, connector_id, snapshot_json, updated_at)
            VALUES (?,?,?,?)
            """,
            (snapshot_id, connector_id, json.dumps(snapshot_data, ensure_ascii=False), now),
        )

        # Mark pairing token as used
        conn.execute(
            """
            UPDATE connector_pairing_tokens
            SET status = 'used', used_at = ?, used_by_connector_id = ?
            WHERE id = ?
            """,
            (now, connector_id, token_id),
        )
        conn.commit()
        conn.close()

        if snapshot_data["agents"]:
            _provision_managed_agents_for_connection(
                user_id=user_id,
                env_id=None,
                connection_id=connector_id,
                base_url=str(payload.openclaw.baseUrl or ""),
                raw_agents=snapshot_data["agents"],
                fallback_agent_id=str(snapshot_data["agents"][0].get("id") or "").strip() or None,
                fallback_agent_name=str(snapshot_data["agents"][0].get("name") or "").strip() or None,
            )

        return ConnectorRegisterOut(
            connectorId=connector_id,
            connectorSecret=connector_secret,
            heartbeatIntervalSec=CONNECTOR_HEARTBEAT_INTERVAL_SEC,
            commandPollIntervalSec=CONNECTOR_COMMAND_POLL_INTERVAL_SEC,
        )

    # ── 3. Heartbeat (connector secret auth) ────────────────────────────
    @app.post("/api/connectors/{connector_id}/heartbeat")
    async def connector_heartbeat(request: Request, connector_id: str, payload: ConnectorHeartbeatIn):
        connector = connector_auth(request, connector_id)
        now = int(time.time())
        status = str(payload.status or "online").strip() or "online"

        conn = db()
        conn.execute(
            """
            UPDATE connectors
            SET status = ?, last_seen_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, now, now, connector_id),
        )

        # Update agent snapshot if openclaw data is provided
        if payload.openclaw and isinstance(payload.openclaw, dict):
            snapshot_id = new_id("csnap")
            conn.execute(
                """
                INSERT INTO connector_agent_snapshots (id, connector_id, snapshot_json, updated_at)
                VALUES (?,?,?,?)
                """,
                (snapshot_id, connector_id, json.dumps(payload.openclaw, ensure_ascii=False), now),
            )

        conn.commit()
        conn.close()

        snapshot_agents = _extract_agents_list(payload.openclaw) or []
        if snapshot_agents:
            base_url = str(payload.openclaw.get("baseUrl") or connector.get("openclaw_base_url") or "").strip()
            _provision_managed_agents_for_connection(
                user_id=str(connector.get("user_id") or ""),
                env_id=None,
                connection_id=connector_id,
                base_url=base_url,
                raw_agents=snapshot_agents,
                fallback_agent_id=str(snapshot_agents[0].get("id") or "").strip() or None if isinstance(snapshot_agents[0], dict) else None,
                fallback_agent_name=str(snapshot_agents[0].get("name") or "").strip() or None if isinstance(snapshot_agents[0], dict) else None,
            )
        return {"ok": True}

    # ── 4. Poll commands (connector secret auth) ────────────────────────
    @app.get("/api/connectors/{connector_id}/commands", response_model=ConnectorCommandsPollOut)
    async def poll_connector_commands(request: Request, connector_id: str, cursor: Optional[str] = None):
        connector = connector_auth(request, connector_id)

        conn = db()
        if cursor:
            # cursor is now a created_at timestamp (integer as string)
            try:
                cursor_ts = int(cursor)
            except (ValueError, TypeError):
                cursor_ts = 0
            rows = conn.execute(
                """
                SELECT id, command_type, payload_json, created_at
                FROM connector_commands
                WHERE connector_id = ? AND status = 'queued' AND created_at > ?
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (connector_id, cursor_ts, CONNECTOR_COMMANDS_BATCH_SIZE),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, command_type, payload_json, created_at
                FROM connector_commands
                WHERE connector_id = ? AND status = 'queued'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (connector_id, CONNECTOR_COMMANDS_BATCH_SIZE),
            ).fetchall()
        conn.close()

        commands = []
        new_cursor = cursor
        if rows:
            print(f"[poll_commands] connector={connector_id} cursor={cursor} found {len(rows)} queued command(s)", flush=True)
        for row in rows:
            payload_raw = str(row["payload_json"] or "{}").strip()
            try:
                payload_data = json.loads(payload_raw)
            except Exception:
                payload_data = {}
            commands.append(ConnectorCommandOut(
                id=str(row["id"]),
                type=str(row["command_type"]),
                payload=payload_data,
                createdAt=int(row["created_at"] or 0),
            ))
            # Use created_at as cursor (guaranteed chronological)
            new_cursor = str(int(row["created_at"] or 0))

        return ConnectorCommandsPollOut(cursor=new_cursor if commands else cursor, commands=commands)

    # ── 5. Submit command result (connector secret auth) ────────────────
    @app.post("/api/connectors/{connector_id}/commands/{command_id}/result")
    async def submit_command_result(request: Request, connector_id: str, command_id: str, payload: ConnectorCommandResultIn):
        connector = connector_auth(request, connector_id)
        now = int(time.time())

        conn = db()
        # Verify command belongs to this connector
        cmd = conn.execute(
            "SELECT id, status FROM connector_commands WHERE id = ? AND connector_id = ?",
            (command_id, connector_id),
        ).fetchone()
        if not cmd:
            conn.close()
            raise HTTPException(404, "Command not found")

        # Insert result
        result_id = new_id("cres")
        result_json = json.dumps({
            "ok": payload.ok,
            "commandId": payload.commandId,
            "type": payload.type,
            "output": payload.output,
            "error": payload.error,
            "startedAt": payload.startedAt,
            "finishedAt": payload.finishedAt,
        }, ensure_ascii=False)

        conn.execute(
            """
            INSERT INTO connector_command_results (id, command_id, connector_id, ok, result_json, created_at)
            VALUES (?,?,?,?,?,?)
            """,
            (result_id, command_id, connector_id, 1 if payload.ok else 0, result_json, now),
        )

        # Update command status
        new_status = "done" if payload.ok else "failed"
        conn.execute(
            """
            UPDATE connector_commands
            SET status = ?, finished_at = ?
            WHERE id = ?
            """,
            (new_status, now, command_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    # ── 6. List connectors (user session required) ──────────────────────
    @app.get("/api/connectors", response_model=List[ConnectorOut])
    async def list_connectors(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            """
            SELECT id, name, status, host_hostname, host_platform, host_arch,
                   openclaw_base_url, openclaw_transport, last_seen_at, created_at
            FROM connectors
            WHERE user_id = ?
            ORDER BY updated_at DESC
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        return [
            ConnectorOut(
                id=str(r["id"]),
                name=str(r["name"] or ""),
                status=str(r["status"] or "offline"),
                host_hostname=r["host_hostname"],
                host_platform=r["host_platform"],
                host_arch=r["host_arch"],
                openclaw_base_url=r["openclaw_base_url"],
                openclaw_transport=r["openclaw_transport"],
                last_seen_at=r["last_seen_at"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    # ── 7. Get connector detail (user session required) ─────────────────
    @app.get("/api/connectors/{connector_id}", response_model=ConnectorDetailOut)
    async def get_connector_detail(request: Request, connector_id: str):
        user_id = get_session_user(request)
        conn = db()
        row = conn.execute(
            """
            SELECT id, name, status, host_hostname, host_platform, host_arch,
                   openclaw_base_url, openclaw_transport, heartbeat_interval_sec,
                   command_poll_interval_sec, last_seen_at, created_at, updated_at
            FROM connectors
            WHERE id = ? AND user_id = ?
            """,
            (connector_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "Connector not found")

        # Fetch latest agent snapshot
        snapshot_row = conn.execute(
            """
            SELECT snapshot_json
            FROM connector_agent_snapshots
            WHERE connector_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (connector_id,),
        ).fetchone()
        conn.close()

        snapshot = None
        if snapshot_row:
            raw = str(snapshot_row["snapshot_json"] or "").strip()
            if raw:
                try:
                    snapshot = json.loads(raw)
                except Exception:
                    snapshot = None

        return ConnectorDetailOut(
            id=str(row["id"]),
            name=str(row["name"] or ""),
            status=str(row["status"] or "offline"),
            host_hostname=row["host_hostname"],
            host_platform=row["host_platform"],
            host_arch=row["host_arch"],
            openclaw_base_url=row["openclaw_base_url"],
            openclaw_transport=row["openclaw_transport"],
            heartbeat_interval_sec=int(row["heartbeat_interval_sec"] or 15),
            command_poll_interval_sec=int(row["command_poll_interval_sec"] or 5),
            last_seen_at=row["last_seen_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            agent_snapshot=snapshot,
        )
