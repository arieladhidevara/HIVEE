from hivee_shared import *
import ipaddress
from urllib.parse import urlparse

def register_routes(app: FastAPI) -> None:
    def _normalize_openclaw_base_url(raw_value: Any) -> str:
        base = str(raw_value or "").strip().rstrip("/")
        if not base:
            return ""
        if not (base.startswith("http://") or base.startswith("https://")):
            raise HTTPException(400, "openclaw_base_url must start with http:// or https://")
        return base
    def _openclaw_public_url_help() -> str:
        return (
            "OpenClaw base URL must be publicly reachable (HTTPS/domain). "
            "If not public yet, set it up based on your system: "
            "Linux/macOS -> SSH reverse tunnel or public reverse proxy; "
            "Windows -> cloudflared/ngrok or SSH tunnel via PowerShell/WSL; "
            "Docker/NAS -> publish OpenClaw port and route it through a public HTTPS domain/proxy."
        )

    def _ensure_openclaw_base_url_public(base_url: str) -> None:
        parsed = urlparse(str(base_url or "").strip())
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            raise HTTPException(400, f"openclaw_base_url host is invalid. {_openclaw_public_url_help()}")

        blocked_hosts = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal"}
        if host in blocked_hosts or host.endswith(".local"):
            raise HTTPException(400, f"openclaw_base_url is local-only. {_openclaw_public_url_help()}")

        try:
            ip = ipaddress.ip_address(host)
        except Exception:
            return

        if (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_unspecified
            or ip.is_reserved
            or ip.is_multicast
        ):
            raise HTTPException(400, f"openclaw_base_url is not publicly reachable. {_openclaw_public_url_help()}")

    def _normalize_openclaw_ws_url(raw_value: Any) -> Optional[str]:
        ws = str(raw_value or "").strip().rstrip("/")
        if not ws:
            return None
        lowered = ws.lower()
        if lowered.startswith("http://") or lowered.startswith("https://"):
            return ws
        raise HTTPException(400, "openclaw_ws_url must start with http:// or https://")

    def _normalize_openclaw_stage_source(raw_value: Any) -> Optional[str]:
        source = str(raw_value or "").strip()
        if not source:
            return None
        return source[:120]

    def _expire_openclaw_stage_rows(conn: sqlite3.Connection, env_id: str, now: int) -> int:
        return conn.execute(
            """
            UPDATE environment_openclaw_staging
            SET status = ?, updated_at = ?
            WHERE env_id = ? AND status = ? AND expires_at <= ?
            """,
            (
                ENV_OPENCLAW_STAGE_STATUS_EXPIRED,
                now,
                env_id,
                ENV_OPENCLAW_STAGE_STATUS_STAGED,
                now,
            ),
        ).rowcount

    def _select_active_openclaw_stage(conn: sqlite3.Connection, env_id: str, now: int) -> Optional[sqlite3.Row]:
        return conn.execute(
            """
            SELECT id, env_id, staged_by_agent_id, openclaw_base_url, openclaw_ws_url, openclaw_name,
                   api_key_encrypted, source, status, created_at, updated_at, expires_at
            FROM environment_openclaw_staging
            WHERE env_id = ? AND status = ? AND expires_at > ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (env_id, ENV_OPENCLAW_STAGE_STATUS_STAGED, now),
        ).fetchone()

    @app.post("/api/a2a/environments/bootstrap")
    async def bootstrap_a2a_environment(request: Request, payload: A2AEnvironmentBootstrapIn):
        env_id = new_id("env")
        now = int(time.time())
        agent_id = _derive_bootstrap_agent_id(request, payload.agent_id)
        display_name = str(payload.display_name or "").strip()[:160] or f"env-{env_id[-6:]}"
        workspace = _ensure_environment_workspace(env_id)
        conn = db()
        conn.execute(
            """
            INSERT INTO environments (id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at, archived_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                env_id,
                None,
                display_name,
                ENV_STATUS_PENDING_CLAIM,
                str(workspace.get("workspace_root") or _environment_workspace_root_dir(env_id).as_posix()),
                now,
                None,
                None,
            ),
        )
        agent_token, agent_expires_at = _issue_environment_agent_session(
            conn,
            env_id=env_id,
            agent_id=agent_id,
            scopes=["env.read", "env.bootstrap", "env.claim.start", "env.openclaw.stage", "env.handoff.wait"],
            ttl_sec=payload.session_ttl_sec,
        )
        claim_code, claim_expires_at = _issue_environment_claim_code(
            conn,
            env_id=env_id,
            ttl_sec=payload.claim_ttl_sec,
            created_by_agent_id=agent_id,
        )
        conn.commit()
        conn.close()
        claim_url = _build_claim_url(request, env_id, claim_code)
        return {
            "ok": True,
            "environment_id": env_id,
            "agent_id": agent_id,
            "status": ENV_STATUS_PENDING_CLAIM,
            "workspace_root": workspace.get("workspace_root"),
            "templates_root": workspace.get("templates_root"),
            "agent_session_token": agent_token,
            "agent_session_expires_at": agent_expires_at,
            "claim_code": claim_code,
            "claim_code_expires_at": claim_expires_at,
            "claim_url": claim_url,
            "message": "Environment bootstrap complete. Stage a public OpenClaw base URL, then send temporary claim_url in OpenClaw chat so user can claim.",
        }

    @app.post("/api/a2a/environments/{env_id}/claim/start")
    async def start_a2a_environment_claim(request: Request, env_id: str, payload: A2AEnvironmentClaimStartIn):
        owner_user_id = None
        session_user = get_optional_session_user(request)
        agent_access = None
        if session_user:
            owner_user_id = str(session_user)
        else:
            agent_access = _resolve_environment_agent_access(request, env_id, required_scope="env.claim.start")

        conn = db()
        env_row = conn.execute(
            "SELECT id, owner_user_id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")
        status = str(env_row["status"] or "").strip() or ENV_STATUS_PENDING_BOOTSTRAP
        if status == ENV_STATUS_ARCHIVED:
            conn.close()
            raise HTTPException(400, "Environment is archived")
        if owner_user_id and str(env_row["owner_user_id"] or "").strip() != owner_user_id:
            conn.close()
            raise HTTPException(403, "Only owner can generate claim link for this environment")

        claim_code, claim_expires_at = _issue_environment_claim_code(
            conn,
            env_id=env_id,
            ttl_sec=payload.claim_ttl_sec,
            created_by_agent_id=(agent_access.get("agent_id") if agent_access else None),
        )
        if status == ENV_STATUS_PENDING_BOOTSTRAP:
            conn.execute(
                "UPDATE environments SET status = ? WHERE id = ?",
                (ENV_STATUS_PENDING_CLAIM, env_id),
            )
        conn.commit()
        conn.close()
        claim_url = _build_claim_url(request, env_id, claim_code)
        return {
            "ok": True,
            "environment_id": env_id,
            "status": ENV_STATUS_PENDING_CLAIM if status == ENV_STATUS_PENDING_BOOTSTRAP else status,
            "claim_code": claim_code,
            "claim_code_expires_at": claim_expires_at,
            "claim_url": claim_url,
        }

    @app.post("/api/a2a/environments/{env_id}/openclaw/stage", response_model=A2AEnvironmentOpenClawStageOut)
    async def stage_a2a_environment_openclaw(request: Request, env_id: str, payload: A2AEnvironmentOpenClawStageIn):
        access = _resolve_environment_agent_access(request, env_id, required_scope="env.openclaw.stage")
        now = int(time.time())
        agent_id = str(access.get("agent_id") or "").strip() or "agent"
        openclaw_base_url = _normalize_openclaw_base_url(payload.openclaw_base_url)
        openclaw_ws_url = _normalize_openclaw_ws_url(payload.openclaw_ws_url)
        openclaw_api_key = str(payload.openclaw_api_key or payload.openclaw_auth_token or "").strip()
        openclaw_name = str(payload.openclaw_name or "").strip()[:180] or None
        source = _normalize_openclaw_stage_source(payload.source) or ("agent_staged" if openclaw_api_key else "agent_base_hint")

        if not openclaw_base_url:
            raise HTTPException(400, "openclaw_base_url is required")
        _ensure_openclaw_base_url_public(openclaw_base_url)

        token_provided = bool(openclaw_api_key)
        if token_provided:
            health = await openclaw_health(openclaw_base_url, openclaw_api_key)
            if not health.get("ok"):
                raise HTTPException(
                    400,
                    {
                        "message": "Could not verify OpenClaw health while staging",
                        "details": health,
                    },
                )

        conn = db()
        env_row = conn.execute(
            "SELECT id, owner_user_id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")

        status = str(env_row["status"] or "").strip() or ENV_STATUS_PENDING_BOOTSTRAP
        if status == ENV_STATUS_ARCHIVED:
            conn.close()
            raise HTTPException(400, "Environment is archived")

        _expire_openclaw_stage_rows(conn, env_id, now)
        conn.execute(
            """
            UPDATE environment_openclaw_staging
            SET status = ?, updated_at = ?
            WHERE env_id = ? AND status = ?
            """,
            (
                ENV_OPENCLAW_STAGE_STATUS_REPLACED,
                now,
                env_id,
                ENV_OPENCLAW_STAGE_STATUS_STAGED,
            ),
        )

        stage_ttl = max(60, min(int(payload.stage_ttl_sec or ENV_OPENCLAW_STAGE_TTL_SEC), 60 * 60 * 24))
        stage_expires_at = now + stage_ttl
        conn.execute(
            """
            INSERT INTO environment_openclaw_staging (
                id, env_id, staged_by_agent_id, openclaw_base_url, openclaw_ws_url, openclaw_name,
                api_key_encrypted, source, status, created_at, updated_at, expires_at, consumed_at, consumed_by_user_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                new_id("ocstg"),
                env_id,
                agent_id,
                openclaw_base_url,
                openclaw_ws_url,
                openclaw_name,
                _seal_secret_value(openclaw_api_key),
                source,
                ENV_OPENCLAW_STAGE_STATUS_STAGED,
                now,
                now,
                stage_expires_at,
                None,
                None,
            ),
        )

        claim_code, claim_code_expires_at = _issue_environment_claim_code(
            conn,
            env_id=env_id,
            ttl_sec=payload.claim_ttl_sec,
            created_by_agent_id=agent_id,
        )
        if status == ENV_STATUS_PENDING_BOOTSTRAP:
            conn.execute(
                "UPDATE environments SET status = ? WHERE id = ?",
                (ENV_STATUS_PENDING_CLAIM, env_id),
            )

        conn.commit()
        conn.close()
        claim_url = _build_claim_url(request, env_id, claim_code)
        return A2AEnvironmentOpenClawStageOut(
            ok=True,
            environment_id=env_id,
            agent_id=agent_id,
            staged=True,
            openclaw_base_url=openclaw_base_url,
            openclaw_ws_url=openclaw_ws_url,
            openclaw_name=openclaw_name,
            source=source,
            stage_expires_at=stage_expires_at,
            claim_url=claim_url,
            claim_code_expires_at=claim_code_expires_at,
            message=(
                "OpenClaw base URL staged (token also received). Send temporary claim_url in OpenClaw chat; "
                "user signs up/logs in and can claim."
                if token_provided
                else "OpenClaw base URL staged. Send temporary claim_url in OpenClaw chat; "
                "user signs up/logs in and fills API key/token on claim page."
            ),
        )

    @app.get("/api/a2a/environments/{env_id}/claim/context", response_model=A2AEnvironmentClaimContextOut)
    async def get_a2a_environment_claim_context(env_id: str, code: str):
        env_id = str(env_id or "").strip()
        claim_code = str(code or "").strip()
        if not env_id:
            raise HTTPException(400, "env_id is required")
        if not claim_code:
            raise HTTPException(400, "code is required")

        now = int(time.time())
        conn = db()
        env_row = conn.execute(
            "SELECT id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")

        env_status = str(env_row["status"] or "").strip() or ENV_STATUS_PENDING_BOOTSTRAP
        if env_status == ENV_STATUS_ARCHIVED:
            conn.close()
            raise HTTPException(400, "Environment is archived")

        claim_row = conn.execute(
            """
            SELECT id, expires_at, used_at
            FROM environment_claim_codes
            WHERE env_id = ? AND code_hash = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (env_id, _hash_access_token(claim_code)),
        ).fetchone()

        if not claim_row:
            conn.close()
            return A2AEnvironmentClaimContextOut(
                ok=True,
                environment_id=env_id,
                claim_valid=False,
                claim_expires_at=None,
                staged_openclaw_ready=False,
                staged_openclaw_name=None,
                staged_openclaw_base_url=None,
                staged_openclaw_ws_url=None,
                requires_manual_openclaw=True,
                message="Invalid claim code.",
            )

        claim_expires_at = _to_int(claim_row["expires_at"])
        if _to_int(claim_row["used_at"]) > 0:
            conn.close()
            return A2AEnvironmentClaimContextOut(
                ok=True,
                environment_id=env_id,
                claim_valid=False,
                claim_expires_at=claim_expires_at,
                staged_openclaw_ready=False,
                staged_openclaw_name=None,
                staged_openclaw_base_url=None,
                staged_openclaw_ws_url=None,
                requires_manual_openclaw=True,
                message="Claim code already used.",
            )
        if claim_expires_at <= now:
            conn.close()
            return A2AEnvironmentClaimContextOut(
                ok=True,
                environment_id=env_id,
                claim_valid=False,
                claim_expires_at=claim_expires_at,
                staged_openclaw_ready=False,
                staged_openclaw_name=None,
                staged_openclaw_base_url=None,
                staged_openclaw_ws_url=None,
                requires_manual_openclaw=True,
                message="Claim code expired.",
            )

        _expire_openclaw_stage_rows(conn, env_id, now)
        stage_row = _select_active_openclaw_stage(conn, env_id, now)
        conn.commit()
        conn.close()

        if stage_row:
            stage_name = str(stage_row["openclaw_name"] or "").strip() or None
            stage_base = str(stage_row["openclaw_base_url"] or "").strip() or None
            stage_ws = str(stage_row["openclaw_ws_url"] or "").strip() or None
            if stage_base:
                label = stage_name or stage_base
                return A2AEnvironmentClaimContextOut(
                    ok=True,
                    environment_id=env_id,
                    claim_valid=True,
                    claim_expires_at=claim_expires_at,
                    staged_openclaw_ready=False,
                    staged_openclaw_name=stage_name,
                    staged_openclaw_base_url=stage_base,
                    staged_openclaw_ws_url=stage_ws,
                    requires_manual_openclaw=True,
                    message=f"OpenClaw base URL is prefilled from agent ({label}). Enter API key/token manually.",
                )

        return A2AEnvironmentClaimContextOut(
            ok=True,
            environment_id=env_id,
            claim_valid=True,
            claim_expires_at=claim_expires_at,
            staged_openclaw_ready=False,
            staged_openclaw_name=None,
            staged_openclaw_base_url=None,
            staged_openclaw_ws_url=None,
            requires_manual_openclaw=True,
            message="Enter OpenClaw base URL + API key/token manually to claim this environment. If OpenClaw is local, set up SSH/public gateway first.",
        )

    @app.post("/api/a2a/environments/claim/complete", response_model=A2AEnvironmentClaimCompleteOut)
    async def complete_a2a_environment_claim(request: Request, payload: A2AEnvironmentClaimCompleteIn, response: Response):
        env_id = str(payload.environment_id or "").strip()
        claim_code = str(payload.code or "").strip()
        mode = str(payload.mode or "signup").strip().lower()
        email = _normalize_email(str(payload.email or ""))
        password = str(payload.password or "")
        openclaw_base_url = _normalize_openclaw_base_url(payload.openclaw_base_url)
        openclaw_ws_url = _normalize_openclaw_ws_url(payload.openclaw_ws_url)
        openclaw_api_key = str(payload.openclaw_api_key or "").strip()
        openclaw_name = str(payload.openclaw_name or "").strip() or None

        if mode not in {"signup", "login", "session"}:
            raise HTTPException(400, "mode must be signup, login, or session")
        if not env_id or not claim_code:
            raise HTTPException(400, "environment_id and code are required")

        if not openclaw_api_key:
            raise HTTPException(400, "openclaw_api_key is required")

        if mode in {"signup", "login"} and not email:
            raise HTTPException(400, "email is required")
        if mode in {"signup", "login"} and not password:
            raise HTTPException(400, "password is required")
        if mode == "signup":
            _validate_password_strength(password)

        now = int(time.time())
        conn = db()
        env_row = conn.execute(
            "SELECT id, owner_user_id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")
        if str(env_row["status"] or "").strip() == ENV_STATUS_ARCHIVED:
            conn.close()
            raise HTTPException(400, "Environment is archived")

        claim_row = conn.execute(
            """
            SELECT id, expires_at, used_at, created_by_agent_id
            FROM environment_claim_codes
            WHERE env_id = ? AND code_hash = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (env_id, _hash_access_token(claim_code)),
        ).fetchone()
        if not claim_row:
            conn.close()
            raise HTTPException(400, "Invalid claim code")
        if _to_int(claim_row["used_at"]) > 0:
            conn.close()
            raise HTTPException(400, "Claim code already used")
        if _to_int(claim_row["expires_at"]) <= now:
            conn.close()
            raise HTTPException(400, "Claim code expired")
        claim_agent_id = str(claim_row["created_by_agent_id"] or "").strip()

        _expire_openclaw_stage_rows(conn, env_id, now)
        stage_row = _select_active_openclaw_stage(conn, env_id, now)

        resolved_base_url = openclaw_base_url
        resolved_ws_url = openclaw_ws_url
        resolved_api_key = openclaw_api_key
        resolved_name = openclaw_name
        consumed_stage_id: Optional[str] = None
        connection_source = "manual"

        if not resolved_base_url and stage_row:
            try:
                resolved_base_url = _normalize_openclaw_base_url(stage_row["openclaw_base_url"])
            except HTTPException:
                resolved_base_url = ""
            if not resolved_ws_url:
                try:
                    resolved_ws_url = _normalize_openclaw_ws_url(stage_row["openclaw_ws_url"])
                except HTTPException:
                    resolved_ws_url = None
            if not resolved_name:
                resolved_name = str(stage_row["openclaw_name"] or "").strip() or None
            consumed_stage_id = str(stage_row["id"] or "").strip() or None
            connection_source = "agent_base_url"

        if not resolved_base_url:
            conn.close()
            raise HTTPException(400, "openclaw_base_url is required (or agent must stage base URL first)")
        try:
            _ensure_openclaw_base_url_public(resolved_base_url)
        except HTTPException:
            conn.close()
            raise
        if not resolved_api_key:
            conn.close()
            raise HTTPException(400, "openclaw_api_key is required")

        health = await openclaw_health(resolved_base_url, resolved_api_key)
        if not health.get("ok"):
            conn.close()
            raise HTTPException(
                400,
                {
                    "message": "Could not verify OpenClaw health during claim",
                    "details": health,
                },
            )

        user_id = ""
        if mode == "signup":
            user_id = new_id("usr")
            try:
                conn.execute(
                    "INSERT INTO users (id, email, password, created_at) VALUES (?,?,?,?)",
                    (user_id, email, _hash_password(password), now),
                )
            except sqlite3.IntegrityError:
                conn.close()
                raise HTTPException(400, "Email already registered")
        elif mode == "login":
            user_row = conn.execute(
                "SELECT id, password FROM users WHERE email = ?",
                (email,),
            ).fetchone()
            if not user_row or not _verify_password_and_upgrade(conn, str(user_row["id"]), password, str(user_row["password"] or "")):
                conn.close()
                raise HTTPException(401, "Invalid email/password")
            user_id = str(user_row["id"])
        else:
            session_user = get_optional_session_user(request)
            if not session_user:
                conn.close()
                raise HTTPException(401, "Login session is required for session claim mode")
            user_row = conn.execute(
                "SELECT id, email FROM users WHERE id = ?",
                (session_user,),
            ).fetchone()
            if not user_row:
                conn.close()
                raise HTTPException(404, "User not found")
            user_id = str(user_row["id"])
            email = _normalize_email(str(user_row["email"] or ""))

        owner_user_id = str(env_row["owner_user_id"] or "").strip()
        if owner_user_id and owner_user_id != user_id:
            conn.close()
            raise HTTPException(409, "Environment already claimed by another user")

        bootstrap = await _bootstrap_connection_workspace(user_id, resolved_base_url, resolved_api_key)
        if not bootstrap.get("ok"):
            conn.close()
            raise HTTPException(
                400,
                {
                    "message": "OpenClaw validation/bootstrap failed during claim",
                    "details": bootstrap,
                },
            )

        token = new_id("sess")
        conn_id = new_id("oc")
        secret_id = _store_connection_api_key_secret(
            conn,
            user_id=user_id,
            connection_id=conn_id,
            api_key=resolved_api_key,
        )
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)",
            (token, user_id, now),
        )
        user_workspace = _ensure_user_workspace(user_id)
        _merge_environment_workspace_into_user_workspace(env_id, user_id)
        conn.execute(
            """
            UPDATE environments
            SET owner_user_id = ?, status = ?, workspace_root = ?, claimed_at = COALESCE(claimed_at, ?)
            WHERE id = ?
            """,
            (
                user_id,
                ENV_STATUS_ACTIVE,
                str(user_workspace.get("workspace_root") or _user_workspace_root_dir(user_id).as_posix()),
                now,
                env_id,
            ),
        )
        conn.execute(
            "UPDATE environment_claim_codes SET used_at = ?, used_by_user_id = ? WHERE id = ?",
            (now, user_id, claim_row["id"]),
        )

        _expire_openclaw_stage_rows(conn, env_id, now)
        if consumed_stage_id:
            conn.execute(
                """
                UPDATE environment_openclaw_staging
                SET status = ?, updated_at = ?, consumed_at = ?, consumed_by_user_id = ?
                WHERE id = ? AND status = ?
                """,
                (
                    ENV_OPENCLAW_STAGE_STATUS_CONSUMED,
                    now,
                    now,
                    user_id,
                    consumed_stage_id,
                    ENV_OPENCLAW_STAGE_STATUS_STAGED,
                ),
            )
            conn.execute(
                """
                UPDATE environment_openclaw_staging
                SET status = ?, updated_at = ?
                WHERE env_id = ? AND status = ? AND id <> ?
                """,
                (
                    ENV_OPENCLAW_STAGE_STATUS_REPLACED,
                    now,
                    env_id,
                    ENV_OPENCLAW_STAGE_STATUS_STAGED,
                    consumed_stage_id,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE environment_openclaw_staging
                SET status = ?, updated_at = ?
                WHERE env_id = ? AND status = ?
                """,
                (
                    ENV_OPENCLAW_STAGE_STATUS_REVOKED,
                    now,
                    env_id,
                    ENV_OPENCLAW_STAGE_STATUS_STAGED,
                ),
            )

        _transition_environment_agent_sessions_to_handoff(
            conn,
            env_id=env_id,
            ttl_sec=ENV_AGENT_HANDOFF_TTL_SEC,
        )
        if claim_agent_id:
            conn.execute(
                """
                UPDATE environment_agent_links
                SET status = ?, revoked_at = ?
                WHERE env_id = ? AND agent_id = ? AND status = ?
                """,
                (
                    ENV_AGENT_LINK_STATUS_REVOKED,
                    now,
                    env_id,
                    claim_agent_id,
                    ENV_AGENT_LINK_STATUS_ACTIVE,
                ),
            )
        conn.execute(
            "INSERT INTO openclaw_connections (id, user_id, env_id, base_url, api_key, api_key_secret_id, name, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                conn_id,
                user_id,
                env_id,
                resolved_base_url,
                "",
                secret_id,
                resolved_name,
                now,
            ),
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
            workspace_root=str(bootstrap.get("workspace_root") or _user_workspace_root_dir(user_id).as_posix()),
            templates_root=str(bootstrap.get("templates_root") or _user_templates_dir(user_id).as_posix()),
        )
        provision = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=env_id,
            connection_id=conn_id,
            base_url=resolved_base_url,
            raw_agents=bootstrap.get("agents") or [],
            fallback_agent_id=bootstrap.get("main_agent_id") or claim_agent_id,
            fallback_agent_name=bootstrap.get("main_agent_name") or claim_agent_id,
        )

        _set_session_cookie(response, token)
        return A2AEnvironmentClaimCompleteOut(
            token=token,
            environment_id=env_id,
            status=ENV_STATUS_ACTIVE,
            user_id=user_id,
            email=email,
            connection_id=conn_id,
            connection_name=resolved_name,
            connection_source=connection_source,
            agent_provision=provision,
        )
    @app.get("/api/a2a/environments/{env_id}")
    async def get_a2a_environment_status(request: Request, env_id: str):
        session_user = get_optional_session_user(request)
        conn = db()
        env_row = conn.execute(
            "SELECT id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")
    
        owner_user_id = str(env_row["owner_user_id"] or "").strip()
        access_mode = ""
        agent_access: Optional[Dict[str, Any]] = None
        if session_user and owner_user_id and str(session_user) == owner_user_id:
            access_mode = "owner"
        else:
            conn.close()
            agent_access = _resolve_environment_agent_access(request, env_id, required_scope="env.read")
            access_mode = f"agent:{agent_access.get('agent_id')}"
            conn = db()
            env_row = conn.execute(
                "SELECT id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at FROM environments WHERE id = ?",
                (env_id,),
            ).fetchone()
            owner_user_id = str(env_row["owner_user_id"] or "").strip()
    
        outstanding: List[Dict[str, Any]] = []
        if owner_user_id:
            rows = conn.execute(
                """
                SELECT id, title, plan_status, execution_status, progress_pct, created_at
                FROM projects
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 40
                """,
                (owner_user_id,),
            ).fetchall()
            for row in rows:
                exec_status = _coerce_execution_status(row["execution_status"])
                plan_status = _coerce_plan_status(row["plan_status"])
                if exec_status == EXEC_STATUS_COMPLETED and plan_status == PLAN_STATUS_APPROVED:
                    continue
                outstanding.append(
                    {
                        "project_id": str(row["id"]),
                        "title": str(row["title"] or ""),
                        "plan_status": plan_status,
                        "execution_status": exec_status,
                        "progress_pct": _clamp_progress(row["progress_pct"]),
                        "created_at": row["created_at"],
                    }
                )
        conn.close()
        env_status = str(env_row["status"] or "").strip()
        claimed = bool(owner_user_id)
        return {
            "ok": True,
            "environment": {
                "id": str(env_row["id"]),
                "display_name": str(env_row["display_name"] or ""),
                "status": env_status,
                "claimed": claimed,
                "workspace_root": str(env_row["workspace_root"] or ""),
                "created_at": env_row["created_at"],
                "claimed_at": env_row["claimed_at"],
                "owner_user_id": owner_user_id or None,
            },
            "access_mode": access_mode,
            "agent_session": (
                {
                    "status": str(agent_access.get("status") or ""),
                    "expires_at": _to_int(agent_access.get("expires_at")),
                    "handoff_pending": str(agent_access.get("status") or "") == ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING,
                }
                if agent_access
                else None
            ),
            "outstanding_count": len(outstanding),
            "outstanding_projects": outstanding[:20],
        }
    
    @app.get("/api/a2a/environments/{env_id}/handoff/wait", response_model=A2AEnvironmentHandoffWaitOut)
    async def wait_a2a_environment_handoff(
        request: Request,
        env_id: str,
        timeout_sec: int = 45,
        poll_interval_ms: int = 1000,
    ):
        timeout_sec = max(1, min(int(timeout_sec or 45), 300))
        poll_interval_ms = max(200, min(int(poll_interval_ms or 1000), 5000))
        started = time.monotonic()
        deadline = started + float(timeout_sec)
        while True:
            _resolve_environment_agent_access(request, env_id, required_scope="env.handoff.wait")
            conn = db()
            env_row = conn.execute(
                "SELECT id, owner_user_id, status, claimed_at FROM environments WHERE id = ?",
                (env_id,),
            ).fetchone()
            conn.close()
            if not env_row:
                raise HTTPException(404, "Environment not found")
            owner_user_id = str(env_row["owner_user_id"] or "").strip()
            status = str(env_row["status"] or "").strip()
            claimed = bool(owner_user_id) and status == ENV_STATUS_ACTIVE
            waited_ms = int(max(0.0, (time.monotonic() - started) * 1000.0))
            if claimed:
                return A2AEnvironmentHandoffWaitOut(
                    ok=True,
                    environment_id=env_id,
                    event="claimed",
                    claimed=True,
                    status=status,
                    owner_user_id=owner_user_id,
                    claimed_at=_to_int(env_row["claimed_at"]) or None,
                    waited_ms=waited_ms,
                )
            if time.monotonic() >= deadline:
                return A2AEnvironmentHandoffWaitOut(
                    ok=True,
                    environment_id=env_id,
                    event="timeout",
                    claimed=False,
                    status=status,
                    owner_user_id=owner_user_id or None,
                    claimed_at=_to_int(env_row["claimed_at"]) or None,
                    waited_ms=waited_ms,
                )
            await asyncio.sleep(poll_interval_ms / 1000.0)
    
    @app.post("/api/a2a/environments/{env_id}/handoff/ack", response_model=A2AEnvironmentHandoffAckOut)
    async def ack_a2a_environment_handoff(request: Request, env_id: str):
        access = _resolve_environment_agent_access(request, env_id, required_scope="env.handoff.ack")
        now = int(time.time())
        agent_id = str(access.get("agent_id") or "").strip() or "agent"
        conn = db()
        env_row = conn.execute(
            "SELECT id, owner_user_id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")
        owner_user_id = str(env_row["owner_user_id"] or "").strip()
        if not owner_user_id:
            conn.close()
            raise HTTPException(409, "Environment is not claimed yet")
        link_token, link_expires_at = _issue_environment_agent_link_token(
            conn,
            env_id=env_id,
            agent_id=agent_id,
            ttl_sec=ENV_AGENT_LINK_TOKEN_TTL_SEC,
        )
        revoked = conn.execute(
            """
            UPDATE environment_agent_sessions
            SET status = ?, revoked_at = ?
            WHERE env_id = ? AND agent_id = ? AND status IN (?, ?)
            """,
            (
                ENV_AGENT_SESSION_STATUS_REVOKED,
                now,
                env_id,
                agent_id,
                ENV_AGENT_SESSION_STATUS_ACTIVE,
                ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING,
            ),
        ).rowcount
        conn.commit()
        conn.close()
        return A2AEnvironmentHandoffAckOut(
            ok=True,
            environment_id=env_id,
            status=ENV_AGENT_SESSION_STATUS_REVOKED,
            revoked_sessions=max(0, int(revoked or 0)),
            link_token=link_token,
            link_token_expires_at=link_expires_at,
        )
    
    @app.post("/api/a2a/agent-links/session/start", response_model=A2AAgentLinkSessionStartOut)
    async def start_a2a_session_from_agent_link(payload: A2AAgentLinkSessionStartIn):
        raw_link = str(payload.link_token or "").strip()
        if not raw_link:
            raise HTTPException(400, "link_token is required")
        now = int(time.time())
        conn = db()
        link_row = conn.execute(
            """
            SELECT id, env_id, agent_id, status, expires_at
            FROM environment_agent_links
            WHERE token_hash = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (_hash_access_token(raw_link),),
        ).fetchone()
        if not link_row:
            conn.close()
            raise HTTPException(401, "Invalid agent link token")
        link_status = str(link_row["status"] or "").strip().lower()
        if link_status != ENV_AGENT_LINK_STATUS_ACTIVE:
            conn.close()
            raise HTTPException(403, "Agent link token is not active")
        if _to_int(link_row["expires_at"]) <= now:
            conn.execute(
                "UPDATE environment_agent_links SET status = ?, revoked_at = ? WHERE id = ?",
                (ENV_AGENT_LINK_STATUS_EXPIRED, now, str(link_row["id"])),
            )
            conn.commit()
            conn.close()
            raise HTTPException(401, "Agent link token expired")
        env_id = str(link_row["env_id"] or "").strip()
        agent_id = str(link_row["agent_id"] or "").strip() or "agent"
        env_row = conn.execute(
            "SELECT id, owner_user_id, status FROM environments WHERE id = ?",
            (env_id,),
        ).fetchone()
        if not env_row:
            conn.close()
            raise HTTPException(404, "Environment not found")
        owner_user_id = str(env_row["owner_user_id"] or "").strip()
        if not owner_user_id or str(env_row["status"] or "").strip() != ENV_STATUS_ACTIVE:
            conn.close()
            raise HTTPException(409, "Environment is not active for runtime agent session")
    
        conn.execute(
            """
            UPDATE environment_agent_sessions
            SET status = ?, revoked_at = ?
            WHERE env_id = ? AND agent_id = ? AND status IN (?, ?)
            """,
            (
                ENV_AGENT_SESSION_STATUS_REVOKED,
                now,
                env_id,
                agent_id,
                ENV_AGENT_SESSION_STATUS_ACTIVE,
                ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING,
            ),
        )
        runtime_ttl = max(60, min(int(payload.session_ttl_sec or ENV_AGENT_RUNTIME_SESSION_TTL_SEC), 60 * 60))
        runtime_scopes = ["env.read", "project.read", "project.write", "project.chat", "project.state.write"]
        session_token, session_expires_at = _issue_environment_agent_session(
            conn,
            env_id=env_id,
            agent_id=agent_id,
            scopes=runtime_scopes,
            ttl_sec=runtime_ttl,
        )
        conn.execute(
            "UPDATE environment_agent_links SET last_used_at = ? WHERE id = ?",
            (now, str(link_row["id"])),
        )
        conn.commit()
        conn.close()
        return A2AAgentLinkSessionStartOut(
            ok=True,
            environment_id=env_id,
            agent_id=agent_id,
            session_token=session_token,
            session_expires_at=session_expires_at,
            scopes=runtime_scopes,
        )
    
    def _json_loads_or(raw: Any, fallback: Any) -> Any:
        text = str(raw or "").strip()
        if not text:
            return fallback
        try:
            return json.loads(text)
        except Exception:
            return fallback
    
    def _resolve_managed_agent_row(user_id: str, agent_id: str, connection_id: Optional[str] = None) -> Dict[str, Any]:
        aid = str(agent_id or "").strip()
        if not aid:
            raise HTTPException(400, "agent_id is required")
        conn = db()
        if connection_id:
            row = conn.execute(
                """
                SELECT *
                FROM managed_agents
                WHERE user_id = ? AND agent_id = ? AND connection_id = ?
                LIMIT 1
                """,
                (user_id, aid, str(connection_id).strip()),
            ).fetchone()
            conn.close()
            if not row:
                raise HTTPException(404, "Managed agent not found")
            return dict(row)
    
        rows = conn.execute(
            """
            SELECT *
            FROM managed_agents
            WHERE user_id = ? AND agent_id = ?
            ORDER BY updated_at DESC
            LIMIT 2
            """,
            (user_id, aid),
        ).fetchall()
        conn.close()
        if not rows:
            raise HTTPException(404, "Managed agent not found")
        if len(rows) > 1:
            raise HTTPException(409, "Multiple agents share this id. Pass connection_id to disambiguate.")
        return dict(rows[0])
    
    @app.get("/api/a2a/agents")
    async def list_managed_a2a_agents(request: Request, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        conn = db()
        if connection_id:
            rows = conn.execute(
                """
                SELECT connection_id, env_id, agent_id, agent_name, status, root_path, provisioned_at, updated_at
                FROM managed_agents
                WHERE user_id = ? AND connection_id = ?
                ORDER BY updated_at DESC, agent_name ASC
                """,
                (user_id, str(connection_id).strip()),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT connection_id, env_id, agent_id, agent_name, status, root_path, provisioned_at, updated_at
                FROM managed_agents
                WHERE user_id = ?
                ORDER BY updated_at DESC, agent_name ASC
                """,
                (user_id,),
            ).fetchall()
        conn.close()
        return {
            "ok": True,
            "count": len(rows),
            "agents": [dict(r) for r in rows],
        }
    
    @app.get("/api/a2a/agents/{agent_id}/card")
    async def get_managed_a2a_agent_card(request: Request, agent_id: str, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "agent_name": row["agent_name"],
            "connection_id": row["connection_id"],
            "env_id": row["env_id"],
            "card_version": row["card_version"],
            "card": _json_loads_or(row["card_json"], {}),
            "root_path": row["root_path"],
            "updated_at": row["updated_at"],
        }
    
    @app.get("/api/a2a/agents/{agent_id}/memory")
    async def get_managed_a2a_agent_memory(request: Request, agent_id: str, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        conn = db()
        mem_rows = conn.execute(
            """
            SELECT memory_scope, summary, payload_json, updated_at
            FROM managed_agent_memory
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            ORDER BY memory_scope ASC
            """,
            (user_id, row["connection_id"], row["agent_id"]),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "connection_id": row["connection_id"],
            "memory": [
                {
                    "scope": str(r["memory_scope"]),
                    "summary": str(r["summary"] or ""),
                    "payload": _json_loads_or(r["payload_json"], {}),
                    "updated_at": _to_int(r["updated_at"]),
                }
                for r in mem_rows
            ],
        }
    
    @app.get("/api/a2a/agents/{agent_id}/history")
    async def get_managed_a2a_agent_history(
        request: Request,
        agent_id: str,
        connection_id: Optional[str] = None,
        limit: int = 50,
    ):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        cap = max(1, min(int(limit or 50), 300))
        conn = db()
        history_rows = conn.execute(
            """
            SELECT event_kind, event_text, event_payload_json, created_at
            FROM managed_agent_history
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, row["connection_id"], row["agent_id"], cap),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "connection_id": row["connection_id"],
            "count": len(history_rows),
            "events": [
                {
                    "kind": str(r["event_kind"] or ""),
                    "text": str(r["event_text"] or ""),
                    "payload": _json_loads_or(r["event_payload_json"], {}),
                    "created_at": _to_int(r["created_at"]),
                }
                for r in history_rows
            ],
        }
    
    @app.get("/api/a2a/agents/{agent_id}/checkpoints")
    async def get_managed_a2a_agent_checkpoints(request: Request, agent_id: str, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        conn = db()
        cp_rows = conn.execute(
            """
            SELECT checkpoint_key, state_json, status, created_at, updated_at
            FROM managed_agent_checkpoints
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            ORDER BY updated_at DESC
            """,
            (user_id, row["connection_id"], row["agent_id"]),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "connection_id": row["connection_id"],
            "checkpoints": [
                {
                    "key": str(r["checkpoint_key"] or ""),
                    "status": str(r["status"] or ""),
                    "state": _json_loads_or(r["state_json"], {}),
                    "created_at": _to_int(r["created_at"]),
                    "updated_at": _to_int(r["updated_at"]),
                }
                for r in cp_rows
            ],
        }
    
    @app.get("/api/a2a/agents/{agent_id}/metrics")
    async def get_managed_a2a_agent_metrics(request: Request, agent_id: str, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        conn = db()
        metric_row = conn.execute(
            """
            SELECT success_count, failure_count, total_calls, total_prompt_tokens, total_completion_tokens,
                   total_latency_ms, last_error, last_seen_at, updated_at
            FROM managed_agent_metrics
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            LIMIT 1
            """,
            (user_id, row["connection_id"], row["agent_id"]),
        ).fetchone()
        conn.close()
        if not metric_row:
            return {"ok": True, "agent_id": row["agent_id"], "connection_id": row["connection_id"], "metrics": None}
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "connection_id": row["connection_id"],
            "metrics": dict(metric_row),
        }
    
    @app.get("/api/a2a/agents/{agent_id}/policies")
    async def get_managed_a2a_agent_policies(request: Request, agent_id: str, connection_id: Optional[str] = None):
        user_id = get_session_user(request)
        row = _resolve_managed_agent_row(user_id, agent_id, connection_id)
        conn = db()
        perm_row = conn.execute(
            """
            SELECT scopes_json, tools_json, path_allowlist_json, secrets_policy_json, approval_required, updated_at
            FROM managed_agent_permissions
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            LIMIT 1
            """,
            (user_id, row["connection_id"], row["agent_id"]),
        ).fetchone()
        approval_rows = conn.execute(
            """
            SELECT rule_key, policy_json, is_enabled, created_at, updated_at
            FROM managed_agent_approval_rules
            WHERE user_id = ? AND connection_id = ? AND agent_id = ?
            ORDER BY rule_key ASC
            """,
            (user_id, row["connection_id"], row["agent_id"]),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "agent_id": row["agent_id"],
            "connection_id": row["connection_id"],
            "permissions": (
                {
                    "scopes": _json_loads_or(perm_row["scopes_json"], []),
                    "tools": _json_loads_or(perm_row["tools_json"], []),
                    "path_allowlist": _json_loads_or(perm_row["path_allowlist_json"], []),
                    "secrets_policy": _json_loads_or(perm_row["secrets_policy_json"], {}),
                    "approval_required": bool(_to_int(perm_row["approval_required"])),
                    "updated_at": _to_int(perm_row["updated_at"]),
                }
                if perm_row
                else None
            ),
            "approval_rules": [
                {
                    "rule_key": str(r["rule_key"] or ""),
                    "policy": _json_loads_or(r["policy_json"], {}),
                    "is_enabled": bool(_to_int(r["is_enabled"])),
                    "created_at": _to_int(r["created_at"]),
                    "updated_at": _to_int(r["updated_at"]),
                }
                for r in approval_rows
            ],
        }
    
