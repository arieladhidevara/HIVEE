from hivee_shared import *

def register_routes(app: FastAPI) -> None:
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return FileResponse("static/index.html")

    @app.get("/docs", response_class=HTMLResponse)
    async def docs():
        return FileResponse("static/docs.html")

    @app.post("/api/signup", response_model=SessionOut)
    async def signup(payload: SignupIn, response: Response):
        email = _normalize_email(payload.email)
        _validate_password_strength(payload.password)
        conn = db()
        user_id = new_id("usr")
        try:
            conn.execute(
                "INSERT INTO users (id, email, password, created_at) VALUES (?,?,?,?)",
                (user_id, email, _hash_password(payload.password), int(time.time())),
            )
            username = _ensure_user_username(user_id, email, conn)
            token = _issue_user_session(conn, user_id)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            raise HTTPException(400, "Email already registered")
        conn.close()
        _ensure_user_workspace(user_id)
        _ensure_primary_environment_for_user(user_id, email=email)
        _set_session_cookie(response, token)
        return SessionOut(token=token, username=username)

    @app.post("/api/login", response_model=SessionOut)
    async def login(payload: LoginIn, response: Response):
        email = _normalize_email(payload.email)
        conn = db()
        row = conn.execute(
            "SELECT id, email, password FROM users WHERE email = ?",
            (email,),
        ).fetchone()
        if not row or not _verify_password_and_upgrade(conn, str(row["id"]), payload.password, str(row["password"] or "")):
            conn.close()
            raise HTTPException(401, "Invalid email/password")
        user_id = str(row["id"])
        token = _issue_user_session(conn, user_id)
        username = _ensure_user_username(user_id, str(row["email"] or email), conn)
        conn.commit()
        conn.close()
        _ensure_user_workspace(user_id)
        _ensure_primary_environment_for_user(user_id, email=str(row["email"] or email))
        _set_session_cookie(response, token)
        return SessionOut(token=token, username=username)
    
    @app.post("/api/oauth/{provider}/start", response_model=OAuthStartOut)
    async def oauth_start(request: Request, provider: str, payload: OAuthStartIn):
        provider_cfg = _oauth_provider_config(provider)
        provider_key = str(provider_cfg["provider"])
        next_path = _sanitize_next_path(payload.next_path)
        state = _new_oauth_state_token()
        now = int(time.time())
        conn = db()
        conn.execute("DELETE FROM oauth_states WHERE expires_at <= ?", (now,))
        conn.execute(
            "INSERT INTO oauth_states (state, provider, redirect_path, created_at, expires_at) VALUES (?,?,?,?,?)",
            (state, provider_key, next_path, now, now + OAUTH_STATE_TTL_SEC),
        )
        conn.commit()
        conn.close()
        redirect_uri = _oauth_callback_url(request, provider_key)
        auth_url = _build_oauth_authorize_url(provider_cfg, redirect_uri=redirect_uri, state=state)
        return OAuthStartOut(provider=provider_key, auth_url=auth_url)
    
    @app.get("/api/oauth/providers", response_model=OAuthProvidersOut)
    async def oauth_providers():
        return OAuthProvidersOut(providers=_oauth_providers_public_status())
    
    @app.get("/api/oauth/{provider}/callback", name="oauth_callback")
    async def oauth_callback(
        request: Request,
        provider: str,
        code: str = "",
        state: str = "",
        error: Optional[str] = None,
    ):
        provider_key = str(provider or "").strip().lower()
        if error:
            return _oauth_redirect_with_message("/", error=f"{provider_key} OAuth error: {error}")
        if not code or not state:
            return _oauth_redirect_with_message("/", error="Missing OAuth code/state.")
    
        now = int(time.time())
        next_path = "/"
        conn = db()
        row = conn.execute(
            "SELECT provider, redirect_path, expires_at FROM oauth_states WHERE state = ?",
            (state,),
        ).fetchone()
        conn.execute("DELETE FROM oauth_states WHERE expires_at <= ?", (now,))
        if not row:
            conn.commit()
            conn.close()
            return _oauth_redirect_with_message("/", error="OAuth state invalid or expired.")
        next_path = _sanitize_next_path(row["redirect_path"])
        if str(row["provider"] or "").strip().lower() != provider_key:
            conn.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
            conn.commit()
            conn.close()
            return _oauth_redirect_with_message(next_path, error="OAuth state/provider mismatch.")
        if int(row["expires_at"] or 0) <= now:
            conn.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
            conn.commit()
            conn.close()
            return _oauth_redirect_with_message(next_path, error="OAuth state expired.")
        conn.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
        conn.commit()
        conn.close()
    
        try:
            provider_cfg = _oauth_provider_config(provider_key)
            redirect_uri = _oauth_callback_url(request, provider_key)
            access_token = await _oauth_exchange_code_for_token(provider_cfg, code=str(code), redirect_uri=redirect_uri)
            profile = await _oauth_fetch_profile(provider_cfg, access_token=access_token)
        except HTTPException as exc:
            return _oauth_redirect_with_message(next_path, error=detail_to_text(exc.detail))
        except Exception:
            return _oauth_redirect_with_message(next_path, error="OAuth login failed. Please retry.")
    
        conn = db()
        try:
            user_id, user_email = _resolve_oauth_user(
                conn,
                provider=provider_key,
                provider_user_id=str(profile.get("provider_user_id") or ""),
                email=str(profile.get("email") or ""),
                display_name=str(profile.get("display_name") or ""),
            )
            token = _issue_user_session(conn, user_id)
            username = _ensure_user_username(user_id, user_email, conn)
            conn.commit()
        finally:
            conn.close()

        _ensure_user_workspace(user_id)
        _ensure_primary_environment_for_user(user_id, email=user_email)
        # Build the final redirect URL: /{username}/ + preserved query + auth token in hash fragment
        from urllib.parse import urlparse, urlunparse, quote as url_quote_path
        parsed = urlparse(next_path)
        # Strip any existing /{username} prefix to avoid doubling
        raw_path = parsed.path if parsed.path not in ("", "/") else "/"
        if raw_path.startswith(f"/{username}"):
            base_path = raw_path
        else:
            base_path = f"/{username}/" + raw_path.lstrip("/")
        # Embed token in hash fragment so JS can pick it up without a cookie round-trip
        auth_fragment = f"_oauth_token={url_quote_path(token, safe='')}"
        existing_fragment = parsed.fragment
        fragment = f"{existing_fragment}&{auth_fragment}" if existing_fragment else auth_fragment
        final_path = urlunparse(("", "", base_path, parsed.params, parsed.query, fragment))
        redirect = _oauth_redirect_with_message(final_path)
        _set_session_cookie(redirect, token)
        return redirect
    

    @app.get("/api/session/token")
    async def get_session_token(request: Request):
        """Return the active session token + username so JS can sync cookie session into localStorage."""
        from core.session_project_access import _bearer_token
        token = _bearer_token(request)
        if not token:
            raise HTTPException(401, "No active session")
        conn = db()
        sess_row = conn.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
        if not sess_row:
            conn.close()
            raise HTTPException(401, "Invalid session token")
        user_row = conn.execute("SELECT email, username FROM users WHERE id = ?", (str(sess_row["user_id"]),)).fetchone()
        if not user_row:
            conn.close()
            raise HTTPException(401, "User not found")
        username = str(user_row["username"] or "").strip()
        if not username:
            username = _ensure_user_username(str(sess_row["user_id"]), str(user_row["email"] or ""), conn)
            conn.commit()
        conn.close()
        return {"token": token, "username": username}

    @app.get("/api/me", response_model=AccountProfileOut)
    async def get_account_profile(request: Request):
        user_id = get_session_user(request)
        _ensure_user_workspace(user_id)
        workspace_root = _user_home_dir(user_id).resolve()
        conn = db()
        row = conn.execute(
            "SELECT id, email, username, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "User not found")
        username = str(row["username"] or "").strip()
        if not username:
            username = _ensure_user_username(user_id, str(row["email"] or ""), conn)
            conn.commit()
        project_count_row = conn.execute(
            "SELECT COUNT(1) AS c FROM projects WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        connection_count_row = conn.execute(
            "SELECT COUNT(1) AS c FROM connectors WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        conn.close()
        return AccountProfileOut(
            id=str(row["id"]),
            email=str(row["email"] or ""),
            username=username,
            created_at=int(row["created_at"] or 0),
            workspace_root=workspace_root.as_posix(),
            projects_count=int((project_count_row or {"c": 0})["c"] or 0),
            connections_count=int((connection_count_row or {"c": 0})["c"] or 0),
        )
    
    @app.get("/api/me/oauth-providers", response_model=AccountOAuthProvidersOut)
    async def get_account_oauth_providers(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            "SELECT DISTINCT provider FROM oauth_identities WHERE user_id = ? ORDER BY provider ASC",
            (user_id,),
        ).fetchall()
        conn.close()
        pretty: List[str] = []
        for row in rows:
            key = str(row["provider"] or "").strip().lower()
            if not key:
                continue
            cfg = OAUTH_PROVIDERS.get(key)
            pretty.append(str((cfg or {}).get("display") or key.title()))
        return AccountOAuthProvidersOut(providers=pretty)
    
    @app.get("/api/me/environments")
    async def list_my_environments(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            """
            SELECT id, display_name, status, workspace_root, created_at, claimed_at
            FROM environments
            WHERE owner_user_id = ?
            ORDER BY COALESCE(claimed_at, created_at) DESC
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        return {
            "ok": True,
            "environments": [
                {
                    "id": str(r["id"]),
                    "display_name": str(r["display_name"] or ""),
                    "status": str(r["status"] or ""),
                    "workspace_root": str(r["workspace_root"] or ""),
                    "created_at": r["created_at"],
                    "claimed_at": r["claimed_at"],
                }
                for r in rows
            ],
        }
    
    @app.post("/api/me/password")
    async def change_account_password(request: Request, payload: AccountPasswordChangeIn):
        user_id = get_session_user(request)
        current_token = _bearer_token(request)
        current_password = str(payload.current_password or "")
        new_password = str(payload.new_password or "")
        _validate_password_strength(new_password, field_name="New password")
        if current_password == new_password:
            raise HTTPException(400, "New password must be different from current password")
        conn = db()
        row = conn.execute(
            "SELECT password FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "User not found")
        if not _verify_password(current_password, str(row["password"] or "")):
            conn.close()
            raise HTTPException(401, "Current password is incorrect")
        conn.execute(
            "UPDATE users SET password = ? WHERE id = ?",
            (_hash_password(new_password), user_id),
        )
        if current_token:
            conn.execute(
                "DELETE FROM sessions WHERE user_id = ? AND token <> ?",
                (user_id, current_token),
            )
        conn.commit()
        conn.close()
        return {"ok": True, "message": "Password updated successfully."}
    
    @app.post("/api/me/delete")
    async def delete_account(request: Request, response: Response, payload: AccountDeleteIn):
        user_id = get_session_user(request)
        confirm = str(payload.confirm_text or "").strip().upper()
        if confirm != "DELETE":
            raise HTTPException(400, "Type DELETE to confirm account deletion.")
    
        conn = db()
        row = conn.execute(
            "SELECT id, password FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "User not found")
        if not _verify_password(str(payload.current_password or ""), str(row["password"] or "")):
            raise HTTPException(401, "Current password is incorrect")
    
        deleted = _delete_account_with_resources(user_id=user_id)
        if not deleted.get("ok"):
            raise HTTPException(400, deleted.get("error") or "Failed to delete account")
    
        _clear_session_cookie(response)
        return {"ok": True, "message": "Account deleted permanently.", "details": deleted}
    
    @app.get("/api/users/lookup")
    async def lookup_user_by_email(request: Request, email: str = ""):
        get_session_user(request)
        normalized = _normalize_email(email)
        if not normalized or len(normalized) < 3:
            return {"found": False}
        conn = db()
        row = conn.execute(
            "SELECT email FROM users WHERE email = ?", (normalized,)
        ).fetchone()
        conn.close()
        if not row:
            return {"found": False}
        return {"found": True, "email": str(row["email"] or "")}

    @app.get("/api/me/inbox/invites")
    async def get_inbox_invites(request: Request):
        user_id = get_session_user(request)
        conn = db()
        user_row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user_row:
            conn.close()
            return {"invites": []}
        user_email = _normalize_email(str(user_row["email"] or ""))
        now = int(time.time())
        rows = conn.execute(
            """
            SELECT pi.id, pi.project_id, pi.owner_user_id, pi.role, pi.invite_note,
                   pi.status, pi.expires_at, pi.created_at,
                   pi.requested_agent_id, pi.requested_agent_name,
                   p.title AS project_title, p.goal AS project_goal,
                   u.email AS owner_email
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            JOIN users u ON u.id = pi.owner_user_id
            WHERE pi.target_email = ? AND pi.status = 'pending' AND pi.expires_at > ?
            ORDER BY pi.created_at DESC
            LIMIT 50
            """,
            (user_email, now),
        ).fetchall()
        conn.close()
        return {
            "invites": [
                {
                    "id": str(r["id"]),
                    "project_id": str(r["project_id"]),
                    "project_title": str(r["project_title"] or ""),
                    "project_goal": str(r["project_goal"] or ""),
                    "owner_email": str(r["owner_email"] or ""),
                    "role": str(r["role"] or ""),
                    "note": str(r["invite_note"] or ""),
                    "requested_agent_id": str(r["requested_agent_id"] or "") or None,
                    "requested_agent_name": str(r["requested_agent_name"] or "") or None,
                    "status": str(r["status"] or "pending"),
                    "expires_at": int(r["expires_at"] or 0),
                    "created_at": int(r["created_at"] or 0),
                }
                for r in rows
            ]
        }

    @app.get("/api/me/managed-agents")
    async def list_my_managed_agents(request: Request):
        user_id = get_session_user(request)
        conn = db()
        # Fetch agents from old system (openclaw_connections → managed_agents)
        rows = conn.execute(
            """
            SELECT ma.agent_id, ma.agent_name, ma.connection_id, ma.status,
                   COALESCE(oc.name, con.name) AS connection_name
            FROM managed_agents ma
            LEFT JOIN openclaw_connections oc ON oc.id = ma.connection_id
            LEFT JOIN connectors con ON con.openclaw_base_url = oc.base_url AND con.user_id = ma.user_id
            WHERE ma.user_id = ? AND ma.status = 'active'
            ORDER BY ma.updated_at DESC, ma.agent_name ASC
            LIMIT 200
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        return {
            "agents": [
                {
                    "agent_id": str(r["agent_id"] or ""),
                    "agent_name": str(r["agent_name"] or ""),
                    "connection_id": str(r["connection_id"] or ""),
                    "connection_name": str(r["connection_name"] or ""),
                }
                for r in rows
                if str(r["agent_id"] or "").strip()
            ]
        }

    @app.post("/api/me/inbox/invites/{invite_id}/accept")
    async def accept_inbox_invite(request: Request, invite_id: str, payload: InboxInviteAcceptIn):
        user_id = get_session_user(request)
        inv_id = str(invite_id or "").strip()
        if not inv_id:
            raise HTTPException(400, "invite_id is required")

        connection_id = str(payload.connection_id or "").strip()
        agent_id = str(payload.agent_id or "").strip()
        agent_name = str(payload.agent_name or "").strip()
        if not connection_id or not agent_id:
            raise HTTPException(400, "connection_id and agent_id are required")

        now = int(time.time())
        conn = db()

        # Verify connection belongs to user — accept both old openclaw_connections.id and new connectors.id
        legacy_conn_id = connection_id  # the ID used for managed_agents lookups
        oc_row = conn.execute(
            "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
            (connection_id, user_id),
        ).fetchone()
        if not oc_row:
            # Try as a new-system connector ID
            con_row = conn.execute(
                "SELECT id, openclaw_base_url FROM connectors WHERE id = ? AND user_id = ?",
                (connection_id, user_id),
            ).fetchone()
            if not con_row:
                conn.close()
                raise HTTPException(403, "Connection not found or not owned by you")
            # Bridge: find the corresponding openclaw_connections entry by base_url
            base_url = str(con_row["openclaw_base_url"] or "").strip()
            oc_bridge = conn.execute(
                "SELECT id FROM openclaw_connections WHERE user_id = ? AND base_url = ? LIMIT 1",
                (user_id, base_url),
            ).fetchone() if base_url else None
            legacy_conn_id = str(oc_bridge["id"]).strip() if oc_bridge else connection_id

        # Verify agent exists on that connection (using legacy_conn_id for managed_agents table)
        agent_row = conn.execute(
            "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id = ?",
            (user_id, legacy_conn_id, agent_id),
        ).fetchone()
        resolved_name = str((agent_row["agent_name"] if agent_row else None) or agent_name or agent_id).strip()

        # Look up invite
        user_row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user_row:
            conn.close()
            raise HTTPException(404, "User not found")
        user_email = _normalize_email(str(user_row["email"] or ""))

        invite_row = conn.execute(
            """
            SELECT pi.id, pi.project_id, pi.owner_user_id, pi.target_email,
                   pi.requested_agent_id, pi.requested_agent_name, pi.role,
                   pi.status, pi.expires_at, pi.invite_doc_relpath,
                   p.project_root
            FROM project_external_agent_invites pi
            JOIN projects p ON p.id = pi.project_id
            WHERE pi.id = ?
            LIMIT 1
            """,
            (inv_id,),
        ).fetchone()
        if not invite_row:
            conn.close()
            raise HTTPException(404, "Invite not found")

        invite_status = str(invite_row["status"] or "pending").strip().lower()
        if invite_status != "pending":
            conn.close()
            raise HTTPException(409, f"Invite is not pending (status={invite_status})")

        expires_at = int(invite_row["expires_at"] or 0)
        if expires_at <= now:
            conn.execute(
                "UPDATE project_external_agent_invites SET status = 'expired' WHERE id = ?",
                (inv_id,),
            )
            conn.commit()
            conn.close()
            raise HTTPException(410, "Invite has expired")

        target_email = _normalize_email(str(invite_row["target_email"] or ""))
        if target_email and user_email != target_email:
            conn.close()
            raise HTTPException(403, "This invite was issued to a different email address")

        owner_user_id = str(invite_row["owner_user_id"] or "").strip()
        if user_id == owner_user_id:
            conn.close()
            raise HTTPException(400, "Project owner cannot accept their own invite")

        project_id = str(invite_row["project_id"] or "").strip()
        locked_agent_id = str(invite_row["requested_agent_id"] or "").strip()
        if locked_agent_id and agent_id != locked_agent_id:
            conn.close()
            raise HTTPException(403, f"Invite requires agent_id '{locked_agent_id}'")

        invite_role = str(invite_row["role"] or "").strip()

        # Build the full list of agents to add: primary + any additional from selected_agents
        selected_agents_payload = list(payload.selected_agents or [])
        agents_to_add = [(agent_id, agent_name)]
        for sel in selected_agents_payload:
            sel_id = str(getattr(sel, "agent_id", "") or "").strip()
            if sel_id and sel_id != agent_id:
                sel_name = str(getattr(sel, "agent_name", "") or "").strip()
                agents_to_add.append((sel_id, sel_name))

        def _upsert_agent(aid: str, aname: str) -> str:
            a_row = conn.execute(
                "SELECT agent_id, agent_name FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id = ?",
                (user_id, legacy_conn_id, aid),
            ).fetchone()
            resolved = str((a_row["agent_name"] if a_row else None) or aname or aid).strip()

            existing = conn.execute(
                "SELECT agent_id, source_type, source_user_id, source_connection_id FROM project_agents WHERE project_id = ? AND agent_id = ?",
                (project_id, aid),
            ).fetchone()
            if existing:
                ex_source = str(existing["source_type"] or "owner").strip()
                ex_user = str(existing["source_user_id"] or "").strip()
                ex_conn = str(existing["source_connection_id"] or "").strip()
                if ex_source != "external" or ex_user != user_id or ex_conn not in (connection_id, legacy_conn_id):
                    return resolved  # already in project under different owner — skip silently
                conn.execute(
                    """
                    UPDATE project_agents SET agent_name=?, role=?, source_type='external',
                        source_user_id=?, source_connection_id=?, joined_via_invite_id=?, added_at=?
                    WHERE project_id=? AND agent_id=?
                    """,
                    (resolved, invite_role, user_id, legacy_conn_id, inv_id, now, project_id, aid),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO project_agents
                        (project_id, agent_id, agent_name, is_primary, role, source_type,
                         source_user_id, source_connection_id, joined_via_invite_id, added_at)
                    VALUES (?,?,?,0,?,'external',?,?,?,?)
                    """,
                    (project_id, aid, resolved, invite_role, user_id, legacy_conn_id, inv_id, now),
                )

            mem_row = conn.execute(
                "SELECT id FROM project_external_agent_memberships WHERE project_id=? AND member_user_id=? AND member_connection_id=? AND agent_id=?",
                (project_id, user_id, legacy_conn_id, aid),
            ).fetchone()
            if mem_row:
                conn.execute(
                    "UPDATE project_external_agent_memberships SET owner_user_id=?, agent_name=?, role=?, invite_id=?, status='active', updated_at=? WHERE id=?",
                    (owner_user_id, resolved, invite_role, inv_id, now, str(mem_row["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO project_external_agent_memberships
                        (id, project_id, owner_user_id, member_user_id, member_connection_id,
                         agent_id, agent_name, role, invite_id, status, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,'active',?,?)
                    """,
                    (new_id("pmem"), project_id, owner_user_id, user_id, legacy_conn_id,
                     aid, resolved, invite_role, inv_id, now, now),
                )
            return resolved

        resolved_name = _upsert_agent(agent_id, agent_name)
        for extra_id, extra_name in agents_to_add[1:]:
            _upsert_agent(extra_id, extra_name)

        # Issue project agent access token
        raw_token = _new_agent_access_token()
        conn.execute(
            """
            INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, token_plain, created_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(project_id, agent_id) DO UPDATE SET
                token_hash = excluded.token_hash,
                token_plain = excluded.token_plain,
                created_at = excluded.created_at
            """,
            (project_id, agent_id, _hash_access_token(raw_token), raw_token, now),
        )

        # Mark invite accepted
        conn.execute(
            "UPDATE project_external_agent_invites SET status='accepted', accepted_at=?, accepted_by_user_id=?, accepted_connection_id=?, accepted_agent_id=? WHERE id=?",
            (now, user_id, legacy_conn_id, agent_id, inv_id),
        )

        # Fetch member username for notification display
        member_username_row = conn.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
        member_username = str(member_username_row["username"] or user_email or user_id).strip() if member_username_row else (user_email or user_id)
        project_title_row = conn.execute("SELECT title FROM projects WHERE id = ?", (project_id,)).fetchone()
        project_title = str(project_title_row["title"] or project_id).strip() if project_title_row else project_id

        # Store inbox notification for project owner
        notif_agents = [{"agent_id": aid, "agent_name": aname} for aid, aname in agents_to_add]
        conn.execute(
            """
            INSERT INTO user_inbox_notifications (id, user_id, kind, project_id, data_json, is_read, created_at)
            VALUES (?, ?, 'invite_accepted', ?, ?, 0, ?)
            """,
            (
                new_id("notif"),
                owner_user_id,
                project_id,
                json.dumps({
                    "agents": notif_agents,
                    "member_username": member_username,
                    "project_title": project_title,
                    "invite_id": inv_id,
                }),
                now,
            ),
        )

        conn.commit()
        conn.close()

        # Emit project events so real-time chat shows system messages
        for aid, aname in agents_to_add:
            try:
                await emit(project_id, "project.external_agent.joined", {
                    "agent_id": aid,
                    "agent_name": aname,
                    "role": invite_role,
                    "joined_via": "inbox_accept",
                    "member_username": member_username,
                })
            except Exception:
                pass

        return {
            "ok": True,
            "project_id": project_id,
            "agent_id": agent_id,
            "agent_name": resolved_name,
            "project_agent_access_token": raw_token,
        }

    @app.post("/api/me/inbox/invites/{invite_id}/decline")
    async def decline_inbox_invite(request: Request, invite_id: str):
        user_id = get_session_user(request)
        inv_id = str(invite_id or "").strip()
        if not inv_id:
            raise HTTPException(400, "invite_id is required")
        now = int(time.time())
        conn = db()
        user_row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user_row:
            conn.close()
            raise HTTPException(404, "User not found")
        user_email = _normalize_email(str(user_row["email"] or ""))
        invite_row = conn.execute(
            "SELECT id, target_email, status FROM project_external_agent_invites WHERE id = ? LIMIT 1",
            (inv_id,),
        ).fetchone()
        if not invite_row:
            conn.close()
            raise HTTPException(404, "Invite not found")
        target_email = _normalize_email(str(invite_row["target_email"] or ""))
        if target_email and user_email != target_email:
            conn.close()
            raise HTTPException(403, "This invite was issued to a different email address")
        if str(invite_row["status"] or "") not in ("pending", ""):
            conn.close()
            raise HTTPException(409, f"Invite is not pending (status={invite_row['status']})")
        conn.execute(
            "UPDATE project_external_agent_invites SET status='declined', accepted_at=? WHERE id=?",
            (now, inv_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    @app.get("/api/me/inbox/notifications")
    async def get_inbox_notifications(request: Request):
        user_id = get_session_user(request)
        conn = db()
        rows = conn.execute(
            """
            SELECT id, kind, project_id, data_json, is_read, created_at
            FROM user_inbox_notifications
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (user_id,),
        ).fetchall()
        conn.close()
        notifications = []
        for r in rows:
            data = {}
            try:
                data = json.loads(str(r["data_json"] or "{}"))
            except Exception:
                pass
            notifications.append({
                "id": str(r["id"]),
                "kind": str(r["kind"]),
                "project_id": str(r["project_id"] or ""),
                "data": data,
                "is_read": bool(r["is_read"]),
                "created_at": int(r["created_at"]),
            })
        return {"ok": True, "notifications": notifications}

    @app.post("/api/me/inbox/notifications/{notif_id}/read")
    async def mark_notification_read(request: Request, notif_id: str):
        user_id = get_session_user(request)
        conn = db()
        conn.execute(
            "UPDATE user_inbox_notifications SET is_read = 1 WHERE id = ? AND user_id = ?",
            (str(notif_id).strip(), user_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    @app.post("/api/logout")
    async def logout(request: Request, response: Response):
        token = _bearer_token(request)
        if token:
            conn = db()
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            conn.close()
        _clear_session_cookie(response)
        return {"ok": True}


