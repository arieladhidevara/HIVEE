from hivee_shared import *

def register_routes(app: FastAPI) -> None:
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return FileResponse("static/index.html")
    
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
            token = _issue_user_session(conn, user_id)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            raise HTTPException(400, "Email already registered")
        conn.close()
        _ensure_user_workspace(user_id)
        _ensure_primary_environment_for_user(user_id, email=email)
        _set_session_cookie(response, token)
        return SessionOut(token=token)
    
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
        token = _issue_user_session(conn, str(row["id"]))
        conn.commit()
        conn.close()
        _ensure_user_workspace(row["id"])
        _ensure_primary_environment_for_user(str(row["id"]), email=str(row["email"] or email))
        _set_session_cookie(response, token)
        return SessionOut(token=token)
    
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
            conn.commit()
        finally:
            conn.close()
    
        _ensure_user_workspace(user_id)
        _ensure_primary_environment_for_user(user_id, email=user_email)
        redirect = _oauth_redirect_with_message(next_path)
        _set_session_cookie(redirect, token)
        return redirect
    

    @app.get("/api/me", response_model=AccountProfileOut)
    async def get_account_profile(request: Request):
        user_id = get_session_user(request)
        _ensure_user_workspace(user_id)
        workspace_root = _user_home_dir(user_id).resolve()
        conn = db()
        row = conn.execute(
            "SELECT id, email, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, "User not found")
        project_count_row = conn.execute(
            """
            SELECT COUNT(DISTINCT pm.project_id) AS c
            FROM project_memberships pm
            WHERE pm.user_id = ?
            """,
            (user_id,),
        ).fetchone()
        connection_count_row = conn.execute(
            "SELECT COUNT(1) AS c FROM connections WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        conn.close()
        return AccountProfileOut(
            id=str(row["id"]),
            email=str(row["email"] or ""),
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
    
