from .db import *

def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return slug or "project"

def detail_to_text(detail: Any) -> str:
    if detail is None:
        return ""
    if isinstance(detail, (str, int, float, bool)):
        return str(detail)
    try:
        return json.dumps(detail, ensure_ascii=False)[:3000]
    except Exception:
        return str(detail)

def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except Exception:
        return "-"

def _hash_access_token(token: str) -> str:
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()

def _normalize_email(raw_email: str) -> str:
    return str(raw_email or "").strip().lower()

def _validate_password_strength(password: str, *, field_name: str = "Password") -> None:
    raw = str(password or "")
    if len(raw) < PASSWORD_MIN_LENGTH:
        raise HTTPException(400, f"{field_name} must be at least {PASSWORD_MIN_LENGTH} characters.")
    if not re.search(r"[a-z]", raw):
        raise HTTPException(400, f"{field_name} must include at least one lowercase letter.")
    if not re.search(r"[A-Z]", raw):
        raise HTTPException(400, f"{field_name} must include at least one uppercase letter.")
    if not re.search(r"[0-9]", raw):
        raise HTTPException(400, f"{field_name} must include at least one number.")
    if not re.search(r"[^A-Za-z0-9]", raw):
        raise HTTPException(400, f"{field_name} must include at least one symbol.")

def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return f"{PASSWORD_HASH_PREFIX}${PASSWORD_HASH_ITERATIONS}${salt.hex()}${digest.hex()}"

def _verify_password(password: str, stored_password: str) -> bool:
    provided = str(password or "")
    stored = str(stored_password or "")
    prefix = f"{PASSWORD_HASH_PREFIX}$"
    if not stored.startswith(prefix):
        return secrets.compare_digest(stored, provided)
    parts = stored.split("$")
    if len(parts) != 4:
        return False
    _, iter_raw, salt_hex, digest_hex = parts
    try:
        iterations = int(iter_raw)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except Exception:
        return False
    computed = hashlib.pbkdf2_hmac("sha256", provided.encode("utf-8"), salt, iterations)
    return secrets.compare_digest(computed, expected)

def _is_password_hashed(stored_password: str) -> bool:
    return str(stored_password or "").startswith(f"{PASSWORD_HASH_PREFIX}$")

def _verify_password_and_upgrade(conn: sqlite3.Connection, user_id: str, provided_password: str, stored_password: str) -> bool:
    if not _verify_password(provided_password, stored_password):
        return False
    if _is_password_hashed(stored_password):
        return True
    conn.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (_hash_password(provided_password), user_id),
    )
    return True

def _sanitize_next_path(raw_path: Optional[str]) -> str:
    value = str(raw_path or "/").strip()
    if not value:
        return "/"
    if not value.startswith("/") or value.startswith("//"):
        return "/"
    return value

def _request_origin(request: Request) -> str:
    proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    host = str(request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc or "").split(",")[0].strip()
    if not host:
        return str(request.base_url).rstrip("/")
    return f"{proto}://{host}"

def _oauth_callback_url(request: Request, provider: str) -> str:
    return f"{_request_origin(request)}{app.url_path_for('oauth_callback', provider=provider)}"

def _new_oauth_state_token() -> str:
    return f"oas_{secrets.token_urlsafe(24)}"

def _oauth_provider_config(provider: str) -> Dict[str, str]:
    key = str(provider or "").strip().lower()
    cfg = OAUTH_PROVIDERS.get(key)
    if not cfg:
        raise HTTPException(404, "OAuth provider not supported")
    client_id = str(os.getenv(cfg["client_id_env"], "")).strip()
    client_secret = str(os.getenv(cfg["client_secret_env"], "")).strip()
    if not client_id or not client_secret:
        raise HTTPException(
            400,
            f"{cfg['display']} OAuth is not configured on server. Missing {cfg['client_id_env']} or {cfg['client_secret_env']}.",
        )
    return {
        **cfg,
        "provider": key,
        "client_id": client_id,
        "client_secret": client_secret,
    }

def _oauth_providers_public_status() -> List["OAuthProviderOut"]:
    providers: List["OAuthProviderOut"] = []
    for key, cfg in OAUTH_PROVIDERS.items():
        client_id = str(os.getenv(str(cfg.get("client_id_env") or ""), "")).strip()
        client_secret = str(os.getenv(str(cfg.get("client_secret_env") or ""), "")).strip()
        providers.append(
            OAuthProviderOut(
                provider=key,
                display_name=str(cfg.get("display") or key.title()),
                configured=bool(client_id and client_secret),
            )
        )
    return providers

def _build_oauth_authorize_url(provider_cfg: Dict[str, str], *, redirect_uri: str, state: str) -> str:
    provider = str(provider_cfg.get("provider") or "").strip().lower()
    params: Dict[str, str] = {
        "client_id": str(provider_cfg["client_id"]),
        "redirect_uri": str(redirect_uri),
        "response_type": "code",
        "scope": str(provider_cfg["scope"]),
        "state": str(state),
    }
    if provider == "google":
        params["access_type"] = "online"
        params["prompt"] = "select_account"
    return f"{provider_cfg['authorize_url']}?{urlencode(params)}"

async def _oauth_exchange_code_for_token(
    provider_cfg: Dict[str, str],
    *,
    code: str,
    redirect_uri: str,
) -> str:
    provider = str(provider_cfg.get("provider") or "").strip().lower()
    payload = {
        "client_id": provider_cfg["client_id"],
        "client_secret": provider_cfg["client_secret"],
        "code": code,
        "redirect_uri": redirect_uri,
    }
    if provider == "google":
        payload["grant_type"] = "authorization_code"
    headers = {"Accept": "application/json"}
    async with httpx.AsyncClient(timeout=20) as client:
        token_resp = await client.post(provider_cfg["token_url"], data=payload, headers=headers)
    if token_resp.status_code >= 400:
        raise HTTPException(400, f"{provider_cfg['display']} OAuth token exchange failed ({token_resp.status_code}).")
    try:
        token_payload = token_resp.json()
    except Exception:
        raise HTTPException(400, f"{provider_cfg['display']} OAuth token response is invalid.")
    if token_payload.get("error"):
        raise HTTPException(400, f"{provider_cfg['display']} OAuth token exchange failed: {detail_to_text(token_payload.get('error_description') or token_payload.get('error'))}")
    token = str(token_payload.get("access_token") or "").strip()
    if not token:
        raise HTTPException(400, f"{provider_cfg['display']} OAuth did not return access token.")
    return token

async def _oauth_fetch_profile(provider_cfg: Dict[str, str], *, access_token: str) -> Dict[str, str]:
    provider = str(provider_cfg.get("provider") or "").strip().lower()
    display_name = str(provider_cfg.get("display") or provider.title())
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "hivee-oauth",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        if provider == "google":
            profile_resp = await client.get("https://openidconnect.googleapis.com/v1/userinfo", headers=headers)
            if profile_resp.status_code >= 400:
                raise HTTPException(400, f"{display_name} OAuth userinfo failed ({profile_resp.status_code}).")
            payload = profile_resp.json()
            provider_user_id = str(payload.get("sub") or "").strip()
            email = _normalize_email(str(payload.get("email") or ""))
            name = str(payload.get("name") or payload.get("given_name") or "").strip()
        elif provider == "github":
            profile_resp = await client.get("https://api.github.com/user", headers=headers)
            if profile_resp.status_code >= 400:
                raise HTTPException(400, f"{display_name} OAuth user profile failed ({profile_resp.status_code}).")
            payload = profile_resp.json()
            provider_user_id = str(payload.get("id") or "").strip()
            email = _normalize_email(str(payload.get("email") or ""))
            name = str(payload.get("name") or payload.get("login") or "").strip()
            if not email:
                emails_resp = await client.get("https://api.github.com/user/emails", headers=headers)
                if emails_resp.status_code < 400:
                    try:
                        email_payload = emails_resp.json()
                    except Exception:
                        email_payload = []
                    entries = email_payload if isinstance(email_payload, list) else []
                    picked = ""
                    for item in entries:
                        if not isinstance(item, dict):
                            continue
                        if item.get("primary") and item.get("verified") and item.get("email"):
                            picked = str(item.get("email"))
                            break
                    if not picked:
                        for item in entries:
                            if isinstance(item, dict) and item.get("verified") and item.get("email"):
                                picked = str(item.get("email"))
                                break
                    email = _normalize_email(picked)
        else:
            raise HTTPException(404, "OAuth provider not supported")
    if not provider_user_id:
        raise HTTPException(400, f"{display_name} OAuth did not return provider user id.")
    return {
        "provider_user_id": provider_user_id,
        "email": email,
        "display_name": name,
    }

def _resolve_oauth_user(conn: sqlite3.Connection, *, provider: str, provider_user_id: str, email: str, display_name: str) -> Tuple[str, str]:
    now = int(time.time())
    normalized_email = _normalize_email(email)
    provider_key = str(provider or "").strip().lower()
    p_user_id = str(provider_user_id or "").strip()
    if not provider_key or not p_user_id:
        raise HTTPException(400, "Invalid OAuth identity")

    identity = conn.execute(
        "SELECT user_id, email FROM oauth_identities WHERE provider = ? AND provider_user_id = ?",
        (provider_key, p_user_id),
    ).fetchone()
    if identity:
        user_row = conn.execute(
            "SELECT id, email FROM users WHERE id = ?",
            (str(identity["user_id"]),),
        ).fetchone()
        if user_row:
            return str(user_row["id"]), str(user_row["email"] or normalized_email)

    user_row = None
    if normalized_email:
        user_row = conn.execute(
            "SELECT id, email FROM users WHERE email = ?",
            (normalized_email,),
        ).fetchone()
    if user_row:
        user_id = str(user_row["id"])
        user_email = str(user_row["email"] or normalized_email)
    else:
        user_id = new_id("usr")
        user_email = normalized_email
        if not user_email:
            user_email = f"{provider_key}_{p_user_id}@oauth.hivee.local"
        inserted = False
        while not inserted:
            try:
                conn.execute(
                    "INSERT INTO users (id, email, password, created_at) VALUES (?,?,?,?)",
                    (user_id, user_email, _hash_password(secrets.token_urlsafe(32)), now),
                )
                inserted = True
            except sqlite3.IntegrityError:
                user_email = f"{provider_key}_{p_user_id}_{secrets.token_hex(3)}@oauth.hivee.local"

    conn.execute(
        """
        INSERT OR REPLACE INTO oauth_identities (provider, provider_user_id, user_id, email, display_name, created_at)
        VALUES (?,?,?,?,?,?)
        """,
        (
            provider_key,
            p_user_id,
            user_id,
            normalized_email or None,
            str(display_name or "").strip() or None,
            now,
        ),
    )
    return user_id, user_email

def _issue_user_session(conn: sqlite3.Connection, user_id: str) -> str:
    token = new_id("sess")
    conn.execute(
        "INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)",
        (token, user_id, int(time.time())),
    )
    return token

def _oauth_redirect_with_message(next_path: str, *, error: Optional[str] = None) -> RedirectResponse:
    base = _sanitize_next_path(next_path)
    if not error:
        return RedirectResponse(url=base, status_code=302)
    sep = "&" if "?" in base else "?"
    msg = url_quote(str(error or "OAuth failed")[:220], safe="")
    return RedirectResponse(url=f"{base}{sep}oauth_error={msg}", status_code=302)

def _new_agent_access_token() -> str:
    return f"agtok_{secrets.token_urlsafe(24)}"

def _new_environment_claim_code() -> str:
    return f"jnc_{secrets.token_urlsafe(18)}"

def _new_environment_agent_session_token() -> str:
    return f"a2as_{secrets.token_urlsafe(24)}"

def _new_environment_agent_link_token() -> str:
    return f"a2al_{secrets.token_urlsafe(24)}"

def _derive_bootstrap_agent_id(request: Request, explicit_agent_id: Optional[str] = None) -> str:
    for raw in [
        explicit_agent_id,
        request.headers.get(ENV_AGENT_ID_HEADER),
        request.headers.get("X-Agent-Id"),
        request.headers.get("X-OpenClaw-Agent-Id"),
    ]:
        candidate = str(raw or "").strip()
        if candidate:
            safe = re.sub(r"[^a-zA-Z0-9._:-]+", "-", candidate).strip("-")
            if safe:
                return safe[:80]
    return "bootstrap-agent"


__all__ = [name for name in globals() if not name.startswith('__')]
