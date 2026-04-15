from .db import *

def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return slug or "project"

def _generate_username(email: str, conn) -> str:
    """Derive a unique URL-safe username from an email address."""
    prefix = str(email or "").split("@")[0]
    base = re.sub(r"[^a-z0-9]+", "-", prefix.lower()).strip("-")[:32] or "user"
    candidate = base
    suffix = 2
    while True:
        row = conn.execute("SELECT id FROM users WHERE username = ?", (candidate,)).fetchone()
        if not row:
            return candidate
        candidate = f"{base}-{suffix}"
        suffix += 1

def _ensure_user_username(user_id: str, email: str, conn) -> str:
    """Return the user's username, generating and saving one if missing."""
    row = conn.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
    existing = str(row["username"] or "").strip() if row else ""
    if existing:
        return existing
    username = _generate_username(email, conn)
    conn.execute("UPDATE users SET username = ? WHERE id = ?", (username, user_id))
    return username

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

def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(10)}"

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
    explicit_base = str(os.getenv("PUBLIC_BASE_URL") or os.getenv("APP_BASE_URL") or "").strip().rstrip("/")
    if explicit_base:
        parsed = urlparse(explicit_base)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return explicit_base

    railway_domain = str(os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip().strip("/")
    if railway_domain:
        if railway_domain.startswith("http://") or railway_domain.startswith("https://"):
            parsed = urlparse(railway_domain)
            if parsed.netloc:
                return f"https://{parsed.netloc}"
        return f"https://{railway_domain}"

    proto_candidates = [
        part.strip().lower()
        for part in str(request.headers.get("x-forwarded-proto") or "").split(",")
        if part.strip()
    ]
    host_candidates = [
        part.strip()
        for part in str(request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")
        if part.strip()
    ]

    if "https" in proto_candidates:
        proto = "https"
    elif proto_candidates:
        proto = proto_candidates[0]
    else:
        proto = str(request.url.scheme or "http").strip().lower() or "http"

    host = host_candidates[0] if host_candidates else str(request.url.netloc or "").strip()
    if not host:
        return str(request.base_url).rstrip("/")

    if proto == "http":
        host_only = host.split(":", 1)[0].strip().lower()
        if host_only.endswith(".railway.app"):
            proto = "https"

    return f"{proto}://{host}"

def _oauth_callback_url(request: Request, provider: str) -> str:
    callback_path = str(request.app.url_path_for('oauth_callback', provider=provider))
    return f"{_request_origin(request)}{callback_path}"

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

def _oauth_providers_public_status() -> List[Dict[str, Any]]:
    providers: List[Dict[str, Any]] = []
    for key, cfg in OAUTH_PROVIDERS.items():
        client_id = str(os.getenv(str(cfg.get("client_id_env") or ""), "")).strip()
        client_secret = str(os.getenv(str(cfg.get("client_secret_env") or ""), "")).strip()
        providers.append(
            {
                "provider": key,
                "display_name": str(cfg.get("display") or key.title()),
                "configured": bool(client_id and client_secret),
            }
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


_SECRET_MASTER_KEY_CACHE: Optional[bytes] = None


def _safe_to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_secret_key(raw_key: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9._:-]+", "-", str(raw_key or "").strip().lower()).strip("-")
    return key[:180]


def _master_secret_key() -> bytes:
    global _SECRET_MASTER_KEY_CACHE
    if _SECRET_MASTER_KEY_CACHE:
        return _SECRET_MASTER_KEY_CACHE

    configured = str(os.getenv(SECRET_MASTER_KEY_ENV) or "").strip()
    key_path = Path(SECRET_MASTER_KEY_FILE)
    if not configured:
        try:
            if key_path.exists():
                configured = str(key_path.read_text(encoding="utf-8") or "").strip()
        except Exception:
            configured = ""
    if not configured:
        configured = secrets.token_urlsafe(48)
        try:
            key_path.parent.mkdir(parents=True, exist_ok=True)
            key_path.write_text(configured, encoding="utf-8")
        except Exception:
            pass

    _SECRET_MASTER_KEY_CACHE = hashlib.sha256(configured.encode("utf-8")).digest()
    return _SECRET_MASTER_KEY_CACHE


def _xor_stream(data: bytes, key: bytes, nonce: bytes) -> bytes:
    if not data:
        return b""
    out = bytearray(len(data))
    offset = 0
    counter = 0
    while offset < len(data):
        block = hashlib.sha256(key + nonce + counter.to_bytes(8, "big")).digest()
        take = min(len(block), len(data) - offset)
        for i in range(take):
            out[offset + i] = data[offset + i] ^ block[i]
        offset += take
        counter += 1
    return bytes(out)


def _seal_secret_value(raw_value: str) -> str:
    plain = str(raw_value or "").encode("utf-8")
    nonce = secrets.token_bytes(16)
    key = _master_secret_key()
    cipher = _xor_stream(plain, key, nonce)
    mac = hashlib.sha256(key + nonce + cipher).hexdigest()
    return f"{SECRET_CIPHER_VERSION}:{nonce.hex()}:{mac}:{cipher.hex()}"


def _unseal_secret_value(encrypted_value: str) -> str:
    raw = str(encrypted_value or "").strip()
    if not raw:
        return ""
    parts = raw.split(":", 3)
    if len(parts) != 4:
        return raw
    version, nonce_hex, mac_hex, cipher_hex = parts
    if version != SECRET_CIPHER_VERSION:
        return raw

    nonce = bytes.fromhex(nonce_hex)
    cipher = bytes.fromhex(cipher_hex)
    key = _master_secret_key()
    expected_mac = hashlib.sha256(key + nonce + cipher).hexdigest()
    if not secrets.compare_digest(expected_mac, mac_hex):
        raise ValueError("Secret integrity check failed")
    plain = _xor_stream(cipher, key, nonce)
    return plain.decode("utf-8")


def _store_user_secret(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    secret_key: str,
    secret_value: str,
    kind: str = "",
    description: Optional[str] = None,
) -> str:
    uid = str(user_id or "").strip()
    normalized_key = _normalize_secret_key(secret_key)
    if not uid or not normalized_key:
        raise HTTPException(400, "Invalid user secret key")

    now = int(time.time())
    encrypted_value = _seal_secret_value(secret_value)
    existing = conn.execute(
        "SELECT id, latest_version FROM user_secrets WHERE user_id = ? AND secret_key = ? LIMIT 1",
        (uid, normalized_key),
    ).fetchone()

    if existing:
        secret_id = str(existing["id"])
        next_version = max(0, _safe_to_int(existing["latest_version"], 0)) + 1
        conn.execute(
            """
            UPDATE user_secrets
            SET kind = ?, description = ?, latest_version = ?, updated_at = ?
            WHERE id = ?
            """,
            (str(kind or "")[:80], (str(description or "").strip()[:300] or None), next_version, now, secret_id),
        )
    else:
        secret_id = new_id("sec")
        next_version = 1
        conn.execute(
            """
            INSERT INTO user_secrets (id, user_id, secret_key, kind, description, latest_version, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                secret_id,
                uid,
                normalized_key,
                str(kind or "")[:80],
                str(description or "").strip()[:300] or None,
                next_version,
                now,
                now,
            ),
        )

    conn.execute(
        """
        INSERT INTO user_secret_versions (id, secret_id, version, encrypted_value, created_at)
        VALUES (?,?,?,?,?)
        """,
        (new_id("secv"), secret_id, next_version, encrypted_value, now),
    )
    return secret_id


def _read_user_secret_value(conn: sqlite3.Connection, *, user_id: str, secret_id: str) -> Optional[str]:
    uid = str(user_id or "").strip()
    sid = str(secret_id or "").strip()
    if not uid or not sid:
        return None

    row = conn.execute(
        """
        SELECT usv.encrypted_value
        FROM user_secrets us
        JOIN user_secret_versions usv
            ON usv.secret_id = us.id AND usv.version = us.latest_version
        WHERE us.id = ? AND us.user_id = ?
        LIMIT 1
        """,
        (sid, uid),
    ).fetchone()
    if not row:
        return None
    try:
        return _unseal_secret_value(str(row["encrypted_value"] or ""))
    except Exception:
        return None


def _store_connection_api_key_secret(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    connection_id: str,
    api_key: str,
) -> str:
    cid = str(connection_id or "").strip()
    if not cid:
        raise HTTPException(400, "Invalid connection id for secret storage")
    return _store_user_secret(
        conn,
        user_id=str(user_id or "").strip(),
        secret_key=f"openclaw.connection.{cid}.api_key",
        secret_value=str(api_key or ""),
        kind="openclaw_api_key",
        description=f"OpenClaw API key for connection {cid}",
    )


def _resolve_connection_api_key_from_row(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    row: Any,
) -> str:
    if not row:
        return ""

    secret_id = ""
    try:
        secret_id = str(row["api_key_secret_id"] or "").strip()
    except Exception:
        secret_id = ""

    if secret_id:
        secret_value = _read_user_secret_value(conn, user_id=str(user_id or "").strip(), secret_id=secret_id)
        if secret_value:
            return secret_value

    try:
        return str(row["api_key"] or "").strip()
    except Exception:
        return ""
__all__ = [name for name in globals() if not name.startswith('__')]
