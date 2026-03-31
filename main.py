import asyncio
import hashlib
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote as url_quote, urlencode, urlparse, urlunparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

DB_PATH = "app.db"
HIVEE_ROOT = "HIVEE"
HIVEE_TEMPLATES_ROOT = f"{HIVEE_ROOT}/TEMPLATES"
NEW_USER_ASSETS_DIR = Path("assets") / "new_user"
SERVER_WORKSPACES_DIR = Path("server_workspaces")
MAX_TEMPLATE_FILE_BYTES = 96_000
MAX_TEMPLATE_FILES = 100
MAX_TEMPLATE_PAYLOAD_BYTES = 250_000
MAX_SETUP_TEMPLATE_PROMPT_CHARS = 2_400
MAX_SETUP_QUESTION_ITEMS = 12
MAX_SETUP_QUESTION_CHARS = 220
MAX_SETUP_TRANSCRIPT_ITEMS = 16
MAX_SETUP_TRANSCRIPT_ITEM_CHARS = 280
MAX_SETUP_TRANSCRIPT_CHARS = 6_000
MAX_WORKSPACE_TREE_CHARS = 8000
MAX_TREE_ENTRIES = 400
MAX_FILE_PREVIEW_BYTES = 120_000
MAX_AGENT_FILE_WRITES = 40
MAX_AGENT_FILE_BYTES = 300_000
MAX_PROJECT_CONTEXT_TREE_CHARS = 2_800
MAX_PROJECT_CONTEXT_FILE_CHARS = 2_400
MAX_PROJECT_CONTEXT_TOTAL_CHARS = 12_000
MAX_PROJECT_CONTEXT_FILES = 8
SAFE_PROVIDER_MAX_OUTPUT_TOKENS = 8_192
PROJECT_INFO_DIRNAME = "Project Info"
USER_OUTPUTS_DIRNAME = "Outputs"
LEGACY_OUTPUTS_DIRNAME = "outputs"
SETUP_CHAT_HISTORY_FILE = f"{PROJECT_INFO_DIRNAME}/setup-chat-history.txt"
SETUP_CHAT_HISTORY_COMPAT_FILE = f"{PROJECT_INFO_DIRNAME}/SETUP-CHAT.txt"
PROJECT_INFO_FILE = f"{PROJECT_INFO_DIRNAME}/PROJECT-INFO.MD"
PROJECT_DELEGATION_FILE = f"{PROJECT_INFO_DIRNAME}/PROJECT-DELEGATION.MD"
OVERVIEW_FILE = f"{PROJECT_INFO_DIRNAME}/overview.md"
PROJECT_PLAN_FILE = f"{PROJECT_INFO_DIRNAME}/project-plan.md"
USAGE_FILE = f"{PROJECT_INFO_DIRNAME}/usage.md"
TRACKER_FILE = f"{PROJECT_INFO_DIRNAME}/tracker.md"
README_FILE = f"{PROJECT_INFO_DIRNAME}/README.md"
BRIEF_FILE = f"{PROJECT_INFO_DIRNAME}/brief.md"
GOAL_FILE = f"{PROJECT_INFO_DIRNAME}/goal.md"
PROJECT_SETUP_FILE = f"{PROJECT_INFO_DIRNAME}/project-setup.md"
CHAT_HIVEE_FILE = f"{PROJECT_INFO_DIRNAME}/chat-hivee.md"
PLAN_STATUS_PENDING = "pending"
PLAN_STATUS_GENERATING = "generating"
PLAN_STATUS_AWAITING_APPROVAL = "awaiting_approval"
PLAN_STATUS_APPROVED = "approved"
PLAN_STATUS_FAILED = "failed"
EXEC_STATUS_IDLE = "idle"
EXEC_STATUS_RUNNING = "running"
EXEC_STATUS_PAUSED = "paused"
EXEC_STATUS_STOPPED = "stopped"
EXEC_STATUS_COMPLETED = "completed"
SESSION_COOKIE_NAME = "hivee_session"
SESSION_COOKIE_MAX_AGE_SEC = 60 * 60 * 24 * 30
ENV_STATUS_PENDING_BOOTSTRAP = "pending_bootstrap"
ENV_STATUS_PENDING_CLAIM = "pending_claim"
ENV_STATUS_ACTIVE = "active"
ENV_STATUS_SUSPENDED = "suspended"
ENV_STATUS_ARCHIVED = "archived"
ENV_CLAIM_CODE_TTL_SEC = 60 * 15
ENV_AGENT_SESSION_TTL_SEC = 60 * 60 * 24
ENV_AGENT_SESSION_HEADER = "X-A2A-Agent-Session"
PASSWORD_MIN_LENGTH = 10
PASSWORD_HASH_PREFIX = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 260_000
OAUTH_STATE_TTL_SEC = 60 * 10
OAUTH_PROVIDERS: Dict[str, Dict[str, str]] = {
    "google": {
        "display": "Google",
        "client_id_env": "GOOGLE_OAUTH_CLIENT_ID",
        "client_secret_env": "GOOGLE_OAUTH_CLIENT_SECRET",
        "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scope": "openid email profile",
    },
    "github": {
        "display": "GitHub",
        "client_id_env": "GITHUB_OAUTH_CLIENT_ID",
        "client_secret_env": "GITHUB_OAUTH_CLIENT_SECRET",
        "authorize_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scope": "read:user user:email",
    },
}
DEFAULT_PROJECT_SETUP_MD = """# PROJECT-SETUP
Ask these questions one by one before creating a project:
1. What is the project name?
2. What is the main goal?
3. Who are the target users?
4. What are the key constraints (time, budget, tech, compliance)?
5. What is in-scope and out-of-scope?
6. What is the deadline or milestone cadence?
7. What tools or stack are required?
8. What output should be produced first?
After collecting answers, summarize and ask for confirmation before starting the project workspace.
"""

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_connections (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            base_url TEXT NOT NULL,
            api_key TEXT NOT NULL,
            name TEXT,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            title TEXT NOT NULL,
            brief TEXT NOT NULL,
            goal TEXT NOT NULL,
            setup_json TEXT,
            plan_text TEXT NOT NULL DEFAULT '',
            plan_status TEXT NOT NULL DEFAULT 'pending',
            plan_updated_at INTEGER,
            plan_approved_at INTEGER,
            execution_status TEXT NOT NULL DEFAULT 'idle',
            progress_pct INTEGER NOT NULL DEFAULT 0,
            execution_updated_at INTEGER,
            usage_prompt_tokens INTEGER NOT NULL DEFAULT 0,
            usage_completion_tokens INTEGER NOT NULL DEFAULT 0,
            usage_total_tokens INTEGER NOT NULL DEFAULT 0,
            usage_updated_at INTEGER,
            connection_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(connection_id) REFERENCES openclaw_connections(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_agents (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(project_id, agent_id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS connection_policies (
            connection_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            main_agent_id TEXT,
            main_agent_name TEXT,
            workspace_root TEXT NOT NULL DEFAULT 'HIVEE',
            templates_root TEXT NOT NULL DEFAULT 'HIVEE/TEMPLATES',
            bootstrap_status TEXT NOT NULL DEFAULT 'pending',
            bootstrap_error TEXT,
            workspace_tree TEXT,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY(connection_id) REFERENCES openclaw_connections(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_agent_access_tokens (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            token_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(project_id, agent_id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS environments (
            id TEXT PRIMARY KEY,
            owner_user_id TEXT,
            display_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending_bootstrap',
            workspace_root TEXT,
            created_at INTEGER NOT NULL,
            claimed_at INTEGER,
            archived_at INTEGER,
            FOREIGN KEY(owner_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS environment_claim_codes (
            id TEXT PRIMARY KEY,
            env_id TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            used_at INTEGER,
            used_by_user_id TEXT,
            created_by_agent_id TEXT,
            FOREIGN KEY(env_id) REFERENCES environments(id),
            FOREIGN KEY(used_by_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS environment_agent_sessions (
            id TEXT PRIMARY KEY,
            env_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            token_hash TEXT NOT NULL,
            scopes_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'active',
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            revoked_at INTEGER,
            last_seen_at INTEGER,
            FOREIGN KEY(env_id) REFERENCES environments(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS oauth_identities (
            provider TEXT NOT NULL,
            provider_user_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            email TEXT,
            display_name TEXT,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(provider, provider_user_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS oauth_states (
            state TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            redirect_path TEXT,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_oauth_identities_user_id ON oauth_identities(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_oauth_states_expires_at ON oauth_states(expires_at)")
    cols = [r[1] for r in cur.execute("PRAGMA table_info(project_agents)").fetchall()]
    if "is_primary" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN is_primary INTEGER NOT NULL DEFAULT 0")
    if "role" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN role TEXT NOT NULL DEFAULT ''")
    project_cols = [r[1] for r in cur.execute("PRAGMA table_info(projects)").fetchall()]
    conn_cols = [r[1] for r in cur.execute("PRAGMA table_info(openclaw_connections)").fetchall()]
    if "env_id" not in conn_cols:
        cur.execute("ALTER TABLE openclaw_connections ADD COLUMN env_id TEXT")
    if "workspace_root" not in project_cols:
        cur.execute(f"ALTER TABLE projects ADD COLUMN workspace_root TEXT NOT NULL DEFAULT '{HIVEE_ROOT}'")
    if "project_root" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN project_root TEXT NOT NULL DEFAULT ''")
    if "env_id" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN env_id TEXT")
    if "scope_requires_owner_approval" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN scope_requires_owner_approval INTEGER NOT NULL DEFAULT 1")
    if "setup_json" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN setup_json TEXT")
    if "plan_text" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN plan_text TEXT NOT NULL DEFAULT ''")
    if "plan_status" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN plan_status TEXT NOT NULL DEFAULT 'pending'")
    if "plan_updated_at" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN plan_updated_at INTEGER")
    if "plan_approved_at" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN plan_approved_at INTEGER")
    if "execution_status" not in project_cols:
        cur.execute(f"ALTER TABLE projects ADD COLUMN execution_status TEXT NOT NULL DEFAULT '{EXEC_STATUS_IDLE}'")
    if "progress_pct" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN progress_pct INTEGER NOT NULL DEFAULT 0")
    if "execution_updated_at" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN execution_updated_at INTEGER")
    if "usage_prompt_tokens" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN usage_prompt_tokens INTEGER NOT NULL DEFAULT 0")
    if "usage_completion_tokens" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN usage_completion_tokens INTEGER NOT NULL DEFAULT 0")
    if "usage_total_tokens" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN usage_total_tokens INTEGER NOT NULL DEFAULT 0")
    if "usage_updated_at" not in project_cols:
        cur.execute("ALTER TABLE projects ADD COLUMN usage_updated_at INTEGER")
    policy_cols = [r[1] for r in cur.execute("PRAGMA table_info(connection_policies)").fetchall()]
    if "workspace_tree" not in policy_cols:
        cur.execute("ALTER TABLE connection_policies ADD COLUMN workspace_tree TEXT")
    conn.commit()
    conn.close()

@dataclass
class Event:
    ts: float
    kind: str
    data: Dict[str, Any]

project_queues: Dict[str, "asyncio.Queue[Event]"] = {}

def get_queue(project_id: str) -> "asyncio.Queue[Event]":
    if project_id not in project_queues:
        project_queues[project_id] = asyncio.Queue()
    return project_queues[project_id]

async def emit(project_id: str, kind: str, data: Dict[str, Any]) -> None:
    await get_queue(project_id).put(Event(ts=time.time(), kind=kind, data=data))

HEALTH_PATHS = ["/health", "/api/health", "/v1/health", "/status", "/api/status"]
AGENTS_PATHS = [
    "/agents",
    "/api/agents",
    "/v1/agents",
    "/api/v1/agents",
    "/models",
    "/api/models",
    "/v1/models",
    "/api/v1/models",
]
CHAT_PATHS = [
    "/v1/chat/completions",
    "/chat/completions",
    "/api/chat/completions",
    "/v1/responses",
    "/responses",
    "/chat",
    "/api/chat",
]

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

def _environment_home_dir(env_id: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(env_id or "").strip()).strip("-") or "env"
    return SERVER_WORKSPACES_DIR / f"env_{safe}"

def _environment_workspace_root_dir(env_id: str) -> Path:
    return _environment_home_dir(env_id) / HIVEE_ROOT

def _environment_templates_dir(env_id: str) -> Path:
    return _environment_workspace_root_dir(env_id) / "TEMPLATES"

def _environment_projects_dir(env_id: str) -> Path:
    return _environment_workspace_root_dir(env_id) / "PROJECTS"

def _user_home_dir(user_id: str) -> Path:
    return SERVER_WORKSPACES_DIR / user_id

def _user_workspace_root_dir(user_id: str) -> Path:
    return _user_home_dir(user_id) / HIVEE_ROOT

def _user_templates_dir(user_id: str) -> Path:
    return _user_workspace_root_dir(user_id) / "TEMPLATES"

def _user_projects_dir(user_id: str) -> Path:
    return _user_workspace_root_dir(user_id) / "PROJECTS"

def _path_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False

def _build_claim_url(request: Request, env_id: str, code: str) -> str:
    base = str(request.base_url).rstrip("/")
    return (
        f"{base}/?claim_env_id={url_quote(env_id, safe='')}"
        f"&claim_code={url_quote(code, safe='')}"
    )

def _encode_rel_path_for_url_path(path: str) -> str:
    clean = str(path or "").replace("\\", "/").strip("/")
    if not clean:
        return ""
    return "/".join(url_quote(part, safe="") for part in clean.split("/") if part)

def _latest_file_relative_path(root_dir: Path, base_dir: Path) -> Optional[str]:
    if not root_dir.exists() or not root_dir.is_dir():
        return None
    latest_path: Optional[Path] = None
    latest_mtime = -1.0
    for candidate in root_dir.rglob("*"):
        if not candidate.is_file():
            continue
        try:
            mtime = float(candidate.stat().st_mtime)
        except OSError:
            continue
        if mtime >= latest_mtime:
            latest_path = candidate
            latest_mtime = mtime
    if not latest_path:
        return None
    try:
        return latest_path.relative_to(base_dir).as_posix()
    except Exception:
        return None

def _project_info_dir(project_dir: Path) -> Path:
    return (project_dir / PROJECT_INFO_DIRNAME).resolve()

def _legacy_project_doc_paths(project_dir: Path) -> List[Path]:
    names = [
        "README.md",
        "brief.md",
        "goal.md",
        "project-setup.md",
        "setup-chat-history.txt",
        "SETUP-CHAT.txt",
        "PROJECT-INFO.MD",
        "PROJECT-DELEGATION.MD",
        "overview.md",
        "project-plan.md",
        "project-delegation.md",
        "usage.md",
        "tracker.md",
        "chat-hivee.md",
        "project.md",
    ]
    out: List[Path] = []
    for name in names:
        out.append((project_dir / name).resolve())
    return out

def _rel_path_startswith(rel: str, prefix: str) -> bool:
    left = str(rel or "").strip().replace("\\", "/").strip("/")
    right = str(prefix or "").strip().replace("\\", "/").strip("/")
    if not left or not right:
        return False
    left_low = left.lower()
    right_low = right.lower()
    return left_low == right_low or left_low.startswith(right_low + "/")

def _normalize_user_outputs_prefix(raw_prefix: Optional[str]) -> str:
    raw = _clean_relative_project_path(raw_prefix or "")
    if not raw:
        return f"{USER_OUTPUTS_DIRNAME}/generated"
    if raw.lower() == LEGACY_OUTPUTS_DIRNAME:
        return USER_OUTPUTS_DIRNAME
    if _rel_path_startswith(raw, f"{LEGACY_OUTPUTS_DIRNAME}/"):
        suffix = raw[len(LEGACY_OUTPUTS_DIRNAME):].lstrip("/\\")
        return _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
    if _rel_path_startswith(raw, USER_OUTPUTS_DIRNAME):
        return raw
    return _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{raw}")

def _remap_legacy_project_doc_rel_path(rel: str) -> str:
    normalized = _clean_relative_project_path(rel)
    if not normalized:
        return normalized
    low = normalized.lower()
    legacy_map = {
        "readme.md": README_FILE,
        "brief.md": BRIEF_FILE,
        "goal.md": GOAL_FILE,
        "project-setup.md": PROJECT_SETUP_FILE,
        "setup-chat-history.txt": SETUP_CHAT_HISTORY_FILE,
        "setup-chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "set-up-chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "set_up_chat.txt": SETUP_CHAT_HISTORY_COMPAT_FILE,
        "project-info.md": PROJECT_INFO_FILE,
        "project-info.mdx": PROJECT_INFO_FILE,
        "project-info.txt": PROJECT_INFO_FILE,
        "project-info": PROJECT_INFO_FILE,
        "project-delegation.md": PROJECT_DELEGATION_FILE,
        "overview.md": OVERVIEW_FILE,
        "project-plan.md": PROJECT_PLAN_FILE,
        "usage.md": USAGE_FILE,
        "tracker.md": TRACKER_FILE,
        "chat-hivee.md": CHAT_HIVEE_FILE,
        "project.md": PROJECT_PLAN_FILE,
    }
    return legacy_map.get(low, normalized)

def _render_tree(root: Path, *, max_depth: int = 4, max_entries: int = MAX_TREE_ENTRIES) -> str:
    root = root.resolve()
    if not root.exists():
        return f"{root.name}/\n  (missing)"

    lines: List[str] = [f"{root.name}/"]
    shown = 0

    def _walk(node: Path, prefix: str, depth: int) -> None:
        nonlocal shown
        if depth > max_depth or shown >= max_entries:
            return
        try:
            children = sorted(node.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except Exception:
            lines.append(f"{prefix}(unreadable)")
            return

        for child in children:
            if shown >= max_entries:
                lines.append(f"{prefix}...")
                return
            shown += 1
            suffix = "/" if child.is_dir() else ""
            lines.append(f"{prefix}{child.name}{suffix}")
            if child.is_dir() and depth < max_depth:
                _walk(child, prefix + "  ", depth + 1)

    _walk(root, "  ", 1)
    return "\n".join(lines)[:MAX_WORKSPACE_TREE_CHARS]

def _ensure_user_workspace(user_id: str) -> Dict[str, Any]:
    SERVER_WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
    home = _user_home_dir(user_id)
    workspace_root = _user_workspace_root_dir(user_id)
    templates_root = _user_templates_dir(user_id)
    projects_root = _user_projects_dir(user_id)

    home.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    projects_root.mkdir(parents=True, exist_ok=True)

    payload = _collect_new_user_templates()
    for rel_dir in payload.get("directories") or []:
        target_dir = (templates_root / rel_dir).resolve()
        if not _path_within(target_dir, templates_root):
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
    for item in payload.get("files") or []:
        rel_path = str(item.get("path") or "").strip()
        if not rel_path:
            continue
        target_file = (templates_root / rel_path).resolve()
        if not _path_within(target_file, templates_root):
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        if not target_file.exists():
            target_file.write_text(str(item.get("content") or ""), encoding="utf-8")

    return {
        "workspace_root": workspace_root.as_posix(),
        "templates_root": templates_root.as_posix(),
        "projects_root": projects_root.as_posix(),
        "workspace_tree": _render_tree(workspace_root),
        "template_warnings": payload.get("warnings") or [],
    }

def _ensure_environment_workspace(env_id: str) -> Dict[str, Any]:
    SERVER_WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
    home = _environment_home_dir(env_id)
    workspace_root = _environment_workspace_root_dir(env_id)
    templates_root = _environment_templates_dir(env_id)
    projects_root = _environment_projects_dir(env_id)

    home.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    projects_root.mkdir(parents=True, exist_ok=True)

    payload = _collect_new_user_templates()
    for rel_dir in payload.get("directories") or []:
        target_dir = (templates_root / rel_dir).resolve()
        if not _path_within(target_dir, templates_root):
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
    for item in payload.get("files") or []:
        rel_path = str(item.get("path") or "").strip()
        if not rel_path:
            continue
        target_file = (templates_root / rel_path).resolve()
        if not _path_within(target_file, templates_root):
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        if not target_file.exists():
            target_file.write_text(str(item.get("content") or ""), encoding="utf-8")

    return {
        "workspace_root": workspace_root.as_posix(),
        "templates_root": templates_root.as_posix(),
        "projects_root": projects_root.as_posix(),
        "workspace_tree": _render_tree(workspace_root),
        "template_warnings": payload.get("warnings") or [],
    }

def _issue_environment_claim_code(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    ttl_sec: int = ENV_CLAIM_CODE_TTL_SEC,
    created_by_agent_id: Optional[str] = None,
) -> Tuple[str, int]:
    now = int(time.time())
    ttl = max(60, min(int(ttl_sec or ENV_CLAIM_CODE_TTL_SEC), 60 * 60 * 24))
    expires_at = now + ttl
    raw_code = _new_environment_claim_code()
    conn.execute(
        """
        INSERT INTO environment_claim_codes (id, env_id, code_hash, created_at, expires_at, used_at, used_by_user_id, created_by_agent_id)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            new_id("clm"),
            env_id,
            _hash_access_token(raw_code),
            now,
            expires_at,
            None,
            None,
            (str(created_by_agent_id or "").strip() or None),
        ),
    )
    return raw_code, expires_at

def _issue_environment_agent_session(
    conn: sqlite3.Connection,
    *,
    env_id: str,
    agent_id: str,
    scopes: List[str],
    ttl_sec: int = ENV_AGENT_SESSION_TTL_SEC,
) -> Tuple[str, int]:
    now = int(time.time())
    ttl = max(60, min(int(ttl_sec or ENV_AGENT_SESSION_TTL_SEC), 60 * 60 * 24 * 7))
    expires_at = now + ttl
    raw_token = _new_environment_agent_session_token()
    cleaned_scopes = [str(s).strip() for s in (scopes or []) if str(s).strip()]
    if not cleaned_scopes:
        cleaned_scopes = ["env.read"]
    conn.execute(
        """
        INSERT INTO environment_agent_sessions (
            id, env_id, agent_id, token_hash, scopes_json, status, created_at, expires_at, revoked_at, last_seen_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            new_id("a2a"),
            env_id,
            str(agent_id or "agent").strip() or "agent",
            _hash_access_token(raw_token),
            json.dumps(cleaned_scopes, ensure_ascii=False),
            "active",
            now,
            expires_at,
            None,
            now,
        ),
    )
    return raw_token, expires_at

def _parse_scopes_json(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if str(s).strip()]
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(s).strip() for s in parsed if str(s).strip()]
    except Exception:
        pass
    return []

def _resolve_environment_agent_access(
    request: Request,
    env_id: str,
    *,
    required_scope: Optional[str] = None,
) -> Dict[str, Any]:
    token = str(request.headers.get(ENV_AGENT_SESSION_HEADER) or "").strip()
    if not token:
        raise HTTPException(401, f"Missing {ENV_AGENT_SESSION_HEADER} header")
    token_hash = _hash_access_token(token)
    now = int(time.time())
    conn = db()
    row = conn.execute(
        """
        SELECT id, env_id, agent_id, scopes_json, status, expires_at
        FROM environment_agent_sessions
        WHERE env_id = ? AND token_hash = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (env_id, token_hash),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(401, "Invalid A2A agent session")
    if str(row["status"] or "").strip().lower() != "active":
        conn.close()
        raise HTTPException(403, "A2A agent session is not active")
    if _to_int(row["expires_at"]) <= now:
        conn.execute(
            "UPDATE environment_agent_sessions SET status = ?, revoked_at = ? WHERE id = ?",
            ("expired", now, row["id"]),
        )
        conn.commit()
        conn.close()
        raise HTTPException(401, "A2A agent session expired")

    scopes = _parse_scopes_json(row["scopes_json"])
    if required_scope and (required_scope not in scopes) and ("*" not in scopes):
        conn.close()
        raise HTTPException(403, f"Missing required scope: {required_scope}")
    conn.execute(
        "UPDATE environment_agent_sessions SET last_seen_at = ? WHERE id = ?",
        (now, row["id"]),
    )
    conn.commit()
    conn.close()
    return {
        "session_id": str(row["id"]),
        "env_id": str(row["env_id"]),
        "agent_id": str(row["agent_id"]),
        "scopes": scopes,
        "expires_at": _to_int(row["expires_at"]),
    }

def _get_primary_environment_for_user(user_id: str) -> Optional[Dict[str, Any]]:
    conn = db()
    row = conn.execute(
        """
        SELECT id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at, archived_at
        FROM environments
        WHERE owner_user_id = ?
        ORDER BY
            CASE
                WHEN status = ? THEN 0
                WHEN status = ? THEN 1
                ELSE 2
            END,
            COALESCE(claimed_at, created_at) DESC
        LIMIT 1
        """,
        (user_id, ENV_STATUS_ACTIVE, ENV_STATUS_PENDING_CLAIM),
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def _ensure_primary_environment_for_user(user_id: str, *, email: Optional[str] = None) -> Dict[str, Any]:
    existing = _get_primary_environment_for_user(user_id)
    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace.get("workspace_root") or _user_workspace_root_dir(user_id).as_posix())
    now = int(time.time())
    if existing:
        env_id = str(existing.get("id") or "").strip()
        conn = db()
        conn.execute(
            """
            UPDATE environments
            SET status = ?, workspace_root = ?, claimed_at = COALESCE(claimed_at, ?)
            WHERE id = ? AND owner_user_id = ?
            """,
            (ENV_STATUS_ACTIVE, workspace_root, now, env_id, user_id),
        )
        conn.commit()
        conn.close()
        existing["status"] = ENV_STATUS_ACTIVE
        existing["workspace_root"] = workspace_root
        existing["claimed_at"] = existing.get("claimed_at") or now
        return existing

    env_id = new_id("env")
    display_name = ""
    if email:
        display_name = str(email).split("@")[0].strip()[:120]
    conn = db()
    conn.execute(
        """
        INSERT INTO environments (id, owner_user_id, display_name, status, workspace_root, created_at, claimed_at, archived_at)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            env_id,
            user_id,
            display_name or f"user-{user_id[-6:]}",
            ENV_STATUS_ACTIVE,
            workspace_root,
            now,
            now,
            None,
        ),
    )
    conn.commit()
    conn.close()
    return {
        "id": env_id,
        "owner_user_id": user_id,
        "display_name": display_name,
        "status": ENV_STATUS_ACTIVE,
        "workspace_root": workspace_root,
        "created_at": now,
        "claimed_at": now,
        "archived_at": None,
    }

def _merge_environment_workspace_into_user_workspace(env_id: str, user_id: str) -> Dict[str, Any]:
    env_root = _environment_workspace_root_dir(env_id).resolve()
    if not env_root.exists():
        return {"copied_files": 0, "copied_dirs": 0}
    user_root = _user_workspace_root_dir(user_id).resolve()
    _ensure_user_workspace(user_id)
    copied_files = 0
    copied_dirs = 0
    for node in sorted(env_root.rglob("*"), key=lambda p: p.as_posix()):
        try:
            rel = node.relative_to(env_root)
        except Exception:
            continue
        target = (user_root / rel).resolve()
        if not _path_within(target, user_root):
            continue
        if node.is_dir():
            if not target.exists():
                target.mkdir(parents=True, exist_ok=True)
                copied_dirs += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            continue
        shutil.copy2(node, target)
        copied_files += 1
    return {"copied_files": copied_files, "copied_dirs": copied_dirs}

def _project_setup_template_file(user_id: str) -> Path:
    return _user_templates_dir(user_id) / "PROJECT-SETUP.MD"

def _read_project_setup_template(user_id: str) -> str:
    _ensure_user_workspace(user_id)
    template_file = _project_setup_template_file(user_id)
    if not _path_within(template_file, _user_workspace_root_dir(user_id)):
        return DEFAULT_PROJECT_SETUP_MD
    try:
        content = template_file.read_text(encoding="utf-8")
    except Exception:
        return DEFAULT_PROJECT_SETUP_MD
    cleaned = content.strip()
    if not cleaned:
        return DEFAULT_PROJECT_SETUP_MD
    return cleaned[:MAX_TEMPLATE_PAYLOAD_BYTES]

def _resolve_owner_project_dir(owner_user_id: str, project_root: str) -> Path:
    owner_workspace = _user_workspace_root_dir(owner_user_id).resolve()
    raw = str(project_root or "").strip()
    if not raw:
        raise HTTPException(400, "Project root is not initialized")

    # Preferred path: directly inside owner's server workspace.
    direct = Path(raw).resolve()
    if _path_within(direct, owner_workspace):
        return direct

    # Backward compatibility: map legacy "HIVEE/..." roots into owner workspace.
    normalized = raw.replace("\\", "/")
    if normalized == HIVEE_ROOT:
        mapped = owner_workspace
    elif normalized.startswith(f"{HIVEE_ROOT}/"):
        mapped = (owner_workspace / normalized[len(HIVEE_ROOT) + 1 :]).resolve()
    else:
        raise HTTPException(400, "Project folder is outside owner workspace")

    if not _path_within(mapped, owner_workspace):
        raise HTTPException(400, "Invalid project folder path")
    return mapped

def _delete_project_with_resources(*, owner_user_id: str, project_id: str) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        "SELECT id, title, project_root FROM projects WHERE id = ? AND user_id = ?",
        (project_id, owner_user_id),
    ).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Project not found"}

    project_title = str(row["title"] or project_id)
    project_root = str(row["project_root"] or "")
    conn.execute("DELETE FROM project_agent_access_tokens WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM project_agents WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM projects WHERE id = ? AND user_id = ?", (project_id, owner_user_id))
    conn.commit()
    conn.close()

    deleted_dir = False
    folder_error = ""
    folder_path = ""
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
        folder_path = project_dir.as_posix()
        owner_workspace = _user_workspace_root_dir(owner_user_id).resolve()
        if project_dir.exists() and _path_within(project_dir, owner_workspace):
            shutil.rmtree(project_dir)
            deleted_dir = True
    except Exception as e:
        folder_error = str(e)[:500]

    project_queues.pop(project_id, None)
    return {
        "ok": True,
        "project_id": project_id,
        "title": project_title,
        "project_root": project_root,
        "folder_path": folder_path,
        "folder_deleted": deleted_dir,
        "folder_error": folder_error or None,
    }

def _delete_account_with_resources(*, user_id: str) -> Dict[str, Any]:
    conn = db()
    user_row = conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user_row:
        conn.close()
        return {"ok": False, "error": "User not found"}

    project_rows = conn.execute(
        "SELECT id FROM projects WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    connection_rows = conn.execute(
        "SELECT id FROM openclaw_connections WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    environment_rows = conn.execute(
        "SELECT id FROM environments WHERE owner_user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()

    project_ids = [str(r["id"]) for r in project_rows]
    connection_ids = [str(r["id"]) for r in connection_rows]
    environment_ids = [str(r["id"]) for r in environment_rows]
    deleted_projects = 0
    for project_id in project_ids:
        deleted = _delete_project_with_resources(owner_user_id=user_id, project_id=project_id)
        if deleted.get("ok"):
            deleted_projects += 1
        project_queues.pop(project_id, None)

    conn = db()
    conn.execute(
        "DELETE FROM project_agent_access_tokens WHERE project_id IN (SELECT id FROM projects WHERE user_id = ?)",
        (user_id,),
    )
    conn.execute(
        "DELETE FROM project_agents WHERE project_id IN (SELECT id FROM projects WHERE user_id = ?)",
        (user_id,),
    )
    conn.execute("DELETE FROM projects WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM connection_policies WHERE user_id = ?", (user_id,))
    if connection_ids:
        placeholders = ",".join(["?"] * len(connection_ids))
        conn.execute(f"DELETE FROM connection_policies WHERE connection_id IN ({placeholders})", tuple(connection_ids))
    conn.execute("DELETE FROM openclaw_connections WHERE user_id = ?", (user_id,))
    conn.execute(
        "DELETE FROM environment_claim_codes WHERE env_id IN (SELECT id FROM environments WHERE owner_user_id = ?)",
        (user_id,),
    )
    conn.execute(
        "DELETE FROM environment_agent_sessions WHERE env_id IN (SELECT id FROM environments WHERE owner_user_id = ?)",
        (user_id,),
    )
    conn.execute("DELETE FROM environments WHERE owner_user_id = ?", (user_id,))
    conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    workspace_deleted = False
    workspace_error = ""
    try:
        user_home = _user_home_dir(user_id).resolve()
        workspaces_root = SERVER_WORKSPACES_DIR.resolve()
        if user_home.exists() and _path_within(user_home, workspaces_root):
            shutil.rmtree(user_home)
            workspace_deleted = True
    except Exception as e:
        workspace_error = str(e)[:500]

    deleted_env_workspaces = 0
    for env_id in environment_ids:
        try:
            env_home = _environment_home_dir(env_id).resolve()
            workspaces_root = SERVER_WORKSPACES_DIR.resolve()
            if env_home.exists() and _path_within(env_home, workspaces_root):
                shutil.rmtree(env_home)
                deleted_env_workspaces += 1
        except Exception:
            continue

    return {
        "ok": True,
        "user_id": user_id,
        "projects_deleted": deleted_projects,
        "connections_deleted": len(connection_ids),
        "environments_deleted": len(environment_ids),
        "environment_workspaces_deleted": deleted_env_workspaces,
        "workspace_deleted": workspace_deleted,
        "workspace_error": workspace_error or None,
    }

def _parse_setup_json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return {}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}

def _sanitize_setup_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()[:4000]
    if isinstance(value, list):
        out: List[Any] = []
        for item in value:
            if isinstance(item, dict):
                clean_obj = _sanitize_setup_value(item)
                if clean_obj:
                    out.append(clean_obj)
                continue
            clean = str(item).strip()
            if clean:
                out.append(clean[:400])
        return out[:50]
    if isinstance(value, dict):
        compact: Dict[str, str] = {}
        for k, v in value.items():
            key = str(k).strip()[:80]
            if not key:
                continue
            val = str(v).strip()[:400]
            compact[key] = val
            if len(compact) >= 30:
                break
        return compact
    return str(value).strip()[:4000]

def _normalize_setup_details(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        key = str(k).strip()[:80]
        if not key:
            continue
        out[key] = _sanitize_setup_value(v)
        if len(out) >= 80:
            break
    return out

def _setup_value_markdown(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        lines: List[str] = []
        for x in value:
            if isinstance(x, dict):
                compact = ", ".join([f"{k}: {v}" for k, v in x.items() if str(v).strip()])
                if compact:
                    lines.append(f"- {compact}")
                continue
            text = str(x).strip()
            if text:
                lines.append(f"- {text}")
        return "\n".join(lines)
    if isinstance(value, dict):
        lines = [f"- {k}: {v}" for k, v in value.items() if str(v).strip()]
        return "\n".join(lines)
    text = str(value).strip()
    return text

def _setup_details_markdown(setup_details: Dict[str, Any]) -> str:
    if not setup_details:
        return "No setup details yet.\n"
    lines: List[str] = ["# Project Setup Details", ""]
    for key, value in setup_details.items():
        title = key.replace("_", " ").strip().title() or "Detail"
        body = _setup_value_markdown(value)
        if not body:
            continue
        lines.append(f"## {title}")
        lines.append(body)
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def _setup_detail_compact_text(value: Any, *, max_len: int = 900) -> str:
    text = _setup_value_markdown(value).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max(80, max_len)]

def _project_readme_markdown(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Optional[Dict[str, Any]] = None,
) -> str:
    details = _normalize_setup_details(setup_details or {})
    summary = str(
        details.get("setup_chat_summary")
        or details.get("setup_summary")
        or details.get("conversation_summary")
        or ""
    ).strip()
    lines: List[str] = [
        f"# {title}",
        "",
        "## Brief",
        brief.strip() or "-",
        "",
        "## Goal",
        goal.strip() or "-",
        "",
    ]
    if summary:
        lines.extend(["## Setup Chat Summary", summary, ""])

    key_labels = [
        ("target_users", "Target Users"),
        ("constraints", "Constraints"),
        ("in_scope", "In Scope"),
        ("out_of_scope", "Out of Scope"),
        ("milestones", "Milestones"),
        ("required_stack", "Required Stack"),
        ("first_output", "First Output"),
    ]
    key_lines: List[str] = []
    for key, label in key_labels:
        value_text = _setup_detail_compact_text(details.get(key), max_len=700) if key in details else ""
        key_lines.append(f"- {label}: {value_text or '-'}")
    lines.extend(["## Key Setup Inputs", *key_lines, ""])

    user_answers = details.get("setup_user_answers")
    if isinstance(user_answers, list):
        answer_lines: List[str] = []
        for item in user_answers[:16]:
            text = str(item or "").strip()
            if text:
                answer_lines.append(f"- {text[:260]}")
        if answer_lines:
            lines.extend(["## Setup Answers Snapshot", *answer_lines, ""])
    lines.extend(["## Setup Details File", f"- See `{PROJECT_SETUP_FILE}` for the full structured setup data.", ""])
    return "\n".join(lines).strip() + "\n"

def _project_brief_markdown(*, brief: str, setup_details: Optional[Dict[str, Any]] = None) -> str:
    details = _normalize_setup_details(setup_details or {})
    summary = str(
        details.get("setup_chat_summary")
        or details.get("setup_summary")
        or details.get("conversation_summary")
        or ""
    ).strip()
    lines: List[str] = ["# Brief", "", brief.strip() or "-", ""]
    if summary:
        lines.extend(["## Setup Chat Summary", summary, ""])
    return "\n".join(lines).strip() + "\n"

def _project_context_instruction(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Optional[Dict[str, Any]] = None,
    role_rows: Optional[List[Dict[str, Any]]] = None,
    plan_status: str = PLAN_STATUS_PENDING,
) -> str:
    sections = [
        "Project context (always align your answer to this):",
        f"- Title: {title}",
        f"- Brief: {brief}",
        f"- Goal: {goal}",
    ]
    details = _normalize_setup_details(setup_details or {})
    if details:
        sections.append("- Setup details:")
        for key, value in details.items():
            compact = _setup_value_markdown(value).replace("\n", " | ").strip()
            if compact:
                sections.append(f"  - {key}: {compact[:600]}")
    if role_rows:
        sections.append("- Invited agents and roles:")
        for row in role_rows:
            aid = str(row.get("agent_id") or "").strip()
            name = str(row.get("agent_name") or aid).strip()
            role = str(row.get("role") or "").strip()
            primary = bool(row.get("is_primary"))
            marker = " (primary)" if primary else ""
            role_part = f" | role: {role}" if role else ""
            sections.append(f"  - {name} [{aid}]{marker}{role_part}")
    if plan_status != PLAN_STATUS_APPROVED:
        sections.append("- Project plan is not approved yet. Only planning/discussion is allowed; do not execute tasks.")
        sections.append(f"- Before planning, read `{PROJECT_INFO_FILE}` and align with setup chat history.")
    else:
        sections.append("- Project plan is approved. Execute within assigned scope and update progress in chat.")
        sections.append("- If execution is blocked by missing user info/approval, pause and ask the owner clearly.")
        sections.append("- For pause points, return JSON with `requires_user_input=true`, `pause_reason`, and optional `resume_hint`.")
        sections.append("- If owner explicitly says SKIP for missing info, make reasonable assumptions and continue execution.")
    sections.append("- When handing off dependencies, mention the related invited agent explicitly using @agent_id.")
    sections.append(
        "- If you generate or modify files, include them in JSON field `output_files` as "
        "[{\"path\":\"...\",\"content\":\"...\",\"append\":false}] and include a human sentence in `chat_update`."
    )
    sections.append("- Keep continuity with previous conversation turns in this same project session.")
    return "\n".join(sections)

def _agent_roster_markdown(role_rows: List[Dict[str, Any]]) -> str:
    lines = ["Invited agents roster (use exact agent_id in @mentions):"]
    if not role_rows:
        lines.append("- none")
        return "\n".join(lines)
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        if not aid:
            continue
        name = str(row.get("agent_name") or aid).strip()
        role = str(row.get("role") or "").strip() or "Contributor"
        primary = " (primary)" if bool(row.get("is_primary")) else ""
        lines.append(f"- {aid}{primary}: {name}; role={role}")
    return "\n".join(lines)

def _extract_path_hints_from_text(text: str, *, limit: int = 8) -> List[str]:
    hints: List[str] = []
    seen: set[str] = set()
    raw = str(text or "")
    if not raw:
        return hints
    pattern = re.compile(r"(?:^|[\s`\"'(<])([A-Za-z0-9._-]+(?:[\\/][A-Za-z0-9._-]+)+(?:\.[A-Za-z0-9._-]+)?)")
    for m in pattern.finditer(raw):
        rel = _clean_relative_project_path(str(m.group(1) or "").replace("\\", "/"))
        if not rel:
            continue
        low = rel.lower()
        if low in seen:
            continue
        seen.add(low)
        hints.append(rel)
        if len(hints) >= max(1, limit):
            break
    return hints

def _build_project_file_context(
    *,
    owner_user_id: str,
    project_root: str,
    include_paths: Optional[List[str]] = None,
    request_text: str = "",
    max_total_chars: int = MAX_PROJECT_CONTEXT_TOTAL_CHARS,
    max_file_chars: int = MAX_PROJECT_CONTEXT_FILE_CHARS,
    max_files: int = MAX_PROJECT_CONTEXT_FILES,
    include_tree: bool = True,
) -> str:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    except Exception:
        return ""
    if not project_dir.exists():
        return ""

    max_total = max(1200, _to_int(max_total_chars))
    remaining = max_total
    chunks: List[str] = []

    if include_tree:
        tree = _render_tree(project_dir, max_depth=3, max_entries=140)
        tree = tree[:MAX_PROJECT_CONTEXT_TREE_CHARS].strip()
        if tree:
            block = f"Project folder tree:\n{tree}"
            if len(block) < remaining:
                chunks.append(block)
                remaining -= len(block)

    default_refs = [
        OVERVIEW_FILE,
        PROJECT_PLAN_FILE,
        PROJECT_DELEGATION_FILE,
        PROJECT_INFO_FILE,
        README_FILE,
        BRIEF_FILE,
        GOAL_FILE,
        PROJECT_SETUP_FILE,
        "agents/ROLES.md",
        SETUP_CHAT_HISTORY_FILE,
        SETUP_CHAT_HISTORY_COMPAT_FILE,
        # Legacy docs fallback
        "overview.md",
        "project-plan.md",
        "project-delegation.md",
        "README.md",
        "brief.md",
        "goal.md",
        "project-setup.md",
        "setup-chat-history.txt",
        "SETUP-CHAT.txt",
    ]
    candidates: List[str] = []
    seen: set[str] = set()
    for rel in (include_paths or []) + default_refs + _extract_path_hints_from_text(request_text, limit=10):
        clean = _clean_relative_project_path(rel)
        if not clean:
            continue
        low = clean.lower()
        if low in seen:
            continue
        seen.add(low)
        candidates.append(clean)

    files_added = 0
    for rel in candidates:
        if files_added >= max(1, max_files):
            break
        try:
            target = _resolve_project_relative_path(
                owner_user_id,
                project_root,
                rel,
                require_exists=True,
                require_dir=False,
            )
        except Exception:
            continue
        if not target.is_file():
            continue
        try:
            file_size = int(target.stat().st_size)
            read_limit = max(400, max_file_chars * 4)
            with target.open("rb") as fh:
                raw = fh.read(read_limit + 1)
            truncated = len(raw) > read_limit or file_size > len(raw)
            if len(raw) > read_limit:
                raw = raw[:read_limit]
            text = raw.decode("utf-8", errors="replace")
        except Exception:
            continue
        snippet = text[:max_file_chars].strip()
        if not snippet:
            continue
        if len(text) > max_file_chars:
            truncated = True
        marker = " (truncated)" if truncated else ""
        block = f"FILE `{rel}`{marker}:\n{snippet}"
        if len(block) >= remaining:
            continue
        chunks.append(block)
        remaining -= len(block)
        files_added += 1
        if remaining < 600:
            break

    if not chunks:
        return ""
    return "Server project context (read-only snapshot):\n\n" + "\n\n".join(chunks)

def _roles_markdown(role_rows: List[Dict[str, Any]]) -> str:
    lines = ["# Agent Roles", ""]
    if not role_rows:
        lines.append("No agents invited yet.")
        return "\n".join(lines).strip() + "\n"
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        name = str(row.get("agent_name") or aid).strip()
        role = str(row.get("role") or "").strip() or "No explicit role yet."
        primary = bool(row.get("is_primary"))
        lines.append(f"## {name}")
        lines.append(f"- id: `{aid}`")
        lines.append(f"- primary: {'yes' if primary else 'no'}")
        lines.append(f"- role: {role}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def _usage_markdown(
    *,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    updated_at: Optional[int],
) -> str:
    lines = [
        "# Usage",
        "",
        f"- prompt_tokens: {max(0, _to_int(prompt_tokens))}",
        f"- completion_tokens: {max(0, _to_int(completion_tokens))}",
        f"- total_tokens: {max(0, _to_int(total_tokens))}",
        f"- updated_at: {format_ts(updated_at) if updated_at else '-'}",
        "",
    ]
    return "\n".join(lines).strip() + "\n"

def _tracker_markdown(
    *,
    execution_status: str,
    progress_pct: int,
    execution_updated_at: Optional[int],
    plan_status: str,
) -> str:
    lines = [
        "# Tracker",
        "",
        f"- execution_status: {_coerce_execution_status(execution_status)}",
        f"- progress_pct: {_clamp_progress(progress_pct)}",
        f"- execution_updated_at: {format_ts(execution_updated_at) if execution_updated_at else '-'}",
        f"- plan_status: {_coerce_plan_status(plan_status)}",
        "",
    ]
    return "\n".join(lines).strip() + "\n"

def _project_overview_markdown(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
    plan_status: str,
    plan_text: str,
    execution_status: str = EXEC_STATUS_IDLE,
    progress_pct: int = 0,
    execution_updated_at: Optional[int] = None,
    usage_prompt_tokens: int = 0,
    usage_completion_tokens: int = 0,
    usage_total_tokens: int = 0,
    usage_updated_at: Optional[int] = None,
) -> str:
    lines = [
        f"# {title}",
        "",
        "## Brief",
        brief.strip() or "-",
        "",
        "## Goal",
        goal.strip() or "-",
        "",
        "## Plan Status",
        plan_status,
        "",
        "## Runtime",
        f"- execution_status: {_coerce_execution_status(execution_status)}",
        f"- progress_pct: {_clamp_progress(progress_pct)}",
        f"- execution_updated_at: {format_ts(execution_updated_at) if execution_updated_at else '-'}",
        "",
        "## Usage",
        f"- total_tokens: {max(0, _to_int(usage_total_tokens))}",
        f"- prompt_tokens: {max(0, _to_int(usage_prompt_tokens))}",
        f"- completion_tokens: {max(0, _to_int(usage_completion_tokens))}",
        f"- usage_updated_at: {format_ts(usage_updated_at) if usage_updated_at else '-'}",
        "",
        "## Setup Details",
        _setup_details_markdown(setup_details).strip(),
        "",
        "## Roles",
        _roles_markdown(role_rows).strip(),
        "",
        "## Current Plan",
        (plan_text or "No plan generated yet.").strip(),
        "",
    ]
    return "\n".join(lines).strip() + "\n"

def _write_project_overview_file(
    *,
    owner_user_id: str,
    project_root: str,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
    plan_status: str,
    plan_text: str,
    execution_status: str = EXEC_STATUS_IDLE,
    progress_pct: int = 0,
    execution_updated_at: Optional[int] = None,
    usage_prompt_tokens: int = 0,
    usage_completion_tokens: int = 0,
    usage_total_tokens: int = 0,
    usage_updated_at: Optional[int] = None,
) -> None:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root)
    except Exception:
        return
    project_dir.mkdir(parents=True, exist_ok=True)
    info_dir = _project_info_dir(project_dir)
    if not _path_within(info_dir, project_dir):
        return
    info_dir.mkdir(parents=True, exist_ok=True)
    overview = _project_overview_markdown(
        title=title,
        brief=brief,
        goal=goal,
        setup_details=setup_details,
        role_rows=role_rows,
        plan_status=plan_status,
        plan_text=plan_text,
        execution_status=execution_status,
        progress_pct=progress_pct,
        execution_updated_at=execution_updated_at,
        usage_prompt_tokens=usage_prompt_tokens,
        usage_completion_tokens=usage_completion_tokens,
        usage_total_tokens=usage_total_tokens,
        usage_updated_at=usage_updated_at,
    )
    (info_dir / "overview.md").write_text(overview, encoding="utf-8")
    (info_dir / "project-plan.md").write_text((plan_text or "No plan generated yet.").strip() + "\n", encoding="utf-8")
    (info_dir / "usage.md").write_text(
        _usage_markdown(
            prompt_tokens=usage_prompt_tokens,
            completion_tokens=usage_completion_tokens,
            total_tokens=usage_total_tokens,
            updated_at=usage_updated_at,
        ),
        encoding="utf-8",
    )
    (info_dir / "tracker.md").write_text(
        _tracker_markdown(
            execution_status=execution_status,
            progress_pct=progress_pct,
            execution_updated_at=execution_updated_at,
            plan_status=plan_status,
        ),
        encoding="utf-8",
    )
    for legacy in _legacy_project_doc_paths(project_dir):
        if not legacy.exists():
            continue
        if not _path_within(legacy, project_dir):
            continue
        try:
            legacy.unlink()
        except Exception:
            pass

def _refresh_project_documents(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT id, user_id, title, brief, goal, project_root, setup_json, plan_text, plan_status,
               execution_status, progress_pct, execution_updated_at,
               usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
        FROM projects
        WHERE id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()
    _write_project_overview_file(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=_normalize_setup_details(_parse_setup_json(row["setup_json"])),
        role_rows=role_rows,
        plan_status=_coerce_plan_status(row["plan_status"]),
        plan_text=str(row["plan_text"] or ""),
        execution_status=_coerce_execution_status(row["execution_status"]),
        progress_pct=_clamp_progress(row["progress_pct"]),
        execution_updated_at=row["execution_updated_at"],
        usage_prompt_tokens=max(0, _to_int(row["usage_prompt_tokens"])),
        usage_completion_tokens=max(0, _to_int(row["usage_completion_tokens"])),
        usage_total_tokens=max(0, _to_int(row["usage_total_tokens"])),
        usage_updated_at=row["usage_updated_at"],
    )
    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
        _initialize_project_folder(
            project_dir,
            str(row["title"] or ""),
            str(row["brief"] or ""),
            str(row["goal"] or ""),
            setup_details=_normalize_setup_details(_parse_setup_json(row["setup_json"])),
        )
    except Exception:
        pass

def _fallback_project_title(transcript: List[Dict[str, Any]]) -> str:
    extracted = _extract_title_from_setup_transcript(transcript)
    if extracted:
        return extracted
    for item in transcript:
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "human"}:
            continue
        text = str(item.get("text") or item.get("content") or "").strip()
        if not text:
            continue
        cand = _sanitize_title_candidate(text)
        if cand:
            return cand[:120]
    return f"Project {time.strftime('%Y-%m-%d')}"

def _first_user_lines(transcript: List[Dict[str, Any]], limit: int = 3) -> List[str]:
    lines: List[str] = []
    for item in transcript:
        if str(item.get("role") or "").lower() not in {"user", "human"}:
            continue
        text = str(item.get("text") or item.get("content") or "").strip()
        if not text:
            continue
        lines.append(text[:400])
        if len(lines) >= limit:
            break
    return lines

def _looks_like_question_text(text: str) -> bool:
    low = str(text or "").strip().lower()
    if not low:
        return False
    if "?" in low:
        return True
    return bool(
        re.match(
            r"^(what|who|when|where|why|how|which|can|could|would|should|is|are|do|does|did|please)\b",
            low,
        )
    )

def _sanitize_title_candidate(text: str) -> str:
    clean = re.sub(r"\s+", " ", str(text or "")).strip().strip("`'\"")
    if not clean:
        return ""
    clean = re.sub(
        r"^(?:(?:my|our|the)\s+)?(?:project\s*(name|title)|nama\s*(project|proyek)(?:\s+saya)?|judul\s*(project|proyek))\s*[:=-]\s*",
        "",
        clean,
        flags=re.IGNORECASE,
    ).strip()
    blocked = {
        "start new project setup and ask the first question.",
        "what is the project name?",
        "what is the project title?",
        "what is your project name?",
    }
    low = clean.lower()
    if low in blocked:
        return ""
    if _looks_like_question_text(clean):
        return ""
    return clean[:160]

def _extract_title_from_setup_transcript(transcript: List[Dict[str, Any]]) -> str:
    items = transcript or []
    # Prefer direct answer after assistant asks about project name/title.
    for idx, item in enumerate(items):
        role = str(item.get("role") or "").strip().lower()
        if role not in {"assistant", "agent"}:
            continue
        ask = str(item.get("text") or item.get("content") or "").strip().lower()
        if not ask:
            continue
        if not any(key in ask for key in ["project name", "project title", "nama project", "nama proyek", "judul project", "judul proyek"]):
            continue
        for next_item in items[idx + 1 : idx + 6]:
            next_role = str(next_item.get("role") or "").strip().lower()
            if next_role not in {"user", "human"}:
                continue
            cand = _sanitize_title_candidate(str(next_item.get("text") or next_item.get("content") or ""))
            if cand:
                return cand

    # Then check user lines that explicitly label project name/title.
    for item in items:
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "human"}:
            continue
        raw = str(item.get("text") or item.get("content") or "").strip()
        if not raw:
            continue
        labeled = re.search(
            r"(?:project\s*(?:name|title)|nama\s*(?:project|proyek)|judul\s*(?:project|proyek))\s*[:=-]\s*(.+)$",
            raw,
            flags=re.IGNORECASE,
        )
        if labeled:
            cand = _sanitize_title_candidate(str(labeled.group(1) or ""))
            if cand:
                return cand
        cand = _sanitize_title_candidate(raw)
        if cand and len(cand.split()) >= 2:
            return cand

    return ""

def _build_project_root(project_id: str, title: str, workspace_root: str = HIVEE_ROOT) -> str:
    suffix = project_id.split("_", 1)[-1][:8]
    projects_root = f"{workspace_root}/PROJECTS"
    return f"{projects_root}/{_slugify(title)}-{suffix}"

def _initialize_project_folder(
    project_dir: Path,
    title: str,
    brief: str,
    goal: str,
    *,
    setup_details: Optional[Dict[str, Any]] = None,
    setup_chat_history_text: Optional[str] = None,
) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    info_dir = _project_info_dir(project_dir)
    if not _path_within(info_dir, project_dir):
        return
    info_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "agents").mkdir(parents=True, exist_ok=True)
    (project_dir / USER_OUTPUTS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / "logs").mkdir(parents=True, exist_ok=True)
    details = _normalize_setup_details(setup_details or {})
    readme = info_dir / "README.md"
    readme.write_text(
        _project_readme_markdown(
            title=title,
            brief=brief,
            goal=goal,
            setup_details=details,
        ),
        encoding="utf-8",
    )
    brief_file = info_dir / "brief.md"
    brief_file.write_text(
        _project_brief_markdown(brief=brief, setup_details=details),
        encoding="utf-8",
    )
    goal_file = info_dir / "goal.md"
    goal_file.write_text(goal.strip() + "\n", encoding="utf-8")
    setup_file = info_dir / "project-setup.md"
    setup_file.write_text(_setup_details_markdown(details), encoding="utf-8")
    explicit_history_text = str(setup_chat_history_text or "").replace("\r", "").strip()
    history_text = explicit_history_text
    if not history_text:
        history_text = _fallback_setup_chat_history_text(details)
    history_file = info_dir / "setup-chat-history.txt"
    history_compat_file = info_dir / "SETUP-CHAT.txt"
    if explicit_history_text:
        payload = history_text.strip() + "\n"
        history_file.write_text(payload, encoding="utf-8")
        history_compat_file.write_text(payload, encoding="utf-8")
    else:
        if history_text and (not history_file.exists()):
            history_file.write_text(history_text.strip() + "\n", encoding="utf-8")
        if not history_file.exists():
            history_file.write_text("No setup chat history captured.\n", encoding="utf-8")
        if not history_compat_file.exists():
            try:
                history_compat_file.write_text(history_file.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception:
                history_compat_file.write_text("No setup chat history captured.\n", encoding="utf-8")
    project_info = info_dir / "PROJECT-INFO.MD"
    if not project_info.exists():
        project_info.write_text(
            _seed_project_info_markdown(title=title, brief=brief, goal=goal),
            encoding="utf-8",
        )
    agent_roles = project_dir / "agents" / "ROLES.md"
    if not agent_roles.exists():
        agent_roles.write_text(
            "# Agent Roles\n\nDefine each invited agent role for this project.\n",
            encoding="utf-8",
        )
    chat_file = info_dir / "chat-hivee.md"
    if not chat_file.exists():
        chat_file.write_text(
            "# Chat Hivee\n\nDaily conversation and event logs are saved in `logs/YYYY-MM-DD-chat.md` and `logs/YYYY-MM-DD-events.jsonl`.\n",
            encoding="utf-8",
        )
    for legacy in _legacy_project_doc_paths(project_dir):
        if not legacy.exists():
            continue
        if not _path_within(legacy, project_dir):
            continue
        try:
            legacy.unlink()
        except Exception:
            pass

def _write_project_agent_roles_file(
    *,
    owner_user_id: str,
    project_root: str,
    agents: List[Dict[str, Any]],
) -> None:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root)
    except Exception:
        return
    agents_dir = project_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    lines = ["# Agent Roles", ""]
    if not agents:
        lines.append("No agents invited yet.")
    else:
        for row in agents:
            name = str(row.get("agent_name") or row.get("agent_id") or "agent")
            aid = str(row.get("agent_id") or "").strip()
            role = str(row.get("role") or "").strip()
            primary = bool(row.get("is_primary"))
            lines.append(f"## {name}")
            lines.append(f"- id: `{aid}`")
            lines.append(f"- primary: {'yes' if primary else 'no'}")
            if role:
                lines.append(f"- role: {role}")
            lines.append("")
    (agents_dir / "ROLES.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

def _plan_prompt_from_project(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
    project_info_excerpt: str = "",
) -> str:
    context = _project_context_instruction(
        title=title,
        brief=brief,
        goal=goal,
        setup_details=setup_details,
        role_rows=role_rows,
        plan_status=PLAN_STATUS_PENDING,
    )
    roster = _agent_roster_markdown(role_rows)
    return (
        f"{context}\n\n"
        f"{roster}\n\n"
        "Task for primary agent:\n"
        f"1) Read `{PROJECT_INFO_FILE}` first and align every plan detail with it.\n"
        "2) Build project plan only (do not execute tasks yet).\n"
        "3) Include: work program, timeline/milestones, deliverables, dependencies, risks, and delegation proposal per invited agent.\n"
        "4) Add dependency flow with explicit trigger conditions (example: designbot finishes research, then @techbot starts).\n"
        "5) For every dependency handoff, define required chat trigger sentence that mentions target agent (e.g. @dailybot).\n"
        "6) Define mandatory user-approval pit stops and exact user input required to continue.\n"
        "7) If user says SKIP for missing info, primary agent may make assumptions and continue; mark those assumptions clearly.\n"
        "8) Mention what files should exist in project folder before execution.\n"
        "9) End with: WAITING FOR USER APPROVAL.\n"
        "10) Keep output plain text and concise but complete.\n"
        + (f"\nCurrent `{PROJECT_INFO_FILE}` excerpt:\n{project_info_excerpt[:5000]}\n" if str(project_info_excerpt).strip() else "")
    )

def _delegate_prompt_from_project(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
    plan_text: str,
    project_info_excerpt: str = "",
) -> str:
    context = _project_context_instruction(
        title=title,
        brief=brief,
        goal=goal,
        setup_details=setup_details,
        role_rows=role_rows,
        plan_status=PLAN_STATUS_APPROVED,
    )
    roster_text = _agent_roster_markdown(role_rows)
    roster = []
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        role = str(row.get("role") or "").strip()
        if aid:
            roster.append({"agent_id": aid, "role": role})
    return (
        f"{context}\n\n"
        f"{roster_text}\n\n"
        f"Plan approved by user. Read `{PROJECT_INFO_FILE}` first, then delegate tasks into Markdown documents.\n"
        "Return JSON object only:\n"
        "{\n"
        "  \"project_delegation_md\": \"...\",\n"
        "  \"agent_tasks\": [{\"agent_id\":\"...\",\"task_md\":\"...\"}],\n"
        "  \"notes\": \"...\"\n"
        "}\n"
        "Rules:\n"
        "- Use only invited agent IDs.\n"
        "- task_md should be concrete next actions + expected outputs + dependencies.\n"
        "- Every task_md must explicitly state `Responsible Agent: <agent_id>` using the same invited ID.\n"
        "- Every dependency handoff must include `Trigger Mention: @<agent_id>` for the next responsible agent.\n"
        "- Each task_md must include: prerequisites, start trigger, handoff trigger with @agent_id, expected files, and pit-stop approval gates.\n"
        f"- `project_delegation_md` should become `{PROJECT_DELEGATION_FILE}` and must summarize approved plan, sequencing, and coordination protocol.\n"
        "- Include an execution order showing which agent must finish first and what event triggers the next agent.\n"
        "- If an agent reaches a pit stop or missing credential, they must return requires_user_input=true with pause_reason and resume_hint.\n"
        "- If user chooses to skip missing details, primary agent can decide assumptions and continue; record assumptions.\n"
        f"Invited agents: {json.dumps(roster, ensure_ascii=False)}\n"
        f"Approved plan:\n{(plan_text or '').strip()[:MAX_TEMPLATE_PAYLOAD_BYTES]}\n"
        + (f"\n{PROJECT_INFO_FILE} excerpt:\n{project_info_excerpt[:5000]}\n" if str(project_info_excerpt).strip() else "")
    )

def _project_agent_rows(conn: sqlite3.Connection, project_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
        (project_id,),
    ).fetchall()
    return [dict(r) for r in rows]

def _parse_delegation_payload(text: str) -> Dict[str, Any]:
    parsed = _extract_json_object(text or "")
    if not isinstance(parsed, dict):
        return {}
    return parsed

def _normalize_task_markdown_for_agent(
    *,
    agent_id: str,
    role: str,
    task_md: str,
    next_agent_id: Optional[str] = None,
) -> str:
    raw = str(task_md or "").replace("\r", "").strip()
    title = f"# Task for {agent_id}"
    body = raw
    if raw.startswith("#"):
        first, _, rest = raw.partition("\n")
        if first.strip():
            title = first.strip()
            body = rest.strip()

    body = re.sub(r"(?im)^Responsible Agent\s*:\s*.*$", "", body)
    body = re.sub(r"(?im)^Role\s*:\s*.*$", "", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()

    lines = [
        title,
        "",
        f"Responsible Agent: {agent_id}",
        "",
        f"Role: {role}",
    ]
    if body:
        lines.extend(["", body])
    normalized = "\n".join(lines).strip()

    if next_agent_id and f"@{next_agent_id}".lower() not in normalized.lower():
        normalized = (
            normalized
            + f"\n\nTrigger Mention: @{next_agent_id}\n"
            + f"Handoff: After finishing your current step, post chat_update mentioning @{next_agent_id} so they can continue."
        )
    if "output_files" not in normalized.lower():
        normalized = (
            normalized
            + "\n\nOutput Requirement: include concrete deliverable files in output_files with full content."
        )
    return normalized.strip()

def _ensure_chat_handoff_mentions(chat_update: str, mentions: List[str]) -> str:
    text = str(chat_update or "").strip()
    clean_mentions = [str(m or "").strip() for m in mentions if str(m or "").strip()]
    if not clean_mentions:
        return text
    low = text.lower()
    missing = [m for m in clean_mentions if f"@{m.lower()}" not in low]
    if not missing:
        return text
    mention_blob = " ".join(f"@{m}" for m in missing[:4])
    suffix = f"Handoff: {mention_blob}, please continue with your next step."
    if not text:
        return suffix[:4000]
    return f"{text.rstrip()} {suffix}".strip()[:4000]

def _ensure_owner_mention(chat_update: str) -> str:
    text = str(chat_update or "").strip()
    low = text.lower()
    if "@owner" in low or "@user" in low:
        return text
    suffix = "@owner please provide the missing info so I can continue."
    if not text:
        return suffix
    return f"{text.rstrip()} {suffix}".strip()[:4000]

def _coerce_plan_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {
        PLAN_STATUS_PENDING,
        PLAN_STATUS_GENERATING,
        PLAN_STATUS_AWAITING_APPROVAL,
        PLAN_STATUS_APPROVED,
        PLAN_STATUS_FAILED,
    }:
        return raw
    return PLAN_STATUS_PENDING

def _coerce_execution_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {
        EXEC_STATUS_IDLE,
        EXEC_STATUS_RUNNING,
        EXEC_STATUS_PAUSED,
        EXEC_STATUS_STOPPED,
        EXEC_STATUS_COMPLETED,
    }:
        return raw
    return EXEC_STATUS_IDLE

def _clamp_progress(value: Any) -> int:
    try:
        pct = int(float(value))
    except Exception:
        pct = 0
    return max(0, min(100, pct))

def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0

def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on", "required"}

def _first_non_empty_text(*values: Any, max_len: int = 800) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text[:max(1, max_len)]
    return ""

def _infer_pause_request(
    *,
    chat_update: str,
    notes: str = "",
    explicit_requires_user_input: bool = False,
    explicit_pause_reason: str = "",
    explicit_resume_hint: str = "",
) -> Dict[str, Any]:
    raw_chat_update = str(chat_update or "").strip()
    combined = " ".join(
        [
            raw_chat_update.lower(),
            str(notes or "").strip().lower(),
            str(explicit_pause_reason or "").strip().lower(),
        ]
    )
    pause_keywords = [
        "waiting for user",
        "waiting for owner",
        "awaiting user",
        "awaiting approval",
        "need user approval",
        "needs user approval",
        "requires user approval",
        "need approval",
        "needs approval",
        "requires approval",
        "waiting for confirmation",
        "need confirmation",
        "missing api key",
        "need api key",
        "requires api key",
        "missing credential",
        "missing credentials",
        "need credential",
        "need credentials",
        "requires credential",
        "requires credentials",
        "need input from user",
        "need input from owner",
        "requires user input",
        "requires owner input",
        "awaiting user input",
        "awaiting owner input",
        "waiting for input",
        "waiting for your input",
        "waiting for your confirmation",
        "need your confirmation",
        "requires your confirmation",
        "awaiting your confirmation",
        "menunggu input",
        "butuh input",
        "menunggu jawaban",
        "butuh jawaban",
        "menunggu konfirmasi",
        "butuh konfirmasi",
        "perlu konfirmasi",
        "menunggu persetujuan",
        "butuh persetujuan",
        "perlu persetujuan",
        "menunggu approval",
        "butuh approval",
        "perlu approval",
        "pit stop",
        "blocked waiting",
        "pause until user",
    ]
    owner_context = any(marker in combined for marker in ["@owner", " owner", " user", " pengguna"])
    soft_pause_keywords = [
        "please provide",
        "please share",
        "can you provide",
        "could you provide",
        "need more information",
        "need additional information",
        "insufficient information",
        "missing information",
    ]
    keyword_pause = any(k in combined for k in pause_keywords) or (
        owner_context and any(k in combined for k in soft_pause_keywords)
    )
    owner_question_pause = ("@owner" in combined) and ("?" in raw_chat_update)
    should_pause = bool(explicit_requires_user_input) or keyword_pause or owner_question_pause
    reason = _first_non_empty_text(explicit_pause_reason, chat_update, notes, max_len=900)
    hint = _first_non_empty_text(explicit_resume_hint, max_len=300)
    if should_pause and not reason:
        reason = "Execution paused. Waiting for user approval or required input."
    if should_pause and not hint:
        hint = "Reply with required information, then say CONTINUE (or press Resume)."
    return {"pause": should_pause, "reason": reason, "resume_hint": hint}

def _is_resume_command_message(text: str) -> bool:
    low = str(text or "").strip().lower()
    if not low:
        return False
    if re.search(r"\b(don't|do not|jangan)\s+(resume|continue|lanjut|lanjutkan)\b", low):
        return False
    return bool(re.search(r"\b(resume|continue|lanjut|lanjutkan|proceed|go on|carry on)\b", low))

def _estimate_tokens_from_text(text: Any) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    return max(1, len(raw) // 4)

def _extract_usage_counts(payload: Any) -> Tuple[int, int, int]:
    nodes: List[Dict[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            nodes.append(node)
            usage = node.get("usage")
            if isinstance(usage, dict):
                nodes.append(usage)
            for value in node.values():
                if isinstance(value, (dict, list)):
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    visit(item)

    visit(payload)
    for node in nodes:
        prompt_tokens = _to_int(node.get("prompt_tokens") or node.get("input_tokens"))
        completion_tokens = _to_int(node.get("completion_tokens") or node.get("output_tokens"))
        total_tokens = _to_int(node.get("total_tokens"))
        if total_tokens <= 0:
            total_tokens = prompt_tokens + completion_tokens
        if total_tokens > 0 or prompt_tokens > 0 or completion_tokens > 0:
            return max(0, prompt_tokens), max(0, completion_tokens), max(0, total_tokens)
    return 0, 0, 0

def _set_project_execution_state(
    project_id: str,
    *,
    status: Optional[str] = None,
    progress_pct: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    updates: List[str] = []
    values: List[Any] = []
    if status is not None:
        updates.append("execution_status = ?")
        values.append(_coerce_execution_status(status))
    if progress_pct is not None:
        updates.append("progress_pct = ?")
        values.append(_clamp_progress(progress_pct))
    if not updates:
        return None
    updates.append("execution_updated_at = ?")
    values.append(int(time.time()))
    values.append(project_id)

    conn = db()
    conn.execute(f"UPDATE projects SET {', '.join(updates)} WHERE id = ?", tuple(values))
    conn.commit()
    row = conn.execute(
        "SELECT execution_status, progress_pct, execution_updated_at FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "status": _coerce_execution_status(row["execution_status"]),
        "progress_pct": _clamp_progress(row["progress_pct"]),
        "updated_at": row["execution_updated_at"],
    }

def _update_project_usage_metrics(
    project_id: str,
    *,
    prompt_tokens: int,
    completion_tokens: int,
) -> Optional[Dict[str, Any]]:
    p = max(0, _to_int(prompt_tokens))
    c = max(0, _to_int(completion_tokens))
    conn = db()
    row = conn.execute(
        "SELECT usage_prompt_tokens, usage_completion_tokens FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return None

    total_prompt = max(0, _to_int(row["usage_prompt_tokens"])) + p
    total_completion = max(0, _to_int(row["usage_completion_tokens"])) + c
    total_tokens = total_prompt + total_completion
    now = int(time.time())
    conn.execute(
        """
        UPDATE projects
        SET usage_prompt_tokens = ?, usage_completion_tokens = ?, usage_total_tokens = ?, usage_updated_at = ?
        WHERE id = ?
        """,
        (total_prompt, total_completion, total_tokens, now, project_id),
    )
    conn.commit()
    conn.close()
    return {
        "prompt_tokens": total_prompt,
        "completion_tokens": total_completion,
        "total_tokens": total_tokens,
        "updated_at": now,
    }

def _append_project_daily_log(
    *,
    owner_user_id: str,
    project_root: str,
    kind: str,
    text: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root)
    except Exception:
        return
    logs_dir = project_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    day = time.strftime("%Y-%m-%d")
    ts = time.strftime("%H:%M:%S")
    md_path = logs_dir / f"{day}-chat.md"
    jsonl_path = logs_dir / f"{day}-events.jsonl"
    compact_text = str(text or "").strip().replace("\r", "")
    if compact_text:
        with md_path.open("a", encoding="utf-8") as f:
            f.write(f"\n## {ts} [{kind}]\n")
            for line in compact_text.splitlines():
                clean = line.strip()
                if clean:
                    f.write(f"- {clean}\n")
    record = {"ts": int(time.time()), "kind": kind, "text": compact_text}
    if payload:
        record["payload"] = payload
    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def _summarize_ws_frames(frames: Any, limit: int = 12) -> List[str]:
    if not isinstance(frames, list):
        return []
    notes: List[str] = []
    for frame in frames[-30:]:
        if not isinstance(frame, dict):
            continue
        if frame.get("type") != "event":
            continue
        ev = str(frame.get("event") or "").strip()
        payload = frame.get("payload")
        if ev == "chat" and isinstance(payload, dict):
            state = str(payload.get("state") or "").strip()
            if state:
                notes.append(f"chat state: {state}")
            err = str(payload.get("errorMessage") or payload.get("error") or "").strip()
            if err:
                notes.append(f"chat error: {err[:220]}")
            continue
        if ev == "agent" and isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                phase = str(data.get("phase") or "").strip()
                message = str(data.get("message") or data.get("status") or "").strip()
                if phase and message:
                    notes.append(f"agent {phase}: {message[:220]}")
                elif phase:
                    notes.append(f"agent phase: {phase}")
                elif message:
                    notes.append(f"agent: {message[:220]}")
            continue
        if isinstance(payload, dict):
            txt = str(payload.get("text") or payload.get("message") or "").strip()
            if txt:
                notes.append(f"{ev or 'event'}: {txt[:220]}")
                continue
        if ev:
            notes.append(f"event: {ev}")

    compact: List[str] = []
    seen: set[str] = set()
    for line in notes:
        key = line.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        compact.append(line)
        if len(compact) >= max(1, limit):
            break
    return compact

def _normalize_output_file_items(raw: Any) -> List[Dict[str, Any]]:
    items = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
    out: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        path = str(
            item.get("path")
            or item.get("file")
            or item.get("filename")
            or item.get("target")
            or ""
        ).strip()
        content = item.get("content")
        if content is None:
            content = item.get("text")
        if content is None:
            continue
        text = str(content)
        append = bool(item.get("append"))
        if not path:
            continue
        out.append({"path": path[:300], "content": text, "append": append})
        if len(out) >= MAX_AGENT_FILE_WRITES:
            break
    return out

def _extract_file_blocks_from_text(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "")
    out: List[Dict[str, Any]] = []
    pattern = re.compile(
        r"(?:^|\n)(?:File|Path|Filename)\s*:\s*([^\n`]+?)\s*\n```[^\n]*\n(.*?)\n```",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(raw):
        path = str(m.group(1) or "").strip()
        content = str(m.group(2) or "")
        if not path or not content:
            continue
        out.append({"path": path[:300], "content": content, "append": False})
        if len(out) >= MAX_AGENT_FILE_WRITES:
            break
    return out

def _extract_agent_report_payload(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    parsed = _extract_json_object(raw) or {}
    chat_update = raw
    output_files: List[Dict[str, Any]] = []
    notes = ""
    requires_user_input = False
    pause_reason = ""
    resume_hint = ""

    if isinstance(parsed, dict) and parsed:
        chat_update = str(
            parsed.get("chat_update")
            or parsed.get("conversation_update")
            or parsed.get("message")
            or parsed.get("summary")
            or raw
        ).strip()
        notes = str(parsed.get("notes") or "").strip()
        requires_user_input = _coerce_bool(
            parsed.get("requires_user_input")
            or parsed.get("awaiting_user")
            or parsed.get("needs_approval")
            or parsed.get("needs_user_approval")
            or parsed.get("pause_execution")
            or parsed.get("pause")
        )
        pause_reason = _first_non_empty_text(
            parsed.get("pause_reason"),
            parsed.get("await_reason"),
            parsed.get("blocking_reason"),
            parsed.get("required_input"),
            max_len=1200,
        )
        resume_hint = _first_non_empty_text(
            parsed.get("resume_hint"),
            parsed.get("continue_hint"),
            parsed.get("next_user_action"),
            max_len=500,
        )
        output_files.extend(_normalize_output_file_items(parsed.get("output_files")))
        if not output_files:
            output_files.extend(_normalize_output_file_items(parsed.get("files")))
        if not output_files:
            output_files.extend(_normalize_output_file_items(parsed.get("writes")))
        artifacts = parsed.get("artifacts")
        if isinstance(artifacts, dict):
            for k, v in artifacts.items():
                p = str(k or "").strip()
                c = str(v or "")
                if p and c:
                    output_files.append({"path": p[:300], "content": c, "append": False})
                    if len(output_files) >= MAX_AGENT_FILE_WRITES:
                        break

    if not output_files:
        output_files = _extract_file_blocks_from_text(raw)

    # Fallback: if response is JSON-heavy without user-facing sentence.
    if chat_update.startswith("{") and chat_update.endswith("}"):
        chat_update = "I have finished this step and shared the output."
    if not chat_update:
        chat_update = "I have completed this step."

    return {
        "chat_update": chat_update[:4000],
        "output_files": output_files[:MAX_AGENT_FILE_WRITES],
        "notes": notes[:2000],
        "requires_user_input": bool(requires_user_input),
        "pause_reason": pause_reason[:1200],
        "resume_hint": resume_hint[:500],
    }

def _apply_project_file_writes(
    *,
    owner_user_id: str,
    project_root: str,
    writes: List[Dict[str, Any]],
    default_prefix: str = f"{USER_OUTPUTS_DIRNAME}/generated",
) -> Dict[str, Any]:
    saved: List[Dict[str, Any]] = []
    skipped: List[str] = []
    prefix = _normalize_user_outputs_prefix(default_prefix)
    allowed_roots = [
        USER_OUTPUTS_DIRNAME,
        PROJECT_INFO_DIRNAME,
        "agents",
        "logs",
    ]
    for idx, item in enumerate(writes[:MAX_AGENT_FILE_WRITES], start=1):
        path_raw = str(item.get("path") or "").strip()
        content = str(item.get("content") or "")
        append = bool(item.get("append"))
        if not content:
            skipped.append(f"item {idx}: empty content")
            continue
        if len(content.encode("utf-8")) > MAX_AGENT_FILE_BYTES:
            skipped.append(f"{path_raw or f'item {idx}'}: exceeds {MAX_AGENT_FILE_BYTES} bytes")
            continue
        rel = _clean_relative_project_path(path_raw)
        if rel:
            rel = _remap_legacy_project_doc_rel_path(rel)
            if rel.lower() == LEGACY_OUTPUTS_DIRNAME:
                rel = USER_OUTPUTS_DIRNAME
            elif _rel_path_startswith(rel, LEGACY_OUTPUTS_DIRNAME):
                suffix = rel[len(LEGACY_OUTPUTS_DIRNAME):].lstrip("/\\")
                rel = _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
        if not rel:
            rel = f"{prefix}/artifact-{idx}.txt"
        if not any(_rel_path_startswith(rel, root) for root in allowed_roots):
            rel = _clean_relative_project_path(f"{prefix}/{rel}")
        try:
            target = _resolve_project_relative_path(
                owner_user_id,
                project_root,
                rel,
                require_exists=False,
                require_dir=False,
            )
        except Exception:
            skipped.append(f"{rel}: invalid path")
            continue
        if target.exists() and target.is_dir():
            skipped.append(f"{rel}: target is directory")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        with target.open(mode, encoding="utf-8") as f:
            f.write(content)
        saved.append(
            {
                "path": _clean_relative_project_path(rel),
                "mode": mode,
                "bytes": len(content.encode("utf-8")),
            }
        )
    return {"saved": saved, "skipped": skipped}

def _looks_like_artifact_request(text: str) -> bool:
    low = str(text or "").strip().lower()
    if not low:
        return False
    action_words = [
        "build",
        "create",
        "make",
        "implement",
        "generate",
        "write",
        "develop",
        "scaffold",
        "bikin",
        "buat",
        "buatkan",
        "bikinin",
        "bangun",
        "kerjain",
        "kerjakan",
        "koding",
        "coding",
    ]
    artifact_words = [
        "website",
        "web",
        "web app",
        "landing page",
        "landing",
        "halaman",
        "html",
        "css",
        "javascript",
        "frontend",
        "ui",
        "ux",
        "backend",
        "api",
        "script",
        "file",
        "folder",
        "component",
        "module",
        "page",
        "readme",
        "md",
        "code",
    ]
    return any(w in low for w in action_words) and any(w in low for w in artifact_words)

def _should_request_artifact_followup(
    *,
    user_message: str,
    raw_response: str,
    parsed_payload: Dict[str, Any],
    saved_files: List[Dict[str, Any]],
) -> bool:
    if saved_files:
        return False
    if _looks_like_artifact_request(user_message):
        return True
    raw_low = str(raw_response or "").strip().lower()
    completion_words = [
        "completed",
        "done",
        "finished",
        "implemented",
        "created",
        "generated",
        "built",
        "ready",
    ]
    if "```" in raw_low:
        return True
    if any(w in raw_low for w in completion_words) and _looks_like_artifact_request(user_message):
        return True
    chat_update = str(parsed_payload.get("chat_update") or "").strip().lower()
    if any(w in chat_update for w in completion_words) and _looks_like_artifact_request(user_message):
        return True
    return False

def _build_artifact_followup_prompt(*, user_message: str, previous_response: str) -> str:
    return (
        "Follow-up: return artifacts for this task in strict JSON only (no markdown).\n"
        "Schema:\n"
        "{\n"
        "  \"chat_update\": \"one short human sentence\",\n"
        "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"full file content\",\"append\":false}],\n"
        "  \"notes\": \"optional\",\n"
        "  \"requires_user_input\": false,\n"
        "  \"pause_reason\": \"\",\n"
        "  \"resume_hint\": \"\"\n"
        "}\n"
        "Rules:\n"
        "- If you created or modified files, include all of them in output_files with full content.\n"
        "- Use project-relative paths only.\n"
        "- If you cannot continue without user approval/input, set requires_user_input=true and explain pause_reason.\n"
        "- If no files were produced, return output_files as [] and explain why in chat_update.\n\n"
        f"Original user message:\n{str(user_message or '').strip()[:3000]}\n\n"
        f"Your previous response:\n{str(previous_response or '').strip()[:6000]}"
    )

def _extract_artifacts_from_fenced_code(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "")
    if not raw:
        return []
    pattern = re.compile(r"```([a-zA-Z0-9_.+-]*)\n(.*?)\n```", flags=re.DOTALL)
    name_map = {
        "html": "index.html",
        "htm": "index.html",
        "css": "style.css",
        "js": "app.js",
        "javascript": "app.js",
        "ts": "app.ts",
        "tsx": "app.tsx",
        "jsx": "app.jsx",
        "py": "main.py",
        "md": "README.md",
        "markdown": "README.md",
    }
    out: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for match in pattern.finditer(raw):
        lang = str(match.group(1) or "").strip().lower()
        content = str(match.group(2) or "")
        if not content.strip():
            continue
        base = name_map.get(lang)
        if not base:
            continue
        seen[base] = seen.get(base, 0) + 1
        name = base
        if seen[base] > 1:
            stem = Path(base).stem
            suffix = Path(base).suffix
            name = f"{stem}-{seen[base]}{suffix}"
        out.append({"path": name, "content": content, "append": False})
        if len(out) >= MAX_AGENT_FILE_WRITES:
            break
    return out

def _build_artifact_recovery_prompt(
    *,
    agent_id: str,
    role: str,
    task_text: str,
    previous_response: str,
) -> str:
    fallback_file = f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(agent_id)}-deliverable.md"
    return (
        "Second follow-up: your previous response still did not sync files.\n"
        "Return strict JSON only (no markdown):\n"
        "{\n"
        "  \"chat_update\": \"one short human sentence\",\n"
        "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"full file content\",\"append\":false}],\n"
        "  \"notes\": \"optional\",\n"
        "  \"requires_user_input\": false,\n"
        "  \"pause_reason\": \"\",\n"
        "  \"resume_hint\": \"\"\n"
        "}\n"
        "Hard requirements:\n"
        "- output_files MUST NOT be empty unless requires_user_input=true.\n"
        "- If implementation task, return concrete source files (for websites include at least index.html and style.css).\n"
        f"- If planning/research task, return at least one markdown deliverable file at {fallback_file}.\n"
        "- Use project-relative paths only.\n\n"
        f"Agent: {agent_id}\n"
        f"Role: {role}\n"
        f"Assigned task:\n{str(task_text or '').strip()[:3500]}\n\n"
        f"Previous response:\n{str(previous_response or '').strip()[:7000]}"
    )

def _safe_agent_filename(agent_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-")
    return cleaned or "agent"

def _collect_new_user_templates() -> Dict[str, Any]:
    directories: List[str] = []
    files: List[Dict[str, str]] = []
    warnings: List[str] = []
    total_payload_bytes = 0

    if not NEW_USER_ASSETS_DIR.exists():
        warnings.append(f"Directory not found: {NEW_USER_ASSETS_DIR.as_posix()}")
    else:
        for node in sorted(NEW_USER_ASSETS_DIR.rglob("*"), key=lambda p: p.as_posix()):
            rel = node.relative_to(NEW_USER_ASSETS_DIR).as_posix()
            if not rel:
                continue
            if node.is_dir():
                directories.append(rel)
                continue

            if len(files) >= MAX_TEMPLATE_FILES:
                warnings.append(f"Skipped remaining files after {MAX_TEMPLATE_FILES} items.")
                break

            try:
                raw = node.read_bytes()
            except Exception as e:
                warnings.append(f"Skipped {rel}: {str(e)}")
                continue
            if len(raw) > MAX_TEMPLATE_FILE_BYTES:
                warnings.append(f"Skipped {rel}: {len(raw)} bytes exceeds max {MAX_TEMPLATE_FILE_BYTES}.")
                continue
            if (total_payload_bytes + len(raw)) > MAX_TEMPLATE_PAYLOAD_BYTES:
                warnings.append(f"Skipped {rel}: aggregate payload exceeds {MAX_TEMPLATE_PAYLOAD_BYTES} bytes.")
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                warnings.append(f"Skipped {rel}: non UTF-8 file.")
                continue
            files.append({"path": rel, "content": text})
            total_payload_bytes += len(raw)

    has_setup = any((f.get("path") or "").upper() == "PROJECT-SETUP.MD" for f in files)
    if not has_setup:
        files.append({"path": "PROJECT-SETUP.MD", "content": DEFAULT_PROJECT_SETUP_MD})
        warnings.append("PROJECT-SETUP.MD missing in assets/new_user; fallback template was injected.")

    return {"directories": directories, "files": files, "warnings": warnings}

def _pick_main_agent(agents: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not agents:
        return None

    def _score(agent: Dict[str, Any]) -> int:
        aid = str(agent.get("id") or "").lower()
        name = str(agent.get("name") or "").lower()
        text = f"{aid} {name}"
        score = 0
        if aid in {"main", "primary", "default", "core"}:
            score += 120
        if "main" in text:
            score += 80
        if "primary" in text:
            score += 50
        if "default" in text:
            score += 40
        if "core" in text:
            score += 20
        return score

    ranked = sorted(
        agents,
        key=lambda a: (_score(a), str(a.get("name") or ""), str(a.get("id") or "")),
        reverse=True,
    )
    return ranked[0]

def _workspace_policy_lines(workspace_root: str, project_root: Optional[str] = None) -> List[str]:
    lines = [
        "User workspace preference:",
        f"- Keep operations within `{workspace_root}` whenever possible.",
        f"- Ask for owner approval before accessing paths outside `{workspace_root}`.",
    ]
    if project_root:
        lines.extend(
            [
                f"- For this project, use `{project_root}` as the default working directory.",
                "- Ask project owner first before touching paths outside the project root.",
            ]
        )
    lines.append("- If permission is needed, ask a clear question first and wait for confirmation.")
    return lines

def _compose_guardrailed_message(
    user_message: str,
    *,
    workspace_root: str,
    project_root: Optional[str] = None,
    task_instruction: Optional[str] = None,
) -> str:
    clean_user_message = (user_message or "").strip() or "Continue."
    sections: List[str] = ["\n".join(_workspace_policy_lines(workspace_root, project_root))]
    if task_instruction:
        sections.append(task_instruction.strip())
    sections.append(f"User message:\n{clean_user_message}")
    return "\n\n".join(sections)

def _build_bootstrap_instruction(template_payload: Dict[str, Any]) -> str:
    payload_json = json.dumps(
        {
            "directories": template_payload.get("directories") or [],
            "files": template_payload.get("files") or [],
        },
        ensure_ascii=False,
    )
    warning_lines = template_payload.get("warnings") or []
    warning_block = "\n".join([f"- {w}" for w in warning_lines]) if warning_lines else "- none"
    task = (
        "User requested first-time Hivee workspace setup on your machine:\n"
        f"1) Check whether `{HIVEE_ROOT}` exists.\n"
        f"2) If missing, create `{HIVEE_ROOT}` and `{HIVEE_TEMPLATES_ROOT}`.\n"
        f"3) Recreate directories from payload under `{HIVEE_TEMPLATES_ROOT}`.\n"
        f"4) Write payload files under `{HIVEE_TEMPLATES_ROOT}`.\n"
        f"5) Ensure `{HIVEE_TEMPLATES_ROOT}/PROJECT-SETUP.MD` exists.\n"
        f"6) Return a short folder tree snapshot inside `{HIVEE_ROOT}`.\n"
        "Preferred response format (JSON if possible, plain text is also okay): "
        '{"status":"ok|partial|error","created_paths":["..."],"tree":["HIVEE/..."],"notes":"..."}\n'
        f"Template payload JSON:\n{payload_json}\n"
        f"Payload warnings:\n{warning_block}"
    )
    return _compose_guardrailed_message(
        "Please run this setup now.",
        workspace_root=HIVEE_ROOT,
        task_instruction=task,
    )

def _default_setup_questions() -> List[str]:
    return [
        "What is the project name?",
        "What is the main goal?",
        "Who are the target users?",
        "What are the key constraints (time, budget, tech, compliance)?",
        "What is in-scope and out-of-scope?",
        "What is the deadline or milestone cadence?",
        "What tools or stack are required?",
        "What output should be produced first?",
    ]

def _extract_setup_questions(template_content: str) -> List[str]:
    raw = (template_content or DEFAULT_PROJECT_SETUP_MD).strip()
    if not raw:
        return _default_setup_questions()
    questions: List[str] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        clean = str(line or "").strip()
        if not clean:
            continue
        if clean.startswith("#") or clean.startswith("```"):
            continue
        clean = re.sub(r"^\d+[.)]\s*", "", clean)
        clean = re.sub(r"^[-*+]\s*", "", clean)
        clean = clean.strip()
        if not clean:
            continue
        low = clean.lower()
        if low.startswith("ask these questions"):
            continue
        if low.startswith("after collecting"):
            continue
        looks_like_question = (
            "?" in clean
            or low.startswith("what ")
            or low.startswith("who ")
            or low.startswith("when ")
            or low.startswith("where ")
            or low.startswith("why ")
            or low.startswith("how ")
            or low.startswith("which ")
            or low.startswith("is ")
            or low.startswith("are ")
            or low.startswith("do ")
            or low.startswith("does ")
            or low.startswith("can ")
        )
        if not looks_like_question:
            continue
        if not clean.endswith("?"):
            clean = clean + "?"
        compact = re.sub(r"\s+", " ", clean).strip()[:MAX_SETUP_QUESTION_CHARS]
        key = compact.lower()
        if not compact or key in seen:
            continue
        seen.add(key)
        questions.append(compact)
        if len(questions) >= MAX_SETUP_QUESTION_ITEMS:
            break
    return questions or _default_setup_questions()

def _compact_setup_checklist(template_content: str) -> str:
    items = _extract_setup_questions(template_content)
    lines = [f"{idx}. {q}" for idx, q in enumerate(items, start=1)]
    return "\n".join(lines)[:MAX_SETUP_TEMPLATE_PROMPT_CHARS]

def _extract_setup_details_from_user_lines(user_lines: List[str]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}

    def _pick_first(matchers: List[str]) -> Optional[str]:
        for line in user_lines:
            low = line.lower()
            if any(m in low for m in matchers):
                return line[:5000]
        return None

    target = _pick_first(["target user", "target users", "audience", "pengguna", "target market"])
    if target:
        details["target_users"] = target
    constraints = _pick_first(["constraint", "constraints", "batas", "budget", "deadline", "timeline", "compliance"])
    if constraints:
        details["constraints"] = constraints
    in_scope = _pick_first(["in-scope", "in scope", "scope", "cakupan"])
    if in_scope:
        details["in_scope"] = in_scope
    out_scope = _pick_first(["out-of-scope", "out of scope", "exclude", "not include", "tidak termasuk"])
    if out_scope:
        details["out_of_scope"] = out_scope
    milestones = _pick_first(["milestone", "timeline", "schedule", "jadwal", "sprint", "deadline"])
    if milestones:
        details["milestones"] = milestones
    stack = _pick_first(["stack", "framework", "language", "tech", "tools", "tooling", "library"])
    if stack:
        details["required_stack"] = stack
    first_output = _pick_first(["first output", "deliverable", "output", "hasil pertama", "deliver"])
    if first_output:
        details["first_output"] = first_output
    return details

def _local_setup_draft(transcript: List[Dict[str, Any]]) -> Dict[str, Any]:
    user_lines = _first_user_lines(transcript or [], limit=20)
    details = _extract_setup_details_from_user_lines(user_lines)
    summary_lines: List[str] = []
    pending_question = ""
    for item in (transcript or [])[-80:]:
        role = str(item.get("role") or "").strip().lower()
        text = str(item.get("text") or item.get("content") or "").strip()
        if not text:
            continue
        compact = re.sub(r"\s+", " ", text).strip()[:280]
        if role in {"assistant", "agent"}:
            if _looks_like_question_text(compact):
                pending_question = compact
            continue
        if role not in {"user", "human"}:
            continue
        if pending_question:
            qlow = pending_question.lower()
            if not details.get("target_users") and any(k in qlow for k in ["target user", "target users", "audience", "pengguna"]):
                details["target_users"] = compact[:5000]
            if not details.get("constraints") and any(k in qlow for k in ["constraint", "constraints", "budget", "deadline", "timeline", "compliance", "batas"]):
                details["constraints"] = compact[:5000]
            if not details.get("in_scope") and any(k in qlow for k in ["in-scope", "in scope", "scope", "cakupan"]):
                details["in_scope"] = compact[:5000]
            if not details.get("out_of_scope") and any(k in qlow for k in ["out-of-scope", "out of scope", "exclude", "tidak termasuk"]):
                details["out_of_scope"] = compact[:5000]
            if not details.get("milestones") and any(k in qlow for k in ["milestone", "timeline", "schedule", "jadwal", "sprint"]):
                details["milestones"] = compact[:5000]
            if not details.get("required_stack") and any(k in qlow for k in ["stack", "framework", "tech", "tools", "language"]):
                details["required_stack"] = compact[:5000]
            if not details.get("first_output") and any(k in qlow for k in ["first output", "deliverable", "output", "hasil pertama"]):
                details["first_output"] = compact[:5000]
            summary_lines.append(f"- Q: {pending_question}")
            summary_lines.append(f"  A: {compact}")
            pending_question = ""
        else:
            summary_lines.append(f"- A: {compact}")
        if len(summary_lines) >= 24:
            break
    if summary_lines:
        details["setup_chat_summary"] = "\n".join(["Setup conversation recap:", *summary_lines])[:3500]
    if user_lines:
        details["setup_user_answers"] = user_lines[:16]
    mention_aliases: List[str] = []
    seen_alias: set[str] = set()
    for item in transcript[-40:]:
        text = str(item.get("text") or item.get("content") or "")
        for alias in re.findall(r"@([a-zA-Z0-9._-]{2,32})", text):
            low = alias.lower()
            if low in seen_alias:
                continue
            seen_alias.add(low)
            mention_aliases.append(alias)
            if len(mention_aliases) >= 8:
                break
        if len(mention_aliases) >= 8:
            break
    if mention_aliases:
        details["suggested_agents"] = [{"name": a, "role": "Contributor"} for a in mention_aliases]
    title = (_extract_title_from_setup_transcript(transcript or []) or _fallback_project_title(transcript or []))[:160]
    recap = str(details.get("setup_chat_summary") or "").strip()
    if recap:
        brief = recap[:5000]
    elif user_lines:
        brief = " ".join(user_lines[:10])[:5000]
    else:
        brief = "Project brief drafted from setup conversation."

    goal = ""
    for line in user_lines:
        low = line.lower()
        if any(k in low for k in ["goal", "tujuan", "objective", "build", "create", "develop", "make"]):
            goal = line[:5000]
            break
    if not goal:
        goal = str(details.get("first_output") or details.get("milestones") or "").strip()[:5000]
    if not goal:
        goal = "Produce a practical first deliverable and execution plan for this project."

    return {
        "title": title,
        "brief": brief,
        "goal": goal,
        "setup_details": details,
    }

def _build_new_project_setup_instruction(
    user_message: str,
    template_content: str,
    workspace_root: str = HIVEE_ROOT,
    *,
    start_mode: bool = False,
) -> str:
    clean = (user_message or "").strip()
    checklist = _compact_setup_checklist(template_content)
    if start_mode:
        return (
            "You are setup agent for new project onboarding.\n"
            "Token optimization mode is ON.\n"
            "Rules:\n"
            "- Ask exactly one concise question per turn (max 18 words).\n"
            "- No long explanation, no markdown blocks, no policy text.\n"
            "- Follow checklist order; ask confirmation when enough info exists.\n"
            f"- Keep work inside `{workspace_root}`.\n"
            "Checklist:\n"
            f"{checklist}\n"
            f"User message: {clean or 'Start setup and ask the first question.'}"
        )
    return (
        "Continue setup flow in token optimization mode.\n"
        "Ask only the next missing question or a short confirmation (max 18 words).\n"
        f"User replied: {clean or 'Continue.'}"
    )

def _compact_setup_transcript(transcript: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for item in transcript[-MAX_SETUP_TRANSCRIPT_ITEMS:]:
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant", "agent"}:
            role = "user"
        text = str(item.get("text") or item.get("content") or "").strip()
        if not text:
            continue
        compact = re.sub(r"\s+", " ", text).strip()[:MAX_SETUP_TRANSCRIPT_ITEM_CHARS]
        if compact:
            lines.append(f"{role.upper()}: {compact}")
    joined = "\n".join(lines).strip()
    return joined[:MAX_SETUP_TRANSCRIPT_CHARS]

def _setup_chat_history_text_from_transcript(
    transcript: List[Dict[str, Any]],
    *,
    max_items: int = 240,
    max_chars: int = 120_000,
) -> str:
    lines: List[str] = []
    for item in transcript[: max(1, max_items)]:
        role = str(item.get("role") or "").strip().lower()
        if role in {"human"}:
            role = "user"
        if role not in {"user", "assistant", "agent", "system"}:
            role = "user"
        text = str(item.get("text") or item.get("content") or "").replace("\r", "").strip()
        if not text:
            continue
        compact = re.sub(r"\n{3,}", "\n\n", text).strip()
        if not compact:
            continue
        lines.append(f"{role.upper()}: {compact}")
        if sum(len(x) + 1 for x in lines) >= max_chars:
            break
    return "\n".join(lines)[:max_chars].strip()

def _fallback_setup_chat_history_text(setup_details: Optional[Dict[str, Any]]) -> str:
    details = _normalize_setup_details(setup_details or {})
    explicit = str(
        details.get("setup_chat_history_text")
        or details.get("setup_chat_history")
        or ""
    ).strip()
    if explicit:
        return explicit[:120_000]

    transcript_raw = details.get("setup_chat_transcript")
    if isinstance(transcript_raw, list):
        derived = _setup_chat_history_text_from_transcript(transcript_raw)
        if derived:
            return derived

    summary = str(details.get("setup_chat_summary") or "").strip()
    answers = details.get("setup_user_answers")
    answer_lines: List[str] = []
    if isinstance(answers, list):
        for item in answers[:40]:
            text = str(item or "").strip()
            if text:
                answer_lines.append(f"USER: {text}")
    lines: List[str] = []
    if summary:
        lines.append(summary)
    if answer_lines:
        if lines:
            lines.append("")
        lines.append("Setup answers snapshot:")
        lines.extend(answer_lines)
    return "\n".join(lines).strip()[:120_000]

def _seed_project_info_markdown(*, title: str, brief: str, goal: str) -> str:
    lines = [
        f"# Project Info: {title}",
        "",
        "Status: draft - pending primary agent completion.",
        "",
        "## Brief",
        brief.strip() or "-",
        "",
        "## Goal",
        goal.strip() or "-",
        "",
        "## Setup Chat Summary",
        f"- Read `{SETUP_CHAT_HISTORY_FILE}` and summarize key decisions.",
        "",
        "## Scope",
        "- In scope:",
        "- Out of scope:",
        "",
        "## Constraints",
        "- Timeline:",
        "- Budget:",
        "- Technical:",
        "",
        "## Team Roles",
        "- Read `agents/ROLES.md` and list responsibilities.",
        "",
        "## Open Questions",
        "-",
        "",
        "## Assumptions",
        "-",
        "",
    ]
    return "\n".join(lines).strip() + "\n"

def _python_project_info_markdown(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
) -> str:
    lines = [
        f"# Project Info: {title}",
        "",
        "Status: synthesized by backend fallback.",
        "",
        "## Brief",
        brief.strip() or "-",
        "",
        "## Goal",
        goal.strip() or "-",
        "",
    ]
    summary = str(
        setup_details.get("setup_chat_summary")
        or setup_details.get("setup_summary")
        or ""
    ).strip()
    lines.extend(["## Setup Chat Summary", summary or "-", ""])
    lines.append("## Setup Details")
    if setup_details:
        for key, value in setup_details.items():
            compact = _setup_value_markdown(value).strip()
            if not compact:
                continue
            lines.append(f"- {key}: {compact[:800]}")
    else:
        lines.append("-")
    lines.extend(["", "## Team Roles"])
    if role_rows:
        for row in role_rows:
            aid = str(row.get("agent_id") or "").strip()
            name = str(row.get("agent_name") or aid).strip()
            role = str(row.get("role") or "").strip() or "Contributor"
            primary = " (primary)" if bool(row.get("is_primary")) else ""
            lines.append(f"- {name} [{aid}]{primary}: {role}")
    else:
        lines.append("- No invited agents yet.")
    lines.extend(["", "## Assumptions", "- Project info fallback used due missing primary output.", ""])
    return "\n".join(lines).strip() + "\n"

def _build_setup_draft_instruction(
    *,
    template_content: str,
    transcript: List[Dict[str, Any]],
    workspace_root: str,
) -> str:
    checklist = _compact_setup_checklist(template_content)
    convo = _compact_setup_transcript(transcript)
    if not convo:
        convo = "USER: (empty conversation)"
    return (
        "Convert setup conversation into JSON for project creation.\n"
        "Token optimization mode is ON.\n"
        "Respond JSON object only (no markdown, no extra text).\n"
        "Required keys: title, brief, goal.\n"
        "Optional keys: target_users, constraints, in_scope, out_of_scope, milestones, required_stack, first_output, suggested_agents.\n"
        "For suggested_agents use array of objects: [{\"name\":\"...\",\"role\":\"...\"}].\n"
        "Infer missing details from context but keep required keys non-empty.\n"
        f"Workspace root: `{workspace_root}`.\n"
        "Checklist reference:\n"
        f"{checklist}\n\n"
        "CONVERSATION:\n"
        f"{convo}"
    )

def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    dec = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _ = dec.raw_decode(raw[i:])
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj
    return None

def _derive_workspace_tree(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    parsed = _extract_json_object(text)
    if parsed:
        tree = parsed.get("tree")
        if isinstance(tree, list):
            lines = [str(x).strip() for x in tree if str(x).strip()]
            if lines:
                return "\n".join(lines)[:MAX_WORKSPACE_TREE_CHARS]
        if isinstance(tree, str) and tree.strip():
            return tree.strip()[:MAX_WORKSPACE_TREE_CHARS]
        alt = parsed.get("workspace_tree")
        if isinstance(alt, str) and alt.strip():
            return alt.strip()[:MAX_WORKSPACE_TREE_CHARS]

    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    if len(lines) > 120:
        lines = lines[:120]
    return "\n".join(lines)[:MAX_WORKSPACE_TREE_CHARS]

def _upsert_connection_policy(
    connection_id: str,
    user_id: str,
    *,
    main_agent_id: Optional[str],
    main_agent_name: Optional[str],
    bootstrap_status: str,
    bootstrap_error: Optional[str],
    workspace_tree: Optional[str] = None,
    workspace_root: str = HIVEE_ROOT,
    templates_root: str = HIVEE_TEMPLATES_ROOT,
) -> None:
    conn = db()
    conn.execute(
        """
        INSERT INTO connection_policies (
            connection_id, user_id, main_agent_id, main_agent_name, workspace_root, templates_root, bootstrap_status, bootstrap_error, workspace_tree, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(connection_id) DO UPDATE SET
            user_id=excluded.user_id,
            main_agent_id=excluded.main_agent_id,
            main_agent_name=excluded.main_agent_name,
            workspace_root=excluded.workspace_root,
            templates_root=excluded.templates_root,
            bootstrap_status=excluded.bootstrap_status,
            bootstrap_error=excluded.bootstrap_error,
            workspace_tree=excluded.workspace_tree,
            updated_at=excluded.updated_at
        """,
        (
            connection_id,
            user_id,
            main_agent_id,
            main_agent_name,
            workspace_root,
            templates_root,
            bootstrap_status,
            bootstrap_error,
            workspace_tree,
            int(time.time()),
        ),
    )
    conn.commit()
    conn.close()

async def _bootstrap_connection_workspace(user_id: str, base_url: str, api_key: str) -> Dict[str, Any]:
    main_agent_id: Optional[str] = None
    main_agent_name: Optional[str] = None
    probe = await openclaw_list_agents(base_url, api_key)
    if probe.get("ok"):
        picked = _pick_main_agent(probe.get("agents") or [])
        if picked:
            picked_id = str(picked.get("id") or "").strip()
            main_agent_id = picked_id or None
            main_agent_name = str(picked.get("name") or main_agent_id)

    try:
        workspace = _ensure_user_workspace(user_id)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to provision server workspace: {str(e)}",
            "main_agent_id": main_agent_id,
            "main_agent_name": main_agent_name,
            "agent_probe": probe,
        }

    return {
        "ok": True,
        "main_agent_id": main_agent_id,
        "main_agent_name": main_agent_name,
        "agent_probe": probe,
        "workspace_tree": workspace["workspace_tree"],
        "workspace_root": workspace["workspace_root"],
        "templates_root": workspace["templates_root"],
        "projects_root": workspace["projects_root"],
        "template_warnings": workspace.get("template_warnings") or [],
    }

async def try_get_json(
    client: httpx.AsyncClient, url: str
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    try:
        r = await client.get(url, timeout=10)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text[:2000]
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return True, r.json(), r.status_code, None
        return True, {"raw": r.text[:2000]}, r.status_code, None
    except Exception as e:
        return False, None, None, str(e)

def _is_openclaw_login_html(payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False
    raw = payload.get("raw")
    if not isinstance(raw, str):
        return False
    marker = raw.lower()
    return ("welcome to openclaw" in marker) and ('action="/login"' in marker or "gateway token" in marker)

def _response_looks_like_login_html(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    text = (resp.text or "").lower()
    return ("welcome to openclaw" in text) and ('action="/login"' in text or "gateway token" in text)

def _safe_json_response(resp: httpx.Response) -> Tuple[Optional[Any], Optional[str]]:
    text = (resp.text or "").strip()
    if not text:
        return None, None
    try:
        return resp.json(), None
    except Exception as e:
        return None, str(e)

def _extract_agents_list(data: Any) -> Optional[List[Any]]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None

    for key in ["agents", "subagents", "list", "data", "items", "results", "models"]:
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            # Support map-style config payloads, e.g. {"agents": {"main": {...}, "qa": {...}}}
            if key in {"agents", "subagents"} and value:
                if all(isinstance(v, dict) for v in value.values()):
                    mapped: List[Dict[str, Any]] = []
                    for map_key, map_val in value.items():
                        row = dict(map_val)
                        row.setdefault("id", str(map_key))
                        row.setdefault("name", row.get("id") or str(map_key))
                        mapped.append(row)
                    return mapped
            for nested_key in ["agents", "subagents", "list", "items", "results", "models", "data"]:
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return nested_value

    if any(k in data for k in ["id", "agent_id", "name", "slug", "model"]):
        return [data]
    return None

def _normalize_agents(agents: List[Any]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for a in agents:
        if isinstance(a, str):
            norm.append({"id": a, "name": a})
        elif isinstance(a, dict):
            aid = (
                a.get("id")
                or a.get("agent_id")
                or a.get("name")
                or a.get("slug")
                or a.get("model")
                or "unknown"
            )
            nm = a.get("name") or a.get("title") or a.get("label") or aid
            norm.append({"id": str(aid), "name": str(nm), "raw": a})
    return norm

async def _request_openclaw_with_auth(
    client: httpx.AsyncClient,
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    *,
    timeout: int = 15,
    json_body: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> httpx.Response:
    url = base_url.rstrip("/") + path
    headers = {"Authorization": f"Bearer {api_key}"}
    if extra_headers:
        headers.update(extra_headers)
    res = await client.request(
        method=method,
        url=url,
        headers=headers,
        json=json_body,
        timeout=timeout,
    )

    if res.status_code in (401, 403) or _response_looks_like_login_html(res):
        login = await client.post(base_url.rstrip("/") + "/login", data={"token": api_key}, timeout=timeout)
        if login.status_code < 400:
            res = await client.request(method=method, url=url, headers=headers, json=json_body, timeout=timeout)
    return res

async def openclaw_health(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for p in HEALTH_PATHS:
            r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=10)
            if r.status_code >= 400:
                continue
            ct = r.headers.get("content-type", "")
            if "application/json" in ct:
                payload = r.json()
            else:
                payload = {"raw": r.text[:2000]}
            status = r.status_code
            ok = True
            if ok:
                if _is_openclaw_login_html(payload):
                    return {
                        "ok": False,
                        "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                        "path": p,
                        "status": status,
                    }
                return {"ok": True, "path": p, "status": status, "payload": payload}
        return {
            "ok": False,
            "error": "Could not reach health endpoint on common paths. Check base_url/port/firewall/path prefix.",
        }

async def openclaw_list_agents(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err = None
        for p in AGENTS_PATHS:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=15)
                if _response_looks_like_login_html(r):
                    return {"ok": False, "error": "OpenClaw returned login page. Gateway token is invalid or missing.", "path": p}
                if r.status_code == 401:
                    return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                if r.status_code == 403:
                    return {"ok": False, "error": "Forbidden (403). Token/API key invalid or lacks permission.", "path": p}
                if r.status_code >= 400:
                    last_err = f"{r.status_code}: {r.text[:500]}"
                    continue

                data, parse_err = _safe_json_response(r)
                if data is None:
                    raw = (r.text or "").strip()
                    if not raw:
                        return {"ok": True, "path": p, "agents": []}
                    ctype = r.headers.get("content-type") or "unknown"
                    last_err = f"{p}: expected JSON but got {ctype}; body={raw[:300]}"
                    if parse_err:
                        last_err = f"{last_err}; parse_error={parse_err}"
                    continue

                agents = _extract_agents_list(data) or []
                norm = _normalize_agents(agents)
                return {"ok": True, "path": p, "agents": norm}
            except Exception as e:
                last_err = str(e)

        ws_res = await openclaw_ws_list_agents(base_url, api_key)
        if ws_res.get("ok"):
            return ws_res
        return {
            "ok": False,
            "error": f"Could not list agents on common paths. Last error: {last_err}",
            "ws_fallback_error": ws_res.get("error"),
            "ws_fallback_details": ws_res.get("details"),
            "hint": "This OpenClaw likely does not expose REST JSON agent listing on your base_url path. WS fallback was attempted.",
        }

def _extract_chat_text(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        return payload
    if not isinstance(payload, dict):
        return None

    # OpenAI-style chat completions
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict) and isinstance(msg.get("content"), str):
            return msg["content"]

    # OpenAI responses-style
    output = payload.get("output")
    if isinstance(output, list):
        chunks: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, list):
                for c in content:
                    if isinstance(c, dict) and isinstance(c.get("text"), str):
                        chunks.append(c["text"])
        if chunks:
            return "\n".join(chunks)

    # Generic fallback fields
    for key in ["text", "message", "response", "answer", "content"]:
        val = payload.get(key)
        if isinstance(val, str):
            return val
    return None

def _is_credit_or_max_token_error(detail: Any) -> bool:
    low = detail_to_text(detail).lower()
    if not low:
        return False
    markers = [
        "requires more credits",
        "fewer max_tokens",
        "requested up to",
        "can only afford",
        "insufficient credits",
        "max_tokens",
        "monthly limit",
    ]
    return any(m in low for m in markers)

async def openclaw_chat(
    base_url: str,
    api_key: str,
    message: str,
    agent_id: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    cap = _to_int(max_output_tokens) if max_output_tokens is not None else 0
    if cap <= 0:
        cap = 0
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err = None
        saw_405 = False
        for p in CHAT_PATHS:
            model_hint = f"openclaw:{agent_id}" if agent_id else "openclaw"
            extra_headers: Dict[str, str] = {}
            if agent_id:
                extra_headers["x-openclaw-agent-id"] = agent_id
            if p.endswith("/responses"):
                body: Dict[str, Any] = {"model": model_hint, "input": message}
                if agent_id:
                    body["agent_id"] = agent_id
                if cap > 0:
                    body["max_output_tokens"] = cap
                    # Compatibility fallback for providers/gateways expecting chat-completions naming.
                    body["max_tokens"] = cap
            elif "chat/completions" in p:
                body = {
                    "model": model_hint,
                    "messages": [{"role": "user", "content": message}],
                }
                if cap > 0:
                    body["max_tokens"] = cap
            else:
                body = {"model": model_hint, "message": message, "prompt": message, "input": message}
                if agent_id:
                    body["agent_id"] = agent_id
                if cap > 0:
                    body["max_output_tokens"] = cap
                    body["max_tokens"] = cap

            try:
                r = await _request_openclaw_with_auth(
                    client,
                    "POST",
                    base_url,
                    p,
                    api_key,
                    json_body=body,
                    timeout=30,
                    extra_headers=extra_headers,
                )
                if _response_looks_like_login_html(r):
                    return {"ok": False, "error": "OpenClaw returned login page. Gateway token is invalid or missing.", "path": p}
                if r.status_code == 401:
                    return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                if r.status_code == 405:
                    saw_405 = True
                if r.status_code >= 400:
                    last_err = f"{p}: {r.status_code} {r.text[:300]}"
                    continue

                ctype = r.headers.get("content-type", "")
                if "application/json" in ctype:
                    data: Any = r.json()
                else:
                    data = {"raw": r.text[:4000]}
                return {"ok": True, "path": p, "response": data, "text": _extract_chat_text(data)}
            except Exception as e:
                last_err = f"{p}: {str(e)}"

    if saw_405:
        return {
            "ok": False,
            "error": "Chat endpoint returned 405 Method Not Allowed. On OpenClaw, enable gateway.http.endpoints.chatCompletions.enabled=true (or use WS gateway protocol).",
            "hint": "OpenClaw docs: OpenAI Chat Completions endpoint is disabled by default.",
        }
    return {
        "ok": False,
        "error": f"Could not call chat endpoint on common paths. Last error: {last_err}",
        "hint": "Your OpenClaw may use different chat path(s). Update CHAT_PATHS in main.py.",
    }

def _as_ws_base(base_url: str) -> str:
    parsed = urlparse(base_url.rstrip("/"))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = parsed.path.rstrip("/")
    return urlunparse((scheme, parsed.netloc, path or "", "", "", ""))

def _candidate_ws_urls(base_url: str) -> List[str]:
    ws_base = _as_ws_base(base_url)
    parsed = urlparse(ws_base)
    base_path = parsed.path.rstrip("/")
    candidate_paths = [
        base_path or "",
        (base_path + "/ws") if base_path else "/ws",
        (base_path + "/gateway/ws") if base_path else "/gateway/ws",
        (base_path + "/__openclaw__/ws") if base_path else "/__openclaw__/ws",
    ]
    seen: set[str] = set()
    urls: List[str] = []
    for p in candidate_paths:
        path = p or "/"
        url = urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls

def _gateway_origin(base_url: str) -> str:
    parsed = urlparse(base_url.rstrip("/"))
    scheme = "https" if parsed.scheme == "https" else "http"
    return urlunparse((scheme, parsed.netloc, "", "", "", ""))

def _collect_text_fields(node: Any, out: List[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            lk = key.lower()
            if lk in {"text", "content", "delta", "response", "answer"} and isinstance(value, str):
                text = value.strip()
                if text:
                    out.append(text)
            elif isinstance(value, (dict, list)):
                _collect_text_fields(value, out)
    elif isinstance(node, list):
        for item in node:
            _collect_text_fields(item, out)

def _join_delta_chunks(chunks: List[str]) -> str:
    out = ""
    no_space_before = {".", ",", "!", "?", ";", ":", ")", "]", "}", "%"}
    no_space_after_prev = {"(", "[", "{", "/", "-", "\n"}
    contractions = {"'s", "'re", "'ve", "'m", "'ll", "'d", "n't"}
    for raw in chunks:
        part = raw.strip()
        if not part:
            continue
        if not out:
            out = part
            continue
        if part in no_space_before or part in contractions or part.startswith("'"):
            out += part
            continue
        if out.endswith(tuple(no_space_after_prev)):
            out += part
            continue
        out += " " + part
    return out.strip()

def _derive_ws_session_key(session_key: str, agent_id: Optional[str]) -> str:
    base = (session_key or "main").strip() or "main"
    aid = (agent_id or "").strip()
    if not aid:
        return base
    if base.startswith("agent:"):
        return base
    return f"agent:{aid}:{base}"

async def openclaw_ws_chat(
    base_url: str,
    api_key: str,
    message: str,
    agent_id: Optional[str] = None,
    session_key: str = "main",
    timeout_sec: int = 25,
) -> Dict[str, Any]:
    try:
        import websockets
    except Exception:
        return {
            "ok": False,
            "error": "Python package 'websockets' is missing. Install with: pip install websockets",
        }

    async def _to_json(raw: Any) -> Optional[Dict[str, Any]]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    ws_urls = _candidate_ws_urls(base_url)
    ws_origin = _gateway_origin(base_url)
    errors: List[str] = []

    async def _retry_http_on_budget_error(reason: Any, ws_path: str) -> Optional[Dict[str, Any]]:
        if not _is_credit_or_max_token_error(reason):
            return None
        http_res = await openclaw_chat(
            base_url=base_url,
            api_key=api_key,
            message=message,
            agent_id=agent_id,
            max_output_tokens=SAFE_PROVIDER_MAX_OUTPUT_TOKENS,
        )
        if http_res.get("ok"):
            return {
                "ok": True,
                "transport": "http-fallback",
                "path": str(http_res.get("path") or ws_path),
                "text": http_res.get("text"),
                "response": http_res.get("response"),
                "fallback_reason": detail_to_text(reason)[:1000],
                "max_output_tokens": SAFE_PROVIDER_MAX_OUTPUT_TOKENS,
            }
        return {
            "ok": False,
            "path": ws_path,
            "error": detail_to_text(reason)[:1500],
            "details": http_res.get("error") or http_res.get("details"),
            "hint": (
                f"Retry via HTTP with max_output_tokens={SAFE_PROVIDER_MAX_OUTPUT_TOKENS} failed. "
                "Check key budget caps on provider and OpenRouter key settings."
            ),
        }

    async with httpx.AsyncClient(follow_redirects=True) as http:
        try:
            await http.post(base_url.rstrip("/") + "/login", data={"token": api_key}, timeout=10)
        except Exception:
            pass

        cookie = "; ".join([f"{k}={v}" for k, v in http.cookies.items()]) if http.cookies else ""
        extra_headers: Dict[str, str] = {"User-Agent": "hivee/0.1.0"}
        if cookie:
            extra_headers["Cookie"] = cookie

    for ws_url in ws_urls:
        frames: List[Dict[str, Any]] = []
        text_best = ""
        delta_parts: List[str] = []
        runtime_error: Optional[str] = None
        deadline = time.time() + max(8, min(timeout_sec, 90))
        connect_id = f"connect_{uuid.uuid4().hex[:10]}"
        chat_id = f"chat_{uuid.uuid4().hex[:10]}"
        connect_payload = {
            "type": "req",
            "id": connect_id,
            "method": "connect",
            "params": {
                "minProtocol": 3,
                "maxProtocol": 3,
                # Match OpenClaw webchat client schema expected by gateway
                "client": {
                    "id": "openclaw-control-ui",
                    "version": "vdev",
                    "platform": "web",
                    "mode": "webchat",
                },
                "auth": {"token": api_key},
                "role": "operator",
                "scopes": ["operator.read", "operator.write"],
                "caps": [],
                "commands": [],
                "permissions": {},
                "locale": "en-US",
                "userAgent": "hivee/0.1.0",
            },
        }
        routed_session_key = _derive_ws_session_key(session_key=session_key, agent_id=agent_id)
        chat_params: Dict[str, Any] = {
            "sessionKey": routed_session_key,
            "message": message,
            "idempotencyKey": uuid.uuid4().hex,
            "timeoutMs": 120000,
        }
        # Keep WS payload strict to protocol schema; route by session key.
        chat_payload = {
            "type": "req",
            "id": chat_id,
            "method": "chat.send",
            "params": chat_params,
        }

        try:
            async with websockets.connect(
                ws_url,
                open_timeout=12,
                max_size=4 * 1024 * 1024,
                origin=ws_origin,
                extra_headers=extra_headers,
            ) as ws:
                # Some deployments emit connect.challenge first; capture if present.
                try:
                    peek = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    msg = await _to_json(peek)
                    if msg:
                        frames.append(msg)
                except asyncio.TimeoutError:
                    pass

                await ws.send(json.dumps(connect_payload))
                connected = False
                while time.time() < deadline:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.time()))
                    payload = await _to_json(raw)
                    if not payload:
                        continue
                    frames.append(payload)
                    if payload.get("type") == "res" and payload.get("id") == connect_id:
                        if payload.get("ok") is False or payload.get("error"):
                            return {
                                "ok": False,
                                "path": ws_url,
                                "error": f"WS connect rejected: {payload.get('error') or payload}",
                            }
                        connected = True
                        break
                    if payload.get("type") == "err" and payload.get("id") == connect_id:
                        return {"ok": False, "path": ws_url, "error": f"WS connect error: {payload}"}
                if not connected:
                    errors.append(f"{ws_url}: no connect ack")
                    continue

                await ws.send(json.dumps(chat_payload))
                accepted = False
                first_text_at: Optional[float] = None
                idle_grace_sec = 2.0
                while time.time() < deadline:
                    wait_for = min(2.0, max(0.1, deadline - time.time()))
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=wait_for)
                    except asyncio.TimeoutError:
                        if first_text_at and (time.time() - first_text_at) >= idle_grace_sec:
                            break
                        continue
                    payload = await _to_json(raw)
                    if not payload:
                        continue
                    frames.append(payload)

                    if payload.get("type") == "res" and payload.get("id") == chat_id:
                        if payload.get("ok") is False or payload.get("error"):
                            retry = await _retry_http_on_budget_error(payload.get("error") or payload, ws_url)
                            if retry:
                                return retry
                            return {"ok": False, "path": ws_url, "error": f"chat.send rejected: {payload.get('error') or payload}"}
                        accepted = True
                    if payload.get("type") == "err" and payload.get("id") == chat_id:
                        retry = await _retry_http_on_budget_error(payload, ws_url)
                        if retry:
                            return retry
                        return {"ok": False, "path": ws_url, "error": f"chat.send error: {payload}"}

                    # Detect runtime failures emitted as events so UI gets a concise error.
                    if payload.get("type") == "event":
                        ev = payload.get("event")
                        evp = payload.get("payload")
                        if ev == "chat" and isinstance(evp, dict) and evp.get("state") == "error":
                            runtime_error = str(evp.get("errorMessage") or evp.get("error") or "Chat failed")
                        elif ev == "agent" and isinstance(evp, dict):
                            data = evp.get("data")
                            if isinstance(data, dict) and data.get("phase") == "error":
                                runtime_error = str(data.get("error") or runtime_error or "Agent failed")

                    picks: List[str] = []
                    _collect_text_fields(payload, picks)
                    for p in picks:
                        candidate = p.strip()
                        if not candidate:
                            continue
                        looks_like_sentence = (" " in candidate) or ("\n" in candidate) or len(candidate) >= 24
                        if looks_like_sentence:
                            if (
                                not text_best
                                or candidate.startswith(text_best)
                                or len(candidate) > (len(text_best) + 6)
                            ):
                                text_best = candidate
                                first_text_at = time.time()
                                continue
                        if not delta_parts or delta_parts[-1] != candidate:
                            delta_parts.append(candidate)
                            first_text_at = time.time()

                text = text_best or _join_delta_chunks(delta_parts) or None
                if runtime_error:
                    retry = await _retry_http_on_budget_error(runtime_error, ws_url)
                    if retry:
                        return retry
                    return {
                        "ok": False,
                        "path": ws_url,
                        "error": runtime_error[:1500],
                        "accepted": accepted,
                        "frames": frames[-8:],
                    }
                return {
                    "ok": True,
                    "transport": "ws",
                    "path": ws_url,
                    "accepted": accepted,
                    "text": text,
                    "frames": frames[-12:],
                }
        except Exception as e:
            errors.append(f"{ws_url}: {str(e)}")
            continue

    if errors and _is_credit_or_max_token_error(errors[-1]):
        retry = await _retry_http_on_budget_error(errors[-1], ws_urls[-1] if ws_urls else "ws")
        if retry:
            return retry

    return {
        "ok": False,
        "error": "WS chat failed across all candidate WS paths.",
        "details": errors[-5:],
        "hint": "OpenClaw may require device identity/pairing or a specific WS path behind the provider proxy.",
    }

async def openclaw_ws_list_agents(base_url: str, api_key: str, timeout_sec: int = 12) -> Dict[str, Any]:
    try:
        import websockets
    except Exception:
        return {"ok": False, "error": "Python package 'websockets' is missing.", "details": ["Install with: pip install websockets"]}

    async def _to_json(raw: Any) -> Optional[Dict[str, Any]]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _extract_from_ws_result(result: Any) -> List[Any]:
        direct = _extract_agents_list(result)
        if direct is not None:
            return direct
        if isinstance(result, dict):
            value = result.get("value")
            nested = _extract_agents_list(value)
            if nested is not None:
                return nested
            if isinstance(value, list):
                return value
        if isinstance(result, list):
            return result
        return []

    ws_urls = _candidate_ws_urls(base_url)
    ws_origin = _gateway_origin(base_url)
    errors: List[str] = []

    async with httpx.AsyncClient(follow_redirects=True) as http:
        try:
            await http.post(base_url.rstrip("/") + "/login", data={"token": api_key}, timeout=10)
        except Exception:
            pass
        cookie = "; ".join([f"{k}={v}" for k, v in http.cookies.items()]) if http.cookies else ""
        extra_headers: Dict[str, str] = {"User-Agent": "hivee/0.1.0"}
        if cookie:
            extra_headers["Cookie"] = cookie

    ws_methods: List[Tuple[str, Dict[str, Any], bool]] = [
        ("agents.list", {}, True),
        ("config.get", {"path": "agents.list"}, True),
        ("config.get", {"path": "agents"}, True),
        ("config.get", {"path": "subagents"}, True),
        ("config.get", {"path": "gateway.agents"}, True),
        ("models.list", {}, False),
    ]
    saw_models_only = False

    for ws_url in ws_urls:
        try:
            async with websockets.connect(
                ws_url,
                open_timeout=12,
                max_size=4 * 1024 * 1024,
                origin=ws_origin,
                extra_headers=extra_headers,
            ) as ws:
                connect_id = f"connect_{uuid.uuid4().hex[:10]}"
                connect_payload = {
                    "type": "req",
                    "id": connect_id,
                    "method": "connect",
                    "params": {
                        "minProtocol": 3,
                        "maxProtocol": 3,
                        "client": {"id": "openclaw-control-ui", "version": "vdev", "platform": "web", "mode": "webchat"},
                        "auth": {"token": api_key},
                        "role": "operator",
                        "scopes": ["operator.read", "operator.write"],
                        "caps": [],
                        "commands": [],
                        "permissions": {},
                        "locale": "en-US",
                        "userAgent": "hivee/0.1.0",
                    },
                }

                try:
                    peek = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    _ = await _to_json(peek)
                except asyncio.TimeoutError:
                    pass

                await ws.send(json.dumps(connect_payload))
                deadline = time.time() + max(6, min(timeout_sec, 30))
                connected = False
                while time.time() < deadline:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.time()))
                    msg = await _to_json(raw)
                    if not msg:
                        continue
                    if msg.get("type") == "res" and msg.get("id") == connect_id:
                        if msg.get("ok") is False or msg.get("error"):
                            errors.append(f"{ws_url}: connect rejected: {msg.get('error') or msg}")
                            break
                        connected = True
                        break
                    if msg.get("type") == "err" and msg.get("id") == connect_id:
                        errors.append(f"{ws_url}: connect error: {msg}")
                        break
                if not connected:
                    continue

                for method, params, is_agent_method in ws_methods:
                    req_id = f"req_{uuid.uuid4().hex[:10]}"
                    await ws.send(json.dumps({"type": "req", "id": req_id, "method": method, "params": params}))
                    m_deadline = time.time() + max(4, min(timeout_sec, 20))
                    while time.time() < m_deadline:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, m_deadline - time.time()))
                        msg = await _to_json(raw)
                        if not msg:
                            continue
                        if msg.get("type") == "res" and msg.get("id") == req_id:
                            if msg.get("ok") is False or msg.get("error"):
                                errors.append(f"{ws_url} {method}: rejected: {msg.get('error') or msg}")
                                break
                            result = msg.get("result")
                            if result is None:
                                result = msg.get("payload")
                            raw_agents = _extract_from_ws_result(result)
                            norm = _normalize_agents(raw_agents)
                            if not is_agent_method:
                                if norm:
                                    saw_models_only = True
                                break
                            return {"ok": True, "transport": "ws", "path": ws_url, "method": method, "agents": norm}
                        if msg.get("type") == "err" and msg.get("id") == req_id:
                            errors.append(f"{ws_url} {method}: error: {msg}")
                            break
        except Exception as e:
            errors.append(f"{ws_url}: {str(e)}")
            continue

    return {
        "ok": False,
        "error": "WS agent listing failed across all candidate WS paths/methods.",
        "details": errors[-8:],
        "hint": "If only models.list is available, this gateway may expose model registry but not sub-agent registry on current credentials/path.",
        "models_detected": saw_models_only,
    }

async def _ensure_project_info_document(project_id: str, *, force: bool = False) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               c.base_url, c.api_key, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Project not found"}
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()
    if not role_rows:
        return {"ok": False, "error": "No invited agents configured"}

    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    if not primary_agent_id:
        return {"ok": False, "error": "Primary agent is not configured"}

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
    except Exception as e:
        return {"ok": False, "error": detail_to_text(e)[:300]}

    _initialize_project_folder(
        project_dir,
        str(row["title"] or ""),
        str(row["brief"] or ""),
        str(row["goal"] or ""),
        setup_details=setup_details,
    )
    info_path = project_dir / PROJECT_INFO_FILE
    existing_info = ""
    if info_path.exists():
        try:
            existing_info = info_path.read_text(encoding="utf-8")
        except Exception:
            existing_info = ""
    if (
        existing_info.strip()
        and (not force)
        and "pending primary agent completion" not in existing_info.lower()
        and len(existing_info.strip()) >= 160
    ):
        return {"ok": True, "text": existing_info.strip(), "source": "existing", "agent_id": primary_agent_id}

    context = _project_context_instruction(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        plan_status=PLAN_STATUS_PENDING,
    )
    roster = _agent_roster_markdown(role_rows)
    task = (
        f"{context}\n\n"
        f"{roster}\n\n"
        "Task:\n"
        f"1) Read `{SETUP_CHAT_HISTORY_FILE}` and `agents/ROLES.md`.\n"
        f"2) Write or replace `{PROJECT_INFO_FILE}` with complete project context.\n"
        "3) Include: project summary, user requirements, constraints, assumptions, role responsibilities, execution prerequisites, and open questions.\n"
        "4) If some information is missing, make reasonable assumptions and clearly mark them under `Assumptions`.\n"
        "5) Return JSON only with `chat_update`, `output_files`, optional `notes`, and pause fields.\n"
        "6) Keep language concise and human-readable.\n"
    )
    info_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            "agents/ROLES.md",
            OVERVIEW_FILE,
            PROJECT_SETUP_FILE,
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=str(setup_details.get("setup_chat_summary") or ""),
        max_total_chars=8_500,
        max_files=8,
    )
    if info_context:
        task = f"{task}\n\n{info_context}"

    await emit(project_id, "project.info.generating", {"project_id": project_id})
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=str(row["api_key"]),
        message=task,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:project-info",
        timeout_sec=55,
    )
    p_tokens, c_tokens, _ = _extract_usage_counts(res)
    if p_tokens <= 0:
        p_tokens = _estimate_tokens_from_text(task)
    if c_tokens <= 0:
        c_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=p_tokens, completion_tokens=c_tokens)

    if not res.get("ok"):
        fallback = _python_project_info_markdown(
            title=str(row["title"] or ""),
            brief=str(row["brief"] or ""),
            goal=str(row["goal"] or ""),
            setup_details=setup_details,
            role_rows=role_rows,
        )
        try:
            info_path.write_text(fallback, encoding="utf-8")
        except Exception:
            return {"ok": False, "error": detail_to_text(res.get("error") or res.get("details"))[:1200]}
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="project.info.fallback",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.info.ready", {"status": "fallback", "preview": fallback[:900]})
        return {"ok": True, "text": fallback, "source": "fallback", "agent_id": primary_agent_id}

    raw_text = str(res.get("text") or "").strip()
    parsed = _extract_agent_report_payload(raw_text)
    writes = parsed.get("output_files") if isinstance(parsed.get("output_files"), list) else []
    has_info_write = False
    for item in writes:
        rel = _clean_relative_project_path(str(item.get("path") or ""))
        if rel and rel.lower() in {PROJECT_INFO_FILE.lower(), "project-info.md"}:
            has_info_write = True
            break
    if not has_info_write:
        fallback_content = raw_text.strip()
        if not fallback_content:
            fallback_content = _python_project_info_markdown(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
            )
        elif not fallback_content.lstrip().startswith("#"):
            fallback_content = (
                _seed_project_info_markdown(
                    title=str(row["title"] or ""),
                    brief=str(row["brief"] or ""),
                    goal=str(row["goal"] or ""),
                ).strip()
                + "\n\n## Primary Agent Notes\n"
                + fallback_content
            )
        writes = [{"path": PROJECT_INFO_FILE, "content": fallback_content, "append": False}, *writes]
    write_result = _apply_project_file_writes(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        writes=writes,
        default_prefix=f"{USER_OUTPUTS_DIRNAME}/project-info",
    )
    saved = write_result.get("saved") or []
    if not any(str(item.get("path") or "").strip().lower() == PROJECT_INFO_FILE.lower() for item in saved):
        info_path.write_text(
            _python_project_info_markdown(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
            ),
            encoding="utf-8",
        )
    try:
        text = info_path.read_text(encoding="utf-8").strip()
    except Exception:
        text = ""
    if not text:
        text = _python_project_info_markdown(
            title=str(row["title"] or ""),
            brief=str(row["brief"] or ""),
            goal=str(row["goal"] or ""),
            setup_details=setup_details,
            role_rows=role_rows,
        )
        info_path.write_text(text, encoding="utf-8")
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="project.info.ready",
        text=(parsed.get("chat_update") or raw_text or "Project info updated.")[:1500],
        payload={"saved_files": saved[:12]},
    )
    await emit(
        project_id,
        "project.info.ready",
        {"status": "ok", "agent_id": primary_agent_id, "preview": text[:900], "saved_files": saved[:12]},
    )
    return {"ok": True, "text": text, "source": "agent", "agent_id": primary_agent_id}

async def _generate_project_plan(project_id: str, *, force: bool = False) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id, p.plan_status,
               c.base_url, c.api_key, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    if (not force) and _coerce_plan_status(row["plan_status"]) == PLAN_STATUS_APPROVED:
        conn.close()
        return

    role_rows = _project_agent_rows(conn, project_id)
    if not role_rows:
        now = int(time.time())
        msg = "Invite at least one project agent (and select a primary) before generating plan."
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, msg, now, project_id),
        )
        conn.commit()
        conn.close()
        _refresh_project_documents(project_id)
        await emit(project_id, "project.plan.failed", {"error": msg})
        return
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_GENERATING, int(time.time()), project_id),
    )
    conn.commit()
    conn.close()
    await emit(project_id, "project.plan.generating", {"project_id": project_id})
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.generating",
        text="Primary agent is generating project plan.",
    )

    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None

    info_result = await _ensure_project_info_document(project_id, force=force)
    project_info_excerpt = str(info_result.get("text") or "").strip()[:10_000]
    instruction = _plan_prompt_from_project(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        project_info_excerpt=project_info_excerpt,
    )
    plan_file_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            OVERVIEW_FILE,
            PROJECT_SETUP_FILE,
            "agents/ROLES.md",
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=f"{str(row['brief'] or '')}\n{str(row['goal'] or '')}",
        max_total_chars=7_000,
        max_files=8,
    )
    if plan_file_context:
        instruction = f"{instruction}\n\n{plan_file_context}"
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=str(row["api_key"]),
        message=instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:plan",
        timeout_sec=55,
    )
    prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
    if prompt_tokens <= 0:
        prompt_tokens = _estimate_tokens_from_text(instruction)
    if completion_tokens <= 0:
        completion_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)

    now = int(time.time())
    conn = db()
    if not res.get("ok"):
        error_text = detail_to_text(res.get("error") or res.get("details") or "Failed to generate project plan")
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, error_text[:5000], now, project_id),
        )
        conn.commit()
        conn.close()
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.failed",
            text=error_text[:1200],
        )
        await emit(project_id, "project.plan.failed", {"error": error_text[:1200]})
        return

    plan_text = str(res.get("text") or "").strip()
    if not plan_text:
        plan_text = detail_to_text(res.get("frames") or "Plan generated with empty text")
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_AWAITING_APPROVAL, plan_text[:20000], now, project_id),
    )
    conn.commit()
    conn.close()
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.ready",
        text=(plan_text or "")[:1600],
    )
    await emit(project_id, "project.plan.ready", {"status": PLAN_STATUS_AWAITING_APPROVAL, "preview": plan_text[:1000]})

async def _delegate_project_tasks(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, c.base_url, c.api_key, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()

    if _coerce_plan_status(row["plan_status"]) != PLAN_STATUS_APPROVED:
        await emit(project_id, "project.delegation.skipped", {"reason": "Plan not approved"})
        return
    if not role_rows:
        _set_project_execution_state(project_id, status=EXEC_STATUS_IDLE, progress_pct=0)
        _refresh_project_documents(project_id)
        await emit(project_id, "project.delegation.skipped", {"reason": "No invited agents yet"})
        return

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=15)
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="delegation.started",
        text="Primary agent started delegation planning after plan approval.",
        payload={"agents": [str(r.get("agent_id") or "") for r in role_rows]},
    )
    await emit(project_id, "project.delegation.started", {"agents": [r.get("agent_id") for r in role_rows]})
    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))
    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None

    info_result = await _ensure_project_info_document(project_id, force=False)
    project_info_excerpt = str(info_result.get("text") or "").strip()[:10_000]
    instruction = _delegate_prompt_from_project(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        plan_text=str(row["plan_text"] or ""),
        project_info_excerpt=project_info_excerpt,
    )
    delegate_file_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            OVERVIEW_FILE,
            PROJECT_PLAN_FILE,
            PROJECT_SETUP_FILE,
            "agents/ROLES.md",
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=str(row["plan_text"] or ""),
        max_total_chars=8_000,
        max_files=8,
    )
    if delegate_file_context:
        instruction = f"{instruction}\n\n{delegate_file_context}"
    res = await openclaw_ws_chat(
        base_url=str(row["base_url"]),
        api_key=str(row["api_key"]),
        message=instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:delegate",
        timeout_sec=55,
    )
    prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
    if prompt_tokens <= 0:
        prompt_tokens = _estimate_tokens_from_text(instruction)
    if completion_tokens <= 0:
        completion_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)

    if not res.get("ok"):
        _set_project_execution_state(project_id, status=EXEC_STATUS_STOPPED)
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="delegation.failed",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.delegation.failed", {"error": detail_to_text(res.get("error") or res.get("details"))[:1200]})
        return

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=55)
    primary_reply = str(res.get("text") or "").strip()
    payload = _parse_delegation_payload(primary_reply)
    by_id = {str(r.get("agent_id") or "").strip(): r for r in role_rows}
    project_md = str(payload.get("project_delegation_md") or payload.get("project_md") or "").strip()
    if not project_md:
        project_md = str(row["plan_text"] or "").strip() or "Delegation initialized."
    if primary_reply:
        await emit(
            project_id,
            "agent.primary.update",
            {
                "agent_id": primary_agent_id,
                "agent_name": next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(primary_agent_id or "")), ""),
                "text": primary_reply[:1200],
            },
        )
    for note in _summarize_ws_frames(res.get("frames"), limit=10):
        await emit(project_id, "agent.primary.live", {"agent_id": primary_agent_id, "note": note})
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="agent.primary.update",
        text=primary_reply[:1800] if primary_reply else "Primary agent returned delegation payload.",
    )

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
    except Exception:
        await emit(project_id, "project.delegation.failed", {"error": "Project directory unavailable"})
        return
    project_dir.mkdir(parents=True, exist_ok=True)
    agents_dir = project_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)

    (project_dir / PROJECT_DELEGATION_FILE).write_text(project_md + "\n", encoding="utf-8")
    legacy_delegation = (project_dir / "project-delegation.md").resolve()
    if _path_within(legacy_delegation, project_dir) and legacy_delegation.exists():
        try:
            legacy_delegation.unlink()
        except Exception:
            pass
    assigned_count = 0
    raw_tasks = payload.get("agent_tasks")
    task_map: Dict[str, str] = {}
    if isinstance(raw_tasks, list):
        for item in raw_tasks:
            if not isinstance(item, dict):
                continue
            aid = str(item.get("agent_id") or "").strip()
            task_md = str(item.get("task_md") or "").strip()
            if aid and task_md and aid in by_id:
                task_map[aid] = task_md

    agent_order = list(by_id.keys())
    assigned_task_map: Dict[str, str] = {}
    assigned_mentions_map: Dict[str, List[str]] = {}
    for pos, aid in enumerate(agent_order):
        row_item = by_id.get(aid) or {}
        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        default_task = (
            f"Read {PROJECT_INFO_FILE}, {OVERVIEW_FILE}, {PROJECT_PLAN_FILE}, and {PROJECT_DELEGATION_FILE}, then execute assigned scope and report progress in chat.\n"
            f"- Follow dependency order from {PROJECT_DELEGATION_FILE}.\n"
            "- If your output unblocks another agent, mention them explicitly as @agent_id in chat_update so handoff happens in chat.\n"
            "- Save concrete artifacts into project files using output_files.\n"
            "- If blocked by missing user approval/input (credentials, API key, sign-off, pit stop), set requires_user_input=true with pause_reason and resume_hint.\n"
            "- If user answers SKIP, decide assumptions responsibly and continue.\n"
        )
        next_aid = agent_order[pos + 1] if (pos + 1) < len(agent_order) else None
        task_text = _normalize_task_markdown_for_agent(
            agent_id=aid,
            role=role,
            task_md=task_map.get(aid, default_task),
            next_agent_id=next_aid,
        )
        assigned_task_map[aid] = task_text
        fname = _safe_agent_filename(aid) + ".md"
        (agents_dir / fname).write_text(task_text.strip() + "\n", encoding="utf-8")
        assigned_count += 1
        mention_targets = sorted({m for m in re.findall(r"@([a-zA-Z0-9._-]+)", task_text) if m and m != aid})[:8]
        assigned_mentions_map[aid] = mention_targets
        await emit(
            project_id,
            "agent.task.assigned",
            {
                "agent_id": aid,
                "agent_name": str(row_item.get("agent_name") or aid),
                "role": role,
                "task_file": f"agents/{fname}",
                "task_preview": task_text[:500],
                "mentions": mention_targets,
            },
        )
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agent.task.assigned",
            text=f"{aid}: {task_text[:800]}",
            payload={"task_file": f"agents/{fname}", "mentions": mention_targets},
        )

    _write_project_agent_roles_file(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        agents=role_rows,
    )
    outputs_dir = project_dir / USER_OUTPUTS_DIRNAME
    outputs_dir.mkdir(parents=True, exist_ok=True)
    processed_agents = 0
    failed_agents = 0
    agent_total = max(1, len(agent_order))
    team_roster_text = _agent_roster_markdown(role_rows)
    primary_agent_name = next(
        (
            str(r.get("agent_name") or r.get("agent_id") or "")
            for r in role_rows
            if str(r.get("agent_id") or "").strip() == str(primary_agent_id or "").strip()
        ),
        str(primary_agent_id or "primary"),
    )
    primary_last_chat_update = ""
    primary_last_notes = ""
    primary_last_pause_reason = ""
    primary_last_resume_hint = ""
    primary_pause_resolved = False

    for idx, aid in enumerate(agent_order, start=1):
        row_item = by_id.get(aid) or {}
        while True:
            state, _ = _read_project_execution_state(project_id)
            if state == EXEC_STATUS_PAUSED:
                await asyncio.sleep(0.7)
                continue
            if state == EXEC_STATUS_STOPPED:
                _refresh_project_documents(project_id)
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="delegation.stopped",
                    text="Delegation run stopped by user before all agents reported.",
                )
                await emit(
                    project_id,
                    "project.delegation.stopped",
                    {"processed_agents": processed_agents, "failed_agents": failed_agents, "total_agents": len(agent_order)},
                )
                return
            break

        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        agent_name = str(row_item.get("agent_name") or aid)
        task_text = assigned_task_map.get(aid) or f"# Task for {aid}\n\nRole: {role}\n"
        task_rel = f"agents/{_safe_agent_filename(aid)}.md"
        agent_file_context = _build_project_file_context(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            include_paths=[
                task_rel,
                PROJECT_INFO_FILE,
                PROJECT_DELEGATION_FILE,
                PROJECT_PLAN_FILE,
                OVERVIEW_FILE,
                "agents/ROLES.md",
                SETUP_CHAT_HISTORY_FILE,
            ],
            request_text=task_text,
            max_total_chars=7_500,
            max_files=8,
        )
        await emit(
            project_id,
            "agent.task.started",
            {"agent_id": aid, "agent_name": agent_name, "role": role},
        )

        agent_instruction = (
            _project_context_instruction(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
                plan_status=PLAN_STATUS_APPROVED,
            )
            + "\n\n"
            + f"You are invited agent `{aid}` with role `{role}`.\n"
            + team_roster_text
            + "\n"
            + "Execute your assigned task and return JSON object only:\n"
            + "{\n"
            + "  \"chat_update\": \"Human-friendly update sentence to show in chat\",\n"
            + "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"file content\",\"append\":false}],\n"
            + "  \"notes\": \"optional technical notes\",\n"
            + "  \"requires_user_input\": false,\n"
            + "  \"pause_reason\": \"\",\n"
            + "  \"resume_hint\": \"\"\n"
            + "}\n"
            + "Rules:\n"
            + "- chat_update must read like normal conversation.\n"
            + "- Put every created/updated artifact in output_files.\n"
            + "- Use relative paths inside this project only.\n"
            + "- Use exact IDs from roster when mentioning other agents.\n"
            + "- Mention handoff needs in chat_update with @agent_id if needed.\n\n"
            + "- If blocked by user approval/input or planned pit stop, set requires_user_input=true and explain pause_reason.\n"
            + "- If user says SKIP for missing info, proceed with assumptions and state them briefly in chat_update.\n"
            + "Assigned task:\n"
            + task_text.strip()
        )
        if agent_file_context:
            agent_instruction = f"{agent_instruction}\n\n{agent_file_context}"
        agent_res = await openclaw_ws_chat(
            base_url=str(row["base_url"]),
            api_key=str(row["api_key"]),
            message=agent_instruction,
            agent_id=aid,
            session_key=f"{project_id}:agent:{aid}",
            timeout_sec=50,
        )
        p_tokens, c_tokens, _ = _extract_usage_counts(agent_res)
        if p_tokens <= 0:
            p_tokens = _estimate_tokens_from_text(agent_instruction)
        if c_tokens <= 0:
            c_tokens = _estimate_tokens_from_text(agent_res.get("text"))
        _update_project_usage_metrics(project_id, prompt_tokens=p_tokens, completion_tokens=c_tokens)

        if not agent_res.get("ok"):
            failed_agents += 1
            err_text = detail_to_text(agent_res.get("error") or agent_res.get("details") or "Agent task failed")[:1200]
            await emit(
                project_id,
                "agent.task.failed",
                {"agent_id": aid, "agent_name": agent_name, "error": err_text},
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.task.failed",
                text=f"{aid}: {err_text}",
            )
            continue

        report_text = str(agent_res.get("text") or "").strip()
        if not report_text:
            report_text = detail_to_text(agent_res.get("frames") or "No text response.")
        parsed_report = _extract_agent_report_payload(report_text)
        chat_update = str(parsed_report.get("chat_update") or "").strip() or "I have completed this task step."
        report_notes = str(parsed_report.get("notes") or "").strip()
        requires_user_input = bool(parsed_report.get("requires_user_input"))
        pause_reason = str(parsed_report.get("pause_reason") or "").strip()
        resume_hint = str(parsed_report.get("resume_hint") or "").strip()
        output_files_raw = parsed_report.get("output_files") or []
        write_result = _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files_raw if isinstance(output_files_raw, list) else [],
            default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
        )
        saved_files = write_result.get("saved") or []
        skipped_files = write_result.get("skipped") or []
        artifact_followup_used = False
        artifact_rescue_used = False
        artifact_like_task = _looks_like_artifact_request(task_text)
        if _should_request_artifact_followup(
            user_message=task_text,
            raw_response=report_text,
            parsed_payload=parsed_report,
            saved_files=saved_files,
        ):
            artifact_followup_used = True
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "No synced files detected yet. Requesting explicit output_files payload.",
                },
            )
            followup_prompt = _build_artifact_followup_prompt(
                user_message=task_text,
                previous_response=report_text,
            )
            followup_res = await openclaw_ws_chat(
                base_url=str(row["base_url"]),
                api_key=str(row["api_key"]),
                message=followup_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=45,
            )
            if followup_res.get("ok"):
                fp, fc, _ = _extract_usage_counts(followup_res)
                if fp <= 0:
                    fp = _estimate_tokens_from_text(followup_prompt)
                if fc <= 0:
                    fc = _estimate_tokens_from_text(followup_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=fp, completion_tokens=fc)
                followup_text = str(followup_res.get("text") or "").strip()
                parsed_followup = _extract_agent_report_payload(followup_text)
                followup_chat = str(parsed_followup.get("chat_update") or "").strip()
                followup_writes = parsed_followup.get("output_files") or []
                requires_user_input = requires_user_input or bool(parsed_followup.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_followup.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_followup.get("resume_hint") or "").strip()
                if not report_notes:
                    report_notes = str(parsed_followup.get("notes") or "").strip()
                followup_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=followup_writes if isinstance(followup_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                )
                followup_saved = followup_write_result.get("saved") or []
                followup_skipped = followup_write_result.get("skipped") or []
                if followup_saved:
                    saved_files.extend(followup_saved)
                if followup_skipped:
                    skipped_files.extend(followup_skipped)
                if followup_chat:
                    chat_update = followup_chat
                if followup_text:
                    report_text = (report_text + "\n\n[ARTIFACT FOLLOW-UP]\n" + followup_text).strip()
                for note in _summarize_ws_frames(followup_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_files.append(
                    "artifact follow-up failed: "
                    + detail_to_text(followup_res.get("error") or followup_res.get("details") or "unknown")
                )

        if not saved_files and not requires_user_input and artifact_like_task:
            artifact_rescue_used = True
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "Still no synced files. Forcing concrete deliverables payload.",
                },
            )
            rescue_prompt = _build_artifact_recovery_prompt(
                agent_id=aid,
                role=role,
                task_text=task_text,
                previous_response=report_text,
            )
            rescue_res = await openclaw_ws_chat(
                base_url=str(row["base_url"]),
                api_key=str(row["api_key"]),
                message=rescue_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=45,
            )
            if rescue_res.get("ok"):
                rp, rc, _ = _extract_usage_counts(rescue_res)
                if rp <= 0:
                    rp = _estimate_tokens_from_text(rescue_prompt)
                if rc <= 0:
                    rc = _estimate_tokens_from_text(rescue_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=rp, completion_tokens=rc)
                rescue_text = str(rescue_res.get("text") or "").strip()
                parsed_rescue = _extract_agent_report_payload(rescue_text)
                rescue_chat = str(parsed_rescue.get("chat_update") or "").strip()
                rescue_writes = parsed_rescue.get("output_files") or []
                if not rescue_writes:
                    rescue_writes = _extract_artifacts_from_fenced_code(rescue_text)
                rescue_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=rescue_writes if isinstance(rescue_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                )
                rescue_saved = rescue_write_result.get("saved") or []
                rescue_skipped = rescue_write_result.get("skipped") or []
                if rescue_saved:
                    saved_files.extend(rescue_saved)
                if rescue_skipped:
                    skipped_files.extend(rescue_skipped)
                requires_user_input = requires_user_input or bool(parsed_rescue.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_rescue.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_rescue.get("resume_hint") or "").strip()
                if not report_notes:
                    report_notes = str(parsed_rescue.get("notes") or "").strip()
                if rescue_chat:
                    chat_update = rescue_chat
                if rescue_text:
                    report_text = (report_text + "\n\n[ARTIFACT RESCUE]\n" + rescue_text).strip()
                for note in _summarize_ws_frames(rescue_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_files.append(
                    "artifact rescue failed: "
                    + detail_to_text(rescue_res.get("error") or rescue_res.get("details") or "unknown")
                )

        if not saved_files and not requires_user_input and artifact_like_task:
            fallback_rel = f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}-deliverable.md"
            fallback_content = (
                f"# Deliverable Snapshot: {agent_name}\n\n"
                f"- agent_id: {aid}\n"
                f"- role: {role}\n"
                f"- generated_at: {format_ts(int(time.time()))}\n\n"
                f"## Chat Update\n{chat_update}\n\n"
                f"## Raw Response\n{report_text.strip()}\n"
            )
            fallback_write_result = _apply_project_file_writes(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                writes=[{"path": fallback_rel, "content": fallback_content, "append": False}],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
            )
            fallback_saved = fallback_write_result.get("saved") or []
            fallback_skipped = fallback_write_result.get("skipped") or []
            if fallback_saved:
                saved_files.extend(fallback_saved)
                skipped_files.append("No explicit output_files from agent; saved fallback markdown deliverable.")
            if fallback_skipped:
                skipped_files.extend(fallback_skipped)

        pause_decision = _infer_pause_request(
            chat_update=chat_update,
            notes=report_notes,
            explicit_requires_user_input=requires_user_input,
            explicit_pause_reason=pause_reason,
            explicit_resume_hint=resume_hint,
        )
        if pause_decision.get("pause"):
            pause_reason = str(pause_decision.get("reason") or pause_reason or chat_update).strip()
            resume_hint = str(pause_decision.get("resume_hint") or resume_hint).strip()
            chat_update = _ensure_owner_mention(chat_update)
        else:
            chat_update = _ensure_chat_handoff_mentions(chat_update, assigned_mentions_map.get(aid) or [])

        if str(aid).strip() == str(primary_agent_id or "").strip():
            primary_last_chat_update = chat_update
            primary_last_notes = report_notes
            primary_last_pause_reason = pause_reason
            primary_last_resume_hint = resume_hint

        report_file = outputs_dir / f"{_safe_agent_filename(aid)}-latest.md"
        report_file.write_text(
            f"# Agent Report: {agent_name}\n\n"
            f"- agent_id: {aid}\n"
            f"- role: {role}\n"
            f"- generated_at: {format_ts(int(time.time()))}\n"
            f"- files_saved: {len(saved_files)}\n\n"
            f"- artifact_followup_used: {'yes' if artifact_followup_used else 'no'}\n\n"
            f"- artifact_rescue_used: {'yes' if artifact_rescue_used else 'no'}\n\n"
            f"- requires_user_input: {'yes' if pause_decision.get('pause') else 'no'}\n"
            f"- pause_reason: {pause_reason or '-'}\n"
            f"- resume_hint: {resume_hint or '-'}\n\n"
            f"## Chat Update\n{chat_update}\n\n"
            f"## Raw Response\n{report_text.strip()}\n",
            encoding="utf-8",
        )
        processed_agents += 1
        pct = min(95, 55 + int((idx / agent_total) * 40))
        _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=pct)
        for note in _summarize_ws_frames(agent_res.get("frames"), limit=8):
            await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
        for item in saved_files:
            await emit(
                project_id,
                "project.file.written",
                {
                    "path": str(item.get("path") or ""),
                    "mode": str(item.get("mode") or "w"),
                    "bytes": int(item.get("bytes") or 0),
                    "actor": f"agent:{aid}",
                },
            )
        await emit(
            project_id,
            "agent.task.reported",
            {
                "agent_id": aid,
                "agent_name": agent_name,
                "text": chat_update[:1200],
                "output_file": f"{USER_OUTPUTS_DIRNAME}/{report_file.name}",
                "saved_files": saved_files[:20],
                "skipped_files": skipped_files[:10],
                "requires_user_input": bool(pause_decision.get("pause")),
                "pause_reason": pause_reason[:500],
                "resume_hint": resume_hint[:300],
            },
        )
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agent.task.reported",
            text=f"{aid}: {chat_update[:1600]}",
            payload={
                "output_file": f"{USER_OUTPUTS_DIRNAME}/{report_file.name}",
                "saved_files": saved_files[:20],
                "skipped_files": skipped_files[:10],
                "requires_user_input": bool(pause_decision.get("pause")),
                "pause_reason": pause_reason[:500],
                "resume_hint": resume_hint[:300],
            },
        )
        _refresh_project_documents(project_id)
        if pause_decision.get("pause"):
            state_now, pct_now = _read_project_execution_state(project_id)
            if state_now not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
                pause_pct = max(5, _clamp_progress(pct_now if pct_now > 0 else pct))
                _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
                _refresh_project_documents(project_id)
                summary = pause_reason or "Execution paused. Waiting for owner input."
                await emit(
                    project_id,
                    "project.execution.auto_paused",
                    {
                        "status": EXEC_STATUS_PAUSED,
                        "progress_pct": pause_pct,
                        "agent_id": aid,
                        "agent_name": agent_name,
                        "reason": summary[:900],
                        "resume_hint": (resume_hint or "Reply with required input, then say CONTINUE or press Resume.")[:300],
                    },
                )
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="execution.auto_paused",
                    text=f"{aid}: {summary[:1200]}",
                    payload={"agent_id": aid, "resume_hint": resume_hint[:300]},
                )
                while True:
                    wait_state, _ = _read_project_execution_state(project_id)
                    if wait_state == EXEC_STATUS_PAUSED:
                        await asyncio.sleep(0.7)
                        continue
                    if wait_state == EXEC_STATUS_STOPPED:
                        _refresh_project_documents(project_id)
                        _append_project_daily_log(
                            owner_user_id=str(row["user_id"]),
                            project_root=str(row["project_root"] or ""),
                            kind="delegation.stopped",
                            text="Delegation run stopped by user while waiting for resume.",
                        )
                        await emit(
                            project_id,
                            "project.delegation.stopped",
                            {"processed_agents": processed_agents, "failed_agents": failed_agents, "total_agents": len(agent_order)},
                        )
                        return
                    break
                _append_project_daily_log(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    kind="execution.resumed",
                    text=f"Execution resumed after pause request from {aid}.",
                )
                await emit(
                    project_id,
                    "project.execution.resumed_after_pause",
                    {"status": EXEC_STATUS_RUNNING, "agent_id": aid, "agent_name": agent_name},
                )
                if str(aid).strip() == str(primary_agent_id or "").strip():
                    primary_pause_resolved = True

    final_primary_pause = _infer_pause_request(
        chat_update=primary_last_chat_update,
        notes=primary_last_notes,
        explicit_requires_user_input=False,
        explicit_pause_reason=primary_last_pause_reason,
        explicit_resume_hint=primary_last_resume_hint,
    )
    if (
        str(primary_agent_id or "").strip()
        and not primary_pause_resolved
        and final_primary_pause.get("pause")
    ):
        state_now, pct_now = _read_project_execution_state(project_id)
        if state_now not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
            pause_pct = max(5, _clamp_progress(pct_now if pct_now > 0 else 95))
            _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
            _refresh_project_documents(project_id)
            summary = str(
                final_primary_pause.get("reason")
                or "Primary agent still needs owner input before finishing."
            ).strip()
            hint = str(
                final_primary_pause.get("resume_hint")
                or "Reply with required information, then say CONTINUE (or press Resume)."
            ).strip()
            await emit(
                project_id,
                "project.execution.auto_paused",
                {
                    "status": EXEC_STATUS_PAUSED,
                    "progress_pct": pause_pct,
                    "agent_id": primary_agent_id,
                    "agent_name": primary_agent_name,
                    "reason": summary[:900],
                    "resume_hint": hint[:300],
                },
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="execution.auto_paused",
                text=f"{primary_agent_id}: {summary[:1200]}",
                payload={"agent_id": primary_agent_id, "resume_hint": hint[:300]},
            )
            return

    _set_project_execution_state(project_id, status=EXEC_STATUS_COMPLETED, progress_pct=100)
    _refresh_project_documents(project_id)
    project_files_link = f"/api/projects/{project_id}/files"
    outputs_folder_link = f"/api/projects/{project_id}/files?path={url_quote(USER_OUTPUTS_DIRNAME, safe='')}"
    latest_output_rel = _latest_file_relative_path(outputs_dir, project_dir)
    latest_preview_link = (
        f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(latest_output_rel)}"
        if latest_output_rel
        else ""
    )
    owner_notice_parts = [
        f"@owner project `{str(row['title'] or project_id)}` is completed.",
        f"Open project files: {project_files_link}",
        f"Outputs folder: {outputs_folder_link}",
    ]
    if latest_preview_link:
        owner_notice_parts.append(f"Latest file preview: {latest_preview_link}")
    primary_done_update = " ".join(owner_notice_parts).strip()
    await emit(
        project_id,
        "agent.primary.update",
        {
            "agent_id": primary_agent_id or "primary",
            "agent_name": primary_agent_name,
            "text": primary_done_update[:1200],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
        },
    )
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="delegation.ready",
        text=f"Delegation documents generated for {assigned_count} invited agents. Reports: {processed_agents}, failed: {failed_agents}. {primary_done_update}",
        payload={
            "agents": assigned_count,
            "processed_agents": processed_agents,
            "failed_agents": failed_agents,
            "notes": str(payload.get("notes") or "")[:1000],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
        },
    )
    await emit(
        project_id,
        "project.delegation.ready",
        {
            "agents": assigned_count,
            "processed_agents": processed_agents,
            "failed_agents": failed_agents,
            "notes": str(payload.get("notes") or "")[:1000],
            "project_files_link": project_files_link,
            "outputs_folder_link": outputs_folder_link,
            "latest_preview_link": latest_preview_link,
            "owner_message": primary_done_update[:1200],
        },
    )

class SignupIn(BaseModel):
    email: str = Field(..., examples=["you@example.com"])
    password: str = Field(..., min_length=PASSWORD_MIN_LENGTH)

class LoginIn(BaseModel):
    email: str
    password: str

class OAuthStartIn(BaseModel):
    next_path: Optional[str] = "/"

class OAuthStartOut(BaseModel):
    provider: str
    auth_url: str

class OAuthProviderOut(BaseModel):
    provider: str
    display_name: str
    configured: bool

class OAuthProvidersOut(BaseModel):
    providers: List[OAuthProviderOut]

class SessionOut(BaseModel):
    token: str

class AccountProfileOut(BaseModel):
    id: str
    email: str
    created_at: int
    workspace_root: str
    projects_count: int = 0
    connections_count: int = 0

class AccountPasswordChangeIn(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=PASSWORD_MIN_LENGTH)

class AccountDeleteIn(BaseModel):
    current_password: str = Field(..., min_length=1)
    confirm_text: str = Field(..., min_length=1, description="Type DELETE to confirm account deletion")

class ConnectIn(BaseModel):
    base_url: str = Field(..., description="OpenClaw base URL, e.g. https://claw.yourdomain.com or http://1.2.3.4:3000")
    api_key: str = Field(..., description="Bearer token / API key from OpenClaw")
    name: Optional[str] = Field(None, description="Friendly name, e.g. 'Ariel VPS'")

class ConnectionOut(BaseModel):
    id: str
    base_url: str
    name: Optional[str]

class ConnectionPolicyOut(BaseModel):
    connection_id: str
    workspace_root: str
    templates_root: str
    main_agent_id: Optional[str]
    main_agent_name: Optional[str]
    bootstrap_status: str
    bootstrap_error: Optional[str] = None
    workspace_tree: Optional[str] = None

class ProjectCreateIn(BaseModel):
    title: str
    brief: str
    goal: str
    connection_id: str
    setup_details: Optional[Dict[str, Any]] = None
    setup_chat_history: Optional[str] = None

class ProjectOut(BaseModel):
    id: str
    title: str
    brief: str
    goal: str
    connection_id: str
    created_at: int
    workspace_root: Optional[str] = None
    project_root: Optional[str] = None
    setup_details: Optional[Dict[str, Any]] = None
    plan_status: str = PLAN_STATUS_PENDING
    plan_text: Optional[str] = None
    plan_updated_at: Optional[int] = None
    plan_approved_at: Optional[int] = None
    execution_status: str = EXEC_STATUS_IDLE
    progress_pct: int = 0
    execution_updated_at: Optional[int] = None
    usage_prompt_tokens: int = 0
    usage_completion_tokens: int = 0
    usage_total_tokens: int = 0
    usage_updated_at: Optional[int] = None

class ProjectAgentsIn(BaseModel):
    agent_ids: List[str]
    agent_names: List[str]
    agent_roles: Optional[List[str]] = None
    primary_agent_id: Optional[str] = None

class OpenClawChatIn(BaseModel):
    message: str = Field(..., min_length=1)
    agent_id: Optional[str] = None

class OpenClawWsChatIn(BaseModel):
    message: str = Field(..., min_length=1)
    agent_id: Optional[str] = None
    session_key: str = "main"
    timeout_sec: int = 25

class ProjectSetupChatIn(BaseModel):
    connection_id: str
    message: str = ""
    agent_id: Optional[str] = None
    session_key: str = "new-project"
    timeout_sec: int = 35
    start: bool = False
    optimize_tokens: bool = True

class ProjectSetupDraftIn(BaseModel):
    connection_id: str
    transcript: List[Dict[str, Any]] = Field(default_factory=list)
    agent_id: Optional[str] = None
    session_key: str = "new-project"
    timeout_sec: int = 35
    optimize_tokens: bool = True

class ProjectPlanOut(BaseModel):
    project_id: str
    status: str
    text: str
    updated_at: Optional[int] = None
    approved_at: Optional[int] = None

class ProjectPlanApproveIn(BaseModel):
    approve: bool = True

class ProjectExecutionOut(BaseModel):
    project_id: str
    status: str
    progress_pct: int
    updated_at: Optional[int] = None

class ProjectExecutionControlIn(BaseModel):
    action: str = Field(..., description="pause | resume | stop")

class ProjectUsageOut(BaseModel):
    project_id: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    updated_at: Optional[int] = None

class WorkspaceTreeOut(BaseModel):
    workspace_root: str
    tree: str

class ProjectWorkspaceTreeOut(BaseModel):
    project_id: str
    project_root: str
    tree: str
    access_mode: str

class ProjectFileEntryOut(BaseModel):
    name: str
    path: str
    kind: str
    size: Optional[int] = None
    modified_at: Optional[int] = None

class ProjectFilesOut(BaseModel):
    project_id: str
    project_root: str
    current_path: str
    parent_path: Optional[str] = None
    access_mode: str
    entries: List[ProjectFileEntryOut]

class WorkspaceFilesOut(BaseModel):
    workspace_root: str
    current_path: str
    parent_path: Optional[str] = None
    entries: List[ProjectFileEntryOut]

class ProjectFileContentOut(BaseModel):
    project_id: str
    path: str
    size: int
    truncated: bool
    content: str

class WorkspaceFileContentOut(BaseModel):
    workspace_root: str
    path: str
    size: int
    truncated: bool
    content: str

class ProjectFileWriteIn(BaseModel):
    path: str = Field(..., min_length=1)
    content: str = ""
    append: bool = False

class A2AEnvironmentBootstrapIn(BaseModel):
    agent_id: str = Field("bootstrap-agent", min_length=1)
    display_name: Optional[str] = None
    claim_ttl_sec: int = ENV_CLAIM_CODE_TTL_SEC
    session_ttl_sec: int = ENV_AGENT_SESSION_TTL_SEC

class A2AEnvironmentClaimStartIn(BaseModel):
    claim_ttl_sec: int = ENV_CLAIM_CODE_TTL_SEC

class A2AEnvironmentClaimCompleteIn(BaseModel):
    environment_id: str = Field(..., min_length=1)
    code: str = Field(..., min_length=1)
    mode: str = Field("signup", description="signup | login | session")
    email: Optional[str] = Field(None, min_length=3)
    password: Optional[str] = Field(None, min_length=PASSWORD_MIN_LENGTH)
    openclaw_base_url: str = Field(..., min_length=8)
    openclaw_api_key: str = Field(..., min_length=1)
    openclaw_name: Optional[str] = None

class A2AEnvironmentClaimCompleteOut(BaseModel):
    token: str
    environment_id: str
    status: str
    user_id: str
    email: str
    connection_id: str
    connection_name: Optional[str] = None

def _bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "").strip()
    if token:
        return token
    return str(request.cookies.get(SESSION_COOKIE_NAME) or "").strip()

def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=str(token or "").strip(),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=SESSION_COOKIE_MAX_AGE_SEC,
        path="/",
    )

def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/", samesite="lax")

def get_optional_session_user(request: Request) -> Optional[str]:
    token = _bearer_token(request)
    if not token:
        return None
    conn = db()
    row = conn.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid session token")
    return row["user_id"]

def get_session_user(request: Request) -> str:
    token = _bearer_token(request)
    if not token:
        raise HTTPException(401, "Missing Authorization: Bearer <token>")
    conn = db()
    row = conn.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid session token")
    return row["user_id"]

def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(10)}"

def _project_out_from_row(row: sqlite3.Row) -> ProjectOut:
    payload = dict(row)
    setup_raw = payload.pop("setup_json", None)
    payload["setup_details"] = _normalize_setup_details(_parse_setup_json(setup_raw))
    payload["plan_status"] = str(payload.get("plan_status") or PLAN_STATUS_PENDING)
    payload["plan_text"] = str(payload.get("plan_text") or "")
    payload["plan_updated_at"] = payload.get("plan_updated_at")
    payload["plan_approved_at"] = payload.get("plan_approved_at")
    payload["execution_status"] = _coerce_execution_status(payload.get("execution_status"))
    payload["progress_pct"] = _clamp_progress(payload.get("progress_pct"))
    payload["execution_updated_at"] = payload.get("execution_updated_at")
    payload["usage_prompt_tokens"] = max(0, _to_int(payload.get("usage_prompt_tokens")))
    payload["usage_completion_tokens"] = max(0, _to_int(payload.get("usage_completion_tokens")))
    payload["usage_total_tokens"] = max(0, _to_int(payload.get("usage_total_tokens")))
    payload["usage_updated_at"] = payload.get("usage_updated_at")
    return ProjectOut(**payload)

def _resolve_project_workspace_access(request: Request, project_id: str) -> Dict[str, Any]:
    session_user = get_optional_session_user(request)
    conn = db()
    project = conn.execute(
        "SELECT id, user_id, project_root, workspace_root FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if not project:
        conn.close()
        raise HTTPException(404, "Project not found")

    if session_user and project["user_id"] == session_user:
        conn.close()
        return {"mode": "owner", "project": dict(project), "user_id": session_user}

    agent_id = (request.headers.get("X-Project-Agent-Id") or "").strip()
    agent_token = (request.headers.get("X-Project-Agent-Token") or "").strip()
    if agent_id and agent_token:
        token_hash = _hash_access_token(agent_token)
        row = conn.execute(
            """
            SELECT 1
            FROM project_agents pa
            JOIN project_agent_access_tokens pat
                ON pat.project_id = pa.project_id AND pat.agent_id = pa.agent_id
            WHERE pa.project_id = ? AND pa.agent_id = ? AND pat.token_hash = ?
            """,
            (project_id, agent_id, token_hash),
        ).fetchone()
        if row:
            conn.close()
            return {"mode": "agent", "project": dict(project), "agent_id": agent_id}

    conn.close()
    if session_user:
        raise HTTPException(403, "Only project owner or invited agent can access this folder")
    raise HTTPException(401, "Missing authorization. Use owner token or agent access headers.")

def _clean_relative_project_path(raw_path: Optional[str]) -> str:
    raw = str(raw_path or "").strip().replace("\\", "/")
    while raw.startswith("/"):
        raw = raw[1:]
    return raw

def _resolve_project_relative_path(
    owner_user_id: str,
    project_root: str,
    relative_path: Optional[str],
    *,
    require_exists: bool = True,
    require_dir: bool = False,
) -> Path:
    project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    rel = _clean_relative_project_path(relative_path)
    target = (project_dir / rel).resolve() if rel else project_dir
    if not _path_within(target, project_dir):
        raise HTTPException(400, "Path is outside project root")
    if require_exists and not target.exists():
        raise HTTPException(404, "Path not found")
    if require_dir and target.exists() and not target.is_dir():
        raise HTTPException(400, "Path is not a directory")
    return target

def _resolve_workspace_relative_path(
    user_id: str,
    relative_path: Optional[str],
    *,
    require_exists: bool = True,
    require_dir: bool = False,
) -> Tuple[Path, Path]:
    workspace_root = _user_home_dir(user_id).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    rel = _clean_relative_project_path(relative_path)
    target = (workspace_root / rel).resolve() if rel else workspace_root
    if not _path_within(target, workspace_root):
        raise HTTPException(400, "Path is outside workspace root")
    if require_exists and not target.exists():
        raise HTTPException(404, "Path not found")
    if require_dir and target.exists() and not target.is_dir():
        raise HTTPException(400, "Path is not a directory")
    return workspace_root, target

app = FastAPI(title="hivee (Prototype)")
init_db()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/new-user", StaticFiles(directory=str(NEW_USER_ASSETS_DIR), check_dir=False), name="new_user_assets")

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

@app.post("/api/a2a/environments/bootstrap")
async def bootstrap_a2a_environment(request: Request, payload: A2AEnvironmentBootstrapIn):
    env_id = new_id("env")
    now = int(time.time())
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
    agent_id = str(payload.agent_id or "bootstrap-agent").strip() or "bootstrap-agent"
    agent_token, agent_expires_at = _issue_environment_agent_session(
        conn,
        env_id=env_id,
        agent_id=agent_id,
        scopes=["env.read", "env.bootstrap", "env.claim.start"],
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
        "status": ENV_STATUS_PENDING_CLAIM,
        "workspace_root": workspace.get("workspace_root"),
        "templates_root": workspace.get("templates_root"),
        "agent_session_token": agent_token,
        "agent_session_expires_at": agent_expires_at,
        "claim_code": claim_code,
        "claim_code_expires_at": claim_expires_at,
        "claim_url": claim_url,
        "message": "Environment bootstrap complete. Share claim_url with user to claim this environment.",
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

@app.post("/api/a2a/environments/claim/complete", response_model=A2AEnvironmentClaimCompleteOut)
async def complete_a2a_environment_claim(request: Request, payload: A2AEnvironmentClaimCompleteIn, response: Response):
    env_id = str(payload.environment_id or "").strip()
    claim_code = str(payload.code or "").strip()
    mode = str(payload.mode or "signup").strip().lower()
    email = _normalize_email(str(payload.email or ""))
    password = str(payload.password or "")
    openclaw_base_url = str(payload.openclaw_base_url or "").strip()
    openclaw_api_key = str(payload.openclaw_api_key or "").strip()
    openclaw_name = str(payload.openclaw_name or "").strip() or None
    if mode not in {"signup", "login", "session"}:
        raise HTTPException(400, "mode must be signup, login, or session")
    if not env_id or not claim_code:
        raise HTTPException(400, "environment_id and code are required")
    if not (openclaw_base_url.startswith("http://") or openclaw_base_url.startswith("https://")):
        raise HTTPException(400, "openclaw_base_url must start with http:// or https://")
    if not openclaw_api_key:
        raise HTTPException(400, "openclaw_api_key is required")
    if mode in {"signup", "login"} and not email:
        raise HTTPException(400, "email is required")
    if mode in {"signup", "login"} and not password:
        raise HTTPException(400, "password is required")
    if mode == "signup":
        _validate_password_strength(password)

    now = int(time.time())
    health = await openclaw_health(openclaw_base_url.rstrip("/"), openclaw_api_key)
    if not health.get("ok"):
        raise HTTPException(
            400,
            {
                "message": "Could not verify OpenClaw health during claim",
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
    if str(env_row["status"] or "").strip() == ENV_STATUS_ARCHIVED:
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
        raise HTTPException(400, "Invalid claim code")
    if _to_int(claim_row["used_at"]) > 0:
        conn.close()
        raise HTTPException(400, "Claim code already used")
    if _to_int(claim_row["expires_at"]) <= now:
        conn.close()
        raise HTTPException(400, "Claim code expired")

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

    bootstrap = await _bootstrap_connection_workspace(user_id, openclaw_base_url.rstrip("/"), openclaw_api_key)
    if not bootstrap.get("ok"):
        conn.close()
        raise HTTPException(
            400,
            {
                "message": "OpenClaw verified, but workspace bootstrap failed during claim",
                "details": bootstrap,
            },
        )

    token = new_id("sess")
    conn_id = new_id("oc")
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
    conn.execute(
        "UPDATE environment_agent_sessions SET status = ?, revoked_at = ? WHERE env_id = ? AND status = ?",
        ("revoked", now, env_id, "active"),
    )
    conn.execute(
        "INSERT INTO openclaw_connections (id, user_id, env_id, base_url, api_key, name, created_at) VALUES (?,?,?,?,?,?,?)",
        (
            conn_id,
            user_id,
            env_id,
            openclaw_base_url.rstrip("/"),
            openclaw_api_key,
            openclaw_name,
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

    _set_session_cookie(response, token)
    return A2AEnvironmentClaimCompleteOut(
        token=token,
        environment_id=env_id,
        status=ENV_STATUS_ACTIVE,
        user_id=user_id,
        email=email,
        connection_id=conn_id,
        connection_name=openclaw_name,
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
    if session_user and owner_user_id and str(session_user) == owner_user_id:
        access_mode = "owner"
    else:
        conn.close()
        access = _resolve_environment_agent_access(request, env_id, required_scope="env.read")
        access_mode = f"agent:{access.get('agent_id')}"
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
    return {
        "ok": True,
        "environment": {
            "id": str(env_row["id"]),
            "display_name": str(env_row["display_name"] or ""),
            "status": str(env_row["status"] or ""),
            "workspace_root": str(env_row["workspace_root"] or ""),
            "created_at": env_row["created_at"],
            "claimed_at": env_row["claimed_at"],
            "owner_user_id": owner_user_id or None,
        },
        "access_mode": access_mode,
        "outstanding_count": len(outstanding),
        "outstanding_projects": outstanding[:20],
    }

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
        "SELECT COUNT(1) AS c FROM projects WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    connection_count_row = conn.execute(
        "SELECT COUNT(1) AS c FROM openclaw_connections WHERE user_id = ?",
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

@app.post("/api/openclaw/connect")
async def connect_openclaw(request: Request, payload: ConnectIn):
    user_id = get_session_user(request)
    primary_env = _ensure_primary_environment_for_user(user_id)
    env_id = str(primary_env.get("id") or "").strip() or None
    if not (payload.base_url.startswith("http://") or payload.base_url.startswith("https://")):
        raise HTTPException(400, "base_url must start with http:// or https://")

    health = await openclaw_health(payload.base_url, payload.api_key)
    if not health.get("ok"):
        raise HTTPException(400, {"message": "Could not verify OpenClaw health", "details": health})

    bootstrap = await _bootstrap_connection_workspace(user_id, payload.base_url.rstrip("/"), payload.api_key)
    if not bootstrap.get("ok"):
        raise HTTPException(
            400,
            {
                "message": "OpenClaw connected, but Hivee workspace bootstrap failed.",
                "details": bootstrap,
            },
        )

    conn = db()
    conn_id = new_id("oc")
    conn.execute(
        "INSERT INTO openclaw_connections (id, user_id, env_id, base_url, api_key, name, created_at) VALUES (?,?,?,?,?,?,?)",
        (conn_id, user_id, env_id, payload.base_url.rstrip("/"), payload.api_key, payload.name, int(time.time())),
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
        workspace_root=str(bootstrap.get("workspace_root") or HIVEE_ROOT),
        templates_root=str(bootstrap.get("templates_root") or HIVEE_TEMPLATES_ROOT),
    )

    return {
        "ok": True,
        "connection": {"id": conn_id, "base_url": payload.base_url.rstrip("/"), "name": payload.name},
        "health": health,
        "bootstrap": bootstrap,
    }

@app.post("/api/openclaw/{connection_id}/bootstrap")
async def bootstrap_openclaw_connection(request: Request, connection_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    bootstrap = await _bootstrap_connection_workspace(user_id, row["base_url"], row["api_key"])
    _upsert_connection_policy(
        connection_id,
        user_id,
        main_agent_id=bootstrap.get("main_agent_id"),
        main_agent_name=bootstrap.get("main_agent_name"),
        bootstrap_status="ok" if bootstrap.get("ok") else "failed",
        bootstrap_error=None if bootstrap.get("ok") else detail_to_text(bootstrap.get("ws_result")),
        workspace_tree=bootstrap.get("workspace_tree"),
        workspace_root=str(bootstrap.get("workspace_root") or HIVEE_ROOT),
        templates_root=str(bootstrap.get("templates_root") or HIVEE_TEMPLATES_ROOT),
    )
    if not bootstrap.get("ok"):
        raise HTTPException(400, bootstrap)
    return bootstrap

@app.get("/api/openclaw/connections", response_model=List[ConnectionOut])
async def list_connections(request: Request):
    user_id = get_session_user(request)
    conn = db()
    rows = conn.execute(
        "SELECT id, base_url, name FROM openclaw_connections WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    conn.close()
    return [ConnectionOut(id=r["id"], base_url=r["base_url"], name=r["name"]) for r in rows]

@app.get("/api/openclaw/{connection_id}/agents")
async def list_agents(request: Request, connection_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    res = await openclaw_list_agents(row["base_url"], row["api_key"])
    if not res.get("ok"):
        raise HTTPException(400, res)
    return res

@app.get("/api/openclaw/{connection_id}/policy", response_model=ConnectionPolicyOut)
async def get_connection_policy(request: Request, connection_id: str):
    user_id = get_session_user(request)
    workspace = _ensure_user_workspace(user_id)
    conn = db()
    exists = conn.execute(
        "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    if not exists:
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

@app.get("/api/workspace/tree", response_model=WorkspaceTreeOut)
async def get_workspace_tree(request: Request):
    user_id = get_session_user(request)
    _ensure_user_workspace(user_id)
    workspace_root = _user_home_dir(user_id).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    if not _path_within(workspace_root, _user_home_dir(user_id)):
        raise HTTPException(500, "Workspace root is outside user home")
    return WorkspaceTreeOut(
        workspace_root=workspace_root.as_posix(),
        tree=_render_tree(workspace_root),
    )

@app.get("/api/workspace/files", response_model=WorkspaceFilesOut)
async def list_workspace_files(request: Request, path: str = ""):
    user_id = get_session_user(request)
    workspace_root, target = _resolve_workspace_relative_path(
        user_id,
        path,
        require_exists=True,
        require_dir=True,
    )
    current_rel = ""
    if target != workspace_root:
        current_rel = target.relative_to(workspace_root).as_posix()
    parent_rel: Optional[str] = None
    if current_rel:
        parent_rel = str(Path(current_rel).parent).replace("\\", "/")
        if parent_rel == ".":
            parent_rel = ""

    entries: List[ProjectFileEntryOut] = []
    for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        rel = child.relative_to(workspace_root).as_posix()
        stat = child.stat()
        entries.append(
            ProjectFileEntryOut(
                name=child.name,
                path=rel,
                kind="dir" if child.is_dir() else "file",
                size=None if child.is_dir() else int(stat.st_size),
                modified_at=int(stat.st_mtime),
            )
        )

    return WorkspaceFilesOut(
        workspace_root=workspace_root.as_posix(),
        current_path=current_rel,
        parent_path=parent_rel,
        entries=entries,
    )

@app.get("/api/workspace/files/content", response_model=WorkspaceFileContentOut)
async def read_workspace_file(request: Request, path: str):
    user_id = get_session_user(request)
    workspace_root, target = _resolve_workspace_relative_path(
        user_id,
        path,
        require_exists=True,
        require_dir=False,
    )
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    data = target.read_bytes()
    size = len(data)
    truncated = size > MAX_FILE_PREVIEW_BYTES
    if truncated:
        data = data[:MAX_FILE_PREVIEW_BYTES]
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError:
        content = data.decode("utf-8", errors="replace")
    rel = target.relative_to(workspace_root).as_posix()
    return WorkspaceFileContentOut(
        workspace_root=workspace_root.as_posix(),
        path=rel,
        size=size,
        truncated=truncated,
        content=content,
    )

@app.get("/api/workspace/files/raw")
async def read_workspace_file_raw(request: Request, path: str):
    user_id = get_session_user(request)
    _, target = _resolve_workspace_relative_path(
        user_id,
        path,
        require_exists=True,
        require_dir=False,
    )
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    guessed, _ = mimetypes.guess_type(target.name)
    media_type = guessed or "application/octet-stream"
    return FileResponse(str(target), media_type=media_type)

@app.get("/api/workspace/preview/{path:path}")
async def preview_workspace_file(request: Request, path: str):
    user_id = get_session_user(request)
    _, target = _resolve_workspace_relative_path(
        user_id,
        path,
        require_exists=True,
        require_dir=False,
    )
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    guessed, _ = mimetypes.guess_type(target.name)
    media_type = guessed or "application/octet-stream"
    return FileResponse(str(target), media_type=media_type)

@app.post("/api/openclaw/{connection_id}/chat")
async def chat_openclaw(request: Request, connection_id: str, payload: OpenClawChatIn):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    policy = conn.execute(
        "SELECT main_agent_id, workspace_root FROM connection_policies WHERE connection_id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
    effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
    scoped_message = _compose_guardrailed_message(payload.message.strip(), workspace_root=workspace_root)
    res = await openclaw_chat(
        row["base_url"],
        row["api_key"],
        scoped_message,
        effective_agent_id,
        max_output_tokens=SAFE_PROVIDER_MAX_OUTPUT_TOKENS,
    )
    if not res.get("ok"):
        raise HTTPException(400, res)
    res["resolved_agent_id"] = effective_agent_id
    res["workspace_root"] = workspace_root
    return res

@app.post("/api/openclaw/{connection_id}/ws-chat")
async def chat_openclaw_ws(request: Request, connection_id: str, payload: OpenClawWsChatIn):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    policy = conn.execute(
        "SELECT main_agent_id, workspace_root FROM connection_policies WHERE connection_id = ? AND user_id = ?",
        (connection_id, user_id),
    ).fetchone()
    session_key = (payload.session_key or "main").strip() or "main"
    project_scope = None
    role_rows: List[Dict[str, Any]] = []
    project_primary_agent_id: Optional[str] = None
    if session_key.startswith("prj_"):
        project_scope = conn.execute(
            "SELECT project_root, title, brief, goal, setup_json, plan_status, execution_status, progress_pct FROM projects WHERE id = ? AND user_id = ?",
            (session_key, user_id),
        ).fetchone()
        if project_scope:
            raw_roles = conn.execute(
                "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
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
            if payload.agent_id:
                allowed = {str(r.get("agent_id") or "").strip() for r in role_rows}
                if payload.agent_id not in allowed:
                    conn.close()
                    raise HTTPException(403, "Only invited project agents can be targeted in this project chat")
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    workspace_root = str(policy["workspace_root"]) if (policy and policy["workspace_root"]) else HIVEE_ROOT
    project_root = str(project_scope["project_root"]) if (project_scope and project_scope["project_root"]) else None
    project_instruction = None
    if project_scope:
        project_instruction = _project_context_instruction(
            title=str(project_scope["title"] or ""),
            brief=str(project_scope["brief"] or ""),
            goal=str(project_scope["goal"] or ""),
            setup_details=_parse_setup_json(project_scope["setup_json"]),
            role_rows=role_rows,
            plan_status=_coerce_plan_status(project_scope["plan_status"]),
        )
        roster_text = _agent_roster_markdown(role_rows)
        project_file_context = _build_project_file_context(
            owner_user_id=user_id,
            project_root=str(project_scope["project_root"] or ""),
            include_paths=[
                PROJECT_INFO_FILE,
                PROJECT_DELEGATION_FILE,
                OVERVIEW_FILE,
                PROJECT_PLAN_FILE,
                "agents/ROLES.md",
                SETUP_CHAT_HISTORY_FILE,
                SETUP_CHAT_HISTORY_COMPAT_FILE,
            ],
            request_text=payload.message,
            max_total_chars=8_000,
            max_files=8,
        )
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
        if project_file_context:
            sections.append(project_file_context)
        project_instruction = "\n\n".join([s for s in sections if str(s or "").strip()])
    if project_scope:
        is_paused_scope = _coerce_execution_status(project_scope["execution_status"]) == EXEC_STATUS_PAUSED
        if is_paused_scope:
            effective_agent_id = project_primary_agent_id or payload.agent_id
        else:
            effective_agent_id = payload.agent_id or project_primary_agent_id
        if not effective_agent_id:
            raise HTTPException(400, "No primary project agent configured")
    else:
        effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
    scoped_message = _compose_guardrailed_message(
        payload.message.strip(),
        workspace_root=workspace_root,
        project_root=project_root,
        task_instruction=project_instruction,
    )
    res = await openclaw_ws_chat(
        base_url=row["base_url"],
        api_key=row["api_key"],
        message=scoped_message,
        agent_id=effective_agent_id,
        session_key=session_key,
        timeout_sec=payload.timeout_sec,
    )
    if not res.get("ok"):
        if project_scope:
            _append_project_daily_log(
                owner_user_id=user_id,
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
        write_result = _apply_project_file_writes(
            owner_user_id=user_id,
            project_root=str(project_scope["project_root"] or ""),
            writes=write_payload if isinstance(write_payload, list) else [],
            default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
        )
        saved_writes = write_result.get("saved") or []
        skipped_writes = write_result.get("skipped") or []
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
            followup_res = await openclaw_ws_chat(
                base_url=row["base_url"],
                api_key=row["api_key"],
                message=followup_prompt,
                agent_id=effective_agent_id,
                session_key=session_key,
                timeout_sec=max(10, min(payload.timeout_sec, 60)),
            )
            if followup_res.get("ok"):
                followup_text = str(followup_res.get("text") or "").strip()
                followup_parsed = _extract_agent_report_payload(followup_text)
                followup_chat_update = str(followup_parsed.get("chat_update") or "").strip()
                followup_writes_raw = followup_parsed.get("output_files") or []
                requires_user_input = requires_user_input or bool(followup_parsed.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(followup_parsed.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(followup_parsed.get("resume_hint") or "").strip()
                if not parsed_notes:
                    parsed_notes = str(followup_parsed.get("notes") or "").strip()
                followup_write_result = _apply_project_file_writes(
                    owner_user_id=user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    writes=followup_writes_raw if isinstance(followup_writes_raw, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                )
                followup_saved = followup_write_result.get("saved") or []
                followup_skipped = followup_write_result.get("skipped") or []
                if followup_saved:
                    saved_writes.extend(followup_saved)
                if followup_skipped:
                    skipped_writes.extend(followup_skipped)
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
            rescue_res = await openclaw_ws_chat(
                base_url=row["base_url"],
                api_key=row["api_key"],
                message=rescue_prompt,
                agent_id=effective_agent_id,
                session_key=session_key,
                timeout_sec=max(10, min(payload.timeout_sec, 60)),
            )
            if rescue_res.get("ok"):
                rescue_text = str(rescue_res.get("text") or "").strip()
                rescue_parsed = _extract_agent_report_payload(rescue_text)
                rescue_chat_update = str(rescue_parsed.get("chat_update") or "").strip()
                rescue_writes_raw = rescue_parsed.get("output_files") or []
                if not rescue_writes_raw:
                    rescue_writes_raw = _extract_artifacts_from_fenced_code(rescue_text)
                rescue_write_result = _apply_project_file_writes(
                    owner_user_id=user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    writes=rescue_writes_raw if isinstance(rescue_writes_raw, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
                )
                rescue_saved = rescue_write_result.get("saved") or []
                rescue_skipped = rescue_write_result.get("skipped") or []
                if rescue_saved:
                    saved_writes.extend(rescue_saved)
                if rescue_skipped:
                    skipped_writes.extend(rescue_skipped)
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
                owner_user_id=user_id,
                project_root=str(project_scope["project_root"] or ""),
                writes=[{"path": fallback_rel, "content": fallback_content, "append": False}],
                default_prefix=f"{USER_OUTPUTS_DIRNAME}/chat-generated",
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
                    owner_user_id=user_id,
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
                    owner_user_id=user_id,
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
                    owner_user_id=user_id,
                    project_root=str(project_scope["project_root"] or ""),
                    kind="execution.resume",
                    text="Execution resumed after user continue message in chat.",
                )
        _refresh_project_documents(session_key)
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
        _append_project_daily_log(
            owner_user_id=user_id,
            project_root=str(project_scope["project_root"] or ""),
            kind="chat.hivee",
            text=(
                f"USER: {payload.message.strip()}\n"
                f"AGENT({effective_agent_id or 'auto'}): {str(res.get('text') or '').strip()}\n"
                f"FILES_SAVED: {len(saved_writes)}\n"
                f"ARTIFACT_FOLLOWUP_USED: {'yes' if artifact_followup_used else 'no'}\n"
                f"ARTIFACT_RESCUE_USED: {'yes' if artifact_rescue_used else 'no'}"
            ),
            payload={
                "saved_files": saved_writes,
                "skipped_files": skipped_writes[:10],
                "requires_user_input": bool(res.get("requires_user_input")),
                "pause_reason": str(res.get("pause_reason") or "")[:500],
                "resume_hint": str(res.get("resume_hint") or "")[:300],
            },
        )
    res["resolved_agent_id"] = effective_agent_id
    res["workspace_root"] = workspace_root
    if project_root:
        res["project_root"] = project_root
    return res

@app.post("/api/projects/setup-chat")
async def project_setup_chat(request: Request, payload: ProjectSetupChatIn):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (payload.connection_id, user_id),
    ).fetchone()
    policy = conn.execute(
        "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
        (payload.connection_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace["workspace_root"])
    templates_root = str(workspace["templates_root"])
    setup_template = _read_project_setup_template(user_id)
    effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
    instruction = _build_new_project_setup_instruction(
        payload.message or "Start new project setup and ask the first question.",
        template_content=setup_template,
        workspace_root=workspace_root,
        start_mode=bool(payload.start),
    )
    session_key = (payload.session_key or "new-project").strip() or "new-project"
    res = await openclaw_ws_chat(
        base_url=row["base_url"],
        api_key=row["api_key"],
        message=instruction,
        agent_id=effective_agent_id,
        session_key=f"project-setup:{session_key}",
        timeout_sec=max(10, min(payload.timeout_sec, 45 if payload.optimize_tokens else 90)),
    )
    if not res.get("ok"):
        raise HTTPException(400, res)
    res["resolved_agent_id"] = effective_agent_id
    res["workspace_root"] = workspace_root
    res["templates_root"] = templates_root
    return res

@app.post("/api/projects/setup-draft")
async def project_setup_draft(request: Request, payload: ProjectSetupDraftIn):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT base_url, api_key FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (payload.connection_id, user_id),
    ).fetchone()
    policy = conn.execute(
        "SELECT main_agent_id FROM connection_policies WHERE connection_id = ? AND user_id = ?",
        (payload.connection_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Connection not found")

    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace["workspace_root"])
    transcript = payload.transcript or []
    local_draft = _local_setup_draft(transcript)
    local_details = _normalize_setup_details(local_draft.get("setup_details") or {})
    local_details["autofill_used"] = True
    local_details["draft_source"] = "local_optimized"

    if payload.optimize_tokens:
        return {
            "ok": True,
            "title": str(local_draft.get("title") or "")[:160],
            "brief": str(local_draft.get("brief") or "")[:5000],
            "goal": str(local_draft.get("goal") or "")[:5000],
            "setup_details": local_details,
            "raw": "LOCAL_DRAFT_OPTIMIZED",
        }

    setup_template = _read_project_setup_template(user_id)
    effective_agent_id = payload.agent_id or (str(policy["main_agent_id"]) if (policy and policy["main_agent_id"]) else None)
    instruction = _build_setup_draft_instruction(
        template_content=setup_template,
        transcript=transcript,
        workspace_root=workspace_root,
    )
    session_key = (payload.session_key or "new-project").strip() or "new-project"
    res = await openclaw_ws_chat(
        base_url=row["base_url"],
        api_key=row["api_key"],
        message=instruction,
        agent_id=effective_agent_id,
        session_key=f"project-setup-draft:{session_key}",
        timeout_sec=max(10, min(payload.timeout_sec, 60)),
    )

    text = ""
    parsed: Dict[str, Any] = {}
    if res.get("ok"):
        text = str(res.get("text") or "").strip()
        parsed = _extract_json_object(text) or {}
    else:
        text = "LOCAL_DRAFT_FALLBACK"

    details = _normalize_setup_details(parsed)
    merged_details: Dict[str, Any] = dict(local_details)
    if details:
        merged_details.update(details)

    title = str(parsed.get("title") or local_draft.get("title") or "").strip()[:160]
    brief = str(parsed.get("brief") or local_draft.get("brief") or "").strip()[:5000]
    goal = str(parsed.get("goal") or local_draft.get("goal") or "").strip()[:5000]
    if not title:
        title = _fallback_project_title(transcript)[:160]
    if not brief:
        user_lines = _first_user_lines(transcript)
        brief = (" ".join(user_lines) if user_lines else "Project brief drafted from setup conversation.")[:5000]
    if not goal:
        fallback_goal = str(merged_details.get("first_output") or merged_details.get("milestones") or "").strip()
        goal = (fallback_goal or "Produce a practical first deliverable and execution plan for this project.")[:5000]

    merged_details["autofill_used"] = (not bool(parsed.get("title"))) or (not bool(parsed.get("brief"))) or (not bool(parsed.get("goal")))
    if res.get("ok"):
        merged_details["draft_source"] = "agent"
    else:
        merged_details["draft_source"] = "local_fallback"
    merged_details.pop("title", None)
    merged_details.pop("brief", None)
    merged_details.pop("goal", None)

    return {
        "ok": True,
        "title": title,
        "brief": brief,
        "goal": goal,
        "setup_details": merged_details,
        "raw": text[:2000] if text else "LOCAL_DRAFT_FALLBACK",
    }

@app.post("/api/projects", response_model=ProjectOut)
async def create_project(request: Request, payload: ProjectCreateIn):
    user_id = get_session_user(request)
    primary_env = _ensure_primary_environment_for_user(user_id)
    env_id = str(primary_env.get("id") or "").strip() or None

    conn = db()
    c = conn.execute(
        "SELECT id FROM openclaw_connections WHERE id = ? AND user_id = ?",
        (payload.connection_id, user_id),
    ).fetchone()
    if not c:
        conn.close()
        raise HTTPException(400, "Invalid connection_id (not found for this user)")

    pid = new_id("prj")
    now = int(time.time())
    setup_details = _normalize_setup_details(payload.setup_details or {})
    setup_chat_history_text = str(payload.setup_chat_history or "").replace("\r", "").strip()[:120_000]
    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace["workspace_root"])
    project_root = _build_project_root(pid, payload.title, workspace_root=workspace_root)
    project_dir = Path(project_root)
    if not _path_within(project_dir, Path(workspace_root)):
        conn.close()
        raise HTTPException(400, "Invalid project root derived from workspace")
    _initialize_project_folder(
        project_dir,
        payload.title,
        payload.brief,
        payload.goal,
        setup_details=setup_details,
        setup_chat_history_text=setup_chat_history_text,
    )

    conn.execute(
        """
        INSERT INTO projects (
            id, user_id, env_id, title, brief, goal, setup_json, plan_text, plan_status, plan_updated_at, plan_approved_at,
            execution_status, progress_pct, execution_updated_at,
            usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
            connection_id, workspace_root, project_root, scope_requires_owner_approval, created_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            pid,
            user_id,
            env_id,
            payload.title,
            payload.brief,
            payload.goal,
            json.dumps(setup_details, ensure_ascii=False),
            "",
            PLAN_STATUS_PENDING,
            int(time.time()),
            None,
            EXEC_STATUS_IDLE,
            0,
            int(time.time()),
            0,
            0,
            0,
            int(time.time()),
            payload.connection_id,
            workspace_root,
            project_root,
            1,
            now,
        ),
    )
    conn.commit()
    conn.close()

    _refresh_project_documents(pid)

    await emit(pid, "project.created", {"title": payload.title})
    await emit(pid, "project.scope_initialized", {"project_root": project_root})
    _append_project_daily_log(
        owner_user_id=user_id,
        project_root=project_root,
        kind="project.created",
        text=f"Project created: {payload.title}",
        payload={"brief": payload.brief[:500], "goal": payload.goal[:500]},
    )
    return ProjectOut(
        id=pid,
        title=payload.title,
        brief=payload.brief,
        goal=payload.goal,
        connection_id=payload.connection_id,
        created_at=now,
        workspace_root=workspace_root,
        project_root=project_root,
        setup_details=setup_details,
        plan_status=PLAN_STATUS_PENDING,
        plan_text="",
        plan_updated_at=int(time.time()),
        plan_approved_at=None,
        execution_status=EXEC_STATUS_IDLE,
        progress_pct=0,
        execution_updated_at=int(time.time()),
        usage_prompt_tokens=0,
        usage_completion_tokens=0,
        usage_total_tokens=0,
        usage_updated_at=int(time.time()),
    )

@app.get("/api/projects", response_model=List[ProjectOut])
async def list_projects(request: Request):
    user_id = get_session_user(request)
    conn = db()
    rows = conn.execute(
        """
        SELECT id, title, brief, goal, connection_id, created_at, workspace_root, project_root, setup_json,
               plan_text, plan_status, plan_updated_at, plan_approved_at,
               execution_status, progress_pct, execution_updated_at,
               usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
        FROM projects
        WHERE user_id = ?
        ORDER BY created_at DESC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    return [_project_out_from_row(r) for r in rows]

@app.get("/api/projects/{project_id}", response_model=ProjectOut)
async def get_project(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        """
        SELECT id, title, brief, goal, connection_id, created_at, workspace_root, project_root, setup_json,
               plan_text, plan_status, plan_updated_at, plan_approved_at,
               execution_status, progress_pct, execution_updated_at,
               usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
        FROM projects
        WHERE id = ? AND user_id = ?
        """,
        (project_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Project not found")
    return _project_out_from_row(row)

@app.delete("/api/projects/{project_id}")
async def delete_project(request: Request, project_id: str):
    user_id = get_session_user(request)
    deleted = _delete_project_with_resources(owner_user_id=user_id, project_id=project_id)
    if not deleted.get("ok"):
        raise HTTPException(404, "Project not found")
    return deleted

@app.get("/api/projects/{project_id}/plan", response_model=ProjectPlanOut)
async def get_project_plan(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT id, plan_status, plan_text, plan_updated_at, plan_approved_at FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Project not found")
    return ProjectPlanOut(
        project_id=project_id,
        status=_coerce_plan_status(row["plan_status"]),
        text=str(row["plan_text"] or ""),
        updated_at=row["plan_updated_at"],
        approved_at=row["plan_approved_at"],
    )

@app.post("/api/projects/{project_id}/plan/regenerate", response_model=ProjectPlanOut)
async def regenerate_project_plan(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT id, plan_text FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Project not found")
    now = int(time.time())
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_GENERATING, now, project_id),
    )
    conn.commit()
    conn.close()
    asyncio.create_task(_generate_project_plan(project_id, force=True))
    await emit(project_id, "project.plan.regenerate_requested", {"project_id": project_id})
    return ProjectPlanOut(
        project_id=project_id,
        status=PLAN_STATUS_GENERATING,
        text=str(row["plan_text"] or ""),
        updated_at=now,
        approved_at=None,
    )

@app.post("/api/projects/{project_id}/plan/approve", response_model=ProjectPlanOut)
async def approve_project_plan(request: Request, project_id: str, payload: ProjectPlanApproveIn):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        """
        SELECT id, user_id, title, brief, goal, project_root, setup_json, plan_text, plan_status, plan_updated_at, plan_approved_at
        FROM projects
        WHERE id = ? AND user_id = ?
        """,
        (project_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Project not found")

    now = int(time.time())
    if payload.approve:
        new_status = PLAN_STATUS_APPROVED
        approved_at = now
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_approved_at = ?, plan_updated_at = ? WHERE id = ?",
            (new_status, approved_at, now, project_id),
        )
        conn.execute(
            "UPDATE projects SET execution_status = ?, progress_pct = ?, execution_updated_at = ? WHERE id = ?",
            (EXEC_STATUS_RUNNING, 5, now, project_id),
        )
    else:
        new_status = PLAN_STATUS_AWAITING_APPROVAL
        approved_at = row["plan_approved_at"]
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
            (new_status, now, project_id),
        )
        conn.execute(
            "UPDATE projects SET execution_status = ?, execution_updated_at = ? WHERE id = ?",
            (EXEC_STATUS_IDLE, now, project_id),
        )
    conn.commit()
    conn.close()

    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="plan.approve" if payload.approve else "plan.revert",
        text="User approved project plan." if payload.approve else "User reverted plan to waiting approval.",
    )

    if payload.approve:
        await emit(project_id, "project.plan.approved", {"project_id": project_id})
        asyncio.create_task(_delegate_project_tasks(project_id))
    else:
        await emit(project_id, "project.plan.awaiting_approval", {"project_id": project_id})

    return ProjectPlanOut(
        project_id=project_id,
        status=new_status,
        text=str(row["plan_text"] or ""),
        updated_at=now,
        approved_at=approved_at,
    )

@app.get("/api/projects/{project_id}/workspace/tree", response_model=ProjectWorkspaceTreeOut)
async def get_project_workspace_tree(request: Request, project_id: str):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    _ensure_user_workspace(owner_user_id)
    project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or ""))
    project_dir.mkdir(parents=True, exist_ok=True)
    return ProjectWorkspaceTreeOut(
        project_id=project_id,
        project_root=project_dir.as_posix(),
        tree=_render_tree(project_dir),
        access_mode=access["mode"],
    )

@app.get("/api/projects/{project_id}/files", response_model=ProjectFilesOut)
async def list_project_files(request: Request, project_id: str, path: str = ""):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or "")).resolve()
    target = _resolve_project_relative_path(
        owner_user_id,
        str(project.get("project_root") or ""),
        path,
        require_exists=True,
        require_dir=True,
    ).resolve()
    current_rel = ""
    if target != project_dir:
        current_rel = target.relative_to(project_dir).as_posix()
    parent_rel: Optional[str] = None
    if current_rel:
        parent_rel = str(Path(current_rel).parent).replace("\\", "/")
        if parent_rel == ".":
            parent_rel = ""

    entries: List[ProjectFileEntryOut] = []
    for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        rel = child.relative_to(project_dir).as_posix()
        stat = child.stat()
        entries.append(
            ProjectFileEntryOut(
                name=child.name,
                path=rel,
                kind="dir" if child.is_dir() else "file",
                size=None if child.is_dir() else int(stat.st_size),
                modified_at=int(stat.st_mtime),
            )
        )

    return ProjectFilesOut(
        project_id=project_id,
        project_root=project_dir.as_posix(),
        current_path=current_rel,
        parent_path=parent_rel,
        access_mode=access["mode"],
        entries=entries,
    )

@app.get("/api/projects/{project_id}/files/content", response_model=ProjectFileContentOut)
async def read_project_file(request: Request, project_id: str, path: str):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    project_dir = _resolve_owner_project_dir(owner_user_id, str(project.get("project_root") or "")).resolve()
    target = _resolve_project_relative_path(
        owner_user_id,
        str(project.get("project_root") or ""),
        path,
        require_exists=True,
        require_dir=False,
    ).resolve()
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    data = target.read_bytes()
    size = len(data)
    truncated = size > MAX_FILE_PREVIEW_BYTES
    if truncated:
        data = data[:MAX_FILE_PREVIEW_BYTES]
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError:
        content = data.decode("utf-8", errors="replace")
    rel = target.relative_to(project_dir).as_posix()
    return ProjectFileContentOut(
        project_id=project_id,
        path=rel,
        size=size,
        truncated=truncated,
        content=content,
    )

@app.get("/api/projects/{project_id}/files/raw")
async def read_project_file_raw(request: Request, project_id: str, path: str):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    project_root = str(project.get("project_root") or "")
    target = _resolve_project_relative_path(
        owner_user_id,
        project_root,
        path,
        require_exists=True,
        require_dir=False,
    ).resolve()
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    guessed, _ = mimetypes.guess_type(target.name)
    media_type = guessed or "application/octet-stream"
    return FileResponse(str(target), media_type=media_type)

@app.get("/api/projects/{project_id}/preview/{path:path}")
async def preview_project_file(request: Request, project_id: str, path: str):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    project_root = str(project.get("project_root") or "")
    target = _resolve_project_relative_path(
        owner_user_id,
        project_root,
        path,
        require_exists=True,
        require_dir=False,
    ).resolve()
    if target.is_dir():
        raise HTTPException(400, "Path is a directory")
    guessed, _ = mimetypes.guess_type(target.name)
    media_type = guessed or "application/octet-stream"
    return FileResponse(str(target), media_type=media_type)

@app.post("/api/projects/{project_id}/files/write")
async def write_project_file(request: Request, project_id: str, payload: ProjectFileWriteIn):
    access = _resolve_project_workspace_access(request, project_id)
    project = access["project"]
    owner_user_id = str(project["user_id"])
    project_root = str(project.get("project_root") or "")
    project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    target = _resolve_project_relative_path(
        owner_user_id,
        project_root,
        payload.path,
        require_exists=False,
        require_dir=False,
    ).resolve()
    if target.exists() and target.is_dir():
        raise HTTPException(400, "Target path is a directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if bool(payload.append) else "w"
    content = str(payload.content or "")
    with target.open(mode, encoding="utf-8") as f:
        f.write(content)
    rel = target.relative_to(project_dir).as_posix()
    actor = f"owner:{access.get('user_id')}" if access.get("mode") == "owner" else f"agent:{access.get('agent_id')}"
    _append_project_daily_log(
        owner_user_id=owner_user_id,
        project_root=project_root,
        kind="file.write",
        text=f"{actor} wrote {rel}",
        payload={"mode": mode, "bytes": len(content.encode('utf-8'))},
    )
    await emit(
        project_id,
        "project.file.written",
        {"path": rel, "mode": mode, "bytes": len(content.encode("utf-8")), "actor": actor},
    )
    return {"ok": True, "path": rel, "mode": mode, "bytes": len(content.encode("utf-8"))}

@app.get("/api/projects/{project_id}/usage", response_model=ProjectUsageOut)
async def get_project_usage(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        """
        SELECT usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at
        FROM projects
        WHERE id = ? AND user_id = ?
        """,
        (project_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Project not found")
    return ProjectUsageOut(
        project_id=project_id,
        prompt_tokens=max(0, _to_int(row["usage_prompt_tokens"])),
        completion_tokens=max(0, _to_int(row["usage_completion_tokens"])),
        total_tokens=max(0, _to_int(row["usage_total_tokens"])),
        updated_at=row["usage_updated_at"],
    )

@app.get("/api/projects/{project_id}/execution", response_model=ProjectExecutionOut)
async def get_project_execution(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    row = conn.execute(
        "SELECT execution_status, progress_pct, execution_updated_at FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Project not found")
    return ProjectExecutionOut(
        project_id=project_id,
        status=_coerce_execution_status(row["execution_status"]),
        progress_pct=_clamp_progress(row["progress_pct"]),
        updated_at=row["execution_updated_at"],
    )

@app.post("/api/projects/{project_id}/execution/control", response_model=ProjectExecutionOut)
async def control_project_execution(request: Request, project_id: str, payload: ProjectExecutionControlIn):
    user_id = get_session_user(request)
    action = str(payload.action or "").strip().lower()
    if action not in {"pause", "resume", "stop"}:
        raise HTTPException(400, "Invalid action. Use pause, resume, or stop.")

    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.workspace_root,
               p.plan_status, p.execution_status, p.progress_pct, p.connection_id,
               c.base_url, c.api_key, cp.main_agent_id
        FROM projects p
        JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ? AND p.user_id = ?
        """,
        (project_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Project not found")
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()

    current_status = _coerce_execution_status(row["execution_status"])
    current_progress = _clamp_progress(row["progress_pct"])
    new_status = current_status
    new_progress = current_progress
    if action == "pause" and current_status not in {EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}:
        new_status = EXEC_STATUS_PAUSED
    elif action == "resume" and current_status != EXEC_STATUS_COMPLETED:
        new_status = EXEC_STATUS_RUNNING
        if new_progress <= 0:
            new_progress = 5
    elif action == "stop":
        new_status = EXEC_STATUS_STOPPED

    state = _set_project_execution_state(project_id, status=new_status, progress_pct=new_progress)
    _refresh_project_documents(project_id)

    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None

    control_summary = f"Execution action `{action}` accepted."
    if primary_agent_id:
        command_text = {
            "pause": "Owner pressed PAUSE. Pause current execution and wait for resume instruction.",
            "resume": "Owner pressed RESUME. Continue execution from latest checkpoint.",
            "stop": "Owner pressed STOP. Stop all ongoing execution immediately and report final status.",
        }[action]
        instruction = _project_context_instruction(
            title=str(row["title"] or ""),
            brief=str(row["brief"] or ""),
            goal=str(row["goal"] or ""),
            setup_details=_parse_setup_json(row["setup_json"]),
            role_rows=role_rows,
            plan_status=_coerce_plan_status(row["plan_status"]),
        )
        scoped_message = _compose_guardrailed_message(
            command_text,
            workspace_root=str(row["workspace_root"] or _user_workspace_root_dir(user_id).as_posix()),
            project_root=str(row["project_root"] or ""),
            task_instruction=instruction,
        )
        ctrl_res = await openclaw_ws_chat(
            base_url=str(row["base_url"]),
            api_key=str(row["api_key"]),
            message=scoped_message,
            agent_id=primary_agent_id,
            session_key=f"{project_id}:control",
            timeout_sec=25,
        )
        if ctrl_res.get("ok"):
            control_summary = str(ctrl_res.get("text") or control_summary)[:1000]
            ptk, ctk, _ = _extract_usage_counts(ctrl_res)
            if ptk <= 0:
                ptk = _estimate_tokens_from_text(scoped_message)
            if ctk <= 0:
                ctk = _estimate_tokens_from_text(ctrl_res.get("text"))
            _update_project_usage_metrics(project_id, prompt_tokens=ptk, completion_tokens=ctk)
            _refresh_project_documents(project_id)
        else:
            control_summary = f"Action saved, but agent ack failed: {detail_to_text(ctrl_res.get('error') or ctrl_res.get('details'))[:700]}"

    _append_project_daily_log(
        owner_user_id=user_id,
        project_root=str(row["project_root"] or ""),
        kind=f"execution.{action}",
        text=control_summary,
        payload={"status": new_status, "progress_pct": new_progress},
    )
    await emit(
        project_id,
        f"project.execution.{action}",
        {"status": new_status, "progress_pct": new_progress, "summary": control_summary},
    )
    final = state or {
        "status": new_status,
        "progress_pct": new_progress,
        "updated_at": int(time.time()),
    }
    return ProjectExecutionOut(
        project_id=project_id,
        status=_coerce_execution_status(final.get("status")),
        progress_pct=_clamp_progress(final.get("progress_pct")),
        updated_at=final.get("updated_at"),
    )

@app.post("/api/projects/{project_id}/agents")
async def set_project_agents(request: Request, project_id: str, payload: ProjectAgentsIn):
    user_id = get_session_user(request)
    if len(payload.agent_ids) != len(payload.agent_names):
        raise HTTPException(400, "agent_ids and agent_names must have same length")
    role_values = payload.agent_roles or []
    if role_values and len(role_values) != len(payload.agent_ids):
        raise HTTPException(400, "agent_roles must have same length as agent_ids when provided")
    if not payload.agent_ids:
        raise HTTPException(400, "Select at least one agent")
    if len(set(payload.agent_ids)) != len(payload.agent_ids):
        raise HTTPException(400, "agent_ids must be unique")
    if payload.primary_agent_id and payload.primary_agent_id not in payload.agent_ids:
        raise HTTPException(400, "primary_agent_id must be one of selected agent_ids")

    conn = db()
    proj = conn.execute(
        "SELECT id, project_root, title, brief, goal, setup_json, plan_text, plan_status FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    if not proj:
        conn.close()
        raise HTTPException(404, "Project not found")

    conn.execute("DELETE FROM project_agents WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM project_agent_access_tokens WHERE project_id = ?", (project_id,))
    primary_id = payload.primary_agent_id or payload.agent_ids[0]
    now = int(time.time())
    issued_tokens: List[Dict[str, Any]] = []
    role_map: Dict[str, str] = {}
    for idx, (aid, name) in enumerate(zip(payload.agent_ids, payload.agent_names)):
        role = str(role_values[idx]).strip()[:500] if idx < len(role_values) else ""
        role_map[aid] = role
        conn.execute(
            "INSERT INTO project_agents (project_id, agent_id, agent_name, is_primary, role) VALUES (?,?,?,?,?)",
            (project_id, aid, name, 1 if aid == primary_id else 0, role),
        )
        raw_token = _new_agent_access_token()
        conn.execute(
            "INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, created_at) VALUES (?,?,?,?)",
            (project_id, aid, _hash_access_token(raw_token), now),
        )
        issued_tokens.append(
            {
                "agent_id": aid,
                "agent_name": name,
                "token": raw_token,
                "is_primary": aid == primary_id,
                "role": role,
            }
        )
    conn.commit()
    conn.close()

    _write_project_agent_roles_file(
        owner_user_id=user_id,
        project_root=str(proj["project_root"] or ""),
        agents=[
            {
                "agent_id": aid,
                "agent_name": name,
                "role": role_map.get(aid, ""),
                "is_primary": aid == primary_id,
            }
            for aid, name in zip(payload.agent_ids, payload.agent_names)
        ],
    )
    _refresh_project_documents(project_id)

    _append_project_daily_log(
        owner_user_id=user_id,
        project_root=str(proj["project_root"] or ""),
        kind="agents.updated",
        text=f"Invited agents updated. Primary agent: {primary_id}.",
        payload={"count": len(payload.agent_ids)},
    )
    await emit(project_id, "project.agents_set", {"count": len(payload.agent_ids), "primary_agent_id": primary_id})
    asyncio.create_task(_generate_project_plan(project_id, force=True))
    await emit(project_id, "project.plan.regenerate_requested", {"project_id": project_id, "source": "agents_set"})
    return {"ok": True, "primary_agent_id": primary_id, "agent_access_tokens": issued_tokens}

@app.get("/api/projects/{project_id}/agents")
async def get_project_agents(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    proj = conn.execute(
        "SELECT id FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    if not proj:
        conn.close()
        raise HTTPException(404, "Project not found")
    rows = conn.execute(
        "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
        (project_id,),
    ).fetchall()
    conn.close()
    agents = [
        {
            "id": r["agent_id"],
            "name": r["agent_name"],
            "is_primary": bool(r["is_primary"]),
            "role": str(r["role"] or ""),
        }
        for r in rows
    ]
    primary = next((a for a in agents if a["is_primary"]), None)
    return {"ok": True, "agents": agents, "primary_agent": primary}

def _read_project_execution_state(project_id: str) -> Tuple[str, int]:
    conn = db()
    row = conn.execute(
        "SELECT execution_status, progress_pct FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    conn.close()
    if not row:
        return EXEC_STATUS_IDLE, 0
    return _coerce_execution_status(row["execution_status"]), _clamp_progress(row["progress_pct"])

@app.post("/api/projects/{project_id}/run")
async def run_project(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    proj = conn.execute(
        """
        SELECT id, user_id, title, brief, goal, plan_status, project_root, execution_status, progress_pct
        FROM projects WHERE id = ? AND user_id = ?
        """,
        (project_id, user_id),
    ).fetchone()
    agents = conn.execute(
        "SELECT agent_id, agent_name, is_primary FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
        (project_id,),
    ).fetchall()
    conn.close()
    if not proj:
        raise HTTPException(404, "Project not found")
    if _coerce_plan_status(proj["plan_status"]) != PLAN_STATUS_APPROVED:
        raise HTTPException(400, "Project plan must be approved first")
    if not agents:
        raise HTTPException(400, "No agents assigned to this project yet")

    current_progress = _clamp_progress(proj["progress_pct"])
    start_progress = max(10, current_progress)
    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=start_progress)
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(proj["user_id"]),
        project_root=str(proj["project_root"] or ""),
        kind="run.started",
        text="User started project execution run.",
        payload={"agents": len(agents)},
    )
    asyncio.create_task(simulate_run(project_id, dict(proj), [dict(a) for a in agents]))
    return {"ok": True}

async def simulate_run(project_id: str, project: Dict[str, Any], agents: List[Dict[str, Any]]) -> None:
    primary = next((a["agent_name"] for a in agents if a.get("is_primary")), None)
    await emit(
        project_id,
        "run.started",
        {"project": project["title"], "agents": [a["agent_name"] for a in agents], "primary_agent": primary},
    )
    await asyncio.sleep(0.6)
    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=15)
    _refresh_project_documents(project_id)

    steps = [
        ("Planner", "I will break down the brief into steps and assign roles."),
        ("Researcher", "I'll research constraints and best practices, then summarize."),
        ("Builder", "I'll draft an implementation plan / code skeleton."),
        ("Critic", "I'll point out risks, gaps, and propose improvements."),
    ]

    chosen = []
    for i, a in enumerate(agents):
        role = steps[i % len(steps)][0]
        chosen.append((role, a["agent_name"]))

    for i, (role, agent_name) in enumerate(chosen, start=1):
        while True:
            state, _ = _read_project_execution_state(project_id)
            if state == EXEC_STATUS_PAUSED:
                await asyncio.sleep(0.7)
                continue
            if state == EXEC_STATUS_STOPPED:
                _refresh_project_documents(project_id)
                _append_project_daily_log(
                    owner_user_id=str(project.get("user_id") or ""),
                    project_root=str(project.get("project_root") or ""),
                    kind="run.stopped",
                    text="Execution stopped before completion.",
                )
                await emit(project_id, "run.stopped", {"summary": "Execution stopped by user."})
                return
            break
        await emit(project_id, "agent.message", {"agent": agent_name, "role": role, "text": steps[(i - 1) % len(steps)][1]})
        pct = min(95, 15 + int((i / max(1, len(chosen))) * 75))
        _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=pct)
        _refresh_project_documents(project_id)
        await asyncio.sleep(0.9)

    _set_project_execution_state(project_id, status=EXEC_STATUS_COMPLETED, progress_pct=100)
    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=str(project.get("user_id") or ""),
        project_root=str(project.get("project_root") or ""),
        kind="run.completed",
        text="Execution completed.",
    )
    await emit(project_id, "run.completed", {"summary": "Prototype run completed. Next: wire this to real OpenClaw endpoints once you confirm API paths."})

@app.get("/api/projects/{project_id}/events")
async def project_events(request: Request, project_id: str):
    user_id = get_session_user(request)
    conn = db()
    proj = conn.execute(
        "SELECT id FROM projects WHERE id = ? AND user_id = ?",
        (project_id, user_id),
    ).fetchone()
    conn.close()
    if not proj:
        raise HTTPException(404, "Project not found")

    q = get_queue(project_id)

    async def event_generator():
        yield "event: hello\ndata: {}\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                ev = await asyncio.wait_for(q.get(), timeout=10)
                payload = {"ts": ev.ts, "kind": ev.kind, "data": ev.data}
                yield f"event: {ev.kind}\ndata: {json.dumps(payload)}\n\n"
            except asyncio.TimeoutError:
                yield "event: ping\ndata: {}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
