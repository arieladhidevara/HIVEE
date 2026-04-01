from .constants import *

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
            source_type TEXT NOT NULL DEFAULT 'owner',
            source_user_id TEXT,
            source_connection_id TEXT,
            joined_via_invite_id TEXT,
            added_at INTEGER,
            PRIMARY KEY(project_id, agent_id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_agent_permissions (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            can_chat_project INTEGER NOT NULL DEFAULT 1,
            can_read_files INTEGER NOT NULL DEFAULT 1,
            can_write_files INTEGER NOT NULL DEFAULT 1,
            write_paths_json TEXT NOT NULL DEFAULT '[]',
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(project_id, agent_id),
            FOREIGN KEY(project_id, agent_id) REFERENCES project_agents(project_id, agent_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_project_agent_permissions_project ON project_agent_permissions(project_id)")
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
        CREATE TABLE IF NOT EXISTS project_external_agent_invites (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            owner_user_id TEXT NOT NULL,
            target_email TEXT,
            requested_agent_id TEXT,
            requested_agent_name TEXT,
            role TEXT NOT NULL DEFAULT '',
            invite_note TEXT,
            token_hash TEXT NOT NULL UNIQUE,
            invite_doc_relpath TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            expires_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            accepted_at INTEGER,
            accepted_by_user_id TEXT,
            accepted_connection_id TEXT,
            accepted_agent_id TEXT,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            FOREIGN KEY(owner_user_id) REFERENCES users(id),
            FOREIGN KEY(accepted_by_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_project_external_agent_invites_project ON project_external_agent_invites(project_id, status, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_project_external_agent_invites_expiry ON project_external_agent_invites(expires_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_external_agent_memberships (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            owner_user_id TEXT NOT NULL,
            member_user_id TEXT NOT NULL,
            member_connection_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            invite_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(project_id, member_user_id, member_connection_id, agent_id),
            FOREIGN KEY(project_id) REFERENCES projects(id),
            FOREIGN KEY(owner_user_id) REFERENCES users(id),
            FOREIGN KEY(member_user_id) REFERENCES users(id),
            FOREIGN KEY(member_connection_id) REFERENCES openclaw_connections(id),
            FOREIGN KEY(invite_id) REFERENCES project_external_agent_invites(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_project_external_agent_memberships_project ON project_external_agent_memberships(project_id, status, updated_at DESC)")
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
        CREATE TABLE IF NOT EXISTS environment_agent_links (
            id TEXT PRIMARY KEY,
            env_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'active',
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            revoked_at INTEGER,
            last_used_at INTEGER,
            FOREIGN KEY(env_id) REFERENCES environments(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_environment_agent_links_env_agent ON environment_agent_links(env_id, agent_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_environment_agent_links_expires_at ON environment_agent_links(expires_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agents (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            card_version TEXT NOT NULL DEFAULT '1.0',
            card_json TEXT NOT NULL,
            root_path TEXT NOT NULL,
            provisioned_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(connection_id) REFERENCES openclaw_connections(id),
            FOREIGN KEY(env_id) REFERENCES environments(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_memory (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            memory_scope TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id, memory_scope)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_history (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            event_text TEXT NOT NULL DEFAULT '',
            event_payload_json TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_checkpoints (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            checkpoint_key TEXT NOT NULL,
            state_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'ready',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id, checkpoint_key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_permissions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            scopes_json TEXT NOT NULL DEFAULT '[]',
            tools_json TEXT NOT NULL DEFAULT '[]',
            path_allowlist_json TEXT NOT NULL DEFAULT '[]',
            secrets_policy_json TEXT NOT NULL DEFAULT '{}',
            approval_required INTEGER NOT NULL DEFAULT 1,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_metrics (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            success_count INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            total_calls INTEGER NOT NULL DEFAULT 0,
            total_prompt_tokens INTEGER NOT NULL DEFAULT 0,
            total_completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_latency_ms INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_seen_at INTEGER,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_agent_approval_rules (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            env_id TEXT,
            connection_id TEXT,
            agent_id TEXT NOT NULL,
            rule_key TEXT NOT NULL,
            policy_json TEXT NOT NULL DEFAULT '{}',
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, connection_id, agent_id, rule_key)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_agents_user ON managed_agents(user_id, updated_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_agents_lookup ON managed_agents(user_id, agent_id, connection_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_agent_history_lookup ON managed_agent_history(user_id, agent_id, connection_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_agent_memory_lookup ON managed_agent_memory(user_id, agent_id, connection_id, memory_scope)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_agent_checkpoints_lookup ON managed_agent_checkpoints(user_id, agent_id, connection_id, checkpoint_key)")
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
    if "source_type" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN source_type TEXT NOT NULL DEFAULT 'owner'")
    if "source_user_id" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN source_user_id TEXT")
    if "source_connection_id" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN source_connection_id TEXT")
    if "joined_via_invite_id" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN joined_via_invite_id TEXT")
    if "added_at" not in cols:
        cur.execute("ALTER TABLE project_agents ADD COLUMN added_at INTEGER")
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


__all__ = [name for name in globals() if not name.startswith('__')]

