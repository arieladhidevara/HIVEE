from services.managed_agents import *

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

class AccountOAuthProvidersOut(BaseModel):
    providers: List[str]

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

class ProjectExternalAgentInviteCreateIn(BaseModel):
    target_email: Optional[str] = None
    requested_agent_id: Optional[str] = None
    requested_agent_name: Optional[str] = None
    role: str = ""
    note: str = ""
    expires_in_sec: int = PROJECT_EXTERNAL_INVITE_TTL_SEC

class ProjectExternalAgentInviteAcceptIn(BaseModel):
    connection_id: str
    agent_id: Optional[str] = None
    agent_name: Optional[str] = None

class OpenClawChatIn(BaseModel):
    message: str = Field(..., min_length=1)
    agent_id: Optional[str] = None

class OpenClawWsChatIn(BaseModel):
    message: str = Field(..., min_length=1)
    agent_id: Optional[str] = None
    context_mode: str = Field("auto", description="auto | workspace | project")
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

class ProjectReadinessCheckOut(BaseModel):
    key: str
    label: str
    ok: bool
    required: bool = True
    cta: Optional[str] = None

class ProjectReadinessOut(BaseModel):
    project_id: str
    stage: str
    can_chat_project: bool
    can_run: bool
    invited_agents_count: int
    primary_agent_id: Optional[str] = None
    checks: List[ProjectReadinessCheckOut] = Field(default_factory=list)
    summary: str = ""

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
    agent_id: Optional[str] = Field(None, min_length=1)
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
    agent_provision: Optional[Dict[str, Any]] = None

class A2AEnvironmentHandoffAckOut(BaseModel):
    ok: bool
    environment_id: str
    status: str
    revoked_sessions: int
    link_token: str
    link_token_expires_at: int

class A2AAgentLinkSessionStartIn(BaseModel):
    link_token: str = Field(..., min_length=8)
    session_ttl_sec: int = ENV_AGENT_RUNTIME_SESSION_TTL_SEC

class A2AAgentLinkSessionStartOut(BaseModel):
    ok: bool
    environment_id: str
    agent_id: str
    session_token: str
    session_expires_at: int
    scopes: List[str]

class A2AEnvironmentHandoffWaitOut(BaseModel):
    ok: bool
    environment_id: str
    event: str
    claimed: bool
    status: str
    owner_user_id: Optional[str] = None
    claimed_at: Optional[int] = None
    waited_ms: int

__all__ = [name for name in globals() if not name.startswith('__')]
