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


class ConnectionCreateIn(BaseModel):
    label: str = Field(..., min_length=1, max_length=160)
    runtime_type: str = Field(CONNECTION_RUNTIME_OPENCLAW, description="Runtime adapter key, openclaw first")
    token_ttl_sec: int = PROJECT_INSTALL_TOKEN_DEFAULT_TTL_SEC

class HubInstallCompleteIn(BaseModel):
    install_token: str = Field(..., min_length=8)
    machine_name: Optional[str] = None
    os_type: Optional[str] = None
    hub_version: Optional[str] = None

class HubHeartbeatIn(BaseModel):
    connection_id: str = Field(..., min_length=1)
    install_token: str = Field(..., min_length=8)
    hub_status: Optional[str] = None
    machine_name: Optional[str] = None
    os_type: Optional[str] = None
    hub_version: Optional[str] = None

class HubDiscoveredAgentIn(BaseModel):
    runtime_agent_id: str = Field(..., min_length=1)
    agent_name: Optional[str] = None
    status: str = "online"
    agent_card_json: Optional[Dict[str, Any]] = None

class HubAgentsDiscoveredIn(BaseModel):
    connection_id: str = Field(..., min_length=1)
    install_token: str = Field(..., min_length=8)
    agents: List[HubDiscoveredAgentIn] = Field(default_factory=list)

class HubAgentCardIn(BaseModel):
    connection_id: str = Field(..., min_length=1)
    install_token: str = Field(..., min_length=8)
    agent_card_json: Dict[str, Any] = Field(default_factory=dict)
    agent_card_version: Optional[str] = None

class HubRuntimeJobClaimIn(BaseModel):
    connection_id: str = Field(..., min_length=1)
    install_token: str = Field(..., min_length=8)
    max_wait_sec: int = 1

class HubRuntimeJobCompleteIn(BaseModel):
    connection_id: str = Field(..., min_length=1)
    install_token: str = Field(..., min_length=8)
    status: str = RUNTIME_DISPATCH_STATUS_COMPLETED
    result_text: Optional[str] = None
    error_text: Optional[str] = None

class ProjectJoinByKeyIn(BaseModel):
    project_api_key: str = Field(..., min_length=8)

class ProjectAttachManagedAgentIn(BaseModel):
    managed_agent_id: str = Field(..., min_length=1)
    role: str = "member"
    is_primary: bool = False

class ProjectChannelMessageCreateIn(BaseModel):
    body: str = Field(..., min_length=1)
    task_id: Optional[str] = None
    target_managed_agent_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class ProjectChannelCreateIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    description: str = ""

class ProjectCreateIn(BaseModel):
    title: Optional[str] = None
    brief: Optional[str] = None
    goal: str = Field(..., min_length=1)
    connection_id: Optional[str] = None
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
    status: str = "active"
    created_via: str = PROJECT_CREATED_VIA_NEW
    owner_user_id: Optional[str] = None
    project_api_key: Optional[str] = None
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

class ProjectExternalAgentInviteSelectedAgentIn(BaseModel):
    agent_id: str
    agent_name: Optional[str] = None
    role: Optional[str] = None

class ProjectExternalAgentInviteAcceptIn(BaseModel):
    connection_id: str
    agent_id: Optional[str] = None
    agent_name: Optional[str] = None
    invite_code: Optional[str] = None
    selected_agents: Optional[List[ProjectExternalAgentInviteSelectedAgentIn]] = None

class ProjectAgentPermissionsUpdateIn(BaseModel):
    can_chat_project: Optional[bool] = None
    can_read_files: Optional[bool] = None
    can_write_files: Optional[bool] = None
    write_paths: Optional[List[str]] = None
    reset_to_default: bool = False

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

class A2AEnvironmentOpenClawStageIn(BaseModel):
    openclaw_base_url: Optional[str] = Field(None, min_length=8)
    openclaw_ws_url: Optional[str] = Field(None, min_length=8)
    openclaw_api_key: Optional[str] = Field(None, min_length=1)
    openclaw_auth_token: Optional[str] = Field(None, min_length=1)
    openclaw_name: Optional[str] = None
    source: Optional[str] = None
    claim_ttl_sec: int = ENV_CLAIM_CODE_TTL_SEC
    stage_ttl_sec: int = ENV_OPENCLAW_STAGE_TTL_SEC

class A2AEnvironmentOpenClawStageOut(BaseModel):
    ok: bool
    environment_id: str
    agent_id: str
    staged: bool
    openclaw_base_url: str
    openclaw_ws_url: Optional[str] = None
    openclaw_name: Optional[str] = None
    source: Optional[str] = None
    stage_expires_at: int
    claim_url: str
    claim_code_expires_at: int
    message: str

class A2AEnvironmentClaimContextOut(BaseModel):
    ok: bool
    environment_id: str
    claim_valid: bool
    claim_expires_at: Optional[int] = None
    staged_openclaw_ready: bool
    staged_openclaw_name: Optional[str] = None
    staged_openclaw_base_url: Optional[str] = None
    staged_openclaw_ws_url: Optional[str] = None
    requires_manual_openclaw: bool
    message: str = ""

class A2AEnvironmentClaimCompleteIn(BaseModel):
    environment_id: str = Field(..., min_length=1)
    code: str = Field(..., min_length=1)
    mode: str = Field("signup", description="signup | login | session")
    email: Optional[str] = Field(None, min_length=3)
    password: Optional[str] = Field(None, min_length=PASSWORD_MIN_LENGTH)
    openclaw_base_url: Optional[str] = Field(None, min_length=8)
    openclaw_ws_url: Optional[str] = Field(None, min_length=8)
    openclaw_api_key: Optional[str] = Field(None, min_length=1)
    openclaw_name: Optional[str] = None

class A2AEnvironmentClaimCompleteOut(BaseModel):
    token: str
    environment_id: str
    status: str
    user_id: str
    email: str
    connection_id: str
    connection_name: Optional[str] = None
    connection_source: Optional[str] = None
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

class ProjectTaskCreateIn(BaseModel):
    title: str = Field(..., min_length=1, max_length=TASK_TITLE_MAX_CHARS)
    description: str = Field("", max_length=TASK_DESCRIPTION_MAX_CHARS)
    status: str = Field(TASK_STATUS_TODO)
    priority: str = Field(TASK_PRIORITY_MEDIUM)
    assignee_agent_id: Optional[str] = None
    due_at: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None

class ProjectTaskUpdateIn(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=TASK_TITLE_MAX_CHARS)
    description: Optional[str] = Field(None, max_length=TASK_DESCRIPTION_MAX_CHARS)
    status: Optional[str] = None
    priority: Optional[str] = None
    assignee_agent_id: Optional[str] = None
    clear_assignee: bool = False
    due_at: Optional[int] = None
    clear_due_at: bool = False
    metadata: Optional[Dict[str, Any]] = None

class ProjectTaskCheckoutIn(BaseModel):
    ttl_sec: int = TASK_CHECKOUT_DEFAULT_TTL_SEC
    note: str = Field("", max_length=300)
    force: bool = False

class ProjectTaskReleaseIn(BaseModel):
    force: bool = False
    reason: str = Field("", max_length=300)

class ProjectTaskCheckoutOut(BaseModel):
    owner_type: str
    owner_id: str
    owner_label: Optional[str] = None
    note: str = ""
    checked_out_at: int
    expires_at: int
    is_active: bool = True

class ProjectTaskOut(BaseModel):
    id: str
    project_id: str
    created_by_user_id: Optional[str] = None
    created_by_agent_id: Optional[str] = None
    title: str
    description: str = ""
    status: str = TASK_STATUS_TODO
    priority: str = TASK_PRIORITY_MEDIUM
    assignee_agent_id: Optional[str] = None
    due_at: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: int
    updated_at: int
    closed_at: Optional[int] = None
    checkout: Optional[ProjectTaskCheckoutOut] = None

class ProjectTaskCommentCreateIn(BaseModel):
    body: str = Field(..., min_length=1, max_length=TASK_COMMENT_MAX_CHARS)

class ProjectTaskCommentUpdateIn(BaseModel):
    body: str = Field(..., min_length=1, max_length=TASK_COMMENT_MAX_CHARS)

class ProjectTaskCommentOut(BaseModel):
    id: str
    task_id: str
    project_id: str
    author_type: str
    author_id: Optional[str] = None
    author_label: Optional[str] = None
    body: str
    created_at: int
    updated_at: int

class ProjectTaskDependencyCreateIn(BaseModel):
    depends_on_task_id: str = Field(..., min_length=1)

class ProjectTaskDependencyOut(BaseModel):
    task_id: str
    depends_on_task_id: str
    depends_on_title: str = ""
    depends_on_status: str = TASK_STATUS_TODO
    created_at: int

class ProjectTaskBlueprintOut(BaseModel):
    id: str
    name: str
    description: str = ""
    tasks_count: int = 0

class ProjectTaskBlueprintApplyIn(BaseModel):
    blueprint_id: str = Field(..., min_length=1)
    assignee_agent_id: Optional[str] = None
    title_prefix: str = Field("", max_length=80)
    include_dependencies: bool = True

class ProjectTaskBlueprintApplyOut(BaseModel):
    ok: bool
    project_id: str
    blueprint_id: str
    created_task_ids: List[str] = Field(default_factory=list)
    created_count: int = 0

class ProjectActivityEventOut(BaseModel):
    id: str
    project_id: str
    actor_type: str
    actor_id: Optional[str] = None
    actor_label: Optional[str] = None
    event_type: str
    summary: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: int
__all__ = [name for name in globals() if not name.startswith('__')]

