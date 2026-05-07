from hivee_shared import *
from services.managed_agents import _provision_managed_agents_for_connection
from services.project_activity import append_project_activity_log_entry
from services.project_utils import (
    _apply_project_actions,
    _build_project_root,
    _create_project_chat_message,
    _initialize_project_folder,
    _project_agent_rows_from_id,
    _project_plan_file_is_substantive,
    _refresh_project_documents,
    _set_project_execution_state,
    _write_project_agents_file,
    _write_project_agent_roles_file,
    _write_project_fundamentals_file,
    _write_project_scope_file,
    _write_project_state_file,
)


DEMO_AGENTS: List[Dict[str, Any]] = [
    {
        "id": "hivee/main",
        "name": "Owner Agent",
        "role": "Primary owner — orchestration, approvals, negotiation, and final readiness review",
        "is_primary": True,
    },
    {
        "id": "hivee/researcher",
        "name": "Research Agent",
        "role": "Lean MVP research, buyer journey, product proof, market positioning, and launch constraints",
        "is_primary": False,
    },
    {
        "id": "hivee/planner",
        "name": "Planner Agent",
        "role": "Execution backlog, dependency sequencing, milestone cards, risk gates, and QA checklist",
        "is_primary": False,
    },
    {
        "id": "hivee/coder",
        "name": "Code Agent",
        "role": "Website prototype, 3D viewer, configurator flow, checkout path, and final build files",
        "is_primary": False,
    },
    {
        "id": "hivee/qa",
        "name": "QA Agent",
        "role": "Cross-browser checks, accessibility, regression scenarios, and launch readiness review",
        "is_primary": False,
    },
    {
        "id": "hivee/copywriter",
        "name": "Copywriter Agent",
        "role": "Headline copy, microcopy, occasion-based message variants, and CTA wording",
        "is_primary": False,
    },
]

# External agents that get auto-provisioned when the owner sends an invite while
# DEMO_MODE is on. The first invite materializes Marketing, second Design, third Legal.
DEMO_EXTERNAL_AGENTS: List[Dict[str, Any]] = [
    {
        "id": "marketing/maya",
        "name": "Marketing Agent",
        "role": "Launch campaign strategy, channel mix, social proof, and organic acquisition plan",
        "external_username": "maya",
        "external_full_name": "Maya — Marketing Studio",
    },
    {
        "id": "design/dana",
        "name": "Design Agent",
        "role": "Visual identity, hero composition, configurator UI polish, and brand kit handoff",
        "external_username": "dana",
        "external_full_name": "Dana — Design Studio",
    },
    {
        "id": "legal/leo",
        "name": "Legal Agent",
        "role": "Terms of service, refund policy, payment compliance, and lenticular IP guardrails",
        "external_username": "leo",
        "external_full_name": "Leo — Legal Counsel",
    },
]

DEMO_PLAN_TITLE = "Website for OhMyBoard"
DEMO_FINAL_URL = "https://ohmyboard.id"


def demo_mode_enabled() -> bool:
    return bool(DEMO_MODE)


def _demo_connector_id(user_id: str) -> str:
    digest = hashlib.sha1(str(user_id or "demo").encode("utf-8")).hexdigest()[:12]
    return f"demo_conn_{digest}"


def _demo_agent_snapshot() -> Dict[str, Any]:
    return {
        "agents": [{"id": item["id"], "name": item["name"]} for item in DEMO_AGENTS],
        "models": ["demo-scripted-agent-runtime"],
        "baseUrl": "demo://hivee-ohmyboard",
        "transport": "scripted-demo",
    }


def ensure_demo_connection(user_id: str) -> str:
    if not demo_mode_enabled():
        return ""
    uid = str(user_id or "").strip()
    if not uid:
        return ""
    now = int(time.time())
    connector_id = _demo_connector_id(uid)
    workspace = _ensure_user_workspace(uid)
    workspace_tree = (
        "HIVEE/\n"
        "  PROJECTS/\n"
        "  TEMPLATES/\n"
        "  DEMO/\n"
    )
    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO connectors (
                id, user_id, name, secret, status, cloud_base_url,
                host_hostname, host_platform, host_arch,
                openclaw_base_url, openclaw_transport,
                heartbeat_interval_sec, command_poll_interval_sec,
                last_seen_at, created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                host_hostname = excluded.host_hostname,
                host_platform = excluded.host_platform,
                host_arch = excluded.host_arch,
                openclaw_base_url = excluded.openclaw_base_url,
                openclaw_transport = excluded.openclaw_transport,
                last_seen_at = excluded.last_seen_at,
                updated_at = excluded.updated_at
            """,
            (
                connector_id,
                uid,
                "Hivee Hub",
                f"demo_secret_{connector_id}",
                "online",
                "demo://cloud",
                "hivee-runtime",
                "scripted",
                "virtual",
                "demo://openclaw",
                "scripted-demo",
                15,
                5,
                now,
                now,
                now,
            ),
        )
        # Retire any agents that were provisioned under older demo IDs so the
        # connector snapshot only ever exposes the current canonical roster.
        canonical_ids = [item["id"] for item in DEMO_AGENTS]
        placeholders = ",".join(["?"] * len(canonical_ids))
        conn.execute(
            f"DELETE FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id NOT IN ({placeholders})",
            (uid, connector_id, *canonical_ids),
        )
        conn.execute(
            "DELETE FROM connector_agent_snapshots WHERE connector_id = ?",
            (connector_id,),
        )
        conn.execute(
            """
            INSERT INTO connector_agent_snapshots (id, connector_id, snapshot_json, updated_at)
            VALUES (?,?,?,?)
            """,
            (new_id("csnap"), connector_id, json.dumps(_demo_agent_snapshot(), ensure_ascii=False), now),
        )
        conn.execute(
            """
            INSERT INTO connection_policies (
                connection_id, user_id, main_agent_id, main_agent_name,
                workspace_root, templates_root, bootstrap_status,
                bootstrap_error, workspace_tree, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(connection_id) DO UPDATE SET
                main_agent_id = excluded.main_agent_id,
                main_agent_name = excluded.main_agent_name,
                workspace_root = excluded.workspace_root,
                templates_root = excluded.templates_root,
                bootstrap_status = excluded.bootstrap_status,
                bootstrap_error = excluded.bootstrap_error,
                workspace_tree = excluded.workspace_tree,
                updated_at = excluded.updated_at
            """,
            (
                connector_id,
                uid,
                "hivee/main",
                "Owner Agent",
                workspace["workspace_root"],
                workspace["templates_root"],
                "ok",
                None,
                workspace_tree,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    _provision_managed_agents_for_connection(
        user_id=uid,
        env_id=None,
        connection_id=connector_id,
        base_url="demo://openclaw",
        raw_agents=[{"id": item["id"], "name": item["name"]} for item in DEMO_AGENTS],
        fallback_agent_id="hivee/main",
        fallback_agent_name="Owner Agent",
    )
    return connector_id


def issue_demo_session(email: str, password: str, response: Response) -> SessionOut:
    normalized = _normalize_email(email) or "demo@hivee.local"
    now = int(time.time())
    conn = db()
    try:
        row = conn.execute("SELECT id, email FROM users WHERE email = ?", (normalized,)).fetchone()
        if row:
            user_id = str(row["id"])
        else:
            user_id = new_id("usr")
            conn.execute(
                "INSERT INTO users (id, email, password, created_at) VALUES (?,?,?,?)",
                (user_id, normalized, _hash_password(password or "demo"), now),
            )
        username = _ensure_user_username(user_id, normalized, conn)
        token = _issue_user_session(conn, user_id)
        conn.commit()
    finally:
        conn.close()
    _ensure_user_workspace(user_id)
    _ensure_primary_environment_for_user(user_id, email=normalized)
    ensure_demo_connection(user_id)
    _set_session_cookie(response, token)
    return SessionOut(token=token, username=username)


def demo_setup_chat_response(message: str, *, start: bool = False) -> Dict[str, Any]:
    text = str(message or "").strip().lower()
    if start:
        reply = (
            "Siap, aku setup agent kamu. Ceritain singkat bisnis lenticular board kamu: "
            "produk utamanya apa, siapa pembelinya, dan output pertama yang mau kamu lihat?"
        )
    elif "lenticular" in text or "board" in text or "website" in text:
        reply = (
            "Got it. Aku nangkep scope-nya: website publik buat bisnis lenticular congratulatory board, "
            "dengan 3D preview, editor sederhana, dan checkout. Aku akan draft project-nya sebagai MVP low-budget "
            "yang tetap kelihatan launch-ready."
        )
    else:
        reply = (
            "Oke, aku catat. Aku akan mengarahkan project-nya ke OhMyBoard: "
            "website lenticular board dengan product story, 3D viewer, configurator, dan payment handoff."
        )
    return {
        "ok": True,
        "text": reply,
        "resolved_agent_id": "hivee/main",
        "workspace_root": HIVEE_ROOT,
        "templates_root": HIVEE_TEMPLATES_ROOT,
        "raw": "DEMO_SETUP_CHAT",
    }


def demo_setup_draft(transcript: List[Dict[str, Any]]) -> Dict[str, Any]:
    user_text = " ".join(
        str(item.get("text") or item.get("content") or "")
        for item in (transcript or [])
        if str(item.get("role") or "user").lower() in {"user", "owner"}
    ).strip()
    brief = (
        user_text
        or "Build a website for a lenticular board business with 3D product preview, simple customization, and checkout."
    )
    return {
        "ok": True,
        "title": DEMO_PLAN_TITLE,
        "brief": brief[:5000],
        "goal": "Launch a working MVP website that lets customers understand the lenticular board, preview it in 3D, customize a message, and start checkout.",
        "setup_details": {
            "target_users": "Gift buyers, event organizers, families, and small businesses with no technical background.",
            "constraints": "ASAP, low budget, credible demo quality.",
            "in_scope": "Responsive website, 3D board viewer, simple configurator, checkout handoff, final demo URL.",
            "out_of_scope": "Real payment capture, real production 3D asset pipeline, admin dashboard, manufacturing integrations.",
            "required_stack": "Scripted Hivee runtime with static HTML/CSS/JS deliverables.",
            "first_output": "Launch-ready OhMyBoard website prototype.",
            "suggested_roles": [
                {"name": "hivee/main", "role": "Primary owner agent"},
                {"name": "hivee/researcher", "role": "Lean MVP research"},
                {"name": "hivee/planner", "role": "Backlog and sequencing"},
                {"name": "hivee/coder", "role": "Prototype implementation"},
                {"name": "hivee/qa", "role": "QA and launch readiness"},
                {"name": "hivee/copywriter", "role": "Copy, microcopy, occasion variants"},
            ],
            "demo_mode": True,
            "draft_source": "scripted_demo",
        },
        "raw": "DEMO_DRAFT",
    }


def create_demo_project_record(user_id: str, payload: Any, *, env_id: Optional[str] = None) -> ProjectOut:
    connector_id = ensure_demo_connection(user_id)
    pid = new_id("prj")
    now = int(time.time())
    title = str(getattr(payload, "title", "") or DEMO_PLAN_TITLE).strip()[:160] or DEMO_PLAN_TITLE
    brief = str(getattr(payload, "brief", "") or "Interactive website for a lenticular board business.").strip()[:5000]
    goal = str(getattr(payload, "goal", "") or "Working website demo.").strip()[:5000]
    setup_details = _normalize_setup_details(getattr(payload, "setup_details", None) or {})
    setup_details["demo_mode"] = True
    setup_details["final_demo_url"] = DEMO_FINAL_URL
    setup_chat_history_text = str(getattr(payload, "setup_chat_history", "") or "").replace("\r", "").strip()[:120_000]
    workspace = _ensure_user_workspace(user_id)
    workspace_root = str(workspace["workspace_root"])
    project_root = _build_project_root(pid, title, workspace_root=workspace_root)
    project_dir = Path(project_root)
    _initialize_project_folder(
        project_dir,
        title,
        brief,
        goal,
        setup_details=setup_details,
        setup_chat_history_text=setup_chat_history_text,
        project_id=pid,
        hivee_api_base=_get_hivee_api_base(pid),
    )

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO projects (
                id, user_id, env_id, title, brief, goal, setup_json, plan_text, plan_status, plan_updated_at, plan_approved_at,
                execution_status, progress_pct, execution_updated_at,
                usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
                connection_id, workspace_root, project_root, scope_requires_owner_approval, created_at,
                backend_mode, connector_id
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pid,
                user_id,
                env_id,
                title,
                brief,
                goal,
                json.dumps(setup_details, ensure_ascii=False),
                "",
                PLAN_STATUS_PENDING,
                now,
                None,
                EXEC_STATUS_IDLE,
                0,
                now,
                0,
                0,
                0,
                now,
                connector_id,
                workspace_root,
                project_root,
                1,
                now,
                "demo",
                connector_id,
            ),
        )
        # Seed only the primary owner agent on creation. The owner picks the rest
        # from the wizard's agent step, just like a real project would.
        primary = next((a for a in DEMO_AGENTS if a.get("is_primary")), DEMO_AGENTS[0])
        conn.execute(
            """
            INSERT INTO project_agents (
                project_id, agent_id, agent_name, is_primary, role,
                source_type, source_user_id, source_connection_id, joined_via_invite_id, added_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pid,
                primary["id"],
                primary["name"],
                1,
                primary["role"],
                "owner",
                user_id,
                connector_id,
                None,
                now,
            ),
        )
        raw_token = _new_agent_access_token()
        conn.execute(
            """
            INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, token_plain, created_at)
            VALUES (?,?,?,?,?)
            """,
            (pid, primary["id"], _hash_access_token(raw_token), raw_token, now),
        )
        append_project_activity_log_entry(
            conn,
            project_id=pid,
            actor_type="system",
            actor_id="hivee-runtime",
            actor_label="Hivee Runtime",
            event_type="project.created",
            summary="Project created with primary owner agent. Owner can invite the rest from the agent step.",
            payload={"primary_agent": primary["id"]},
            created_at=now,
        )
        conn.commit()
    finally:
        conn.close()

    role_rows = _project_agent_rows_from_id(pid)
    hivee_api_base = _get_hivee_api_base(pid)
    _write_project_agent_roles_file(owner_user_id=user_id, project_root=project_root, agents=role_rows)
    _write_project_fundamentals_file(
        project_dir,
        project_id=pid,
        title=title,
        phase="setup",
        plan_status=PLAN_STATUS_PENDING,
        execution_status=EXEC_STATUS_IDLE,
        hivee_api_base=hivee_api_base,
        role_rows=role_rows,
    )
    _write_project_agents_file(project_dir, role_rows=role_rows, project_id=pid, hivee_api_base=hivee_api_base)
    _write_project_state_file(
        project_dir,
        phase="setup",
        plan_status=PLAN_STATUS_PENDING,
        execution_status=EXEC_STATUS_IDLE,
        progress_pct=0,
        agents=[{"agent_id": primary["id"], "agent_name": primary["name"]}],
        hivee_api_base=hivee_api_base,
    )
    _write_project_scope_file(
        project_dir,
        agent_id=primary["id"],
        agent_name=primary["name"],
        is_primary=True,
        write_paths=["*"],
        hivee_api_base=hivee_api_base,
        project_id=pid,
    )
    _refresh_project_documents(pid)
    return ProjectOut(
        id=pid,
        title=title,
        brief=brief,
        goal=goal,
        connection_id=connector_id,
        created_at=now,
        workspace_root=workspace_root,
        project_root=project_root,
        setup_details=setup_details,
        plan_status=PLAN_STATUS_PENDING,
        plan_text="",
        plan_updated_at=now,
        plan_approved_at=None,
        execution_status=EXEC_STATUS_IDLE,
        progress_pct=0,
        execution_updated_at=now,
        usage_prompt_tokens=0,
        usage_completion_tokens=0,
        usage_total_tokens=0,
        usage_updated_at=now,
    )


async def _post_chat(
    project_id: str,
    *,
    author_id: str,
    author_label: str,
    text: str,
    mentions: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    conn = db()
    try:
        message = _create_project_chat_message(
            conn,
            project_id=project_id,
            author_type="project_agent",
            author_id=author_id,
            author_label=author_label,
            text=text,
            mentions=mentions or [],
            metadata=metadata or {"source": "demo_runtime"},
        )
        conn.commit()
    finally:
        conn.close()
    await emit(project_id, "project.chat.message", message)
    for target in (message.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]:
        await emit(
            project_id,
            "project.chat.mention",
            {
                "message_id": str(message.get("id") or ""),
                "project_id": project_id,
                "target": target,
                "author_type": "project_agent",
                "author_id": author_id,
                "author_label": author_label,
                "text": str(message.get("text") or "")[:500],
                "created_at": int(message.get("created_at") or 0),
            },
        )
    return message


async def seed_demo_project_created_chat(project_id: str) -> None:
    await _post_chat(
        project_id,
        author_id="hivee/main",
        author_label="hivee/main",
        text=(
            "@owner Project is created. I'm online as the primary owner agent. "
            "Pick the other internal agents you want from the Agents step, then invite the external "
            "specialists (marketing, design, legal) so we can scope the OhMyBoard launch together."
        ),
        mentions=["owner"],
        metadata={"phase": "project.created"},
    )


def demo_workspace_chat_response(message: str) -> Dict[str, Any]:
    body = str(message or "").strip()
    if not body:
        body = "Start workspace chat."
    return {
        "ok": True,
        "text": (
            "Owner Agent: I'm in scripted runtime mode. I can help frame the OhMyBoard lenticular "
            "board website, draft the project plan, and orchestrate the multi-agent execution."
        ),
        "resolved_agent_id": "hivee/main",
        "workspace_root": HIVEE_ROOT,
        "raw": f"DEMO_WORKSPACE_CHAT: {body[:200]}",
    }


def _demo_plan_text(title: str, brief: str, goal: str) -> str:
    return f"""# Project Plan: {title or DEMO_PLAN_TITLE}

## Objective
Launch a polished website for an OhMyBoard-style lenticular congratulatory board business. The site should help a non-technical buyer understand the product, inspect it in a 3D viewer, personalize the board, and reach a checkout handoff.

## Canonical Inputs
- Brief: {brief or "Interactive website with 3D model view of congratulatory lenticular board"}
- Goal: {goal or "Working website"}
- Constraints: ASAP, lean budget, public audience, launch-ready demo quality.
- In scope: 3D model viewer, payment gateway path, interactive editor, occasion-based messaging.

## Milestones
1. Research lean MVP positioning for gift buyers, event organizers, and small businesses.
2. Lock scope, brand kit, copy variants, and dependency-aware execution backlog.
3. Build the website shell, 3D board viewer, configurator, checkout path, and supporting marketing pages.
4. Run cross-agent QA + legal review, then publish the owner-facing launch handoff.

## Internal Delegation
- `hivee/main`: orchestration, approvals, negotiation, final readiness.
- `hivee/researcher`: buyer journey, proof points, MVP recommendation.
- `hivee/planner`: milestone cards, task dependencies, QA gate, risk log.
- `hivee/coder`: website prototype, 3D viewer, configurator, checkout, final files.
- `hivee/qa`: cross-browser, accessibility, regression scenarios, launch checklist.
- `hivee/copywriter`: hero copy, microcopy, CTA wording, occasion variants.

## External Delegation
- `marketing/maya` (Marketing Agent): launch campaign strategy, channel mix, social proof.
- `design/dana` (Design Agent): visual identity, hero composition, configurator polish, brand kit.
- `legal/leo` (Legal Agent): terms of service, refund policy, payment compliance, IP guardrails.

## Acceptance Criteria
- Project chat shows chronological inter-agent handoffs and negotiation across all 9 agents.
- Task map shows tasks with assignee ownership and explicit dependencies.
- Output files exist under `Outputs/` and final launch files exist under `FINAL/`.
- Primary agent ends with: Website is finished. You can access it at {DEMO_FINAL_URL}.
"""


async def generate_demo_plan(project_id: str, *, force: bool = False) -> None:
    conn = db()
    row = conn.execute(
        "SELECT id, user_id, title, brief, goal, project_root FROM projects WHERE id = ? LIMIT 1",
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    now = int(time.time())
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_updated_at = ?, execution_status = ?, execution_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_GENERATING, now, EXEC_STATUS_IDLE, now, project_id),
    )
    conn.commit()
    conn.close()
    await emit(project_id, "project.plan.generating", {"project_id": project_id, "demo": True, "force": force})
    await _demo_sleep("typing")
    await _post_chat(
        project_id,
        author_id="hivee/main",
        author_label="hivee/main",
        text="@owner Drafting the project plan from the canonical brief now. I'll fold in the internal roster and the external specialists you invited.",
        mentions=["owner"],
        metadata={"phase": "plan.generating"},
    )
    await _demo_sleep("long")

    plan_text = _demo_plan_text(str(row["title"] or ""), str(row["brief"] or ""), str(row["goal"] or ""))
    actions = [
        {"type": "write_file", "path": "plan.md", "content": plan_text},
        {"type": "write_file", "path": PROJECT_PLAN_FILE, "content": plan_text},
        {
            "type": "write_file",
            "path": "Outputs/plan-review-note.md",
            "content": "# Plan Review Note\n\nThe primary agent reviewed this plan against the saved project brief, setup details, and protocol files. The plan is ready for owner approval.\n",
        },
    ]
    await apply_demo_actions(project_id, actor_id="hivee/main", actor_label="hivee/main", actions=actions)
    conn = db()
    try:
        conn.execute(
            "UPDATE projects SET plan_text = ?, plan_status = ?, plan_updated_at = ?, plan_approved_at = NULL WHERE id = ?",
            (plan_text, PLAN_STATUS_AWAITING_APPROVAL, int(time.time()), project_id),
        )
        conn.commit()
    finally:
        conn.close()
    _refresh_project_documents(project_id)
    await _demo_sleep("short")
    await _post_chat(
        project_id,
        author_id="hivee/main",
        author_label="hivee/main",
        text="@owner Plan is ready for approval. Scope: 3D viewer, occasion-based configurator, checkout handoff, marketing pages, legal copy, and launch QA across the full 9-agent crew.",
        mentions=["owner"],
        metadata={"phase": "plan.awaiting_approval"},
    )
    await emit(
        project_id,
        "project.plan.generated",
        {
            "project_id": project_id,
            "status": PLAN_STATUS_AWAITING_APPROVAL,
            "is_valid": _project_plan_file_is_substantive(plan_text),
            "demo": True,
        },
    )


async def apply_demo_actions(
    project_id: str,
    *,
    actor_id: str,
    actor_label: str,
    actions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    conn = db()
    project = conn.execute(
        "SELECT user_id, project_root FROM projects WHERE id = ? LIMIT 1",
        (project_id,),
    ).fetchone()
    conn.close()
    if not project:
        return {"applied": [], "skipped": ["Project not found"]}
    result = _apply_project_actions(
        owner_user_id=str(project["user_id"] or ""),
        project_id=project_id,
        project_root=str(project["project_root"] or ""),
        actions=actions,
        allow_paths=None,
        actor_type="project_agent",
        actor_id=actor_id,
        actor_label=actor_label,
    )
    applied = result.get("applied") or []
    for item in applied:
        event_name = str(item.get("event") or "").strip()
        event_payload = item.get("event_payload") if isinstance(item.get("event_payload"), dict) else {}
        if event_name:
            await emit(project_id, event_name, event_payload)
        for extra in item.get("extra_events") or []:
            if not isinstance(extra, dict):
                continue
            extra_event = str(extra.get("event") or "").strip()
            extra_payload = extra.get("event_payload") if isinstance(extra.get("event_payload"), dict) else {}
            if extra_event:
                await emit(project_id, extra_event, extra_payload)
    if any(str(item.get("type") or "") in {"write_file", "append_file", "upload_file", "update_execution"} for item in applied):
        _refresh_project_documents(project_id)
    return result


_DEMO_DELAY_TIERS: Dict[str, str] = {
    "instant": "HIVEE_DEMO_DELAY_INSTANT",
    "typing": "HIVEE_DEMO_DELAY_TYPING",
    "short": "HIVEE_DEMO_DELAY_SHORT",
    "medium": "HIVEE_DEMO_EVENT_DELAY_SEC",
    "long": "HIVEE_DEMO_DELAY_LONG",
    "xlong": "HIVEE_DEMO_DELAY_XLONG",
}
_DEMO_DELAY_DEFAULTS: Dict[str, float] = {
    "instant": 0.15,
    "typing": 0.7,
    "short": 1.2,
    "medium": 1.8,
    "long": 2.8,
    "xlong": 4.2,
}


def _demo_delay_seconds(tier: str) -> float:
    env_key = _DEMO_DELAY_TIERS.get(tier, _DEMO_DELAY_TIERS["medium"])
    raw = os.environ.get(env_key)
    if raw is None:
        return _DEMO_DELAY_DEFAULTS.get(tier, _DEMO_DELAY_DEFAULTS["medium"])
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return _DEMO_DELAY_DEFAULTS.get(tier, _DEMO_DELAY_DEFAULTS["medium"])


async def _demo_sleep(tier: str = "medium") -> None:
    await asyncio.sleep(_demo_delay_seconds(tier))


def _present_project_agent_ids(project_id: str) -> set:
    conn = db()
    rows = conn.execute(
        "SELECT agent_id FROM project_agents WHERE project_id = ?",
        (project_id,),
    ).fetchall()
    conn.close()
    return {str(r["agent_id"] or "").strip() for r in rows if str(r["agent_id"] or "").strip()}


async def run_demo_execution(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        "SELECT id, title, brief, goal, execution_status FROM projects WHERE id = ? LIMIT 1",
        (project_id,),
    ).fetchone()
    conn.close()
    if not row:
        return

    present = _present_project_agent_ids(project_id)
    if "hivee/main" not in present:
        present.add("hivee/main")  # primary always speaks

    async def chat(
        author_id: str,
        text: str,
        *,
        mentions: Optional[List[str]] = None,
        phase: str,
        tier: str = "medium",
    ) -> None:
        if author_id not in present:
            return
        await _post_chat(
            project_id,
            author_id=author_id,
            author_label=author_id,
            text=text,
            mentions=mentions,
            metadata={"phase": phase},
        )
        await _demo_sleep(tier)

    async def act(
        actor_id: str,
        actions: List[Dict[str, Any]],
        *,
        tier: Optional[str] = "short",
    ) -> None:
        use_actor = actor_id if actor_id in present else "hivee/main"
        await apply_demo_actions(
            project_id,
            actor_id=use_actor,
            actor_label=use_actor,
            actions=actions,
        )
        if tier:
            await _demo_sleep(tier)

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=4)
    await emit(
        project_id,
        "project.execution.updated",
        {"status": EXEC_STATUS_RUNNING, "progress_pct": 4, "summary": "Execution started — primary agent is decomposing the plan."},
    )
    await _demo_sleep("typing")

    # ── Wave 0: primary creates the full task graph ─────────────────────────
    initial_actions: List[Dict[str, Any]] = [
        # Research lane
        {
            "type": "create_task",
            "ref": "research_audience",
            "title": "Map buyer journey, segments, and trust cues",
            "description": "Identify gift buyers, event organizers, and SMB segments. Surface trust cues, objections, and proof points the homepage must answer.",
            "assignee_agent_id": "hivee/researcher",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 6,
            "metadata": {"lane": "research"},
        },
        {
            "type": "create_task",
            "ref": "research_pricing",
            "title": "Benchmark pricing tiers and payment options",
            "description": "Pull comparable lenticular gift pricing, identify tier breakpoints, and recommend a demo-safe payment handoff.",
            "assignee_agent_id": "hivee/researcher",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 5,
            "metadata": {"lane": "research"},
        },
        # Planning lane
        {
            "type": "create_task",
            "ref": "plan_backlog",
            "title": "Shape execution backlog and dependency map",
            "description": "Turn the approved plan into implementation slices, sequencing, and explicit dependencies across all 9 agents.",
            "assignee_agent_id": "hivee/planner",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 6,
            "metadata": {"lane": "planning"},
        },
        {
            "type": "create_task",
            "ref": "plan_qa_gates",
            "title": "Define QA gates and acceptance criteria",
            "description": "Document the launch-readiness checklist, acceptance criteria for each lane, and sign-off owner per gate.",
            "assignee_agent_id": "hivee/planner",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 4,
            "metadata": {"lane": "planning"},
        },
        # Copy lane
        {
            "type": "create_task",
            "ref": "copy_hero",
            "title": "Write hero headline + microcopy variants",
            "description": "Produce three hero headline directions, supporting subhead, and CTA microcopy aligned to research positioning.",
            "assignee_agent_id": "hivee/copywriter",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 5,
            "metadata": {"lane": "copy"},
        },
        {
            "type": "create_task",
            "ref": "copy_occasions",
            "title": "Draft occasion-based message variants",
            "description": "Write reusable congratulations copy for graduation, birthday, wedding, and grand opening occasions.",
            "assignee_agent_id": "hivee/copywriter",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 4,
            "metadata": {"lane": "copy"},
        },
        # Design lane (external)
        {
            "type": "create_task",
            "ref": "design_brand_kit",
            "title": "Lock brand kit and visual identity",
            "description": "Define palette, type scale, logo lockup variants, and motion language for the lenticular product proof.",
            "assignee_agent_id": "design/dana",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 5,
            "metadata": {"lane": "design", "external": True},
        },
        {
            "type": "create_task",
            "ref": "design_hero",
            "title": "Compose hero + configurator visual mock",
            "description": "Produce a hero-section mock with the 3D board, plus configurator panel layout aligned with copy variants.",
            "assignee_agent_id": "design/dana",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 5,
            "metadata": {"lane": "design", "external": True},
        },
        # Marketing lane (external)
        {
            "type": "create_task",
            "ref": "marketing_strategy",
            "title": "Draft launch campaign + channel mix",
            "description": "Outline organic + paid channel mix, social proof angles, and a 4-week launch calendar grounded in research.",
            "assignee_agent_id": "marketing/maya",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 5,
            "metadata": {"lane": "marketing", "external": True},
        },
        {
            "type": "create_task",
            "ref": "marketing_pages",
            "title": "Write marketing landing page content",
            "description": "Provide content blocks for the campaign landing page, FAQ, and social-proof sections that the build agent will wire in.",
            "assignee_agent_id": "marketing/maya",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 4,
            "metadata": {"lane": "marketing", "external": True},
        },
        # Legal lane (external)
        {
            "type": "create_task",
            "ref": "legal_terms",
            "title": "Draft terms of service and refund policy",
            "description": "Write demo-safe ToS and refund policy copy that fits the OhMyBoard launch context.",
            "assignee_agent_id": "legal/leo",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 4,
            "metadata": {"lane": "legal", "external": True},
        },
        {
            "type": "create_task",
            "ref": "legal_compliance",
            "title": "Payment compliance + IP guardrails",
            "description": "Identify checkout disclosures, IP guardrails for lenticular images, and any blockers before public launch.",
            "assignee_agent_id": "legal/leo",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 4,
            "metadata": {"lane": "legal", "external": True},
        },
        # Build lane
        {
            "type": "create_task",
            "ref": "build_shell",
            "title": "Build website shell + 3D lenticular viewer",
            "description": "Implement the public website shell, hero composition, and the 3D-style lenticular board viewer interaction.",
            "assignee_agent_id": "hivee/coder",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 12,
            "metadata": {"lane": "build"},
        },
        {
            "type": "create_task",
            "ref": "build_configurator",
            "title": "Build configurator + checkout handoff",
            "description": "Wire the message configurator (occasion, recipient, finish) and the demo-safe checkout handoff.",
            "assignee_agent_id": "hivee/coder",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 10,
            "metadata": {"lane": "build"},
        },
        {
            "type": "create_task",
            "ref": "build_marketing",
            "title": "Wire marketing pages and legal copy",
            "description": "Implement the marketing landing page, FAQ, ToS, and refund policy using copy from marketing and legal lanes.",
            "assignee_agent_id": "hivee/coder",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 8,
            "metadata": {"lane": "build"},
        },
        # QA lane
        {
            "type": "create_task",
            "ref": "qa_review",
            "title": "Cross-browser, accessibility, and regression sweep",
            "description": "Verify viewer interactions, configurator path, checkout copy, marketing pages, and ToS visibility on three breakpoints.",
            "assignee_agent_id": "hivee/qa",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_HIGH,
            "weight_pct": 8,
            "metadata": {"lane": "qa"},
        },
        # Final review
        {
            "type": "create_task",
            "ref": "final_review",
            "title": "Primary final readiness review and launch handoff",
            "description": "Confirm every lane's output, verify the launch URL, and announce the finished website to the owner.",
            "assignee_agent_id": "hivee/main",
            "status": TASK_STATUS_TODO,
            "priority": TASK_PRIORITY_MEDIUM,
            "weight_pct": 5,
            "metadata": {"lane": "review"},
        },
        # Dependencies
        {"type": "add_task_dependency", "task_ref": "copy_hero", "depends_on_task_ref": "research_audience"},
        {"type": "add_task_dependency", "task_ref": "copy_occasions", "depends_on_task_ref": "research_audience"},
        {"type": "add_task_dependency", "task_ref": "design_hero", "depends_on_task_ref": "design_brand_kit"},
        {"type": "add_task_dependency", "task_ref": "design_hero", "depends_on_task_ref": "copy_hero"},
        {"type": "add_task_dependency", "task_ref": "marketing_strategy", "depends_on_task_ref": "research_audience"},
        {"type": "add_task_dependency", "task_ref": "marketing_strategy", "depends_on_task_ref": "research_pricing"},
        {"type": "add_task_dependency", "task_ref": "marketing_pages", "depends_on_task_ref": "marketing_strategy"},
        {"type": "add_task_dependency", "task_ref": "build_shell", "depends_on_task_ref": "design_hero"},
        {"type": "add_task_dependency", "task_ref": "build_shell", "depends_on_task_ref": "plan_backlog"},
        {"type": "add_task_dependency", "task_ref": "build_configurator", "depends_on_task_ref": "copy_occasions"},
        {"type": "add_task_dependency", "task_ref": "build_configurator", "depends_on_task_ref": "legal_terms"},
        {"type": "add_task_dependency", "task_ref": "build_configurator", "depends_on_task_ref": "build_shell"},
        {"type": "add_task_dependency", "task_ref": "build_marketing", "depends_on_task_ref": "marketing_pages"},
        {"type": "add_task_dependency", "task_ref": "build_marketing", "depends_on_task_ref": "legal_compliance"},
        {"type": "add_task_dependency", "task_ref": "qa_review", "depends_on_task_ref": "build_shell"},
        {"type": "add_task_dependency", "task_ref": "qa_review", "depends_on_task_ref": "build_configurator"},
        {"type": "add_task_dependency", "task_ref": "qa_review", "depends_on_task_ref": "build_marketing"},
        {"type": "add_task_dependency", "task_ref": "qa_review", "depends_on_task_ref": "plan_qa_gates"},
        {"type": "add_task_dependency", "task_ref": "final_review", "depends_on_task_ref": "qa_review"},
        {"type": "update_execution", "progress_pct": 8, "summary": "Task graph created — 17 tasks across 9 agents."},
    ]
    await act("hivee/main", initial_actions, tier="short")
    await chat(
        "hivee/main",
        "@owner Task graph is live: 17 tasks across 9 agents with explicit dependencies. "
        "Research, planning, copy, design, marketing, and legal kick off in parallel. Build waits on their outputs; QA waits on build; I close the loop.",
        mentions=["owner", "hivee/researcher", "hivee/planner", "hivee/copywriter", "design/dana", "marketing/maya", "legal/leo"],
        phase="delegation.kickoff",
        tier="medium",
    )

    # ── Wave 1: parallel acks ───────────────────────────────────────────────
    await chat(
        "hivee/researcher",
        "Roger. Picking up audience mapping first, then pricing benchmarks. I'll surface emotional drivers for the gift buyer angle.",
        phase="research.ack",
        tier="typing",
    )
    await act(
        "hivee/researcher",
        [
            {"type": "update_task", "task_title": "Map buyer journey, segments, and trust cues", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 12, "summary": "Research lane started."},
        ],
        tier="short",
    )
    await chat(
        "hivee/planner",
        "Roger. Drafting the dependency map now — I'll keep the build lane gated on research, copy, design, and legal so nothing jumps ahead.",
        phase="planner.ack",
        tier="typing",
    )
    await act(
        "hivee/planner",
        [
            {"type": "update_task", "task_title": "Shape execution backlog and dependency map", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 15, "summary": "Planner lane started."},
        ],
        tier="short",
    )
    await chat(
        "hivee/copywriter",
        "On it. Holding the hero copy until research lands so the headline matches the actual buyer angle. I can start occasion variants now.",
        mentions=["hivee/researcher"],
        phase="copy.ack",
        tier="typing",
    )
    await act(
        "hivee/copywriter",
        [
            {"type": "update_task", "task_title": "Draft occasion-based message variants", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 18, "summary": "Copy lane started occasion variants."},
        ],
        tier="short",
    )
    await chat(
        "design/dana",
        "Roger. Locking the brand kit first so hero + configurator share one visual language. Standing by for the hero copy direction before composing the hero mock.",
        mentions=["hivee/copywriter"],
        phase="design.ack",
        tier="typing",
    )
    await act(
        "design/dana",
        [
            {"type": "update_task", "task_title": "Lock brand kit and visual identity", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 21, "summary": "Design lane started brand kit."},
        ],
        tier="short",
    )
    await chat(
        "marketing/maya",
        "Roger. I'll wait for research_audience and research_pricing before locking the channel mix — otherwise the campaign will not match the proven buyer journey.",
        mentions=["hivee/researcher"],
        phase="marketing.ack",
        tier="typing",
    )
    await chat(
        "legal/leo",
        "Roger. Starting on ToS + refund policy now since they don't depend on copy. Compliance review will follow once checkout flow scope is locked.",
        phase="legal.ack",
        tier="typing",
    )
    await act(
        "legal/leo",
        [
            {"type": "update_task", "task_title": "Draft terms of service and refund policy", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 24, "summary": "Legal lane started ToS draft."},
        ],
        tier="short",
    )
    await chat(
        "hivee/coder",
        "@hivee/main Acknowledged. I'll prepare the project scaffolding but won't lock copy, viewer assets, or checkout copy until design + legal land.",
        mentions=["hivee/main", "design/dana", "legal/leo"],
        phase="coder.negotiation",
        tier="long",
    )

    # ── Wave 2: research deliverables ───────────────────────────────────────
    await act(
        "hivee/researcher",
        [
            {
                "type": "write_file",
                "path": "Outputs/research/audience-map.md",
                "content": (
                    "# OhMyBoard Audience Map\n\n"
                    "## Primary segments\n"
                    "- Gift buyers (graduations, birthdays, anniversaries) — emotional driver: making the moment feel premium and remembered.\n"
                    "- Event organizers (companies, weddings, milestone parties) — driver: scale + reliability.\n"
                    "- SMBs and creators — driver: low-cost differentiation for grand openings + product launches.\n\n"
                    "## Top objections to neutralize on the homepage\n"
                    "- 'Will the lenticular effect actually look good?' → answer with the 3D viewer interaction.\n"
                    "- 'Can I read the message on the board?' → answer with configurator preview.\n"
                    "- 'Will it ship on time for my occasion?' → answer with explicit lead-time near the CTA.\n\n"
                    "## Trust cues that move the needle\n"
                    "- Real recipient photos with named occasions.\n"
                    "- Demo-safe payment handoff that feels familiar.\n"
                    "- Lead-time and refund visibility before checkout.\n"
                ),
            },
            {"type": "update_task", "task_title": "Map buyer journey, segments, and trust cues", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Benchmark pricing tiers and payment options", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 28, "summary": "Audience map synced."},
        ],
        tier="short",
    )
    await chat(
        "hivee/researcher",
        "@hivee/copywriter @marketing/maya Audience map is live: gift buyers lead, event organizers second, SMBs third. The homepage must answer 3 objections in the first scroll.",
        mentions=["hivee/copywriter", "marketing/maya"],
        phase="research.audience.done",
        tier="medium",
    )
    await act(
        "hivee/researcher",
        [
            {
                "type": "write_file",
                "path": "Outputs/research/pricing-benchmarks.md",
                "content": (
                    "# Pricing + Payment Benchmarks\n\n"
                    "- Lenticular gift competitors: $35–$95 depending on size + finish.\n"
                    "- Recommended demo tiers: Standard $39, Premium $69, Statement $99.\n"
                    "- Demo-safe payment handoff: visible Stripe/PayPal copy without live capture.\n"
                    "- Always show lead-time + refund visibility before checkout.\n"
                ),
            },
            {"type": "update_task", "task_title": "Benchmark pricing tiers and payment options", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 32, "summary": "Pricing benchmarks synced."},
        ],
        tier="short",
    )
    await chat(
        "hivee/researcher",
        "@marketing/maya Pricing is locked at three tiers ($39/$69/$99). You're unblocked for campaign mix.",
        mentions=["marketing/maya"],
        phase="research.pricing.done",
        tier="medium",
    )

    # ── Wave 3: planner + copy deliverables ─────────────────────────────────
    await act(
        "hivee/planner",
        [
            {
                "type": "write_file",
                "path": "Outputs/planning/execution-backlog.md",
                "content": (
                    "# Execution Backlog (sequenced)\n\n"
                    "1. Brand kit + audience map (parallel) → unblocks hero design + copy.\n"
                    "2. Hero copy + occasion copy (parallel) → unblocks design hero + build configurator.\n"
                    "3. Marketing strategy + pages → unblocks marketing-page build.\n"
                    "4. Legal ToS + compliance → unblocks configurator + marketing build wiring.\n"
                    "5. Build shell → build configurator → build marketing pages.\n"
                    "6. QA sweep across all three build outputs and gate before final review.\n"
                    "7. Primary readiness review and launch handoff.\n"
                ),
            },
            {"type": "update_task", "task_title": "Shape execution backlog and dependency map", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Define QA gates and acceptance criteria", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 36, "summary": "Backlog locked."},
        ],
        tier="short",
    )
    await act(
        "hivee/planner",
        [
            {
                "type": "write_file",
                "path": "Outputs/planning/qa-gates.md",
                "content": (
                    "# QA Gates\n\n"
                    "- Gate A — Hero shell: viewer parallax, headline, CTA, no console errors.\n"
                    "- Gate B — Configurator: occasion → recipient → message preview, persisted state.\n"
                    "- Gate C — Checkout: payment handoff copy, lead-time, refund visibility, ToS link.\n"
                    "- Gate D — Marketing pages: campaign hero, FAQ, social-proof block.\n"
                    "- Gate E — Cross-cutting: keyboard nav, color contrast, mobile breakpoint.\n"
                ),
            },
            {"type": "update_task", "task_title": "Define QA gates and acceptance criteria", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 40, "summary": "QA gates defined."},
        ],
        tier="short",
    )
    await chat(
        "hivee/planner",
        "@hivee/coder @hivee/qa Backlog and QA gates are locked. Build follows the sequenced order; QA blocks final review until all five gates pass.",
        mentions=["hivee/coder", "hivee/qa"],
        phase="planner.done",
        tier="medium",
    )
    await act(
        "hivee/copywriter",
        [
            {
                "type": "write_file",
                "path": "Outputs/copy/occasion-variants.md",
                "content": (
                    "# Occasion Message Variants\n\n"
                    "- Graduation: \"Onwards, [Name]. The next chapter is yours.\"\n"
                    "- Birthday: \"Happy [Age], [Name] — make the year unforgettable.\"\n"
                    "- Wedding: \"To [Names], the best is still to come.\"\n"
                    "- Grand opening: \"Welcome to [Brand]. Day one, but never the last.\"\n"
                ),
            },
            {"type": "update_task", "task_title": "Draft occasion-based message variants", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Write hero headline + microcopy variants", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 44, "summary": "Occasion copy delivered; hero copy in progress."},
        ],
        tier="short",
    )
    await act(
        "hivee/copywriter",
        [
            {
                "type": "write_file",
                "path": "Outputs/copy/hero-variants.md",
                "content": (
                    "# Hero Copy Variants\n\n"
                    "## V1 — Emotional pull\nA board that changes with the moment.\nSubhead: Premium lenticular congratulations, made for the people you love.\nCTA: Start order\n\n"
                    "## V2 — Proof first\nSee the lenticular effect before you order.\nSubhead: Move your cursor. The board moves with you.\nCTA: View board\n\n"
                    "## V3 — Occasion-led\nMake the moment unforgettable.\nSubhead: Graduations, birthdays, weddings, grand openings — one board, infinite stories.\nCTA: Personalize\n\n"
                    "Recommendation: lead with V1 hero, anchor V2 below the fold for proof.\n"
                ),
            },
            {"type": "update_task", "task_title": "Write hero headline + microcopy variants", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 48, "summary": "Hero copy locked."},
        ],
        tier="short",
    )
    await chat(
        "hivee/copywriter",
        "@design/dana @hivee/coder Hero copy: V1 above the fold, V2 anchor below. Occasion variants are live for the configurator.",
        mentions=["design/dana", "hivee/coder"],
        phase="copy.done",
        tier="medium",
    )

    # ── Wave 4: design + marketing + legal deliverables ─────────────────────
    await act(
        "design/dana",
        [
            {
                "type": "write_file",
                "path": "Outputs/design/brand-kit.md",
                "content": (
                    "# OhMyBoard Brand Kit\n\n"
                    "- Primary: #E4384F (signal red).\n"
                    "- Secondary: #0C8F75 (mint), #D49B22 (gold), #17202A (ink).\n"
                    "- Type scale: Inter 12/14/17/24/40/64.\n"
                    "- Motion language: lenticular parallax on hero, 200–300ms easing, no autoplay.\n"
                ),
            },
            {"type": "update_task", "task_title": "Lock brand kit and visual identity", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Compose hero + configurator visual mock", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 52, "summary": "Brand kit shipped."},
        ],
        tier="short",
    )
    await act(
        "design/dana",
        [
            {
                "type": "write_file",
                "path": "Outputs/design/hero-composition.md",
                "content": (
                    "# Hero Composition Mock\n\n"
                    "- Two-column grid: copy left, viewer stage right.\n"
                    "- Viewer stage uses gradient backdrop with subtle mint tint to make the lenticular shift readable.\n"
                    "- Configurator card overlaps lower-right of viewer at desktop, full-width below viewer at mobile.\n"
                    "- CTA primary uses signal red on white surface for highest contrast.\n"
                ),
            },
            {"type": "update_task", "task_title": "Compose hero + configurator visual mock", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 56, "summary": "Hero + configurator mock delivered."},
        ],
        tier="short",
    )
    await chat(
        "design/dana",
        "@hivee/coder Visual direction is locked. Two-column hero, configurator card overlaps viewer on desktop, signal-red CTA on white surface. You're unblocked for build_shell.",
        mentions=["hivee/coder"],
        phase="design.done",
        tier="medium",
    )
    await act(
        "marketing/maya",
        [
            {"type": "update_task", "task_title": "Draft launch campaign + channel mix", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 58, "summary": "Marketing strategy started."},
        ],
        tier="instant",
    )
    await act(
        "marketing/maya",
        [
            {
                "type": "write_file",
                "path": "Outputs/marketing/launch-strategy.md",
                "content": (
                    "# 4-Week Launch Strategy\n\n"
                    "- Week 1: organic teaser on Instagram + TikTok, lenticular reveal video.\n"
                    "- Week 2: paid retargeting + creator seeding for graduation occasion.\n"
                    "- Week 3: gift-guide outreach + email capture incentive.\n"
                    "- Week 4: launch live, partner co-promotion, social proof loop.\n\n"
                    "Channel mix: 45% organic social, 30% creator/UGC, 15% paid retargeting, 10% email.\n"
                ),
            },
            {"type": "update_task", "task_title": "Draft launch campaign + channel mix", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Write marketing landing page content", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 62, "summary": "Launch strategy delivered."},
        ],
        tier="short",
    )
    await act(
        "marketing/maya",
        [
            {
                "type": "write_file",
                "path": "Outputs/marketing/landing-content.md",
                "content": (
                    "# Marketing Landing Content\n\n"
                    "## Hero\nThe gift that moves with the moment.\nSubhead: Premium lenticular boards for the people who deserve the best.\n\n"
                    "## Social proof\n- 'It made the entire room go quiet.' — Maya, graduation gift\n- 'Best $69 I've spent on a wedding gift.' — Daniel\n\n"
                    "## FAQ\n- Lead time: 5 business days standard, 2-day rush available.\n- Personalization: occasion, recipient, message, board size, finish.\n- Returns: 30-day satisfaction guarantee.\n"
                ),
            },
            {"type": "update_task", "task_title": "Write marketing landing page content", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 66, "summary": "Marketing landing content delivered."},
        ],
        tier="short",
    )
    await chat(
        "marketing/maya",
        "@hivee/coder Marketing strategy + landing content are synced. Build can wire the campaign hero, social proof, and FAQ blocks.",
        mentions=["hivee/coder"],
        phase="marketing.done",
        tier="medium",
    )
    await act(
        "legal/leo",
        [
            {
                "type": "write_file",
                "path": "Outputs/legal/terms-of-service.md",
                "content": (
                    "# OhMyBoard — Terms of Service (demo draft)\n\n"
                    "1. Orders are personalized — once production starts, refunds follow the satisfaction policy below.\n"
                    "2. Lead times are estimates and reset if the customer changes the configurator after submit.\n"
                    "3. Payment processing is handled by a third-party gateway. OhMyBoard does not store card data.\n"
                    "4. Disputes resolved under buyer's local jurisdiction, with 14-day notice window.\n"
                ),
            },
            {
                "type": "write_file",
                "path": "Outputs/legal/refund-policy.md",
                "content": (
                    "# Refund Policy (demo)\n\n"
                    "- 30-day satisfaction guarantee for unopened boards.\n"
                    "- Personalization defects refunded or remade within 14 days of delivery.\n"
                    "- Shipping damage covered if reported within 7 days with photo evidence.\n"
                ),
            },
            {"type": "update_task", "task_title": "Draft terms of service and refund policy", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Payment compliance + IP guardrails", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 70, "summary": "Legal ToS + refund policy synced."},
        ],
        tier="short",
    )
    await act(
        "legal/leo",
        [
            {
                "type": "write_file",
                "path": "Outputs/legal/compliance-checklist.md",
                "content": (
                    "# Compliance + IP Guardrails (demo)\n\n"
                    "- Show jurisdiction-aware refund window before payment confirmation.\n"
                    "- Personalization disclaimer: customer warrants they own or have rights to uploaded names/messages.\n"
                    "- Lenticular IP: avoid trademarked phrases in default occasion variants; flag user-supplied trademarked text.\n"
                    "- Demo-only: do not capture real card data; use payment-handoff copy only.\n"
                ),
            },
            {"type": "update_task", "task_title": "Payment compliance + IP guardrails", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 73, "summary": "Compliance checklist synced."},
        ],
        tier="short",
    )
    await chat(
        "legal/leo",
        "@hivee/coder ToS, refund policy, and compliance checklist are live. Configurator should warn on trademarked text; checkout must surface refund window before payment copy.",
        mentions=["hivee/coder"],
        phase="legal.done",
        tier="medium",
    )

    # ── Wave 5: build ──────────────────────────────────────────────────────
    await chat(
        "hivee/coder",
        "@hivee/main All upstream lanes have shipped. Starting build_shell now, then configurator, then marketing pages.",
        mentions=["hivee/main"],
        phase="coder.start",
        tier="typing",
    )
    await act(
        "hivee/coder",
        [
            {"type": "update_task", "task_title": "Build website shell + 3D lenticular viewer", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 76, "summary": "Build lane started shell + viewer."},
        ],
        tier="short",
    )
    await act(
        "hivee/coder",
        [
            {
                "type": "write_file",
                "path": "FINAL/website-demo.html",
                "content": _demo_website_html(),
            },
            {"type": "update_task", "task_title": "Build website shell + 3D lenticular viewer", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Build configurator + checkout handoff", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 82, "summary": "Shell + viewer landed."},
        ],
        tier="short",
    )
    await act(
        "hivee/coder",
        [
            {
                "type": "write_file",
                "path": "Outputs/build/configurator-handoff.md",
                "content": (
                    "# Configurator + Checkout Build Notes\n\n"
                    "- Configurator state: occasion → recipient → message → finish → size; persisted in URL params.\n"
                    "- Checkout copy uses Stripe + PayPal placeholders; refund window surfaces before submit.\n"
                    "- Trademarked-text warning shown via inline notice when user input matches the legal blocklist.\n"
                ),
            },
            {"type": "update_task", "task_title": "Build configurator + checkout handoff", "status": TASK_STATUS_DONE},
            {"type": "update_task", "task_title": "Wire marketing pages and legal copy", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 88, "summary": "Configurator + checkout handoff complete."},
        ],
        tier="short",
    )
    await act(
        "hivee/coder",
        [
            {
                "type": "write_file",
                "path": "Outputs/build/marketing-pages.md",
                "content": (
                    "# Marketing + Legal Pages\n\n"
                    "- /campaign — hero, social proof, FAQ block sourced from marketing landing content.\n"
                    "- /terms — ToS rendered from legal output.\n"
                    "- /refunds — refund policy rendered from legal output.\n"
                    "- /compliance-note — internal page with the legal compliance checklist for owner reference.\n"
                ),
            },
            {"type": "update_task", "task_title": "Wire marketing pages and legal copy", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 92, "summary": "Marketing + legal pages wired."},
        ],
        tier="short",
    )
    await chat(
        "hivee/coder",
        "@hivee/qa Build is fully landed: shell + viewer, configurator + checkout, marketing + legal pages. Ready for the QA sweep.",
        mentions=["hivee/qa"],
        phase="coder.done",
        tier="medium",
    )

    # ── Wave 6: QA ─────────────────────────────────────────────────────────
    await chat(
        "hivee/qa",
        "Roger. Running through gates A–E now: viewer parallax, configurator state, checkout copy, marketing pages, accessibility.",
        phase="qa.start",
        tier="typing",
    )
    await act(
        "hivee/qa",
        [
            {"type": "update_task", "task_title": "Cross-browser, accessibility, and regression sweep", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 94, "summary": "QA sweep started."},
        ],
        tier="short",
    )
    await act(
        "hivee/qa",
        [
            {
                "type": "write_file",
                "path": "Outputs/qa/launch-readiness-report.md",
                "content": (
                    "# Launch Readiness Report\n\n"
                    "- Gate A — Hero shell: PASS. Viewer parallax responsive on hover and touch.\n"
                    "- Gate B — Configurator: PASS. State persists in URL; occasion variants render.\n"
                    "- Gate C — Checkout: PASS. Refund window shows before payment copy; ToS link visible.\n"
                    "- Gate D — Marketing pages: PASS. Hero, social proof, FAQ render across breakpoints.\n"
                    "- Gate E — Accessibility: PASS. Keyboard nav and color contrast meet WCAG AA.\n\n"
                    "No blockers. Cleared for primary readiness review.\n"
                ),
            },
            {"type": "update_task", "task_title": "Cross-browser, accessibility, and regression sweep", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "progress_pct": 96, "summary": "QA cleared all five gates."},
        ],
        tier="short",
    )
    await chat(
        "hivee/qa",
        "@hivee/main All five gates pass — no blockers. Cleared for primary readiness review and launch handoff.",
        mentions=["hivee/main"],
        phase="qa.done",
        tier="medium",
    )

    # ── Wave 7: final review ───────────────────────────────────────────────
    await act(
        "hivee/main",
        [
            {"type": "update_task", "task_title": "Primary final readiness review and launch handoff", "status": TASK_STATUS_IN_PROGRESS},
            {"type": "update_execution", "progress_pct": 98, "summary": "Primary agent reviewing all lane outputs."},
        ],
        tier="short",
    )
    await chat(
        "hivee/main",
        "@owner Doing the final readiness review now: cross-checking all 17 tasks, the synced files, and the QA report.",
        mentions=["owner"],
        phase="review.start",
        tier="long",
    )
    await act(
        "hivee/main",
        [
            {
                "type": "write_file",
                "path": "FINAL/launch-summary.md",
                "content": (
                    f"# Launch Summary — OhMyBoard\n\n"
                    f"Status: launch-ready prototype.\n\n"
                    f"Final URL: {DEMO_FINAL_URL}\n\n"
                    "## What shipped\n"
                    "- Hero with 3D-style lenticular viewer.\n"
                    "- Occasion-based configurator (graduation, birthday, wedding, grand opening).\n"
                    "- Checkout handoff with refund window + ToS visibility.\n"
                    "- Marketing landing page (hero, social proof, FAQ).\n"
                    "- Terms of service, refund policy, compliance checklist.\n"
                    "- QA launch readiness report — all five gates passed.\n\n"
                    "## Lane sign-offs\n"
                    "- Research, planner, copywriter, design, marketing, legal, coder, QA.\n"
                    "- Primary readiness: confirmed.\n"
                ),
            },
            {"type": "update_task", "task_title": "Primary final readiness review and launch handoff", "status": TASK_STATUS_DONE},
            {"type": "update_execution", "status": EXEC_STATUS_COMPLETED, "progress_pct": 100, "summary": "Execution complete — launch-ready."},
        ],
        tier="short",
    )
    _set_project_execution_state(project_id, status=EXEC_STATUS_COMPLETED, progress_pct=100)
    _refresh_project_documents(project_id)
    await chat(
        "hivee/main",
        f"@owner Website is finished. You can access it at {DEMO_FINAL_URL}",
        mentions=["owner"],
        phase="finished",
        tier="instant",
    )
    await emit(
        project_id,
        "project.execution.completed",
        {"project_id": project_id, "status": EXEC_STATUS_COMPLETED, "progress_pct": 100, "demo": True},
    )


def _demo_website_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OhMyBoard - Demo</title>
  <style>
    :root { color-scheme: light; --ink:#17202a; --muted:#627080; --line:#d9e2ec; --brand:#e4384f; --mint:#0c8f75; --gold:#d49b22; --paper:#fbfcfd; }
    * { box-sizing:border-box; }
    body { margin:0; font-family:Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; color:var(--ink); background:var(--paper); }
    main { min-height:100vh; display:grid; grid-template-columns:minmax(320px, 0.95fr) minmax(360px, 1.05fr); }
    .copy { padding:7vw 6vw; display:flex; flex-direction:column; justify-content:center; gap:28px; }
    .eyebrow { color:var(--brand); font-weight:800; text-transform:uppercase; letter-spacing:.08em; font-size:12px; }
    h1 { font-size:clamp(40px, 6vw, 78px); line-height:.94; margin:0; max-width:760px; }
    p { color:var(--muted); line-height:1.7; font-size:17px; max-width:620px; }
    .actions { display:flex; flex-wrap:wrap; gap:12px; }
    button, .button { border:1px solid var(--line); background:white; color:var(--ink); padding:13px 16px; border-radius:8px; font-weight:800; text-decoration:none; cursor:pointer; }
    .primary { background:var(--brand); color:white; border-color:var(--brand); }
    .stage { background:linear-gradient(135deg, #fff 0%, #edf6f5 100%); border-left:1px solid var(--line); min-height:100vh; display:grid; place-items:center; padding:44px; }
    .viewer { width:min(92vw, 620px); aspect-ratio:4/5; position:relative; display:grid; place-items:center; perspective:1000px; }
    .board { width:74%; aspect-ratio:3/4; border-radius:18px; background:linear-gradient(115deg, #ffffff 0%, #eef7ff 40%, #ffe7ec 50%, #fff7d9 62%, #ffffff 100%); border:10px solid #26313d; box-shadow:0 34px 70px rgba(23,32,42,.22); transform:rotateY(-18deg) rotateX(4deg); display:grid; place-items:center; text-align:center; padding:34px; transition:transform .3s ease; }
    .viewer:hover .board { transform:rotateY(18deg) rotateX(4deg); }
    .board h2 { font-size:42px; margin:0; }
    .board span { color:var(--brand); font-weight:900; }
    .config { position:absolute; right:0; bottom:20px; background:white; border:1px solid var(--line); border-radius:8px; padding:18px; width:min(320px, 84vw); box-shadow:0 20px 46px rgba(23,32,42,.15); }
    .config label { display:block; font-size:12px; font-weight:800; color:var(--muted); text-transform:uppercase; margin-bottom:6px; }
    .config input, .config select { width:100%; padding:11px; border:1px solid var(--line); border-radius:7px; margin-bottom:10px; font:inherit; }
    .price { display:flex; justify-content:space-between; align-items:center; font-weight:900; margin-top:8px; }
    @media (max-width:900px) { main { grid-template-columns:1fr; } .stage { min-height:640px; border-left:0; border-top:1px solid var(--line); } }
  </style>
</head>
<body>
  <main>
    <section class="copy">
      <div class="eyebrow">OhMyBoard Lenticular Gifts</div>
      <h1>A board that changes with the moment.</h1>
      <p>Create a premium congratulatory lenticular board for birthdays, graduations, launches, and milestones. Preview the depth effect, personalize the message, and send the order to checkout in one simple flow.</p>
      <div class="actions">
        <a class="button primary" href="https://ohmyboard.id">Start order</a>
        <a class="button" href="#viewer">View board</a>
      </div>
    </section>
    <section class="stage" id="viewer">
      <div class="viewer">
        <div class="board">
          <div>
            <h2>Congrats,<br><span>Ariel</span></h2>
            <p>Move your cursor to preview the lenticular shift.</p>
          </div>
        </div>
        <form class="config">
          <label>Occasion</label>
          <select><option>Graduation</option><option>Birthday</option><option>Grand opening</option></select>
          <label>Recipient</label>
          <input value="Ariel" />
          <label>Message</label>
          <input value="You did it!" />
          <div class="price"><span>Demo checkout</span><strong>$49</strong></div>
        </form>
      </div>
    </section>
  </main>
</body>
</html>
"""


def is_demo_project(project_id: str) -> bool:
    if not demo_mode_enabled():
        return False
    conn = db()
    row = conn.execute(
        "SELECT backend_mode FROM projects WHERE id = ? LIMIT 1",
        (project_id,),
    ).fetchone()
    conn.close()
    return bool(row and str(row["backend_mode"] or "").strip().lower() == "demo")
