from core.workspace_paths import *

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

def _project_meta_dir(project_dir: Path) -> Path:
    return (project_dir / PROJECT_META_DIRNAME).resolve()

def _append_project_meta_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def _is_project_decision_event(kind: str) -> bool:
    low = str(kind or "").strip().lower()
    if not low:
        return False
    if low in {"project.created", "run.completed", "run.stopped", "plan.approve", "plan.revert"}:
        return True
    if low.startswith("execution."):
        return True
    return False

def _project_handoff_markdown(
    *,
    title: str,
    project_id: str,
    plan_status: str,
    execution_status: str,
    progress_pct: int,
    primary_agent_id: Optional[str],
    agents: List[Dict[str, Any]],
    usage_total_tokens: int,
    updated_at: int,
) -> str:
    lines: List[str] = [
        f"# Handoff - {title}",
        "",
        f"- project_id: `{project_id}`",
        f"- plan_status: `{plan_status}`",
        f"- execution_status: `{execution_status}`",
        f"- progress_pct: `{progress_pct}`",
        f"- primary_agent_id: `{primary_agent_id or '-'}`",
        f"- assigned_agents: `{len(agents)}`",
        f"- total_tokens: `{max(0, _to_int(usage_total_tokens))}`",
        f"- updated_at: {format_ts(updated_at)}",
        "",
        "## Next Steps",
    ]
    if execution_status in {EXEC_STATUS_PAUSED, EXEC_STATUS_STOPPED}:
        lines.append("- Resume or rerun after owner confirmation.")
    elif execution_status == EXEC_STATUS_COMPLETED:
        lines.append("- Validate outputs and archive completed tasks.")
    else:
        lines.append("- Continue execution and monitor blockers.")
    lines.extend(
        [
            "",
            "## Active Agents",
        ]
    )
    if not agents:
        lines.append("- No agent assigned yet.")
    else:
        for row in agents:
            name = str(row.get("agent_name") or row.get("agent_id") or "agent")
            aid = str(row.get("agent_id") or "").strip()
            role = str(row.get("role") or "").strip()
            primary = bool(row.get("is_primary"))
            summary = f"- `{aid}` ({name})"
            if primary:
                summary += " [primary]"
            if role:
                summary += f" - {role}"
            lines.append(summary)
    lines.append("")
    return "\n".join(lines).strip() + "\n"

def _write_project_meta_bundle(
    *,
    project_id: str,
    owner_user_id: str,
    env_id: Optional[str],
    connection_id: str,
    created_at: Optional[int],
    project_root: str,
    title: str,
    brief: str,
    goal: str,
    setup_details: Dict[str, Any],
    role_rows: List[Dict[str, Any]],
    plan_status: str,
    plan_text: str,
    execution_status: str,
    progress_pct: int,
    execution_updated_at: Optional[int],
    usage_prompt_tokens: int,
    usage_completion_tokens: int,
    usage_total_tokens: int,
    usage_updated_at: Optional[int],
) -> None:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    except Exception:
        return
    if not project_dir.exists():
        return

    meta_dir = _project_meta_dir(project_dir)
    if not _path_within(meta_dir, project_dir):
        return
    checkpoints_dir = (meta_dir / Path(PROJECT_CHECKPOINTS_DIR).name).resolve()
    if not _path_within(checkpoints_dir, project_dir):
        return
    meta_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    now = int(time.time())
    normalized_plan_status = _coerce_plan_status(plan_status)
    normalized_exec_status = _coerce_execution_status(execution_status)
    normalized_progress = _clamp_progress(progress_pct)
    role_payload = [
        {
            "agent_id": str(r.get("agent_id") or ""),
            "agent_name": str(r.get("agent_name") or r.get("agent_id") or ""),
            "is_primary": bool(r.get("is_primary")),
            "role": str(r.get("role") or ""),
        }
        for r in (role_rows or [])
    ]
    primary_agent_id = next((r["agent_id"] for r in role_payload if r.get("is_primary")), None)

    card_payload = {
        "schemaVersion": "1.0",
        "project_id": project_id,
        "title": str(title or "").strip(),
        "brief": str(brief or "").strip(),
        "goal": str(goal or "").strip(),
        "owner_user_id": owner_user_id,
        "environment_id": env_id,
        "connection_id": connection_id,
        "project_root": project_root,
        "status": {
            "plan_status": normalized_plan_status,
            "execution_status": normalized_exec_status,
            "progress_pct": normalized_progress,
        },
        "assigned_agents": role_payload,
        "primary_agent_id": primary_agent_id,
        "created_at": _to_int(created_at) if created_at else None,
        "updated_at": now,
    }

    memory_path = meta_dir / Path(PROJECT_MEMORY_FILE).name
    existing_memory = _read_json_file(memory_path, {})
    memory_payload = {
        "summary": str(existing_memory.get("summary") or ""),
        "key_facts": [
            {"key": "title", "value": str(title or "").strip()},
            {"key": "goal", "value": str(goal or "").strip()},
            {"key": "brief", "value": str(brief or "").strip()[:600]},
            {"key": "plan_status", "value": normalized_plan_status},
            {"key": "execution_status", "value": normalized_exec_status},
        ],
        "open_questions": existing_memory.get("open_questions") if isinstance(existing_memory.get("open_questions"), list) else [],
        "assumptions": existing_memory.get("assumptions") if isinstance(existing_memory.get("assumptions"), list) else [],
        "decision_refs": existing_memory.get("decision_refs") if isinstance(existing_memory.get("decision_refs"), list) else [],
        "plan_excerpt": str(plan_text or "").strip()[:1500],
        "setup_highlights": {
            "target_users": str(setup_details.get("target_users") or setup_details.get("target_user") or "").strip(),
            "constraints": str(setup_details.get("constraints") or "").strip(),
            "first_output": str(setup_details.get("first_output") or "").strip(),
        },
        "updated_at": now,
    }

    policies_path = meta_dir / Path(PROJECT_POLICIES_FILE).name
    existing_policies = _read_json_file(policies_path, {})
    policies_payload = {
        "workspace_policy": {
            "workspace_root": _user_workspace_root_dir(owner_user_id).resolve().as_posix(),
            "project_root": project_dir.as_posix(),
            "allow_outside_workspace": False,
        },
        "approval_rules": existing_policies.get("approval_rules")
        if isinstance(existing_policies.get("approval_rules"), list)
        else [
            {"rule": "destructive_file_ops", "required": True},
            {"rule": "outside_project_scope", "required": True},
            {"rule": "high_token_budget", "required": True, "max_total_tokens": 120000},
        ],
        "execution_policy": {
            "pause_requires_owner": True,
            "stop_requires_owner": True,
            "max_parallel_agents": max(1, len(role_payload)),
        },
        "updated_at": now,
    }

    metrics_payload = {
        "project_id": project_id,
        "plan_status": normalized_plan_status,
        "execution_status": normalized_exec_status,
        "progress_pct": normalized_progress,
        "assigned_agents_count": len(role_payload),
        "prompt_tokens": max(0, _to_int(usage_prompt_tokens)),
        "completion_tokens": max(0, _to_int(usage_completion_tokens)),
        "total_tokens": max(0, _to_int(usage_total_tokens)),
        "usage_updated_at": usage_updated_at,
        "execution_updated_at": execution_updated_at,
        "updated_at": now,
    }

    risks_path = meta_dir / Path(PROJECT_RISKS_FILE).name
    existing_risks = _read_json_file(risks_path, {})
    existing_items = existing_risks.get("risks") if isinstance(existing_risks.get("risks"), list) else []
    risks_payload = {
        "risks": existing_items
        if existing_items
        else [
            {
                "id": "R-001",
                "title": "Scope drift",
                "severity": "medium",
                "status": "open",
                "mitigation": "Revalidate scope at each plan checkpoint.",
                "owner": "primary-agent",
            },
            {
                "id": "R-002",
                "title": "Missing credentials or external access",
                "severity": "high",
                "status": "open",
                "mitigation": "Pause and request owner approval when blockers appear.",
                "owner": "owner",
            },
        ],
        "updated_at": now,
    }

    checkpoint_latest_path = checkpoints_dir / "latest.json"
    previous_checkpoint = _read_json_file(checkpoint_latest_path, {})
    checkpoint_payload = {
        "project_id": project_id,
        "plan_status": normalized_plan_status,
        "execution_status": normalized_exec_status,
        "progress_pct": normalized_progress,
        "usage_total_tokens": max(0, _to_int(usage_total_tokens)),
        "updated_at": now,
    }
    milestone_changed = (
        isinstance(previous_checkpoint, dict)
        and previous_checkpoint
        and (
            str(previous_checkpoint.get("plan_status") or "") != normalized_plan_status
            or str(previous_checkpoint.get("execution_status") or "") != normalized_exec_status
            or _to_int(previous_checkpoint.get("progress_pct")) != normalized_progress
        )
    )

    _write_json_file(meta_dir / Path(PROJECT_CARD_FILE).name, card_payload)
    _write_json_file(memory_path, memory_payload)
    _write_json_file(policies_path, policies_payload)
    _write_json_file(meta_dir / Path(PROJECT_METRICS_FILE).name, metrics_payload)
    _write_json_file(risks_path, risks_payload)
    _write_json_file(checkpoint_latest_path, checkpoint_payload)
    if milestone_changed and (normalized_plan_status == PLAN_STATUS_APPROVED or normalized_exec_status in {EXEC_STATUS_PAUSED, EXEC_STATUS_STOPPED, EXEC_STATUS_COMPLETED}):
        checkpoint_name = f"checkpoint-{now}-{normalized_plan_status}-{normalized_exec_status}.json"
        _write_json_file(checkpoints_dir / checkpoint_name, checkpoint_payload)

    history_path = meta_dir / Path(PROJECT_HISTORY_FILE).name
    decisions_path = meta_dir / Path(PROJECT_DECISIONS_FILE).name
    if not history_path.exists():
        history_path.touch()
    if not decisions_path.exists():
        decisions_path.touch()

    handoff_text = _project_handoff_markdown(
        title=title,
        project_id=project_id,
        plan_status=normalized_plan_status,
        execution_status=normalized_exec_status,
        progress_pct=normalized_progress,
        primary_agent_id=primary_agent_id,
        agents=role_payload,
        usage_total_tokens=max(0, _to_int(usage_total_tokens)),
        updated_at=now,
    )
    (meta_dir / Path(PROJECT_HANDOFF_FILE).name).write_text(handoff_text, encoding="utf-8")

def _append_project_meta_event(
    *,
    project_dir: Path,
    kind: str,
    text: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    meta_dir = _project_meta_dir(project_dir)
    if not _path_within(meta_dir, project_dir):
        return
    meta_dir.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    event_record = {
        "ts": now,
        "kind": str(kind or "").strip(),
        "text": str(text or "").strip(),
        "payload": payload or {},
    }
    _append_project_meta_jsonl(meta_dir / Path(PROJECT_HISTORY_FILE).name, event_record)
    if _is_project_decision_event(kind):
        decision_record = {
            "ts": now,
            "decision_kind": str(kind or "").strip(),
            "decision": str(text or "").strip(),
            "context": payload or {},
        }
        _append_project_meta_jsonl(meta_dir / Path(PROJECT_DECISIONS_FILE).name, decision_record)

def _read_jsonl_tail(path: Path, limit: int = 40) -> List[Dict[str, Any]]:
    cap = max(1, min(int(limit or 40), 400))
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for raw in lines[-cap:]:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                out.append(parsed)
        except Exception:
            continue
    return out

def _load_project_meta_snapshot(
    *,
    owner_user_id: str,
    project_root: str,
    history_limit: int = 40,
) -> Dict[str, Any]:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    except Exception:
        return {"ok": False, "error": "Project path not accessible"}
    meta_dir = _project_meta_dir(project_dir)
    if not _path_within(meta_dir, project_dir):
        return {"ok": False, "error": "Project meta path is invalid"}
    handoff_text = ""
    handoff_path = meta_dir / Path(PROJECT_HANDOFF_FILE).name
    if handoff_path.exists():
        try:
            handoff_text = handoff_path.read_text(encoding="utf-8")
        except Exception:
            handoff_text = ""
    return {
        "ok": True,
        "meta_dir": meta_dir.as_posix(),
        "card": _read_json_file(meta_dir / Path(PROJECT_CARD_FILE).name, {}),
        "memory": _read_json_file(meta_dir / Path(PROJECT_MEMORY_FILE).name, {}),
        "metrics": _read_json_file(meta_dir / Path(PROJECT_METRICS_FILE).name, {}),
        "policies": _read_json_file(meta_dir / Path(PROJECT_POLICIES_FILE).name, {}),
        "risks": _read_json_file(meta_dir / Path(PROJECT_RISKS_FILE).name, {}),
        "latest_checkpoint": _read_json_file(meta_dir / Path(PROJECT_CHECKPOINTS_DIR).name / "latest.json", {}),
        "history": _read_jsonl_tail(meta_dir / Path(PROJECT_HISTORY_FILE).name, limit=history_limit),
        "decisions": _read_jsonl_tail(meta_dir / Path(PROJECT_DECISIONS_FILE).name, limit=history_limit),
        "handoff_md": handoff_text,
    }

def _refresh_project_documents(project_id: str) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT id, user_id, env_id, connection_id, title, brief, goal, project_root, setup_json, plan_text, plan_status,
               execution_status, progress_pct, execution_updated_at,
               usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, usage_updated_at,
               created_at
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
    _write_project_meta_bundle(
        project_id=str(row["id"] or project_id),
        owner_user_id=str(row["user_id"]),
        env_id=(str(row["env_id"]).strip() if row["env_id"] is not None else None),
        connection_id=str(row["connection_id"] or ""),
        created_at=row["created_at"],
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
    meta_dir = _project_meta_dir(project_dir)
    if not _path_within(info_dir, project_dir):
        return
    if not _path_within(meta_dir, project_dir):
        return
    info_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / Path(PROJECT_CHECKPOINTS_DIR).name).mkdir(parents=True, exist_ok=True)
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
    history_file = meta_dir / Path(PROJECT_HISTORY_FILE).name
    if not history_file.exists():
        history_file.touch()
    decisions_file = meta_dir / Path(PROJECT_DECISIONS_FILE).name
    if not decisions_file.exists():
        decisions_file.touch()
    handoff_file = meta_dir / Path(PROJECT_HANDOFF_FILE).name
    if not handoff_file.exists():
        handoff_file.write_text("# Handoff\n\nNo handoff summary generated yet.\n", encoding="utf-8")
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
        "pit stop",
        "blocked waiting",
        "pause until user",
    ]
    owner_context = any(marker in combined for marker in ["@owner", " owner", " user"])
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
    if re.search(r"\b(don't|do not)\s+(resume|continue|proceed|go on|carry on)\b", low):
        return False
    return bool(re.search(r"\b(resume|continue|proceed|go on|carry on)\b", low))

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
    try:
        _append_project_meta_event(
            project_dir=project_dir.resolve(),
            kind=kind,
            text=compact_text,
            payload=payload,
        )
    except Exception:
        pass

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

    target = _pick_first(["target user", "target users", "audience", "target market"])
    if target:
        details["target_users"] = target
    constraints = _pick_first(["constraint", "constraints", "budget", "deadline", "timeline", "compliance"])
    if constraints:
        details["constraints"] = constraints
    in_scope = _pick_first(["in-scope", "in scope", "scope"])
    if in_scope:
        details["in_scope"] = in_scope
    out_scope = _pick_first(["out-of-scope", "out of scope", "exclude", "not include"])
    if out_scope:
        details["out_of_scope"] = out_scope
    milestones = _pick_first(["milestone", "timeline", "schedule", "sprint", "deadline"])
    if milestones:
        details["milestones"] = milestones
    stack = _pick_first(["stack", "framework", "language", "tech", "tools", "tooling", "library"])
    if stack:
        details["required_stack"] = stack
    first_output = _pick_first(["first output", "deliverable", "output", "deliver"])
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
            if not details.get("target_users") and any(k in qlow for k in ["target user", "target users", "audience", "target market"]):
                details["target_users"] = compact[:5000]
            if not details.get("constraints") and any(k in qlow for k in ["constraint", "constraints", "budget", "deadline", "timeline", "compliance"]):
                details["constraints"] = compact[:5000]
            if not details.get("in_scope") and any(k in qlow for k in ["in-scope", "in scope", "scope"]):
                details["in_scope"] = compact[:5000]
            if not details.get("out_of_scope") and any(k in qlow for k in ["out-of-scope", "out of scope", "exclude", "not include"]):
                details["out_of_scope"] = compact[:5000]
            if not details.get("milestones") and any(k in qlow for k in ["milestone", "timeline", "schedule", "sprint"]):
                details["milestones"] = compact[:5000]
            if not details.get("required_stack") and any(k in qlow for k in ["stack", "framework", "tech", "tools", "language"]):
                details["required_stack"] = compact[:5000]
            if not details.get("first_output") and any(k in qlow for k in ["first output", "deliverable", "output", "deliver"]):
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


__all__ = [name for name in globals() if not name.startswith('__')]
