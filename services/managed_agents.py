import asyncio
import json
import os
import re

from services.project_utils import *
from services.project_activity import append_project_activity_log_entry


def _append_project_activity(
    *,
    project_id: str,
    actor_type: str,
    actor_id: Optional[str],
    actor_label: Optional[str],
    event_type: str,
    summary: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    conn = db()
    try:
        append_project_activity_log_entry(
            conn,
            project_id=project_id,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_label=actor_label,
            event_type=event_type,
            summary=summary,
            payload=payload or {},
        )
        conn.commit()
    finally:
        conn.close()


def _task_title_from_text(task_text: Any, *, role: str, agent_name: str) -> str:
    task_title = ""
    for line in str(task_text or "").splitlines():
        line = line.strip().lstrip("#").strip()
        if line:
            task_title = line
            break
    if not task_title:
        task_title = f"{role} \u2014 {agent_name}"
    return task_title[:120]


def _latest_project_task_id_for_agent(project_id: str, agent_id: str) -> str:
    pid = str(project_id or "").strip()
    aid = str(agent_id or "").strip()
    if not pid or not aid:
        return ""
    conn = db()
    try:
        row = conn.execute(
            """
            SELECT id
            FROM project_tasks
            WHERE project_id = ? AND assignee_agent_id = ?
            ORDER BY created_at DESC, updated_at DESC
            LIMIT 1
            """,
            (pid, aid),
        ).fetchone()
    finally:
        conn.close()
    return str(row["id"] or "").strip() if row else ""


async def _emit_project_chat_message_payload(
    project_id: str,
    message_payload: Dict[str, Any],
    *,
    dispatch_mentions: bool = True,
) -> None:
    if not isinstance(message_payload, dict):
        return
    await emit(project_id, "project.chat.message", message_payload)
    for target in (message_payload.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]:
        mention_payload = {
            "message_id": str(message_payload.get("id") or ""),
            "project_id": project_id,
            "target": target,
            "author_type": str(message_payload.get("author_type") or ""),
            "author_id": message_payload.get("author_id"),
            "author_label": message_payload.get("author_label"),
            "text": str(message_payload.get("text") or "")[:500],
            "created_at": int(message_payload.get("created_at") or 0),
        }
        await emit(project_id, "project.chat.mention", mention_payload)
        if dispatch_mentions:
            asyncio.create_task(
                _dispatch_chat_mention_to_connector(
                    project_id=project_id,
                    mention_target=str(mention_payload.get("target") or ""),
                    message_text=str(mention_payload.get("text") or ""),
                    from_agent_id=str(
                        mention_payload.get("author_id")
                        or mention_payload.get("author_label")
                        or "agent"
                    ),
                    from_label=str(mention_payload.get("author_label") or "Agent"),
                )
            )


async def _post_project_agent_status_message(
    *,
    project_id: str,
    agent_id: Optional[str],
    agent_name: Optional[str],
    text: str,
    mentions: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    body = str(text or "").strip()
    if not body:
        return None
    conn = db()
    try:
        payload = _create_project_chat_message(
            conn,
            project_id=project_id,
            author_type="project_agent",
            author_id=str(agent_id or "").strip() or None,
            author_label=str(agent_name or agent_id or "Agent").strip() or "Agent",
            text=body,
            mentions=list(mentions or []),
            metadata={
                "source": "delegate.status",
                **(metadata if isinstance(metadata, dict) else {}),
            },
        )
        conn.commit()
    finally:
        conn.close()
    await _emit_project_chat_message_payload(
        project_id,
        payload,
        dispatch_mentions=bool(mentions),
    )
    return payload


async def _emit_project_action_results(project_id: str, applied_actions: Any) -> None:
    for item in applied_actions or []:
        if not isinstance(item, dict):
            continue
        event_name = str(item.get("event") or "").strip()
        event_payload = item.get("event_payload") if isinstance(item.get("event_payload"), dict) else {}
        if event_name:
            await emit(project_id, event_name, event_payload)
        for extra in (item.get("extra_events") or []):
            if not isinstance(extra, dict):
                continue
            extra_event_name = str(extra.get("event") or "").strip()
            extra_event_payload = extra.get("event_payload") if isinstance(extra.get("event_payload"), dict) else {}
            if extra_event_name:
                await emit(project_id, extra_event_name, extra_event_payload)
            if extra_event_name == "project.chat.mention" and isinstance(extra_event_payload, dict):
                asyncio.create_task(
                    _dispatch_chat_mention_to_connector(
                        project_id=project_id,
                        mention_target=str(extra_event_payload.get("target") or ""),
                        message_text=str(extra_event_payload.get("text") or ""),
                        from_agent_id=str(
                            extra_event_payload.get("author_id")
                            or extra_event_payload.get("author_label")
                            or "agent"
                        ),
                        from_label=str(extra_event_payload.get("author_label") or "Agent"),
                    )
                )


def _applied_actions_include_kind(applied_actions: Any, expected_kind: str) -> bool:
    normalized_expected = _normalize_agent_action_kind(expected_kind)
    for item in applied_actions or []:
        if not isinstance(item, dict):
            continue
        current_kind = _normalize_agent_action_kind(
            item.get("type")
            or item.get("method")
            or item.get("action")
            or item.get("name")
        )
        if current_kind == normalized_expected:
            return True
    return False


def _applied_actions_have_project_work(applied_actions: Any) -> bool:
    for item in applied_actions or []:
        if not isinstance(item, dict):
            continue
        kind = _normalize_agent_action_kind(
            item.get("type")
            or item.get("method")
            or item.get("action")
            or item.get("name")
        )
        if kind and kind != "post_chat_message":
            return True
    return False


def _delegation_payload_has_structured_work(parsed: Any) -> bool:
    if not isinstance(parsed, dict):
        return False
    output_files = parsed.get("output_files")
    actions = parsed.get("actions")
    agent_tasks = parsed.get("agent_tasks")
    parallel_groups = parsed.get("parallel_groups")
    has_project_action = False
    for item in actions if isinstance(actions, list) else []:
        if not isinstance(item, dict):
            continue
        kind = _normalize_agent_action_kind(
            item.get("type")
            or item.get("method")
            or item.get("action")
            or item.get("name")
        )
        if kind and kind != "post_chat_message":
            has_project_action = True
            break
    return (
        (isinstance(output_files, list) and len(output_files) > 0)
        or has_project_action
        or (isinstance(agent_tasks, list) and len(agent_tasks) > 0)
        or (isinstance(parallel_groups, list) and len(parallel_groups) > 0)
    )


def _write_execution_kickoff_artifact(
    *,
    owner_user_id: str,
    project_root: str,
    project_id: str,
    title: str,
    primary_agent_id: Optional[str],
    agent_count: int,
) -> Dict[str, Any]:
    content = (
        "# Execution Kickoff\n\n"
        f"- project_id: {project_id}\n"
        f"- project: {title}\n"
        f"- primary_agent_id: {primary_agent_id or '-'}\n"
        f"- assigned_agents: {max(0, int(agent_count or 0))}\n"
        f"- started_at: {format_ts(int(time.time()))}\n\n"
        "Hivee created this artifact when execution started. The next steps must add "
        "project files, task updates, or execution progress actions; chat alone is not "
        "considered execution work.\n"
    )
    return _apply_project_file_writes(
        owner_user_id=owner_user_id,
        project_root=project_root,
        writes=[{"path": f"{USER_OUTPUTS_DIRNAME}/execution-kickoff.md", "content": content, "append": False}],
        default_prefix=USER_OUTPUTS_DIRNAME,
        allow_paths=None,
    )


def _read_project_delegation_state(project_id: str) -> Dict[str, Any]:
    conn = db()
    try:
        task_rows = conn.execute(
            """
            SELECT id, title, assignee_agent_id, weight_pct
            FROM project_tasks
            WHERE project_id = ?
            ORDER BY created_at ASC, updated_at ASC
            """,
            (project_id,),
        ).fetchall()
        mention_rows = conn.execute(
            """
            SELECT DISTINCT mention_target
            FROM project_chat_mentions
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchall()
    finally:
        conn.close()

    task_payload = []
    task_assignees: Set[str] = set()
    for row in task_rows:
        task_id = str(row["id"] or "").strip()
        assignee = str(row["assignee_agent_id"] or "").strip()
        if assignee:
            task_assignees.add(assignee)
        if not task_id:
            continue
        task_payload.append(
            {
                "id": task_id,
                "title": str(row["title"] or "").strip(),
                "assignee_agent_id": assignee,
                "weight_pct": max(0, min(100, int(row["weight_pct"] or 0))),
            }
        )

    mention_targets = {
        str(row["mention_target"] or "").strip()
        for row in mention_rows
        if str(row["mention_target"] or "").strip()
    }
    return {
        "task_rows": task_payload,
        "task_assignees": task_assignees,
        "mention_targets": mention_targets,
    }


def _build_minimal_progress_map(
    *,
    task_rows: List[Dict[str, Any]],
    parallel_groups: List[List[str]],
    by_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    node_ids_by_agent: Dict[str, List[str]] = {}

    if task_rows:
        for row in task_rows:
            node_id = str(row.get("id") or "").strip()
            if not node_id:
                continue
            agent_id = str(row.get("assignee_agent_id") or "").strip()
            title = str(row.get("title") or "").strip() or f"Task for {agent_id or 'agent'}"
            node = {
                "id": node_id,
                "label": title[:160],
                "agent": agent_id,
                "weight_pct": max(0, min(100, int(row.get("weight_pct") or 0))),
                "depends_on": [],
            }
            nodes.append(node)
            if agent_id:
                node_ids_by_agent.setdefault(agent_id, []).append(node_id)
    else:
        agent_ids = list(by_id.keys())
        total_agents = len(agent_ids)
        base_weight = (100 // total_agents) if total_agents else 0
        remainder = (100 - (base_weight * total_agents)) if total_agents else 0
        for idx, aid in enumerate(agent_ids):
            row_item = by_id.get(aid) or {}
            label = (
                str(row_item.get("role") or "").strip()
                or str(row_item.get("agent_name") or "").strip()
                or aid
            )
            node_id = f"delegation-{_safe_agent_filename(aid)}"
            node = {
                "id": node_id,
                "label": label[:160],
                "agent": aid,
                "weight_pct": base_weight + (1 if idx < remainder else 0),
                "depends_on": [],
            }
            nodes.append(node)
            node_ids_by_agent.setdefault(aid, []).append(node_id)

    node_map = {str(node.get("id") or ""): node for node in nodes if str(node.get("id") or "").strip()}
    groups: List[List[str]] = []
    prev_group_ids: List[str] = []

    for grp in parallel_groups:
        group_ids: List[str] = []
        seen_group: Set[str] = set()
        for aid in grp:
            for node_id in node_ids_by_agent.get(str(aid).strip(), []):
                if not node_id or node_id in seen_group:
                    continue
                seen_group.add(node_id)
                group_ids.append(node_id)
        if not group_ids:
            continue
        groups.append(group_ids)
        if prev_group_ids:
            for node_id in group_ids:
                node = node_map.get(node_id)
                if node is None:
                    continue
                node["depends_on"] = list(prev_group_ids)
        prev_group_ids = list(group_ids)

    if not groups and nodes:
        groups = [[str(node.get("id") or "")] for node in nodes if str(node.get("id") or "").strip()]

    return {"nodes": nodes, "groups": groups}
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


def _sync_connector_agent_state(
    *,
    user_id: str,
    connection_id: str,
    provision_from_snapshot: bool = False,
    persist_policy: bool = True,
) -> Dict[str, Any]:
    conn = db()
    connector_row = conn.execute(
        """
        SELECT id, name, status, last_seen_at, openclaw_base_url
        FROM connectors
        WHERE id = ? AND user_id = ?
        LIMIT 1
        """,
        (connection_id, user_id),
    ).fetchone()
    if not connector_row:
        conn.close()
        return {
            "ok": False,
            "exists": False,
            "connection_id": connection_id,
            "agents": [],
            "agent_count": 0,
            "main_agent_id": None,
            "main_agent_name": None,
            "bootstrap_status": "missing",
            "bootstrap_error": "Connector not found",
            "has_snapshot": False,
            "is_online": False,
            "status": "missing",
            "last_seen_at": None,
            "workspace_root": "",
            "templates_root": "",
            "workspace_tree": "",
            "provision": None,
        }

    policy_row = conn.execute(
        """
        SELECT main_agent_id, main_agent_name, bootstrap_status, bootstrap_error,
               workspace_root, templates_root, workspace_tree
        FROM connection_policies
        WHERE connection_id = ? AND user_id = ?
        LIMIT 1
        """,
        (connection_id, user_id),
    ).fetchone()
    snapshot_row = conn.execute(
        """
        SELECT snapshot_json, updated_at
        FROM connector_agent_snapshots
        WHERE connector_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (connection_id,),
    ).fetchone()
    managed_rows = conn.execute(
        """
        SELECT agent_id, agent_name
        FROM managed_agents
        WHERE user_id = ? AND connection_id = ? AND status = 'active'
        ORDER BY updated_at DESC, provisioned_at DESC, agent_name ASC
        """,
        (user_id, connection_id),
    ).fetchall()
    conn.close()

    snapshot_payload: Dict[str, Any] = {}
    if snapshot_row:
        try:
            snapshot_payload = json.loads(str(snapshot_row["snapshot_json"] or "{}"))
            if not isinstance(snapshot_payload, dict):
                snapshot_payload = {}
        except Exception:
            snapshot_payload = {}

    snapshot_agents = _normalize_managed_agent_candidates(_extract_agents_list(snapshot_payload) or [])
    provision_result = None
    if provision_from_snapshot and snapshot_agents:
        first_snapshot = snapshot_agents[0]
        provision_result = _provision_managed_agents_for_connection(
            user_id=user_id,
            env_id=None,
            connection_id=connection_id,
            base_url=str(connector_row["openclaw_base_url"] or "").strip(),
            raw_agents=[agent.get("raw") or {"id": agent["id"], "name": agent["name"]} for agent in snapshot_agents],
            fallback_agent_id=str(first_snapshot.get("id") or "").strip() or None,
            fallback_agent_name=str(first_snapshot.get("name") or "").strip() or None,
        )

    combined_agents: List[Dict[str, Any]] = []
    seen_agent_ids: set[str] = set()
    for item in snapshot_agents:
        agent_id = str(item.get("id") or "").strip()
        if not agent_id or agent_id in seen_agent_ids:
            continue
        seen_agent_ids.add(agent_id)
        combined_agents.append({"id": agent_id, "name": str(item.get("name") or agent_id).strip() or agent_id})
    for row in managed_rows or []:
        agent_id = str(row["agent_id"] or "").strip()
        if not agent_id or _is_default_placeholder_agent(agent_id) or agent_id in seen_agent_ids:
            continue
        seen_agent_ids.add(agent_id)
        combined_agents.append(
            {
                "id": agent_id,
                "name": str(row["agent_name"] or agent_id).strip() or agent_id,
            }
        )

    agent_name_by_id = {str(item.get("id") or "").strip(): str(item.get("name") or item.get("id") or "").strip() for item in combined_agents}
    auto_primary = None
    for item in combined_agents:
        aid = str(item.get("id") or "").strip()
        name = str(item.get("name") or aid).strip()
        haystack = f"{aid} {name}".lower()
        if "main" in haystack:
            auto_primary = {"id": aid, "name": name}
            break
    if auto_primary is None and combined_agents:
        first_agent = combined_agents[0]
        auto_primary = {
            "id": str(first_agent.get("id") or "").strip(),
            "name": str(first_agent.get("name") or first_agent.get("id") or "").strip(),
        }
    policy_main_agent_id = str(policy_row["main_agent_id"] or "").strip() if policy_row else ""
    policy_main_agent_name = str(policy_row["main_agent_name"] or "").strip() if policy_row else ""
    if policy_main_agent_id and _is_default_placeholder_agent(policy_main_agent_id):
        policy_main_agent_id = ""
        policy_main_agent_name = ""

    main_agent_id = policy_main_agent_id or str((auto_primary or {}).get("id") or "").strip()
    main_agent_name = (
        policy_main_agent_name
        or str((auto_primary or {}).get("name") or "").strip()
        or agent_name_by_id.get(main_agent_id, "")
        or main_agent_id
    ).strip()

    has_snapshot = snapshot_row is not None
    if main_agent_id:
        bootstrap_status = "ok"
        bootstrap_error = None
    elif has_snapshot:
        bootstrap_status = "no_agents"
        bootstrap_error = "No real agents found in the latest hub snapshot."
    else:
        bootstrap_status = "awaiting_agent_snapshot"
        bootstrap_error = "Hub has not published any agent snapshot yet."

    workspace = _ensure_user_workspace(user_id)
    if persist_policy:
        current_main_agent_id = str(policy_row["main_agent_id"] or "").strip() if policy_row else ""
        current_main_agent_name = str(policy_row["main_agent_name"] or "").strip() if policy_row else ""
        current_bootstrap_status = str(policy_row["bootstrap_status"] or "").strip() if policy_row else ""
        current_bootstrap_error = str(policy_row["bootstrap_error"] or "").strip() if policy_row else ""
        current_workspace_root = str(policy_row["workspace_root"] or "").strip() if policy_row else ""
        current_templates_root = str(policy_row["templates_root"] or "").strip() if policy_row else ""
        current_workspace_tree = str(policy_row["workspace_tree"] or "").strip() if policy_row else ""
        next_workspace_root = str(workspace["workspace_root"] or "").strip()
        next_templates_root = str(workspace["templates_root"] or "").strip()
        next_workspace_tree = str(workspace.get("workspace_tree") or "").strip()
        if (
            not policy_row
            or current_main_agent_id != main_agent_id
            or current_main_agent_name != main_agent_name
            or current_bootstrap_status != bootstrap_status
            or current_bootstrap_error != str(bootstrap_error or "")
            or current_workspace_root != next_workspace_root
            or current_templates_root != next_templates_root
            or current_workspace_tree != next_workspace_tree
        ):
            _upsert_connection_policy(
                connection_id,
                user_id,
                main_agent_id=main_agent_id or None,
                main_agent_name=main_agent_name or None,
                bootstrap_status=bootstrap_status,
                bootstrap_error=bootstrap_error,
                workspace_tree=workspace.get("workspace_tree"),
                workspace_root=next_workspace_root,
                templates_root=next_templates_root,
            )

    connector_status = str(connector_row["status"] or "").strip().lower() or "unknown"
    last_seen_at = _to_int(connector_row["last_seen_at"])
    is_online = bool(
        connector_status in {"online", "active", "ready"}
        and last_seen_at > 0
        and (time.time() - last_seen_at) <= 60
    )

    return {
        "ok": bool(main_agent_id),
        "exists": True,
        "connection_id": connection_id,
        "agents": combined_agents,
        "agent_count": len(combined_agents),
        "main_agent_id": main_agent_id or None,
        "main_agent_name": main_agent_name or None,
        "bootstrap_status": bootstrap_status,
        "bootstrap_error": bootstrap_error,
        "has_snapshot": has_snapshot,
        "is_online": is_online,
        "status": connector_status,
        "last_seen_at": last_seen_at or None,
        "workspace_root": workspace["workspace_root"],
        "templates_root": workspace["templates_root"],
        "workspace_tree": workspace.get("workspace_tree") or "",
        "provision": provision_result,
    }

async def _bootstrap_connection_workspace(user_id: str, base_url: str, api_key: str) -> Dict[str, Any]:
    main_agent_id: Optional[str] = None
    main_agent_name: Optional[str] = None
    discovered_agents: List[Dict[str, Any]] = []
    probe = await openclaw_list_agents(base_url, api_key)
    if not probe.get("ok") and probe.get("error_code") == "missing_operator_write":
        # Token is valid (health likely works) but lacks operator.write — surface this
        # as a specific error code so callers can set connection_state accurately.
        health_check = await openclaw_health(base_url, api_key)
        return {
            "ok": False,
            "error_code": "missing_operator_write",
            "error": probe.get("error"),
            "hint": probe.get("hint"),
            "health_ok": health_check.get("ok"),
            "main_agent_id": None,
            "main_agent_name": None,
            "agents": [],
        }
    if not probe.get("ok"):
        fallback_health = await openclaw_health(base_url, api_key)
        fallback_agents: List[Dict[str, Any]] = []
        if fallback_health.get("ok"):
            fallback_payload = fallback_health.get("payload")
            fallback_raw_agents = _extract_agents_list(fallback_payload) or []
            if fallback_raw_agents:
                fallback_agents = _normalize_agents(fallback_raw_agents)
        if fallback_agents:
            probe = {
                "ok": True,
                "transport": "health-fallback",
                "path": str(fallback_health.get("path") or ""),
                "agents": fallback_agents,
                "warning": "Agent list resolved from health payload fallback.",
                "original_error": detail_to_text(probe.get("error") or probe.get("details") or probe)[:1000],
            }
        elif fallback_health.get("ok"):
            probe = {
                "ok": True,
                "transport": "health-only",
                "path": str(fallback_health.get("path") or ""),
                "agents": [],
                "warning": "Agent list endpoint unavailable; using health-only bootstrap fallback.",
                "original_error": detail_to_text(probe.get("error") or probe.get("details") or probe)[:1000],
            }
        else:
            probe_error = detail_to_text(probe.get("error") or probe.get("details") or probe)[:1200]
            probe_hint = detail_to_text(probe.get("hint") or "")[:400]
            health_error = detail_to_text(fallback_health.get("error") or fallback_health.get("details") or "")[:600]
            composed_error = probe_error or "Could not verify OpenClaw agent endpoint. Check base_url and API key/token."
            if probe_hint:
                composed_error = f"{composed_error} Hint: {probe_hint}"
            if health_error and health_error not in composed_error:
                composed_error = f"{composed_error} Health: {health_error}"
            return {
                "ok": False,
                "error": composed_error[:1800],
                "main_agent_id": None,
                "main_agent_name": None,
                "agent_probe": probe,
                "health_probe": fallback_health,
                "agents": [],
            }
    if probe.get("ok"):
        discovered_agents = [dict(a) for a in (probe.get("agents") or []) if isinstance(a, dict)]
        picked = _pick_main_agent(probe.get("agents") or [])
        if picked:
            picked_id = str(picked.get("id") or "").strip()
            main_agent_id = picked_id or None
            main_agent_name = str(picked.get("name") or main_agent_id)
    if (not discovered_agents) and main_agent_id:
        discovered_agents = [{"id": main_agent_id, "name": main_agent_name or main_agent_id}]

    try:
        workspace = _ensure_user_workspace(user_id)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to provision server workspace: {str(e)}",
            "main_agent_id": main_agent_id,
            "main_agent_name": main_agent_name,
            "agent_probe": probe,
            "agents": discovered_agents,
        }

    return {
        "ok": True,
        "main_agent_id": main_agent_id,
        "main_agent_name": main_agent_name,
        "agent_probe": probe,
        "agents": discovered_agents,
        "workspace_tree": workspace["workspace_tree"],
        "workspace_root": workspace["workspace_root"],
        "templates_root": workspace["templates_root"],
        "projects_root": workspace["projects_root"],
        "template_warnings": workspace.get("template_warnings") or [],
    }

def _is_default_placeholder_agent(agent_id: str) -> bool:
    """Return True for generic placeholder IDs that are not real agents."""
    low = str(agent_id or "").strip().lower()
    return not low or "default" in low


def _normalize_managed_agent_candidates(
    raw_agents: Any,
    *,
    fallback_agent_id: Optional[str] = None,
    fallback_agent_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    seen: set[str] = set()
    candidates = raw_agents if isinstance(raw_agents, list) else []
    for row in candidates:
        if isinstance(row, str):
            aid = str(row).strip()
            nm = aid
            raw = None
        elif isinstance(row, dict):
            aid = str(row.get("id") or row.get("agent_id") or row.get("name") or "").strip()
            nm = str(row.get("name") or row.get("title") or aid).strip()
            raw = row
        else:
            continue
        if not aid:
            continue
        if _is_default_placeholder_agent(aid):
            continue
        if aid in seen:
            continue
        seen.add(aid)
        cleaned.append({"id": aid[:180], "name": (nm or aid)[:220], "raw": raw})
    fallback_id = str(fallback_agent_id or "").strip()
    if fallback_id and fallback_id not in seen and not _is_default_placeholder_agent(fallback_id):
        cleaned.append(
            {
                "id": fallback_id[:180],
                "name": (str(fallback_agent_name or fallback_id).strip() or fallback_id)[:220],
                "raw": None,
            }
        )
    return cleaned

def _managed_agent_capability_key(raw_label: Any) -> str:
    label = str(raw_label or "").strip()
    if not label:
        return ""
    label = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", label)
    label = re.sub(r"[^a-zA-Z0-9]+", "_", label).strip("_").lower()
    return label[:80]

def _merge_managed_agent_capabilities(target: Dict[str, Any], source: Any) -> None:
    if not isinstance(target, dict) or source is None:
        return
    if isinstance(source, dict):
        for raw_key, raw_value in source.items():
            key = _managed_agent_capability_key(raw_key)
            if not key:
                continue
            if isinstance(raw_value, bool):
                target[key] = raw_value
            elif isinstance(raw_value, (int, float)):
                target[key] = raw_value
            elif isinstance(raw_value, dict):
                target[key] = dict(raw_value)
            elif isinstance(raw_value, list):
                target[key] = [str(item).strip() for item in raw_value if str(item).strip()]
            else:
                target[key] = bool(str(raw_value).strip())
        return
    if isinstance(source, (list, tuple, set)):
        for item in source:
            if isinstance(item, dict):
                label = item.get("name") or item.get("id") or item.get("title") or item.get("label")
            else:
                label = item
            key = _managed_agent_capability_key(label)
            if key:
                target[key] = True
        return
    key = _managed_agent_capability_key(source)
    if key:
        target[key] = True

def _normalize_managed_agent_skills(*sources: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        if isinstance(source, (list, tuple, set)):
            candidates = list(source)
        elif isinstance(source, (dict, str)):
            candidates = [source]
        else:
            candidates = []
        for item in candidates:
            if isinstance(item, str):
                skill_name = str(item).strip()
                if not skill_name:
                    continue
                skill_id = _managed_agent_capability_key(skill_name) or "skill"
                skill_payload: Dict[str, Any] = {"id": skill_id, "name": skill_name}
            elif isinstance(item, dict):
                skill_name = str(item.get("name") or item.get("title") or item.get("id") or "").strip()
                if not skill_name:
                    continue
                skill_id = str(item.get("id") or _managed_agent_capability_key(skill_name) or "skill").strip()
                skill_payload = {"id": skill_id[:120], "name": skill_name[:220]}
                skill_desc = str(item.get("description") or item.get("summary") or "").strip()
                if skill_desc:
                    skill_payload["description"] = skill_desc[:320]
                tags = item.get("tags")
                if isinstance(tags, list):
                    skill_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
                    if skill_tags:
                        skill_payload["tags"] = skill_tags[:12]
            else:
                continue
            dedupe_key = str(skill_payload.get("name") or skill_payload.get("id") or "").strip().lower()
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            output.append(skill_payload)
    return output[:12]

def _select_managed_agent_description(
    *,
    agent_id: str,
    raw_agent: Optional[Dict[str, Any]],
    existing_card: Optional[Dict[str, Any]],
) -> str:
    raw = raw_agent if isinstance(raw_agent, dict) else {}
    existing = existing_card if isinstance(existing_card, dict) else {}
    candidates = [
        raw.get("description"),
        raw.get("summary"),
        raw.get("role"),
        existing.get("description"),
        f"Hivee managed profile for agent `{agent_id}`.",
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text[:600]
    return f"Hivee managed profile for agent `{agent_id}`."

def _build_managed_agent_card(
    *,
    agent_id: str,
    agent_name: str,
    base_url: str,
    connection_id: str,
    env_id: Optional[str],
    root_path: str,
    raw_agent: Optional[Dict[str, Any]] = None,
    existing_card: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    safe_skill_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-") or "agent"
    raw = raw_agent if isinstance(raw_agent, dict) else {}
    existing = existing_card if isinstance(existing_card, dict) else {}
    provider = dict(existing.get("provider") or {}) if isinstance(existing.get("provider"), dict) else {}
    provider.setdefault("organization", "Hivee")

    capabilities: Dict[str, Any] = {}
    _merge_managed_agent_capabilities(capabilities, existing.get("capabilities"))
    _merge_managed_agent_capabilities(capabilities, raw.get("capabilities"))
    _merge_managed_agent_capabilities(capabilities, raw.get("tools"))
    _merge_managed_agent_capabilities(capabilities, raw.get("tags"))
    capabilities.setdefault("streaming", True)
    capabilities.setdefault("push_notifications", False)
    capabilities.setdefault("state_transition_history", True)

    skills = _normalize_managed_agent_skills(
        raw.get("skills"),
        existing.get("skills"),
    )
    if not skills:
        skills = [
            {
                "id": f"{safe_skill_id}.execute",
                "name": "Project Execution",
                "description": "Executes scoped project tasks and reports progress.",
                "tags": ["execution", "workflow", "collaboration"],
            }
        ]

    metadata = dict(existing.get("metadata") or {}) if isinstance(existing.get("metadata"), dict) else {}
    metadata.update(
        {
            "managedBy": "hivee",
            "connectionId": connection_id,
            "environmentId": env_id,
            "rootPath": root_path,
            "provisionedAt": now,
            "hiveeProjectOps": [
                "write_file",
                "append_file",
                "upload_file",
                "delete_file",
                "move_file",
                "create_dir",
                "delete_dir",
                "create_task",
                "update_task",
                "delete_task",
                "add_task_dependency",
                "remove_task_dependency",
                "apply_task_blueprint",
                "update_execution",
                "post_chat_message",
            ],
            "hiveeRealtime": ["project.chat.message", "project.chat.mention", "project.execution.updated"],
        }
    )
    model_hint = str(
        raw.get("model")
        or raw.get("adapter_type")
        or metadata.get("agentModel")
        or ""
    ).strip()
    if model_hint:
        metadata["agentModel"] = model_hint[:180]
    source_role = str(raw.get("role") or "").strip()
    if source_role:
        metadata["sourceRole"] = source_role[:180]

    card_payload = dict(existing)
    card_payload.update(
        {
        "schemaVersion": MANAGED_AGENT_CARD_VERSION,
        "name": str(agent_name or agent_id),
        "description": _select_managed_agent_description(
            agent_id=agent_id,
            raw_agent=raw,
            existing_card=existing,
        ),
        "version": str(existing.get("version") or "1.0.0"),
        "provider": provider,
        "supportedInterfaces": [
            {
                "url": str(base_url or "").rstrip("/"),
                "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0",
            }
        ],
        "capabilities": capabilities,
        "defaultInputModes": (
            list(existing.get("defaultInputModes"))
            if isinstance(existing.get("defaultInputModes"), list) and existing.get("defaultInputModes")
            else ["text"]
        ),
        "defaultOutputModes": (
            list(existing.get("defaultOutputModes"))
            if isinstance(existing.get("defaultOutputModes"), list) and existing.get("defaultOutputModes")
            else ["text"]
        ),
        "skills": skills,
        "securityRequirements": (
            list(existing.get("securityRequirements"))
            if isinstance(existing.get("securityRequirements"), list) and existing.get("securityRequirements")
            else [{"type": "bearer", "scopes": ["env.read", "project.read", "project.write", "project.chat", "project.state.write"]}]
        ),
        "metadata": metadata,
        }
    )
    return card_payload

def _append_managed_agent_history_record(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    env_id: Optional[str],
    connection_id: str,
    agent_id: str,
    event_kind: str,
    event_text: str,
    event_payload: Optional[Dict[str, Any]],
    history_file: Optional[Path] = None,
) -> None:
    payload_json = json.dumps(event_payload or {}, ensure_ascii=False)
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO managed_agent_history (
            id, user_id, env_id, connection_id, agent_id, event_kind, event_text, event_payload_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            new_id("mgh"),
            user_id,
            env_id,
            connection_id,
            agent_id,
            str(event_kind or "event")[:120],
            str(event_text or "")[:2000],
            payload_json,
            now,
        ),
    )
    if history_file:
        history_file.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": now,
            "event_kind": str(event_kind or "event")[:120],
            "event_text": str(event_text or "")[:2000],
            "payload": event_payload or {},
        }
        with history_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def _refresh_managed_agents_index(user_id: str) -> None:
    conn = db()
    rows = conn.execute(
        """
        SELECT connection_id, agent_id, agent_name, status, root_path, updated_at
        FROM managed_agents
        WHERE user_id = ?
        ORDER BY updated_at DESC, agent_name ASC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    payload = {
        "generated_at": int(time.time()),
        "count": len(rows),
        "agents": [
            {
                "connection_id": str(r["connection_id"] or ""),
                "agent_id": str(r["agent_id"] or ""),
                "agent_name": str(r["agent_name"] or ""),
                "status": str(r["status"] or ""),
                "root_path": str(r["root_path"] or ""),
                "updated_at": _to_int(r["updated_at"]),
            }
            for r in rows
        ],
    }
    _write_json_file(_user_agents_root_dir(user_id) / "index.json", payload)

def _provision_managed_agents_for_connection(
    *,
    user_id: str,
    env_id: Optional[str],
    connection_id: str,
    base_url: str,
    raw_agents: Any,
    fallback_agent_id: Optional[str] = None,
    fallback_agent_name: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_user_workspace(user_id)
    now = int(time.time())
    workspace_root = _user_workspace_root_dir(user_id).resolve()
    normalized_agents = _normalize_managed_agent_candidates(
        raw_agents,
        fallback_agent_id=fallback_agent_id,
        fallback_agent_name=fallback_agent_name,
    )
    if not normalized_agents:
        return {
            "ok": False,
            "error": "No agents available for managed provisioning",
            "provisioned": 0,
            "updated": 0,
            "failed": 0,
            "agents": [],
            "errors": [],
        }

    conn = db()
    provisioned = 0
    updated = 0
    failed = 0
    errors: List[str] = []
    output_agents: List[Dict[str, Any]] = []
    for agent in normalized_agents:
        agent_id = str(agent.get("id") or "").strip()
        agent_name = str(agent.get("name") or agent_id).strip() or agent_id
        raw_agent = agent.get("raw") if isinstance(agent.get("raw"), dict) else None
        if not agent_id:
            failed += 1
            errors.append("Missing agent id in candidate entry")
            continue
        try:
            parts = _agent_component_paths(user_id, connection_id, agent_id)
            try:
                for path in parts.values():
                    path.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass  # Filesystem may be ephemeral (e.g. Railway) — don't block DB provisioning

            root_path = str(parts["root"].resolve()) if parts["root"].exists() else str(parts["root"])
            memory_payloads = {
                "working": {
                    "scope": "working",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
                "project": {
                    "scope": "project",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
                "long_term": {
                    "scope": "long_term",
                    "summary": "",
                    "entries": [],
                    "updated_at": now,
                },
            }
            checkpoint_state = {
                "checkpoint_key": "latest",
                "status": "ready",
                "notes": "Auto-generated checkpoint seed.",
                "updated_at": now,
            }
            permissions_payload = {
                "scopes": ["env.read", "project.read", "project.write", "project.chat", "project.state.write"],
                "tools": ["workspace.read", "workspace.write", "chat.send", "project.control", "project.agent_ops", "project.chat.post"],
                "path_allowlist": [workspace_root.as_posix(), root_path],
                "secrets_policy": {
                    "mode": "connection-bound",
                    "connection_id": connection_id,
                },
                "approval_required": True,
                "updated_at": now,
            }
            metrics_payload = {
                "success_count": 0,
                "failure_count": 0,
                "total_calls": 0,
                "total_prompt_tokens": 0,
                "total_completion_tokens": 0,
                "total_latency_ms": 0,
                "last_error": None,
                "last_seen_at": None,
                "updated_at": now,
            }
            approval_rules = {
                "destructive_write": {
                    "description": "Require owner approval for destructive file actions.",
                    "required": True,
                    "patterns": ["delete", "remove", "truncate", "reset", "drop table"],
                },
                "outside_workspace": {
                    "description": "Require owner approval for access outside workspace root.",
                    "required": True,
                    "workspace_root": workspace_root.as_posix(),
                },
                "high_token_budget": {
                    "description": "Require owner approval for very large token usage.",
                    "required": True,
                    "max_total_tokens": 120000,
                },
            }

            existing = conn.execute(
                "SELECT id, card_json FROM managed_agents WHERE user_id = ? AND connection_id = ? AND agent_id = ?",
                (user_id, connection_id, agent_id),
            ).fetchone()
            existing_card: Dict[str, Any] = {}
            if existing:
                try:
                    existing_card = json.loads(str(existing["card_json"] or "{}"))
                except Exception:
                    existing_card = {}
            card_payload = _build_managed_agent_card(
                agent_id=agent_id,
                agent_name=agent_name,
                base_url=base_url,
                connection_id=connection_id,
                env_id=env_id,
                root_path=root_path,
                raw_agent=raw_agent,
                existing_card=existing_card,
            )
            card_json = json.dumps(card_payload, ensure_ascii=False)
            if existing:
                conn.execute(
                    """
                    UPDATE managed_agents
                    SET env_id = ?, agent_name = ?, status = ?, card_version = ?, card_json = ?, root_path = ?, updated_at = ?
                    WHERE user_id = ? AND connection_id = ? AND agent_id = ?
                    """,
                    (
                        env_id,
                        agent_name,
                        "active",
                        MANAGED_AGENT_CARD_VERSION,
                        card_json,
                        root_path,
                        now,
                        user_id,
                        connection_id,
                        agent_id,
                    ),
                )
                updated += 1
                event_kind = "agent.synced"
                event_text = "Managed agent resources refreshed."
            else:
                conn.execute(
                    """
                    INSERT INTO managed_agents (
                        id, user_id, env_id, connection_id, agent_id, agent_name, status,
                        card_version, card_json, root_path, provisioned_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        new_id("mga"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        agent_name,
                        "active",
                        MANAGED_AGENT_CARD_VERSION,
                        card_json,
                        root_path,
                        now,
                        now,
                    ),
                )
                provisioned += 1
                event_kind = "agent.provisioned"
                event_text = "Managed agent resources initialized."

            for scope in MANAGED_AGENT_MEMORY_SCOPES:
                scope_payload = memory_payloads.get(scope, {"scope": scope, "summary": "", "entries": [], "updated_at": now})
                conn.execute(
                    """
                    INSERT INTO managed_agent_memory (
                        id, user_id, env_id, connection_id, agent_id, memory_scope, summary, payload_json, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, connection_id, agent_id, memory_scope) DO UPDATE SET
                        env_id=excluded.env_id,
                        summary=CASE
                            WHEN managed_agent_memory.summary IS NULL OR managed_agent_memory.summary = '' THEN excluded.summary
                            ELSE managed_agent_memory.summary
                        END,
                        payload_json=CASE
                            WHEN managed_agent_memory.payload_json IS NULL OR managed_agent_memory.payload_json = '' THEN excluded.payload_json
                            ELSE managed_agent_memory.payload_json
                        END,
                        updated_at=excluded.updated_at
                    """,
                    (
                        new_id("mgm"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        scope,
                        str(scope_payload.get("summary") or ""),
                        json.dumps(scope_payload, ensure_ascii=False),
                        now,
                    ),
                )

            conn.execute(
                """
                INSERT INTO managed_agent_checkpoints (
                    id, user_id, env_id, connection_id, agent_id, checkpoint_key, state_json, status, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id, checkpoint_key) DO UPDATE SET
                    env_id=excluded.env_id,
                    state_json=excluded.state_json,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgc"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    "latest",
                    json.dumps(checkpoint_state, ensure_ascii=False),
                    "ready",
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO managed_agent_permissions (
                    id, user_id, env_id, connection_id, agent_id, scopes_json, tools_json, path_allowlist_json, secrets_policy_json, approval_required, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id) DO UPDATE SET
                    env_id=excluded.env_id,
                    scopes_json=excluded.scopes_json,
                    tools_json=excluded.tools_json,
                    path_allowlist_json=excluded.path_allowlist_json,
                    secrets_policy_json=excluded.secrets_policy_json,
                    approval_required=excluded.approval_required,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgp"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    json.dumps(permissions_payload.get("scopes") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("tools") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("path_allowlist") or [], ensure_ascii=False),
                    json.dumps(permissions_payload.get("secrets_policy") or {}, ensure_ascii=False),
                    1 if _coerce_bool(permissions_payload.get("approval_required")) else 0,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO managed_agent_metrics (
                    id, user_id, env_id, connection_id, agent_id, success_count, failure_count, total_calls,
                    total_prompt_tokens, total_completion_tokens, total_latency_ms, last_error, last_seen_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id, connection_id, agent_id) DO UPDATE SET
                    env_id=excluded.env_id,
                    updated_at=excluded.updated_at
                """,
                (
                    new_id("mgt"),
                    user_id,
                    env_id,
                    connection_id,
                    agent_id,
                    _to_int(metrics_payload.get("success_count")),
                    _to_int(metrics_payload.get("failure_count")),
                    _to_int(metrics_payload.get("total_calls")),
                    _to_int(metrics_payload.get("total_prompt_tokens")),
                    _to_int(metrics_payload.get("total_completion_tokens")),
                    _to_int(metrics_payload.get("total_latency_ms")),
                    metrics_payload.get("last_error"),
                    metrics_payload.get("last_seen_at"),
                    now,
                ),
            )
            for rule_key, policy in approval_rules.items():
                conn.execute(
                    """
                    INSERT INTO managed_agent_approval_rules (
                        id, user_id, env_id, connection_id, agent_id, rule_key, policy_json, is_enabled, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, connection_id, agent_id, rule_key) DO UPDATE SET
                        env_id=excluded.env_id,
                        policy_json=excluded.policy_json,
                        is_enabled=excluded.is_enabled,
                        updated_at=excluded.updated_at
                    """,
                    (
                        new_id("mga"),
                        user_id,
                        env_id,
                        connection_id,
                        agent_id,
                        rule_key,
                        json.dumps(policy, ensure_ascii=False),
                        1 if _coerce_bool(policy.get("required", True)) else 0,
                        now,
                        now,
                    ),
                )

            _write_json_file(parts["card"] / AGENT_CARD_FILENAME, card_payload)
            for scope in MANAGED_AGENT_MEMORY_SCOPES:
                _write_json_file(parts["memory"] / f"{scope}.json", memory_payloads.get(scope, {}))
            _write_json_file(parts["checkpoints"] / "latest.json", checkpoint_state)
            _write_json_file(parts["metrics"] / "summary.json", metrics_payload)
            _write_json_file(parts["approvals"] / "rules.json", approval_rules)
            _write_json_file(parts["approvals"] / "permissions.json", permissions_payload)

            _append_managed_agent_history_record(
                conn,
                user_id=user_id,
                env_id=env_id,
                connection_id=connection_id,
                agent_id=agent_id,
                event_kind=event_kind,
                event_text=event_text,
                event_payload={
                    "agent_name": agent_name,
                    "root_path": root_path,
                    "connection_id": connection_id,
                },
                history_file=parts["history"] / "events.jsonl",
            )
            output_agents.append(
                {
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "root_path": root_path,
                    "status": "active",
                }
            )
        except Exception as e:
            failed += 1
            errors.append(f"{agent_id or 'unknown'}: {str(e)[:220]}")

    conn.commit()
    conn.close()
    _refresh_managed_agents_index(user_id)
    return {
        "ok": failed == 0,
        "provisioned": provisioned,
        "updated": updated,
        "failed": failed,
        "agents": output_agents,
        "errors": errors[:20],
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


def _is_openclaw_starting_html(payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False
    raw = payload.get("raw")
    if not isinstance(raw, str):
        return False
    marker = raw.lower()
    return (
        "starting openclaw" in marker
        or "please wait while we set up your environment" in marker
        or ('id="log-output"' in marker and "/api/logs" in marker)
    )


def _response_looks_like_login_html(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    text = (resp.text or "").lower()
    return ("welcome to openclaw" in text) and ('action="/login"' in text or "gateway token" in text)


def _response_looks_like_starting_html(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    text = (resp.text or "").lower()
    return (
        "starting openclaw" in text
        or "please wait while we set up your environment" in text
        or ('id="log-output"' in text and "/api/logs" in text)
    )

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

    for key in ["agents", "nodes", "subagents", "list", "data", "items", "results", "models"]:
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
            aid = str(a).strip()
            if not aid or _is_default_placeholder_agent(aid):
                continue
            norm.append({"id": aid, "name": aid})
        elif isinstance(a, dict):
            aid = (
                a.get("id")
                or a.get("agent_id")
                or a.get("name")
                or a.get("slug")
                or a.get("model")
                or "unknown"
            )
            if _is_default_placeholder_agent(str(aid)):
                continue
            nm = a.get("name") or a.get("title") or a.get("label") or aid
            norm.append({"id": str(aid), "name": str(nm), "raw": a})
    return norm

def _merge_unique_agents(
    target: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
    *,
    seen_ids: Optional[set[str]] = None,
) -> int:
    if seen_ids is None:
        seen_ids = {
            str(item.get("id") or "").strip().lower()
            for item in target
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
    added = 0
    for item in incoming:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("id") or "").strip()
        if not aid:
            continue
        key = aid.lower()
        if key in seen_ids:
            continue
        seen_ids.add(key)
        target.append(item)
        added += 1
    return added

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

    if res.status_code >= 400:
        print(f"[openclaw] {res.status_code} on {method} {url} — token prefix: {api_key[:6]}... body: {res.text[:400]}", flush=True)
    if res.status_code == 401 or _response_looks_like_login_html(res):
        login = await client.post(base_url.rstrip("/") + "/login", data={"token": api_key}, timeout=timeout)
        if login.status_code < 400:
            res = await client.request(method=method, url=url, headers=headers, json=json_body, timeout=timeout)
    return res

async def openclaw_health(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_status: Optional[int] = None
        last_err: str = ""

        # 1) Prefer explicit health/status routes.
        for p in HEALTH_PATHS:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=10)
            except Exception as e:
                last_err = f"{p}: {str(e)}"
                continue

            last_status = r.status_code
            if _response_looks_like_login_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _response_looks_like_starting_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code == 401:
                return {
                    "ok": False,
                    "error": "Unauthorized (401). Token/API key invalid.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code >= 400:
                last_err = f"{p}: {r.status_code} {r.text[:300]}"
                continue

            ct = r.headers.get("content-type", "")
            if "application/json" in ct:
                payload = r.json()
            else:
                payload = {"raw": r.text[:2000]}
            if _is_openclaw_login_html(payload):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _is_openclaw_starting_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            return {
                "ok": True,
                "path": p,
                "status": r.status_code,
                "payload": payload,
                "probe": "health",
            }

        # 2) Some OpenClaw deployments don't expose /health publicly; allow read-only fallback probes.
        fallback_paths = [
            "/v1/models",
            "/models",
            "/api/models",
            "/v1/agents",
            "/agents",
            "/api/agents",
        ]
        for p in fallback_paths:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=10)
            except Exception as e:
                last_err = f"{p}: {str(e)}"
                continue

            last_status = r.status_code
            if _response_looks_like_login_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _response_looks_like_starting_html(r):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code == 401:
                return {
                    "ok": False,
                    "error": "Unauthorized (401). Token/API key invalid.",
                    "path": p,
                    "status": r.status_code,
                }
            if r.status_code >= 400:
                last_err = f"{p}: {r.status_code} {r.text[:300]}"
                continue

            data, parse_err = _safe_json_response(r)
            payload: Any = data if data is not None else {"raw": (r.text or "")[:2000]}
            if _is_openclaw_login_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw returned login page. Use the correct OpenClaw gateway token in api_key.",
                    "path": p,
                    "status": r.status_code,
                }
            if _is_openclaw_starting_html(payload if isinstance(payload, dict) else {"raw": str(payload)[:2000]}):
                return {
                    "ok": False,
                    "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                    "hint": "Wait for OpenClaw startup to finish, then retry /v1/models and chat endpoints.",
                    "path": p,
                    "status": r.status_code,
                }
            out: Dict[str, Any] = {
                "ok": True,
                "path": p,
                "status": r.status_code,
                "payload": payload,
                "probe": "fallback",
            }
            if parse_err:
                out["warning"] = f"Fallback endpoint returned non-JSON payload: {parse_err}"
            return out

        return {
            "ok": False,
            "error": (
                "Could not verify OpenClaw health/reachability on common paths. "
                "Gateway may be restarting/crashing, or reverse proxy path is incomplete."
            ),
            "last_status": last_status,
            "last_error": last_err[:600],
            "hint": (
                "If OpenClaw logs show ECONNREFUSED to 127.0.0.1:18789, fix upstream service/config first. "
                "Then ensure at least one of: /health, /status, /v1/models is reachable."
            ),
        }

async def openclaw_list_agents(base_url: str, api_key: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err: Optional[str] = None
        rest_agents: List[Dict[str, Any]] = []
        rest_seen_ids: set[str] = set()
        rest_model_agents: List[Dict[str, Any]] = []
        rest_model_seen_ids: set[str] = set()
        rest_ok_paths: List[str] = []
        rest_model_source_path: str = ""

        for p in AGENTS_PATHS:
            try:
                r = await _request_openclaw_with_auth(client, "GET", base_url, p, api_key, timeout=15)
                if _response_looks_like_login_html(r):
                    return {"ok": False, "error": "OpenClaw returned login page. Gateway token is invalid or missing.", "path": p}
                if _response_looks_like_starting_html(r):
                    return {
                        "ok": False,
                        "error": "OpenClaw gateway is still starting. API routes are not ready yet.",
                        "hint": "Wait until startup completes in OpenClaw, then retry listing agents.",
                        "path": p,
                    }
                if r.status_code == 401:
                    return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                if r.status_code == 403:
                    body = r.text[:600]
                    if _is_missing_operator_write_error(body):
                        return {
                            "ok": False,
                            "error": "Token is valid but missing operator.write scope. Agent listing and chat require an operator token.",
                            "error_code": "missing_operator_write",
                            "hint": "In OpenClaw: ensure your gateway token has operator.write scope (gateway.auth.mode=token, operator role).",
                            "path": p,
                        }
                    return {"ok": False, "error": f"Forbidden (403). Token lacks required permissions. {body}", "path": p}
                if r.status_code >= 400:
                    last_err = f"{r.status_code}: {r.text[:500]}"
                    continue

                data, parse_err = _safe_json_response(r)
                if data is None:
                    raw = (r.text or "").strip()
                    if not raw:
                        rest_ok_paths.append(p)
                        continue
                    ctype = r.headers.get("content-type") or "unknown"
                    last_err = f"{p}: expected JSON but got {ctype}; body={raw[:300]}"
                    if parse_err:
                        last_err = f"{last_err}; parse_error={parse_err}"
                    continue

                agents = _extract_agents_list(data) or []
                norm = _normalize_agents(agents)
                print(f"[openclaw] 200 on GET {base_url}{p} - keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__} agents_found={len(norm)}", flush=True)
                if norm:
                    if "/model" in p.lower():
                        # Model listing paths - keep separate; only use as fallback if no real agents found.
                        _merge_unique_agents(rest_model_agents, norm, seen_ids=rest_model_seen_ids)
                        if not rest_model_source_path:
                            rest_model_source_path = p
                    else:
                        _merge_unique_agents(rest_agents, norm, seen_ids=rest_seen_ids)
                rest_ok_paths.append(p)
            except Exception as e:
                last_err = str(e)

        if rest_agents:
            rest_agents.sort(key=lambda a: (str(a.get("name") or "").lower(), str(a.get("id") or "").lower()))
            return {"ok": True, "path": rest_ok_paths[0] if rest_ok_paths else AGENTS_PATHS[0], "agents": rest_agents}

        # No real agents found - fall back to model names if available.
        if rest_model_agents:
            return {
                "ok": False,
                "transport": "rest-models-fallback",
                "path": rest_model_source_path or AGENTS_PATHS[0],
                "error": "Only model endpoints are available. Hivee will wait for real agent listing instead of showing default models.",
                "hint": "Expose /agents on the gateway or let the connector publish an agent snapshot so Hivee can render real agent cards.",
            }

        return {
            "ok": False,
            "error": f"Could not list agents on common paths. Last error: {last_err}",
            "hint": (
                "This OpenClaw likely does not expose REST JSON agent listing on your base_url path. "
                "Expose /agents over HTTP on the gateway or let the hub publish an agent snapshot."
            ),
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


def _is_missing_operator_write_error(text: str) -> bool:
    low = text.lower()
    return "missing scope" in low and "operator.write" in low


def _candidate_openclaw_model_hints(agent_id: Optional[str]) -> List[str]:
    raw = str(agent_id or "").strip()
    hints: List[str] = []

    def _push(value: str) -> None:
        item = str(value or "").strip()
        if not item or item in hints:
            return
        hints.append(item)

    if not raw:
        _push("openclaw/default")
        _push("openclaw:default")
        _push("openclaw")
        return hints

    _push(raw)
    if raw.startswith("openclaw/"):
        suffix = raw.split("/", 1)[1].strip()
        if suffix:
            _push(f"openclaw:{suffix}")
    elif raw.startswith("openclaw:"):
        suffix = raw.split(":", 1)[1].strip()
        if suffix:
            _push(f"openclaw/{suffix}")
    else:
        _push(f"openclaw/{raw}")
        _push(f"openclaw:{raw}")
    return hints


def _is_model_resolution_error(status_code: int, raw_text: str) -> bool:
    if status_code != 400:
        return False
    low = str(raw_text or "").lower()
    if "model" not in low:
        return False
    return any(
        marker in low
        for marker in [
            "not found",
            "unknown",
            "invalid model",
            "unsupported model",
            "does not exist",
        ]
    )


async def openclaw_chat(
    base_url: str,
    api_key: str,
    message: str,
    agent_id: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
    session_key: Optional[str] = None,
    user_id: Optional[str] = None,
    timeout_sec: int = 90,
) -> Dict[str, Any]:
    cap = _to_int(max_output_tokens) if max_output_tokens is not None else 0
    if cap <= 0:
        cap = 0
    model_hints = _candidate_openclaw_model_hints(agent_id)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        last_err = None
        saw_405 = False
        saw_502: Optional[str] = None  # first path that returned 502
        saw_404_paths: List[str] = []

        for p in CHAT_PATHS:
            extra_headers: Dict[str, str] = {}
            if agent_id:
                extra_headers["x-openclaw-agent-id"] = agent_id
            if session_key:
                extra_headers["x-openclaw-session-key"] = str(session_key)[:240]

            for model_idx, model_hint in enumerate(model_hints):
                if p.endswith("/responses"):
                    body: Dict[str, Any] = {"model": model_hint, "input": message}
                    if agent_id:
                        body["agent_id"] = agent_id
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
                    if cap > 0:
                        body["max_output_tokens"] = cap
                        # Compatibility fallback for providers/gateways expecting chat-completions naming.
                        body["max_tokens"] = cap
                elif "chat/completions" in p:
                    body = {
                        "model": model_hint,
                        "messages": [{"role": "user", "content": message}],
                    }
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
                    if cap > 0:
                        body["max_tokens"] = cap
                else:
                    body = {"model": model_hint, "message": message, "prompt": message, "input": message}
                    if agent_id:
                        body["agent_id"] = agent_id
                    if session_key:
                        body["session_key"] = session_key
                        body["sessionKey"] = session_key
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
                        last_err = f"{p}: OpenClaw returned login page. Gateway token is invalid or missing."
                        break
                    if _response_looks_like_starting_html(r):
                        last_err = f"{p}: OpenClaw gateway is still starting. API routes are not ready yet."
                        break
                    if r.status_code == 401:
                        return {"ok": False, "error": "Unauthorized (401). Token/API key invalid.", "path": p}
                    if r.status_code == 403:
                        body_text = r.text[:600]
                        if _is_missing_operator_write_error(body_text):
                            # Token-level scope failure - no point trying other paths.
                            return {
                                "ok": False,
                                "error": "Token is valid but missing operator.write scope. Chat requires an operator token.",
                                "error_code": "missing_operator_write",
                                "hint": "Provide an OpenClaw token with operator.write scope.",
                                "path": p,
                            }
                        last_err = f"{p}: 403 {body_text}"
                        break
                    if r.status_code == 502:
                        # Path exists on the proxy but the upstream LLM/backend is down.
                        # No point probing remaining paths - they will 404.
                        saw_502 = saw_502 or p
                        last_err = f"{p}: 502 {r.text[:300]}"
                        break
                    if r.status_code == 405:
                        saw_405 = True
                    if r.status_code == 404:
                        saw_404_paths.append(p)
                    if r.status_code >= 400:
                        last_err = f"{p}: {r.status_code} {r.text[:300]}"
                        # Try alternate model IDs only when this looks like model-resolution failure.
                        if (model_idx + 1) < len(model_hints) and _is_model_resolution_error(r.status_code, r.text):
                            continue
                        break

                    ctype = r.headers.get("content-type", "")
                    if "application/json" in ctype:
                        data: Any = r.json()
                    else:
                        data = {"raw": r.text[:4000]}
                    return {
                        "ok": True,
                        "path": p,
                        "response": data,
                        "text": _extract_chat_text(data),
                        "model_hint": model_hint,
                    }
                except Exception as e:
                    last_err = f"{p}: {str(e)}"
                    break

    # ── Connector fallback: if direct chat failed, try routing through connector ──
    # This MUST be checked before any error returns so it catches all failure modes
    # (502, 405, 404, login page, etc.)
    if user_id:
        try:
            from services.connector_dispatch import get_user_online_connector, connector_chat_sync
            online_connector = get_user_online_connector(user_id)
            if online_connector:
                print(f"[openclaw_chat] Direct chat failed (last_err={last_err}), trying connector fallback via {online_connector['id']}", flush=True)
                connector_res = await connector_chat_sync(
                    connector_id=str(online_connector["id"]),
                    message=message,
                    agent_id=agent_id,
                    session_key=session_key,
                    timeout_sec=max(timeout_sec, 90),
                )
                if connector_res.get("ok"):
                    return connector_res
                # If connector also failed, fall through to direct error below
                print(f"[openclaw_chat] Connector fallback also failed: {connector_res.get('error')}", flush=True)
        except Exception as e:
            print(f"[openclaw_chat] Connector fallback error: {e}", flush=True)

    if saw_502:
        return {
            "ok": False,
            "error": (
                f"OpenClaw chat endpoint ({saw_502}) returned 502 Bad Gateway. "
                "The OpenClaw gateway proxy is running but its upstream LLM provider is unreachable. "
                "Check: (1) OpenClaw provider key is valid and has credits, "
                "(2) the upstream model/provider is reachable from the OpenClaw server, "
                "(3) OpenClaw service logs for upstream connection errors."
            ),
            "hint": "502 means the path exists on the proxy but the backend is down - this is a server-side OpenClaw config issue, not an auth problem.",
            "path": saw_502,
        }
    if saw_405:
        return {
            "ok": False,
            "error": "Chat endpoint returned 405 Method Not Allowed. On OpenClaw, enable gateway.http.endpoints.chatCompletions.enabled=true.",
            "hint": "OpenClaw docs: OpenAI Chat Completions endpoint is disabled by default.",
        }
    if saw_404_paths and len(saw_404_paths) == len(CHAT_PATHS):
        return {
            "ok": False,
            "error": "OpenClaw chat endpoint is not exposed on this base_url (all candidate POST paths returned 404 Not Found).",
            "error_code": "chat_endpoint_not_exposed",
            "tried_paths": saw_404_paths,
            "hint": (
                "Enable OpenClaw HTTP chat routes (for example gateway.http.endpoints.chatCompletions.enabled=true) "
                "and ensure your reverse proxy forwards POST /v1/chat/completions or /v1/responses."
            ),
        }

    hint = "Your OpenClaw may use different chat path(s). Update CHAT_PATHS in core/db.py."
    if last_err and "403" in str(last_err):
        hint = (
            "Got 403 on all chat paths. In gateway.auth.mode='token', a valid bearer token should "
            "automatically receive full operator scopes. Possible causes: (1) endpoint not enabled "
            "(set gateway.http.endpoints.chatCompletions.enabled=true), (2) token is incorrect or "
            "doesn't match gateway.auth.token, (3) gateway.auth.mode is not set to 'token'."
        )
    return {
        "ok": False,
        "error": f"Could not call chat endpoint on common paths. Last error: {last_err}",
        "hint": hint,
    }

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
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    # HTTP-only mode: keep function name for backward compatibility with existing callers.
    routed_session_key = _derive_ws_session_key(session_key=session_key, agent_id=agent_id)
    http_res = await openclaw_chat(
        base_url=base_url,
        api_key=api_key,
        message=message,
        agent_id=agent_id,
        session_key=routed_session_key,
        user_id=user_id,
        timeout_sec=timeout_sec,
    )
    if http_res.get("ok"):
        return {
            "ok": True,
            "transport": "http",
            "path": str(http_res.get("path") or "http"),
            "text": http_res.get("text"),
            "response": http_res.get("response"),
            "frames": [],
        }
    return {
        "ok": False,
        "transport": "http",
        "path": str(http_res.get("path") or "http"),
        "error": http_res.get("error") or "HTTP chat failed",
        "details": http_res.get("details"),
        "hint": http_res.get("hint"),
        "error_code": http_res.get("error_code"),
        "tried_paths": http_res.get("tried_paths"),
    }
async def openclaw_ws_list_agents(base_url: str, api_key: str, timeout_sec: int = 12) -> Dict[str, Any]:
    # HTTP-only mode: this helper remains for backward compatibility.
    _ = base_url
    _ = api_key
    _ = timeout_sec
    return {
        "ok": False,
        "error": "Legacy realtime transport is disabled in this build. Use HTTP agent endpoints.",
        "hint": "Expose /agents over HTTP on the OpenClaw gateway or let the hub publish an agent snapshot.",
    }


async def _project_chat(
    row: Any,
    connection_api_key: str,
    message: str,
    *,
    agent_id: Optional[str] = None,
    session_key: str = "main",
    timeout_sec: Optional[int] = 120,
    user_id: Optional[str] = None,
    from_agent_id: Optional[str] = None,
    from_label: Optional[str] = None,
    context_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Route a project chat through the correct connector for this project/user."""
    from services.connector_dispatch import connector_chat_sync, get_user_online_connector

    def _sanitize_hivee_dispatch_text(raw_text: Any) -> str:
        text = str(raw_text or "").replace("\r", "")
        if not text:
            return ""
        text = re.sub(r"(?im)^(hivee_project_token:\s*).*$", r"\1[REDACTED]", text)
        text = re.sub(r"(?im)^(\s*X-Project-Agent-Token:\s*).*$", r"\1[REDACTED]", text)
        return text

    backend_mode = ""
    try:
        backend_mode = str(row["backend_mode"] or "").strip().lower()
    except Exception:
        backend_mode = ""

    connector_id = ""
    try:
        connector_id = str(row["connector_id"] or "").strip()
    except Exception:
        connector_id = ""

    candidate_connection_id = ""
    try:
        candidate_connection_id = str(row["connection_id"] or "").strip()
    except Exception:
        candidate_connection_id = ""

    if not connector_id and backend_mode == "connector":
        connector_id = candidate_connection_id

    if not connector_id and candidate_connection_id:
        conn = db()
        try:
            connector_row = conn.execute(
                "SELECT id FROM connectors WHERE id = ? LIMIT 1",
                (candidate_connection_id,),
            ).fetchone()
        finally:
            conn.close()
        if connector_row:
            connector_id = candidate_connection_id

    # Direct OpenClaw projects still need a live Hivee Hub to deliver
    # project-scoped prompts to the runtime agent. Only trust connection_id
    # when it currently resolves to a real connector row.
    if not connector_id and user_id:
        try:
            online_connector = get_user_online_connector(user_id)
        except Exception:
            online_connector = None
        if online_connector:
            connector_id = str(online_connector.get("id") or "").strip()

    if not connector_id:
        return {
            "ok": False,
            "error": "No live Hivee Hub is available for this project. Pair/start a hub, then retry.",
            "transport": "none",
        }
    try:
        project_id = str(row["id"] or "").strip()
    except Exception:
        project_id = ""
    hivee_api_base = _get_hivee_api_base(project_id) if project_id else ""
    project_agent_token = ""
    if project_id and agent_id:
        try:
            project_agent_token = _issue_agent_session_token(project_id, str(agent_id or "").strip())
        except Exception:
            project_agent_token = ""

    owner_user_id = ""
    project_root = ""
    project_title = ""
    project_workspace_root = ""
    try:
        owner_user_id = str(row["user_id"] or "").strip()
    except Exception:
        owner_user_id = ""
    try:
        project_root = str(row["project_root"] or "").strip()
    except Exception:
        project_root = ""
    try:
        project_title = str(row["title"] or "").strip()
    except Exception:
        project_title = ""
    try:
        project_workspace_root = str(row["workspace_root"] or "").strip()
    except Exception:
        project_workspace_root = ""
    if project_id and owner_user_id:
        try:
            root_alignment = _ensure_canonical_project_root(
                project_id=project_id,
                owner_user_id=owner_user_id,
                title=project_title,
                current_project_root=project_root,
                workspace_root=project_workspace_root,
            )
            project_root = str(root_alignment.get("project_root") or project_root or "").strip()
        except Exception:
            pass
    resolved_project_root = project_root
    if owner_user_id and project_root:
        try:
            resolved_project_root = _resolve_owner_project_dir(owner_user_id, project_root).resolve().as_posix()
        except Exception:
            resolved_project_root = project_root
    resolved_workspace_root = ""
    if owner_user_id:
        try:
            resolved_workspace_root = _user_workspace_root_dir(owner_user_id).resolve().as_posix()
        except Exception:
            resolved_workspace_root = ""

    should_log_hivee_dispatch = (
        bool(project_id)
        and bool(owner_user_id)
        and bool(project_root)
        and str(from_agent_id or "").strip().lower() == "hivee"
        and str(context_type or "").strip().lower() != "message"
    )
    if should_log_hivee_dispatch:
        _append_project_daily_log(
            owner_user_id=owner_user_id,
            project_root=project_root,
            kind="chat.hivee",
            text=(
                f"DISPATCH[{str(context_type or 'message').strip() or 'message'}]\n"
                f"SESSION: {session_key}\n"
                f"TARGET_AGENT: {str(agent_id or 'auto').strip() or 'auto'}\n"
                f"FROM: {str(from_label or 'Hivee System').strip() or 'Hivee System'}\n"
                f"PROMPT:\n{_sanitize_hivee_dispatch_text(message)}"
            ).strip(),
            payload={
                "direction": "request",
                "session_key": str(session_key or "").strip(),
                "context_type": str(context_type or "").strip() or "message",
                "agent_id": str(agent_id or "").strip() or None,
                "from_label": str(from_label or "Hivee System").strip() or "Hivee System",
            },
        )

    res = await connector_chat_sync(
        connector_id=connector_id,
        message=message,
        agent_id=agent_id,
        session_key=session_key,
        timeout_sec=timeout_sec,
        from_agent_id=from_agent_id or "hivee",
        from_label=from_label or "Hivee",
        context_type=context_type or "message",
        project_id=project_id,
        hivee_api_base=hivee_api_base,
        project_agent_id=str(agent_id or "").strip(),
        project_agent_token=project_agent_token,
        project_root=resolved_project_root,
        workspace_root=resolved_workspace_root,
    )
    if should_log_hivee_dispatch:
        result_text = str(
            res.get("text")
            or res.get("error")
            or res.get("details")
            or ""
        ).replace("\r", "").strip()
        _append_project_daily_log(
            owner_user_id=owner_user_id,
            project_root=project_root,
            kind="chat.hivee",
            text=(
                f"RESULT[{str(context_type or 'message').strip() or 'message'}]\n"
                f"SESSION: {session_key}\n"
                f"TARGET_AGENT: {str(agent_id or 'auto').strip() or 'auto'}\n"
                f"OK: {'yes' if res.get('ok') else 'no'}\n"
                f"AGENT({str(agent_id or 'auto').strip() or 'auto'}): {result_text or '(empty response)'}"
            ).strip(),
            payload={
                "direction": "response",
                "session_key": str(session_key or "").strip(),
                "context_type": str(context_type or "").strip() or "message",
                "agent_id": str(agent_id or "").strip() or None,
                "ok": bool(res.get("ok")),
                "error": str(res.get("error") or "")[:500] or None,
            },
        )
    return res
async def _ensure_project_info_document(
    project_id: str,
    *,
    force: bool = False,
    start_chat_text: Optional[str] = None,
    done_chat_text: Optional[str] = None,
) -> Dict[str, Any]:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.workspace_root, p.connection_id,
               p.plan_status, p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return {"ok": False, "error": "Project not found"}
    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
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
    primary_agent_name = next(
        (
            str(r.get("agent_name") or r.get("agent_id") or "")
            for r in role_rows
            if str(r.get("agent_id") or "").strip() == str(primary_agent_id or "").strip()
        ),
        str(primary_agent_id or "primary"),
    )
    project_root = str(row["project_root"] or "").strip()
    try:
        root_alignment = _ensure_canonical_project_root(
            project_id=str(row["id"] or project_id),
            owner_user_id=str(row["user_id"] or ""),
            title=str(row["title"] or ""),
            current_project_root=project_root,
            workspace_root=str(row["workspace_root"] or ""),
        )
        project_root = str(root_alignment.get("project_root") or project_root or "").strip()
    except Exception:
        project_root = str(row["project_root"] or "").strip()

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), project_root)
    except Exception as e:
        return {"ok": False, "error": detail_to_text(e)[:300]}

    _initialize_project_folder(
        project_dir,
        str(row["title"] or ""),
        str(row["brief"] or ""),
        str(row["goal"] or ""),
        setup_details=setup_details,
        project_id=str(row["id"] or ""),
        hivee_api_base=_get_hivee_api_base(str(row["id"] or "")),
    )
    info_path = project_dir / PROJECT_INFO_FILE
    existing_info = ""
    if info_path.exists():
        try:
            existing_info = info_path.read_text(encoding="utf-8")
        except Exception:
            existing_info = ""
    info_is_stale = False
    if existing_info.strip() and info_path.exists():
        try:
            info_mtime = info_path.stat().st_mtime
            for rel in (PROJECT_PLAN_FILE, OVERVIEW_FILE, TRACKER_FILE):
                candidate = (project_dir / rel).resolve()
                if _path_within(candidate, project_dir) and candidate.is_file() and candidate.stat().st_mtime > info_mtime:
                    info_is_stale = True
                    break
        except Exception:
            info_is_stale = False
    normalized_plan_status = _coerce_plan_status(row["plan_status"])
    info_lower = existing_info.lower()
    if (
        existing_info.strip()
        and normalized_plan_status == PLAN_STATUS_APPROVED
        and (
            "project plan is not approved" in info_lower
            or "not approved yet" in info_lower
            or "awaiting approval" in info_lower
            or "waiting approval" in info_lower
            or "plan_status: `awaiting_approval`" in info_lower
            or "plan_status: awaiting_approval" in info_lower
        )
    ):
        info_is_stale = True
    if (
        existing_info.strip()
        and (not force)
        and "pending primary agent completion" not in existing_info.lower()
        and len(existing_info.strip()) >= 160
        and not info_is_stale
    ):
        return {"ok": True, "text": existing_info.strip(), "source": "existing", "agent_id": primary_agent_id}

    context = _project_context_instruction(
        title=str(row["title"] or ""),
        brief=str(row["brief"] or ""),
        goal=str(row["goal"] or ""),
        setup_details=setup_details,
        role_rows=role_rows,
        project_root=str(row["project_root"] or ""),
        plan_status=normalized_plan_status,
    )
    roster = _agent_roster_markdown(role_rows)
    task = (
        f"{context}\n\n"
        f"{roster}\n\n"
        "Task:\n"
        f"1) Read `{SETUP_CHAT_HISTORY_FILE}`, `agents/ROLES.md`, and `{PROJECT_PROTOCOL_FILE}`.\n"
        f"2) Write or replace `{PROJECT_INFO_FILE}` with complete project context.\n"
        "3) Include: project summary, user requirements, constraints, assumptions, role responsibilities, execution prerequisites, and open questions.\n"
        "4) If some information is missing, make reasonable assumptions and clearly mark them under `Assumptions`.\n"
        "5) Return JSON only with `chat_update`, `output_files`, optional `actions`, optional `notes`, and pause fields.\n"
        "6) Keep language concise and human-readable.\n"
    )
    info_context = _build_project_file_context(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            PROJECT_INFO_FILE,
            "agents/ROLES.md",
            OVERVIEW_FILE,
            PROJECT_PLAN_FILE,
            TRACKER_FILE,
            PROJECT_SETUP_FILE,
            PROJECT_PROTOCOL_FILE,
            SETUP_CHAT_HISTORY_FILE,
            SETUP_CHAT_HISTORY_COMPAT_FILE,
        ],
        request_text=str(setup_details.get("setup_chat_summary") or ""),
        max_total_chars=8_500,
        max_files=8,
    )
    if info_context:
        task = f"{task}\n\n{info_context}"

    start_text = str(start_chat_text or "").strip() or "@owner I am updating project info now."
    done_text = str(done_chat_text or "").strip() or "@owner I finished updating project info."
    try:
        await _post_project_agent_status_message(
            project_id=project_id,
            agent_id=primary_agent_id,
            agent_name=primary_agent_name,
            text=start_text,
            mentions=["owner"] if "@owner" in start_text.lower() else [],
            metadata={"phase": "project_info.start"},
        )
    except Exception:
        pass

    await emit(project_id, "project.info.generating", {"project_id": project_id})
    res = await _project_chat(
        row,
        connection_api_key,
        task,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:project-info",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="control",
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
        try:
            await _post_project_agent_status_message(
                project_id=project_id,
                agent_id=primary_agent_id,
                agent_name=primary_agent_name,
                text=done_text,
                mentions=["owner"] if "@owner" in done_text.lower() else [],
                metadata={"phase": "project_info.done", "source": "fallback"},
            )
        except Exception:
            pass
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
    try:
        await _post_project_agent_status_message(
            project_id=project_id,
            agent_id=primary_agent_id,
            agent_name=primary_agent_name,
            text=done_text,
            mentions=["owner"] if "@owner" in done_text.lower() else [],
            metadata={"phase": "project_info.done", "source": "agent"},
        )
    except Exception:
        pass
    return {"ok": True, "text": text, "source": "agent", "agent_id": primary_agent_id}

async def _generate_project_plan(project_id: str, *, force: bool = False) -> None:
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status,
               p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
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

    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    previous_valid_plan_text = (
        str(row["plan_text"] or "").strip()
        if _project_plan_file_is_substantive(row["plan_text"])
        else ""
    )
    if not role_rows:
        now = int(time.time())
        msg = "Invite at least one project agent (and select a primary) before generating plan."
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, previous_valid_plan_text[:20000], now, project_id),
        )
        conn.commit()
        conn.close()
        _refresh_project_documents(project_id)
        _append_project_activity(
            project_id=project_id,
            actor_type="system",
            actor_id="hivee",
            actor_label="Hivee",
            event_type="project.plan.failed",
            summary="Project plan generation failed before contacting the primary agent",
            payload={"reason": msg},
        )
        await emit(project_id, "project.plan.failed", {"error": msg})
        return
    generation_started_at = int(time.time())
    conn.execute(
        "UPDATE projects SET plan_status = ?, plan_updated_at = ? WHERE id = ?",
        (PLAN_STATUS_GENERATING, generation_started_at, project_id),
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

    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id or "")
    inline_plan_context = _build_project_file_context(
        owner_user_id=str(row["user_id"] or ""),
        project_root=str(row["project_root"] or ""),
        include_paths=[
            FUNDAMENTALS_FILE,
            CONTEXT_FILE,
            SETUP_CHAT_MD_FILE,
            PROJECT_INFO_FILE,
            PROJECT_PROTOCOL_FILE,
            PROJECT_SETUP_FILE,
            OVERVIEW_FILE,
            SETUP_CHAT_HISTORY_FILE,
        ],
        request_text="Generate the initial project plan.",
        max_total_chars=16_000,
        max_file_chars=2_400,
        max_files=8,
        include_tree=False,
    )
    task = (
        f"Read fundamentals.md first, then context.md and setup-chat.md when those Hivee file requests work.\n"
        f"If any Hivee file request for fundamentals/context/setup-chat returns 401/403/auth failure, DO NOT stop.\n"
        f"Use the inline server snapshot attached below as your fallback source of truth and continue planning.\n"
        f"Do not set requires_user_input=true just because project file auth failed.\n"
        f"You MUST persist the finished plan into Hivee project storage using your Hivee API token before you finish.\n"
        f"Write both `plan.md` and `{PROJECT_PLAN_FILE}` in project storage.\n"
        f"Preferred write path: POST {hivee_api_base}/agent-ops with `write_file` actions for those two files.\n"
        f"Direct file-write alternative: POST {hivee_api_base}/files/write with the same headers and JSON body.\n"
        f"Do not rely on local runtime files only.\n"
        f"Build a complete, detailed project plan IN ENGLISH based on the project brief, goals, and agents roster.\n"
        f"The plan must include: milestones, deliverables, agent responsibilities, handoff triggers, pit-stop approval gates, assumptions, risks, and open questions.\n"
        f"Return a JSON object with:\n"
        f"  - chat_update: brief status message ending with 'WAITING FOR USER APPROVAL'\n"
        f"  - output_files: [{{\"path\": \"plan.md\", \"content\": \"<full markdown plan content here>\"}}]\n"
        f"  - actions: include `write_file` for `plan.md`, include `write_file` for `{PROJECT_PLAN_FILE}`, and include `post_chat_message` notifying @owner that the plan is ready\n"
        f"The same full markdown plan must be used for the storage writes and the `output_files` copy.\n"
        f"The plan.md content in output_files must be the full human-readable markdown plan — NOT JSON.\n"
        f"Post your status to chat (@owner) at start and when done."
    )
    instruction = _build_fundamentals_session_prompt(
        task=task,
        project_id=project_id,
        agent_id=primary_agent_id or "",
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )
    if inline_plan_context:
        instruction = (
            f"{instruction}\n\n"
            "Inline fallback project snapshot:\n"
            "- Use this snapshot immediately if Hivee project-file fetches fail.\n"
            "- Continue planning from this snapshot without pausing for approval.\n\n"
            f"{inline_plan_context}"
        )

    plan_start_result = _apply_project_actions(
        owner_user_id=str(row["user_id"]),
        project_id=project_id,
        project_root=str(row["project_root"] or ""),
        actions=[{
            "type": "post_chat_message",
            "text": "@owner I am reading the project context and drafting the plan now.",
            "mentions": ["owner"],
        }],
        allow_paths=None,
        actor_type="project_agent",
        actor_id=str(primary_agent_id or ""),
        actor_label=f"agent:{primary_agent_id}",
    )
    await _emit_project_action_results(project_id, plan_start_result.get("applied") or [])

    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:plan",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="plan_generation",
    )
    prompt_tokens, completion_tokens, _ = _extract_usage_counts(res)
    if prompt_tokens <= 0:
        prompt_tokens = _estimate_tokens_from_text(instruction)
    if completion_tokens <= 0:
        completion_tokens = _estimate_tokens_from_text(res.get("text"))
    _update_project_usage_metrics(project_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)

    now = int(time.time())
    conn = db()
    current_state = conn.execute(
        "SELECT plan_status, plan_updated_at FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    current_plan_status = _coerce_plan_status(current_state["plan_status"] if current_state else None)
    current_plan_updated_at = int((current_state["plan_updated_at"] if current_state else 0) or 0)
    if current_plan_status != PLAN_STATUS_GENERATING or current_plan_updated_at != generation_started_at:
        conn.close()
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.generation.ignored",
            text=(
                "Ignored stale plan-generation result because project state changed "
                f"to `{current_plan_status}` while the agent was still planning."
            )[:1200],
            payload={
                "generation_started_at": generation_started_at,
                "current_plan_status": current_plan_status,
                "current_plan_updated_at": current_plan_updated_at,
            },
        )
        await emit(
            project_id,
            "project.plan.generation_ignored",
            {"status": current_plan_status, "updated_at": current_plan_updated_at},
        )
        return
    if not res.get("ok"):
        error_text = detail_to_text(res.get("error") or res.get("details") or "Failed to generate project plan")
        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_FAILED, previous_valid_plan_text[:20000], now, project_id),
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
        _append_project_activity(
            project_id=project_id,
            actor_type="system",
            actor_id="hivee",
            actor_label="Hivee",
            event_type="project.plan.failed",
            summary="Project plan generation failed because the connector returned an error",
            payload={"reason": error_text[:1200]},
        )
        await emit(project_id, "project.plan.failed", {"error": error_text[:1200]})
        return

    try:
        raw_plan_text = str(res.get("text") or "").strip()
        if not raw_plan_text:
            raw_plan_text = detail_to_text(res.get("frames") or "Plan generated with empty text")

        # Parse the agent's JSON response and extract actual plan content from
        # output_files, write_file actions, or the persisted project storage file.
        # Require a substantive plan so the Overview shows a real plan, not a one-liner.
        parsed_plan = _extract_agent_report_payload(raw_plan_text)

        def _plan_looks_substantive(txt: str) -> bool:
            return _project_plan_file_is_substantive(txt)

        def _extract_plan_content_from_payload(items: List[Dict[str, Any]]) -> str:
            best_effort = ""
            for item in items:
                if not isinstance(item, dict):
                    continue
                rel = _remap_legacy_project_doc_rel_path(_clean_relative_project_path(str(item.get("path") or "")))
                if str(rel or "").strip().lower() not in {"plan.md", PROJECT_PLAN_FILE.lower()}:
                    continue
                content = str(item.get("content") or "").strip()
                if not content:
                    continue
                if _plan_looks_substantive(content):
                    return content
                if not best_effort:
                    best_effort = content
            return best_effort

        plan_writes = parsed_plan.get("output_files") if isinstance(parsed_plan.get("output_files"), list) else []
        plan_actions = parsed_plan.get("actions") if isinstance(parsed_plan.get("actions"), list) else []
        plan_text_from_file = _extract_plan_content_from_payload(plan_writes)
        plan_text_from_action = _extract_plan_content_from_payload(
            [
                item
                for item in plan_actions
                if isinstance(item, dict) and _normalize_agent_action_kind(item.get("type")) in {"write_file", "append_file"}
            ]
        )
        existing_plan_text = ""
        try:
            project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
            for rel in (PROJECT_PLAN_FILE, "plan.md"):
                candidate = (project_dir / rel).resolve()
                if not _path_within(candidate, project_dir) or not candidate.is_file():
                    continue
                candidate_mtime = int(candidate.stat().st_mtime)
                if candidate_mtime < generation_started_at:
                    continue
                candidate_text = candidate.read_text(encoding="utf-8").strip()
                if not candidate_text:
                    continue
                if _plan_looks_substantive(candidate_text):
                    existing_plan_text = candidate_text
                    break
                if not existing_plan_text:
                    existing_plan_text = candidate_text
        except Exception:
            existing_plan_text = ""

        if _plan_looks_substantive(plan_text_from_file):
            plan_text = plan_text_from_file
        elif _plan_looks_substantive(plan_text_from_action):
            plan_text = plan_text_from_action
        elif _plan_looks_substantive(existing_plan_text):
            plan_text = existing_plan_text
        else:
            fallback_text = (
                plan_text_from_file
                or plan_text_from_action
                or existing_plan_text
                or str(parsed_plan.get("chat_update") or raw_plan_text).strip()
            )
            error_message = (
                "Primary agent's response did not persist a complete plan into Hivee project storage "
                "or include a complete `plan.md` / `Project Info/project-plan.md` payload. "
                "Click Regenerate Plan to retry.\n\n"
                f"Agent response preview:\n{fallback_text[:1200]}"
            )
            conn.execute(
                "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
                (PLAN_STATUS_FAILED, previous_valid_plan_text[:20000], now, project_id),
            )
            conn.commit()
            conn.close()
            _refresh_project_documents(project_id)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="plan.failed",
                text=error_message[:1200],
            )
            _append_project_activity(
                project_id=project_id,
                actor_type="system",
                actor_id="hivee",
                actor_label="Hivee",
                event_type="project.plan.invalid_artifact",
                summary="Rejected invalid project plan artifact",
                payload={"reason": error_message[:1200]},
            )
            await emit(project_id, "project.plan.failed", {"error": error_message[:600]})
            return

        conn.execute(
            "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
            (PLAN_STATUS_AWAITING_APPROVAL, plan_text[:20000], now, project_id),
        )
        conn.commit()
        conn.close()
        _set_project_execution_state(project_id, status=EXEC_STATUS_IDLE, progress_pct=10)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.persisted",
            text="Project plan draft saved. Waiting for owner approval.",
        )
        await emit(
            project_id,
            "project.plan.awaiting_approval",
            {"project_id": project_id, "status": PLAN_STATUS_AWAITING_APPROVAL},
        )

        # Save plan.md to project folder
        try:
            project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
            plan_md_content = plan_text.strip() + "\n"
            non_plan_writes = []
            for item in plan_writes:
                if not isinstance(item, dict):
                    continue
                rel = _remap_legacy_project_doc_rel_path(_clean_relative_project_path(str(item.get("path") or "")))
                if str(rel or "").strip().lower() in {"plan.md", PROJECT_PLAN_FILE.lower()}:
                    continue
                non_plan_writes.append(item)
            if non_plan_writes:
                _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=non_plan_writes,
                    default_prefix="",
                    allow_paths=None,
                )
            applied_plan_actions: Dict[str, Any] = {"applied": [], "skipped": []}
            if plan_actions:
                applied_plan_actions = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=plan_actions,
                    allow_paths=None,
                    actor_type="project_agent",
                    actor_id=str(primary_agent_id or ""),
                    actor_label=f"agent:{primary_agent_id}",
                )
                await _emit_project_action_results(project_id, applied_plan_actions.get("applied") or [])
            (project_dir / "plan.md").write_text(plan_md_content, encoding="utf-8")
            (project_dir / PROJECT_PLAN_FILE).write_text(plan_md_content, encoding="utf-8")
            # Fallback: if primary did not post to chat, post a ready notice now.
            if not _applied_actions_include_kind(applied_plan_actions.get("applied") or [], "post_chat_message"):
                plan_notice_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=[{
                        "type": "post_chat_message",
                        "text": "@owner Plan is ready for your review. Saved to `plan.md` and `Project Info/project-plan.md`. Please approve or request changes.",
                        "mentions": ["owner"],
                    }],
                    allow_paths=None,
                    actor_type="project_agent",
                    actor_id=str(primary_agent_id or ""),
                    actor_label=f"agent:{primary_agent_id}",
                )
                await _emit_project_action_results(project_id, plan_notice_result.get("applied") or [])
        except Exception:
            pass

        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.ready",
            text=(plan_text or "")[:1600],
        )
        try:
            _refresh_project_documents(project_id)
            asyncio.create_task(_ensure_project_info_document(project_id, force=True))
        except Exception as exc:
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="plan.refresh.warning",
                text=(
                    "Project plan was saved, but document refresh failed: "
                    f"{detail_to_text(exc)[:800]}"
                ),
            )
        await emit(project_id, "project.plan.ready", {"status": PLAN_STATUS_AWAITING_APPROVAL, "preview": plan_text[:1000]})
    except Exception as exc:
        try:
            conn.close()
        except Exception:
            pass
        error_text = f"Unexpected error while finalizing generated project plan: {detail_to_text(exc)}"
        fail_now = int(time.time())
        fail_conn = db()
        try:
            fail_row = fail_conn.execute(
                "SELECT plan_status FROM projects WHERE id = ?",
                (project_id,),
            ).fetchone()
            if _coerce_plan_status(fail_row["plan_status"] if fail_row else None) == PLAN_STATUS_GENERATING:
                fail_conn.execute(
                    "UPDATE projects SET plan_status = ?, plan_text = ?, plan_updated_at = ? WHERE id = ?",
                    (PLAN_STATUS_FAILED, previous_valid_plan_text[:20000], fail_now, project_id),
                )
                fail_conn.commit()
        finally:
            fail_conn.close()
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="plan.finalize.error",
            text=error_text[:1200],
        )
        _append_project_activity(
            project_id=project_id,
            actor_type="system",
            actor_id="hivee",
            actor_label="Hivee",
            event_type="project.plan.failed",
            summary="Project plan finalization failed",
            payload={"reason": error_text[:1200]},
        )
        await emit(project_id, "project.plan.failed", {"error": error_text[:600]})

async def _run_agent_subplan_phase(
    project_id: str,
    row: Any,
    connection_api_key: str,
    *,
    agent_id: str,
    agent_name: str,
    primary_agent_id: str,
    primary_agent_name: str,
    task_text: str,
) -> Tuple[bool, str]:
    """
    Two-step sub-plan phase for a single agent:
    1. Invoke agent to write a detailed sub-plan and @mention primary for approval.
    2. Invoke primary to review and approve/reject via chat.
    Returns (approved: bool, subplan_text: str).
    """
    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, agent_id)
    primary_token = _issue_agent_session_token(project_id, primary_agent_id)

    # ── Step 1: Agent writes sub-plan ─────────────────────────────────────────
    subplan_instruction = (
        f"hivee_agent_id: {agent_id}\n"
        f"hivee_project_token: {agent_token}\n"
        f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
        f"  Headers: X-Project-Agent-Id: {agent_id}\n"
        f"           X-Project-Agent-Token: {agent_token}\n\n"
        f"All Hivee API requests must include:\n"
        f"  X-Project-Agent-Id: {agent_id}\n"
        f"  X-Project-Agent-Token: {agent_token}\n\n"
        f"You are agent `{agent_id}` ({agent_name}).\n"
        f"You have been assigned this high-level task:\n\n"
        f"{task_text}\n\n"
        f"Before executing, write a DETAILED SUB-PLAN. Save it to "
        f"`agents/{agent_id}-subplan.md` in output_files. Include:\n"
        f"- Approach and methodology\n"
        f"- Step-by-step sub-tasks you will create on the progress map\n"
        f"- Timeline estimate per sub-task\n"
        f"- Deliverable files\n"
        f"- Risks, assumptions, dependencies\n\n"
        f"Then post to chat requesting approval:\n"
        f"  @{primary_agent_id} here is my sub-plan for [task title]. "
        f"Read `agents/{agent_id}-subplan.md`. Please approve or provide feedback.\n\n"
        f"Do NOT start executing yet.\n\n"
        f"Return JSON: {{\"chat_update\": \"...\", "
        f"\"output_files\": [{{\"path\": \"agents/{agent_id}-subplan.md\", \"content\": \"...\"}}], "
        f"\"actions\": [{{\"type\": \"post_chat_message\", "
        f"\"text\": \"@{primary_agent_id} sub-plan ready for review...\", "
        f"\"mentions\": [\"{primary_agent_id}\"]}}], "
        f"\"requires_user_input\": true, "
        f"\"pause_reason\": \"Waiting for @{primary_agent_id} sub-plan approval.\"}}"
    )
    subplan_res = await _project_chat(
        row,
        connection_api_key,
        subplan_instruction,
        agent_id=agent_id,
        session_key=f"{project_id}:subplan:{agent_id}",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="subplan",
    )
    _update_project_usage_metrics(
        project_id,
        prompt_tokens=_estimate_tokens_from_text(subplan_instruction),
        completion_tokens=_estimate_tokens_from_text(subplan_res.get("text")),
    )

    subplan_text = str(subplan_res.get("text") or "").strip()
    parsed_subplan = _extract_agent_report_payload(subplan_text)

    # Persist sub-plan file + chat message
    subplan_writes = parsed_subplan.get("output_files") or []
    if isinstance(subplan_writes, list) and subplan_writes:
        _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=subplan_writes,
            default_prefix=USER_OUTPUTS_DIRNAME,
            allow_paths=None,
        )
    _apply_project_actions(
        owner_user_id=str(row["user_id"]),
        project_id=project_id,
        project_root=str(row["project_root"] or ""),
        actions=parsed_subplan.get("actions") or [],
        allow_paths=None,
        actor_type="project_agent",
        actor_id=agent_id,
        actor_label=f"agent:{agent_id}",
    )

    # Extract the actual sub-plan content for primary to read
    actual_subplan = ""
    for f in subplan_writes if isinstance(subplan_writes, list) else []:
        if str(f.get("path") or "").strip().endswith("subplan.md"):
            actual_subplan = str(f.get("content") or "").strip()
            break
    if not actual_subplan:
        actual_subplan = str(parsed_subplan.get("chat_update") or subplan_text)[:3000]

    await emit(project_id, "agent.subplan.submitted", {
        "agent_id": agent_id, "agent_name": agent_name, "preview": actual_subplan[:400],
    })

    if not subplan_res.get("ok"):
        # Sub-plan call failed — default approve so execution isn't blocked
        return True, actual_subplan

    # ── Step 2: Primary reviews sub-plan ──────────────────────────────────────
    review_instruction = (
        f"hivee_agent_id: {primary_agent_id}\n"
        f"hivee_project_token: {primary_token}\n"
        f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
        f"  Headers: X-Project-Agent-Id: {primary_agent_id}\n"
        f"           X-Project-Agent-Token: {primary_token}\n\n"
        f"All Hivee API requests must include:\n"
        f"  X-Project-Agent-Id: {primary_agent_id}\n"
        f"  X-Project-Agent-Token: {primary_token}\n\n"
        f"You are the primary agent `{primary_agent_id}` ({primary_agent_name}).\n"
        f"Agent `{agent_id}` ({agent_name}) submitted their sub-plan for review:\n\n"
        f"---\n{actual_subplan[:3000]}\n---\n\n"
        f"Review this sub-plan against the overall project plan, goals, and constraints "
        f"(read plan.md if needed). Consider: scope alignment, realistic timeline, correct "
        f"dependencies, resource fit.\n\n"
        f"Respond with your decision. Include `\"approved\": true` or `\"approved\": false`.\n"
        f"Post your decision to chat @{agent_id}.\n\n"
        f"Return JSON: {{\"chat_update\": \"...\", \"approved\": true, \"feedback\": \"...\", "
        f"\"actions\": [{{\"type\": \"post_chat_message\", "
        f"\"text\": \"@{agent_id} Sub-plan approved. Proceed.\", "
        f"\"mentions\": [\"{agent_id}\"]}}]}}"
    )
    review_res = await _project_chat(
        row,
        connection_api_key,
        review_instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:review:{agent_id}",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="subplan_review",
    )
    _update_project_usage_metrics(
        project_id,
        prompt_tokens=_estimate_tokens_from_text(review_instruction),
        completion_tokens=_estimate_tokens_from_text(review_res.get("text")),
    )

    if not review_res.get("ok"):
        return True, actual_subplan  # fallback: approve

    review_text = str(review_res.get("text") or "").strip()
    parsed_review = _extract_agent_report_payload(review_text)

    # Post primary's decision to chat
    _apply_project_actions(
        owner_user_id=str(row["user_id"]),
        project_id=project_id,
        project_root=str(row["project_root"] or ""),
        actions=parsed_review.get("actions") or [],
        allow_paths=None,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=f"agent:{primary_agent_id}",
    )

    # Determine approval — check explicit field first, then infer from text
    approved_flag = parsed_review.get("approved")
    if approved_flag is None:
        review_chat = str(parsed_review.get("chat_update") or "").lower()
        approved_flag = any(kw in review_chat for kw in ("approved", "proceed", "looks good", "good to go", "approve"))
    approved = bool(approved_flag)

    reviewed_payload = {
        "agent_id": agent_id,
        "agent_name": agent_name,
        "approved": approved,
        "feedback": str(parsed_review.get("feedback") or "")[:400],
    }
    await emit(project_id, "agent.subplan.reviewed", reviewed_payload)
    _append_project_activity(
        project_id=project_id,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=primary_agent_name,
        event_type="agent.subplan.reviewed",
        summary=f"{agent_name} sub-plan {'approved' if approved else 'needs revision'}",
        payload=reviewed_payload,
    )
    return approved, actual_subplan


async def _run_agent_subplan_phase_v2(
    project_id: str,
    row: Any,
    connection_api_key: str,
    *,
    agent_id: str,
    agent_name: str,
    primary_agent_id: str,
    primary_agent_name: str,
    task_text: str,
) -> Tuple[bool, str]:
    """
    Improved sub-plan loop:
    - emits live chat/task events for both submission and review
    - retries revisions when primary rejects the sub-plan
    - falls back to explicit chat messages when an agent forgets the action
    """
    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, agent_id)
    primary_token = _issue_agent_session_token(project_id, primary_agent_id)
    actual_subplan = ""
    latest_feedback = ""
    max_review_rounds = 3

    for round_idx in range(1, max_review_rounds + 1):
        if round_idx == 1:
            subplan_task = (
                f"You are agent `{agent_id}` ({agent_name}).\n"
                f"You have been assigned this high-level task:\n\n"
                f"{task_text}\n\n"
                f"Before executing, write a DETAILED SUB-PLAN. Save it to "
                f"`agents/{agent_id}-subplan.md` in output_files. Include:\n"
                f"- Approach and methodology\n"
                f"- Step-by-step sub-tasks you will create on the progress map\n"
                f"- Timeline estimate per sub-task\n"
                f"- Deliverable files\n"
                f"- Risks, assumptions, dependencies\n\n"
                f"Then post to chat requesting approval:\n"
                f"  @{primary_agent_id} here is my sub-plan for [task title]. "
                f"Read `agents/{agent_id}-subplan.md`. Please approve or provide feedback.\n\n"
                f"Do NOT start executing yet.\n\n"
            )
        else:
            subplan_task = (
                f"You are revising your sub-plan for `{agent_id}` ({agent_name}).\n"
                f"Your latest sub-plan draft:\n\n---\n{actual_subplan[:2800]}\n---\n\n"
                f"Primary feedback to address:\n\n"
                f"{latest_feedback or 'Align the sub-plan more closely with scope, dependencies, and deliverables.'}\n\n"
                f"Rewrite the full sub-plan and replace `agents/{agent_id}-subplan.md` in output_files.\n"
                f"Keep it executable, specific, and aligned to the approved project plan.\n"
                f"Then post to chat asking @{primary_agent_id} to review this revised sub-plan.\n"
                f"Do NOT start executing yet.\n\n"
            )

        subplan_instruction = (
            f"hivee_agent_id: {agent_id}\n"
            f"hivee_project_token: {agent_token}\n"
            f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
            f"  Headers: X-Project-Agent-Id: {agent_id}\n"
            f"           X-Project-Agent-Token: {agent_token}\n\n"
            f"All Hivee API requests must include:\n"
            f"  X-Project-Agent-Id: {agent_id}\n"
            f"  X-Project-Agent-Token: {agent_token}\n\n"
            f"{subplan_task}"
            f"Return JSON: {{\"chat_update\": \"...\", "
            f"\"output_files\": [{{\"path\": \"agents/{agent_id}-subplan.md\", \"content\": \"...\"}}], "
            f"\"actions\": [{{\"type\": \"post_chat_message\", "
            f"\"text\": \"@{primary_agent_id} sub-plan ready for review...\", "
            f"\"mentions\": [\"{primary_agent_id}\"]}}], "
            f"\"requires_user_input\": true, "
            f"\"pause_reason\": \"Waiting for @{primary_agent_id} sub-plan approval.\"}}"
        )
        subplan_res = await _project_chat(
            row,
            connection_api_key,
            subplan_instruction,
            agent_id=agent_id,
            session_key=f"{project_id}:subplan:{agent_id}",
            timeout_sec=None,
            user_id=str(row["user_id"] or ""),
            from_agent_id="hivee",
            from_label="Hivee System",
            context_type="subplan",
        )
        _update_project_usage_metrics(
            project_id,
            prompt_tokens=_estimate_tokens_from_text(subplan_instruction),
            completion_tokens=_estimate_tokens_from_text(subplan_res.get("text")),
        )

        subplan_text = str(subplan_res.get("text") or "").strip()
        parsed_subplan = _extract_agent_report_payload(subplan_text)
        subplan_chat_update = str(parsed_subplan.get("chat_update") or "").strip()

        subplan_writes = parsed_subplan.get("output_files") or []
        if isinstance(subplan_writes, list) and subplan_writes:
            _apply_project_file_writes(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                writes=subplan_writes,
                default_prefix=USER_OUTPUTS_DIRNAME,
                allow_paths=None,
            )
        subplan_action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=parsed_subplan.get("actions") or [],
            allow_paths=None,
            actor_type="project_agent",
            actor_id=agent_id,
            actor_label=f"agent:{agent_id}",
        )
        await _emit_project_action_results(project_id, subplan_action_result.get("applied") or [])

        actual_subplan = ""
        for f in subplan_writes if isinstance(subplan_writes, list) else []:
            if str(f.get("path") or "").strip().endswith("subplan.md"):
                actual_subplan = str(f.get("content") or "").strip()
                break
        if not actual_subplan:
            actual_subplan = str(subplan_chat_update or subplan_text)[:3000]

        if not _applied_actions_include_kind(subplan_action_result.get("applied") or [], "post_chat_message"):
            fallback_chat = (
                subplan_chat_update
                or f"@{primary_agent_id} my sub-plan is ready in `agents/{agent_id}-subplan.md`. Please review it."
            )
            chat_conn = db()
            try:
                fallback_message = _create_project_chat_message(
                    chat_conn,
                    project_id=project_id,
                    author_type="project_agent",
                    author_id=agent_id,
                    author_label=agent_name,
                    text=fallback_chat,
                    mentions=[primary_agent_id],
                    metadata={"source": "subplan.phase"},
                )
                chat_conn.commit()
            finally:
                chat_conn.close()
            await _emit_project_chat_message_payload(project_id, fallback_message)

        await emit(
            project_id,
            "agent.subplan.submitted",
            {
                "agent_id": agent_id,
                "agent_name": agent_name,
                "preview": actual_subplan[:400],
                "round": round_idx,
            },
        )

        if not subplan_res.get("ok"):
            return True, actual_subplan

        review_instruction = (
            f"hivee_agent_id: {primary_agent_id}\n"
            f"hivee_project_token: {primary_token}\n"
            f"fundamentals: GET {hivee_api_base}/files/fundamentals.md\n"
            f"  Headers: X-Project-Agent-Id: {primary_agent_id}\n"
            f"           X-Project-Agent-Token: {primary_token}\n\n"
            f"All Hivee API requests must include:\n"
            f"  X-Project-Agent-Id: {primary_agent_id}\n"
            f"  X-Project-Agent-Token: {primary_token}\n\n"
            f"You are the primary agent `{primary_agent_id}` ({primary_agent_name}).\n"
            f"Agent `{agent_id}` ({agent_name}) submitted sub-plan round {round_idx} for review:\n\n"
            f"---\n{actual_subplan[:3000]}\n---\n\n"
            f"Review this sub-plan against the overall project plan, goals, and constraints "
            f"(read plan.md if needed). Consider: scope alignment, realistic timeline, correct "
            f"dependencies, resource fit.\n\n"
            f"Respond with your decision. Include `\"approved\": true` or `\"approved\": false`.\n"
            f"Post your decision to chat @{agent_id}.\n\n"
            f"Return JSON: {{\"chat_update\": \"...\", \"approved\": true, \"feedback\": \"...\", "
            f"\"actions\": [{{\"type\": \"post_chat_message\", "
            f"\"text\": \"@{agent_id} Sub-plan approved. Proceed.\", "
            f"\"mentions\": [\"{agent_id}\"]}}]}}"
        )
        review_res = await _project_chat(
            row,
            connection_api_key,
            review_instruction,
            agent_id=primary_agent_id,
            session_key=f"{project_id}:review:{agent_id}",
            timeout_sec=None,
            user_id=str(row["user_id"] or ""),
            from_agent_id="hivee",
            from_label="Hivee System",
            context_type="subplan_review",
        )
        _update_project_usage_metrics(
            project_id,
            prompt_tokens=_estimate_tokens_from_text(review_instruction),
            completion_tokens=_estimate_tokens_from_text(review_res.get("text")),
        )

        if not review_res.get("ok"):
            return True, actual_subplan

        review_text = str(review_res.get("text") or "").strip()
        parsed_review = _extract_agent_report_payload(review_text)
        review_chat_update = str(parsed_review.get("chat_update") or "").strip()
        review_feedback = str(parsed_review.get("feedback") or "").strip()

        review_action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=parsed_review.get("actions") or [],
            allow_paths=None,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=f"agent:{primary_agent_id}",
        )
        await _emit_project_action_results(project_id, review_action_result.get("applied") or [])

        approved_flag = parsed_review.get("approved")
        if approved_flag is None:
            review_chat = review_chat_update.lower()
            approved_flag = any(kw in review_chat for kw in ("approved", "proceed", "looks good", "good to go", "approve"))
        approved = bool(approved_flag)

        if not _applied_actions_include_kind(review_action_result.get("applied") or [], "post_chat_message"):
            fallback_review_text = review_chat_update or (
                f"@{agent_id} {'Sub-plan approved. Proceed.' if approved else 'Please revise your sub-plan and resubmit.'}"
            )
            review_conn = db()
            try:
                review_message = _create_project_chat_message(
                    review_conn,
                    project_id=project_id,
                    author_type="project_agent",
                    author_id=primary_agent_id,
                    author_label=primary_agent_name,
                    text=fallback_review_text,
                    mentions=[agent_id],
                    metadata={"source": "subplan.review"},
                )
                review_conn.commit()
            finally:
                review_conn.close()
            await _emit_project_chat_message_payload(project_id, review_message)

        reviewed_payload = {
            "agent_id": agent_id,
            "agent_name": agent_name,
            "approved": approved,
            "feedback": review_feedback[:400],
            "round": round_idx,
        }
        await emit(project_id, "agent.subplan.reviewed", reviewed_payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=primary_agent_name,
            event_type="agent.subplan.reviewed",
            summary=f"{agent_name} sub-plan {'approved' if approved else 'needs revision'}",
            payload=reviewed_payload,
        )
        if approved:
            return True, actual_subplan

        latest_feedback = review_feedback or review_chat_update or "Revise the sub-plan to better match scope, dependencies, and deliverables."

    return False, actual_subplan


def _parse_parallel_groups(
    parsed_delegation: Dict[str, Any],
    by_id: Dict[str, Any],
) -> List[List[str]]:
    """
    Extract parallel_groups from the primary agent's delegation response.
    Returns a list of groups; each group is a list of agent_ids to run concurrently.
    Falls back to one-agent-per-group (fully sequential) if not present or invalid.
    """
    raw = parsed_delegation.get("parallel_groups")
    if isinstance(raw, list) and raw:
        groups: List[List[str]] = []
        seen: set = set()
        for g in raw:
            if not isinstance(g, list):
                continue
            valid = [str(a).strip() for a in g if str(a).strip() in by_id and str(a).strip() not in seen]
            if valid:
                groups.append(valid)
                seen.update(valid)
        # Any agents not covered by primary's groups get appended as individual groups
        for aid in by_id:
            if aid not in seen:
                groups.append([aid])
        return groups if groups else [[aid] for aid in by_id]
    # Fallback: each agent in its own sequential group
    return [[aid] for aid in by_id]


async def _delegate_project_tasks(project_id: str) -> None:
    try:
        await _delegate_project_tasks_impl(project_id)
    except Exception as exc:
        reason = detail_to_text(exc)[:1200]
        conn = db()
        try:
            row = conn.execute(
                "SELECT user_id, project_root, progress_pct FROM projects WHERE id = ? LIMIT 1",
                (project_id,),
            ).fetchone()
        finally:
            conn.close()
        pause_pct = max(10, _clamp_progress(row["progress_pct"] if row else 10))
        _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=pause_pct)
        _refresh_project_documents(project_id)
        if row:
            _append_project_daily_log(
                owner_user_id=str(row["user_id"] or ""),
                project_root=str(row["project_root"] or ""),
                kind="execution.error",
                text=f"Execution paused after an internal pipeline error: {reason}",
                payload={"project_id": project_id},
            )
        payload = {
            "status": EXEC_STATUS_PAUSED,
            "progress_pct": pause_pct,
            "reason": f"Execution pipeline error: {reason}",
            "resume_hint": "Review the server log or latest activity, then press Resume to retry once fixed.",
        }
        await emit(project_id, "project.execution.auto_paused", payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="system",
            actor_id="hivee",
            actor_label="Hivee",
            event_type="project.execution.auto_paused",
            summary="Execution paused after an internal pipeline error",
            payload=payload,
        )


async def _delegate_project_tasks_impl(project_id: str) -> None:
    from core.session_project_access import _get_project_agent_permissions
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    permissions_by_agent: Dict[str, Dict[str, Any]] = {}
    for role_row in role_rows:
        agent_key = str(role_row.get("agent_id") or "").strip()
        if not agent_key:
            continue
        permissions_by_agent[agent_key] = _get_project_agent_permissions(
            conn,
            project_id=project_id,
            agent_id=agent_key,
            source_type=str(role_row.get("source_type") or "owner"),
        )
    conn.close()

    if _coerce_plan_status(row["plan_status"]) != PLAN_STATUS_APPROVED:
        await emit(project_id, "project.delegation.skipped", {"reason": "Plan not approved"})
        return
    if not role_rows:
        _set_project_execution_state(project_id, status=EXEC_STATUS_IDLE, progress_pct=0)
        _refresh_project_documents(project_id)
        await emit(project_id, "project.delegation.skipped", {"reason": "No invited agents yet"})
        return

    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    primary_agent_name = next(
        (
            str(r.get("agent_name") or r.get("agent_id") or "")
            for r in role_rows
            if str(r.get("agent_id") or "").strip() == str(primary_agent_id or "").strip()
        ),
        str(primary_agent_id or "primary"),
    )

    kickoff_result = _write_execution_kickoff_artifact(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        project_id=project_id,
        title=str(row["title"] or project_id),
        primary_agent_id=primary_agent_id,
        agent_count=len(role_rows),
    )
    for item in kickoff_result.get("saved") or []:
        await emit(
            project_id,
            "project.file.written",
            {
                "path": str(item.get("path") or ""),
                "mode": str(item.get("mode") or "w"),
                "bytes": int(item.get("bytes") or 0),
                "actor": "system:hivee",
            },
        )
    _append_project_activity(
        project_id=project_id,
        actor_type="system",
        actor_id="hivee",
        actor_label="Hivee",
        event_type="project.execution.kickoff",
        summary="Execution kickoff artifact created",
        payload={"saved_files": kickoff_result.get("saved") or [], "skipped": kickoff_result.get("skipped") or []},
    )

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=15)
    _refresh_project_documents(project_id)
    started_payload = {"agents": [r.get("agent_id") for r in role_rows]}
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="delegation.started",
        text="Primary agent started delegation planning after plan approval.",
        payload={"agents": [str(r.get("agent_id") or "") for r in role_rows]},
    )
    await emit(project_id, "project.delegation.started", started_payload)
    _append_project_activity(
        project_id=project_id,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=primary_agent_name,
        event_type="project.delegation.started",
        summary="Primary agent started delegation",
        payload=started_payload,
    )
    await _post_project_agent_status_message(
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_name=primary_agent_name,
        text=(
            f"@owner I am starting delegation for {len(role_rows)} assigned agent(s). "
            "I am reviewing the approved plan, splitting work, and preparing the progress map."
        ),
        mentions=["owner"],
        metadata={"phase": "delegate.start", "agent_count": len(role_rows)},
    )
    setup_details = _normalize_setup_details(_parse_setup_json(row["setup_json"]))

    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id or "")
    instruction = _delegate_prompt_from_project(
        project_id=project_id,
        agent_id=primary_agent_id or "",
        role_rows=role_rows,
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )
    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:delegate",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="delegation",
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
        failed_payload = {"error": detail_to_text(res.get("error") or res.get("details"))[:1200]}
        await emit(project_id, "project.delegation.failed", failed_payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=primary_agent_name,
            event_type="project.delegation.failed",
            summary=f"Delegation failed: {failed_payload.get('error') or 'unknown error'}",
            payload=failed_payload,
        )
        return

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=55)
    primary_reply = str(res.get("text") or "").strip()
    parsed = _extract_agent_report_payload(primary_reply)
    if not _delegation_payload_has_structured_work(parsed):
        retry_instruction = (
            "Your previous response was treated as chat only. Hivee execution cannot continue from chat alone.\n"
            "Return JSON object only with real project mutations:\n"
            "- `output_files`: include `delegation.md` and `progress_map.json` with full content.\n"
            "- `actions`: include `create_task` actions for each assigned agent, at least one `update_execution`, "
            "and `post_chat_message` handoffs that @mention each target agent.\n"
            "- Do not ask the owner to type 'continue'. Start the execution handoff now.\n"
            "If you are truly blocked by missing owner input, set `requires_user_input=true` and explain the exact blocker."
        )
        retry_res = await _project_chat(
            row,
            connection_api_key,
            retry_instruction,
            agent_id=primary_agent_id,
            session_key=f"{project_id}:delegate",
            timeout_sec=180,
            user_id=str(row["user_id"] or ""),
            from_agent_id="hivee",
            from_label="Hivee System",
            context_type="delegation_execution_contract",
        )
        rp, rc, _ = _extract_usage_counts(retry_res)
        if rp <= 0:
            rp = _estimate_tokens_from_text(retry_instruction)
        if rc <= 0:
            rc = _estimate_tokens_from_text(retry_res.get("text"))
        _update_project_usage_metrics(project_id, prompt_tokens=rp, completion_tokens=rc)
        if retry_res.get("ok"):
            retry_text = str(retry_res.get("text") or "").strip()
            retry_parsed = _extract_agent_report_payload(retry_text)
            if _delegation_payload_has_structured_work(retry_parsed):
                primary_reply = (primary_reply + "\n\n[EXECUTION CONTRACT RETRY]\n" + retry_text).strip()
                parsed = retry_parsed
                await emit(project_id, "project.delegation.contract_recovered", {"project_id": project_id})

    if not _delegation_payload_has_structured_work(parsed):
        pause_reason = (
            "Primary agent returned only conversational text during execution delegation. "
            "Hivee paused instead of leaving the project falsely running."
        )
        _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=15)
        _refresh_project_documents(project_id)
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="execution.waiting_for_artifact",
            text=pause_reason,
            payload={"agent_id": primary_agent_id or "", "stage": "delegation"},
        )
        payload = {
            "status": EXEC_STATUS_PAUSED,
            "progress_pct": 15,
            "agent_id": primary_agent_id or "",
            "agent_name": primary_agent_name,
            "reason": pause_reason,
            "resume_hint": "Press Resume to retry delegation after checking the primary agent response contract.",
        }
        await emit(project_id, "project.execution.waiting_for_artifact", payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=primary_agent_name,
            event_type="project.execution.waiting_for_artifact",
            summary="Execution paused because delegation produced no structured work",
            payload=payload,
        )
        return
    by_id = {str(r.get("agent_id") or "").strip(): r for r in role_rows}

    if primary_reply:
        primary_update_payload = {
            "agent_id": primary_agent_id,
            "agent_name": next((str(r.get("agent_name") or r.get("agent_id") or "") for r in role_rows if str(r.get("agent_id") or "") == str(primary_agent_id or "")), ""),
            "text": primary_reply[:1200],
        }
        await emit(
            project_id,
            "agent.primary.update",
            primary_update_payload,
        )
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=primary_update_payload.get("agent_name") or primary_agent_name,
            event_type="agent.primary.update",
            summary="Primary agent submitted an update",
            payload=primary_update_payload,
        )
    for note in _summarize_ws_frames(res.get("frames"), limit=10):
        await emit(project_id, "agent.primary.live", {"agent_id": primary_agent_id, "note": note})
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="agent.primary.update",
        text=primary_reply[:1800] if primary_reply else "Primary agent returned delegation payload.",
    )
    await _post_project_agent_status_message(
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_name=primary_agent_name,
        text=(
            "@owner I finished the delegation blueprint. "
            "I am now publishing delegation.md, progress_map.json, and the initial task assignments for each agent."
        ),
        mentions=["owner"],
        metadata={"phase": "delegate.publish"},
    )

    try:
        project_dir = _resolve_owner_project_dir(str(row["user_id"]), str(row["project_root"] or ""))
    except Exception:
        failed_payload = {"error": "Project directory unavailable"}
        await emit(project_id, "project.delegation.failed", failed_payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=primary_agent_id,
            actor_label=primary_agent_name,
            event_type="project.delegation.failed",
            summary="Delegation failed: Project directory unavailable",
            payload=failed_payload,
        )
        return
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / USER_OUTPUTS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / USER_OUTPUTS_DIRNAME / HANDOFFS_DIRNAME).mkdir(parents=True, exist_ok=True)

    # Save delegation.md (new) + legacy PROJECT-DELEGATION.MD
    output_files = parsed.get("output_files") if isinstance(parsed.get("output_files"), list) else []
    delegation_content = ""
    for f in output_files:
        rel = str(f.get("path") or "").strip().lower()
        if "delegation" in rel:
            delegation_content = str(f.get("content") or "").strip()
            break
    if not delegation_content:
        # Fallback: use chat_update as delegation summary
        delegation_content = str(parsed.get("chat_update") or primary_reply or "Delegation initialized.").strip()
    (project_dir / "delegation.md").write_text(f"# Delegation\n\n{delegation_content}\n", encoding="utf-8")
    (project_dir / PROJECT_DELEGATION_FILE).write_text(f"# Delegation\n\n{delegation_content}\n", encoding="utf-8")

    # Apply all actions from agent response: creates tasks, posts chat @mentions, writes files
    actions_raw = parsed.get("actions") if isinstance(parsed.get("actions"), list) else []
    action_result: Dict[str, Any] = {"applied": [], "skipped": []}
    if actions_raw:
        action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=actions_raw,
            allow_paths=None,  # primary agent has full access
            actor_type="project_agent",
            actor_id=str(primary_agent_id or ""),
            actor_label=f"agent:{primary_agent_id}",
        )
        await _emit_project_action_results(project_id, action_result.get("applied") or [])

    # Apply output_files writes
    if output_files:
        _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files,
            default_prefix=USER_OUTPUTS_DIRNAME,
            allow_paths=None,
        )

    # Legacy task_map from old payload format (backward compat)
    payload_legacy = _parse_delegation_payload(primary_reply)
    raw_tasks = payload_legacy.get("agent_tasks")
    task_map: Dict[str, str] = {}
    if isinstance(raw_tasks, list):
        for item in raw_tasks:
            if not isinstance(item, dict):
                continue
            aid = str(item.get("agent_id") or "").strip()
            task_md = str(item.get("task_md") or "").strip()
            if aid and task_md and aid in by_id:
                task_map[aid] = task_md

    # Parse parallel groups from primary's delegation response
    parallel_groups = _parse_parallel_groups(parsed, by_id)

    agent_order = list(by_id.keys())
    delegation_state = _read_project_delegation_state(project_id)
    task_assignees = set(delegation_state.get("task_assignees") or set())
    delegated_mentions = set(delegation_state.get("mention_targets") or set())
    assigned_task_map: Dict[str, str] = {}
    assigned_mentions_map: Dict[str, List[str]] = {}
    agents_dir = project_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    assigned_count = 0
    for pos, aid in enumerate(agent_order):
        row_item = by_id.get(aid) or {}
        agent_name = str(row_item.get("agent_name") or aid)
        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        next_aid = agent_order[pos + 1] if (pos + 1) < len(agent_order) else None
        task_text = _normalize_task_markdown_for_agent(
            agent_id=aid,
            role=role,
            task_md=task_map.get(aid, f"Read delegation.md and execute your assigned scope. Report progress via chat with @mentions."),
            next_agent_id=next_aid,
        )
        assigned_task_map[aid] = task_text
        fname = _safe_agent_filename(aid) + ".md"
        (agents_dir / fname).write_text(task_text.strip() + "\n", encoding="utf-8")
        assigned_count += 1
        task_mentions = sorted({m for m in re.findall(r"@([a-zA-Z0-9._-]+)", task_text) if m and m != aid})[:8]
        assigned_mentions_map[aid] = task_mentions
        task_title = _task_title_from_text(task_text, role=role, agent_name=agent_name)

        # Fallback: if this agent does not yet have a chat mention, post one now.
        if aid not in delegated_mentions:
            fallback_chat_result = _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "post_chat_message",
                    "text": f"@{aid} your tasks are assigned. Check `delegation.md` for your scope and start trigger.",
                    "mentions": [aid],
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(primary_agent_id or ""),
                actor_label=f"agent:{primary_agent_id}",
            )
            await _emit_project_action_results(project_id, fallback_chat_result.get("applied") or [])

            if _applied_actions_include_kind(fallback_chat_result.get("applied") or [], "post_chat_message"):
                delegated_mentions.add(aid)

        # Fallback: if this agent still has no task card, create one now.
        if aid not in task_assignees:
            fallback_task_result = _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "create_task",
                    "title": f"{role} — {agent_name}",
                    "description": task_text[:TASK_DESCRIPTION_MAX_CHARS],
                    "assignee_agent_id": aid,
                    "status": "todo",
                    "priority": "high",
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(primary_agent_id or ""),
                actor_label=f"agent:{primary_agent_id}",
            )
            await _emit_project_action_results(project_id, fallback_task_result.get("applied") or [])
            if _applied_actions_include_kind(fallback_task_result.get("applied") or [], "create_task"):
                task_assignees.add(aid)

        resolved_task_id = _latest_project_task_id_for_agent(project_id, aid)
        assigned_payload = {
            "agent_id": aid,
            "agent_name": agent_name,
            "role": role,
            "task_title": task_title,
            "task_id": resolved_task_id or "",
            "task_file": f"agents/{fname}",
            "task_preview": task_text[:500],
            "mentions": task_mentions,
        }
        await emit(
            project_id,
            "agent.task.assigned",
            assigned_payload,
        )
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=aid,
            actor_label=agent_name,
            event_type="agent.task.assigned",
            summary=f"{agent_name} received a delegated task",
            payload=assigned_payload,
        )
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agent.task.assigned",
            text=f"{aid}: {task_text[:800]}",
            payload={
                "task_file": f"agents/{fname}",
                "task_title": task_title,
                "task_id": resolved_task_id or "",
                "mentions": task_mentions,
            },
        )

    await _post_project_agent_status_message(
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_name=primary_agent_name,
        text=(
            f"@owner High-level assignments are live for {assigned_count} agent(s). "
            "I am moving into detailed sub-plan review before execution starts."
        ),
        mentions=["owner"],
        metadata={"phase": "delegate.assignments_ready", "assigned_count": assigned_count},
    )

    delegation_state = _read_project_delegation_state(project_id)
    progress_map_candidate = ""
    for f in output_files:
        rel = _clean_relative_project_path(str(f.get("path") or ""))
        if (rel or "").replace("\\", "/").split("/")[-1].lower() == "progress_map.json":
            progress_map_candidate = str(f.get("content") or "").strip()
            break

    progress_map_payload: Optional[Dict[str, Any]] = None
    if progress_map_candidate:
        try:
            parsed_progress_map = json.loads(progress_map_candidate)
            if isinstance(parsed_progress_map, dict) and isinstance(parsed_progress_map.get("nodes"), list):
                progress_map_payload = parsed_progress_map
        except Exception:
            progress_map_payload = None
    if not isinstance(progress_map_payload, dict):
        progress_map_payload = _build_minimal_progress_map(
            task_rows=list(delegation_state.get("task_rows") or []),
            parallel_groups=parallel_groups,
            by_id=by_id,
        )
    if isinstance(progress_map_payload, dict):
        progress_map_path = project_dir / "progress_map.json"
        progress_map_path.write_text(
            json.dumps(progress_map_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
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

    # ── SUB-PLAN PHASE ────────────────────────────────────────────────────────
    # Each non-primary agent writes a detailed sub-plan; primary reviews it.
    # Agents within the same parallel group submit sub-plans concurrently.
    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=20)
    await emit(project_id, "project.subplan.phase_started", {
        "groups": [[a for a in g] for g in parallel_groups],
    })
    await _post_project_agent_status_message(
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_name=primary_agent_name,
        text=(
            "@owner I am collecting and reviewing detailed sub-plans now. "
            f"Execution groups queued: {json.dumps(parallel_groups, ensure_ascii=False)}"
        ),
        mentions=["owner"],
        metadata={"phase": "delegate.subplan_phase_started", "groups": parallel_groups},
    )
    subplan_map: Dict[str, str] = {}  # agent_id -> approved sub-plan text

    for grp in parallel_groups:
        non_primary_in_group = [a for a in grp if a != primary_agent_id]
        if not non_primary_in_group:
            continue
        await _post_project_agent_status_message(
            project_id=project_id,
            agent_id=primary_agent_id,
            agent_name=primary_agent_name,
            text=(
                f"@owner I am reviewing sub-plans for this execution group: "
                f"{', '.join(non_primary_in_group)}."
            ),
            mentions=["owner"],
            metadata={"phase": "delegate.subplan_group_review", "group": list(non_primary_in_group)},
        )
        state, _ = _read_project_execution_state(project_id)
        if state == EXEC_STATUS_STOPPED:
            break

        async def _collect_subplan(aid: str) -> Tuple[str, bool, str]:
            a_name = str((by_id.get(aid) or {}).get("agent_name") or aid)
            approved, sp_text = await _run_agent_subplan_phase_v2(
                project_id, row, connection_api_key,
                agent_id=aid,
                agent_name=a_name,
                primary_agent_id=primary_agent_id or aid,
                primary_agent_name=primary_agent_name,
                task_text=assigned_task_map.get(aid) or "",
            )
            return aid, approved, sp_text

        results = await asyncio.gather(*[_collect_subplan(a) for a in non_primary_in_group])
        rejected_agents: List[str] = []
        for aid, approved, sp_text in results:
            subplan_map[aid] = sp_text
            a_name = str((by_id.get(aid) or {}).get("agent_name") or aid)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.subplan.reviewed",
                text=f"{aid}: {'approved' if approved else 'needs revision'} — {sp_text[:400]}",
            )
            if not approved:
                rejected_agents.append(aid)
        if rejected_agents:
            rejected_names = [
                str((by_id.get(aid) or {}).get("agent_name") or aid)
                for aid in rejected_agents
            ]
            pause_reason = (
                "Execution paused because some delegated sub-plans still were not approved after retries: "
                + ", ".join(rejected_names)
            )
            _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=25)
            _refresh_project_documents(project_id)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="execution.auto_paused",
                text=pause_reason[:1200],
                payload={"agents": rejected_agents},
            )
            pause_payload = {
                "status": EXEC_STATUS_PAUSED,
                "progress_pct": 25,
                "agent_id": primary_agent_id or "primary",
                "agent_name": primary_agent_name,
                "reason": pause_reason[:900],
                "resume_hint": "Review the sub-plan chat thread, then resume once the team is aligned.",
            }
            await emit(project_id, "project.execution.auto_paused", pause_payload)
            _append_project_activity(
                project_id=project_id,
                actor_type="project_agent",
                actor_id=primary_agent_id,
                actor_label=primary_agent_name,
                event_type="project.execution.auto_paused",
                summary="Execution auto-paused awaiting review/input",
                payload={**pause_payload, "agents": rejected_agents},
            )
            await _post_project_agent_status_message(
                project_id=project_id,
                agent_id=primary_agent_id,
                agent_name=primary_agent_name,
                text=(
                    "@owner I paused execution because some sub-plans still need revision: "
                    f"{', '.join(rejected_names)}. Once those revisions are aligned, we can resume."
                ),
                mentions=["owner"],
                metadata={"phase": "delegate.paused_for_revisions", "agents": rejected_agents},
            )
            return

        await _post_project_agent_status_message(
            project_id=project_id,
            agent_id=primary_agent_id,
            agent_name=primary_agent_name,
            text=(
                f"@owner Sub-plans approved for group: {', '.join(non_primary_in_group)}. "
                "I am moving to the next delegation checkpoint."
            ),
            mentions=["owner"],
            metadata={"phase": "delegate.subplan_group_approved", "group": list(non_primary_in_group)},
        )

    _set_project_execution_state(project_id, status=EXEC_STATUS_RUNNING, progress_pct=40)
    await emit(project_id, "project.subplan.phase_complete", {
        "agents_with_subplans": list(subplan_map.keys()),
    })
    await _post_project_agent_status_message(
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_name=primary_agent_name,
        text=(
            "@owner All delegated sub-plans are approved. "
            "I am handing execution over to the assigned agents now."
        ),
        mentions=["owner"],
        metadata={"phase": "delegate.complete", "agents_with_subplans": list(subplan_map.keys())},
    )

    # ── EXECUTION PHASE (parallel groups) ────────────────────────────────────
    # Inner async function so we can run agents within a group concurrently.
    async def _run_one_agent(aid: str, grp_idx: int) -> None:
        nonlocal processed_agents, failed_agents
        nonlocal primary_last_chat_update, primary_last_notes
        nonlocal primary_last_pause_reason, primary_last_resume_hint, primary_pause_resolved

        row_item = by_id.get(aid) or {}
        # Wait while paused; bail on stop
        while True:
            state, _ = _read_project_execution_state(project_id)
            if state == EXEC_STATUS_PAUSED:
                await asyncio.sleep(0.7)
                continue
            if state == EXEC_STATUS_STOPPED:
                return
            break

        role = str(row_item.get("role") or "").strip() or "Collaborate based on project plan."
        agent_name = str(row_item.get("agent_name") or aid)
        task_text = assigned_task_map.get(aid) or f"# Task for {aid}\n\nRole: {role}\n"
        task_title = _task_title_from_text(task_text, role=role, agent_name=agent_name)
        resolved_task_id = _latest_project_task_id_for_agent(project_id, aid)
        approved_subplan = subplan_map.get(aid, "")
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
                PROJECT_PROTOCOL_FILE,
                "agents/ROLES.md",
                SETUP_CHAT_HISTORY_FILE,
            ],
            request_text=task_text,
            max_total_chars=7_500,
            max_files=8,
        )
        started_payload = {
            "agent_id": aid,
            "agent_name": agent_name,
            "role": role,
            "task_title": task_title,
            "task_id": resolved_task_id or "",
            "task_file": task_rel,
        }
        await emit(project_id, "agent.task.started", started_payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=aid,
            actor_label=agent_name,
            event_type="agent.task.started",
            summary=f"{agent_name} started a delegated task",
            payload=started_payload,
        )
        try:
            await _post_project_agent_status_message(
                project_id=project_id,
                agent_id=aid,
                agent_name=agent_name,
                text=f"I am starting `{task_title}` now. I will share another update when this step is done.",
                mentions=[],
                metadata={"phase": "task.start", "task_title": task_title, "task_id": resolved_task_id or ""},
            )
        except Exception:
            pass

        subplan_section = (
            f"\n\n## Your Approved Sub-Plan\n{approved_subplan[:2000]}\n\n"
            f"Follow this sub-plan. Your FIRST action must be to create detailed sub-task cards "
            f"(via `create_task` actions) matching the steps in your sub-plan, then begin executing.\n"
        ) if approved_subplan else (
            f"\n\nFirst, create detailed sub-task cards (via `create_task` actions) for each step "
            f"of your work, then begin executing.\n"
        )

        agent_instruction = (
            _project_context_instruction(
                title=str(row["title"] or ""),
                brief=str(row["brief"] or ""),
                goal=str(row["goal"] or ""),
                setup_details=setup_details,
                role_rows=role_rows,
                project_root=str(row["project_root"] or ""),
                plan_status=PLAN_STATUS_APPROVED,
            )
            + "\n\n"
            + f"You are invited agent `{aid}` with role `{role}`.\n"
            + team_roster_text
            + subplan_section
            + "\n\n"
            + _build_project_task_snapshot(project_id)
            + "\n"
            + _build_project_chat_snapshot(project_id)
            + "\n"
            + "EXECUTION CONTRACT: this is not casual project chat. Execute your assigned task now and return JSON object only:\n"
            + "{\n"
            + "  \"chat_update\": \"Human-friendly update sentence to show in chat\",\n"
            + "  \"output_files\": [{\"path\":\"relative/path.ext\",\"content\":\"file content\",\"append\":false}],\n"
            + "  \"actions\": [{\"type\":\"create_task\",\"title\":\"...\",\"description\":\"...\"},{\"type\":\"post_chat_message\",\"text\":\"handoff to @agent_id\"},{\"type\":\"update_execution\",\"progress_pct\":45}],\n"
            + "  \"notes\": \"optional technical notes\",\n"
            + "  \"requires_user_input\": false,\n"
            + "  \"pause_reason\": \"\",\n"
            + "  \"resume_hint\": \"\"\n"
            + "}\n"
            + "Rules:\n"
            + "- chat_update must read like normal conversation.\n"
            + "- Put every created/updated artifact in output_files.\n"
            + "- Use `actions` when you need to change real project files, group chat state, or task/progress state.\n"
            + "- Your response must include at least one real project mutation: output_files, create/update task actions, write_file actions, or update_execution.\n"
            + "- A plain chat_update without artifact/action/progress is a failed execution step and Hivee will pause the run.\n"
            + "- FIRST action: create sub-task cards for each step of your sub-plan.\n"
            + "- Use exact IDs from roster when mentioning other agents.\n"
            + "- Mention handoff needs in chat_update with @agent_id.\n"
            + f"- Follow `{PROJECT_PROTOCOL_FILE}` for delegation, mention, and status update rules.\n\n"
            + "- If blocked by user approval/input or planned pit stop, set requires_user_input=true and explain pause_reason.\n"
            + "- If user says SKIP for missing info, proceed with assumptions and state them briefly in chat_update.\n"
            + "Assigned task:\n"
            + task_text.strip()
        )
        if agent_file_context:
            agent_instruction = f"{agent_instruction}\n\n{agent_file_context}"

        agent_res = await _project_chat(
            row,
            connection_api_key,
            agent_instruction,
            agent_id=aid,
            session_key=f"{project_id}:agent:{aid}",
            timeout_sec=None,
            user_id=str(row["user_id"] or ""),
            from_agent_id="hivee",
            from_label="Hivee System",
            context_type="task_execution",
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
            failed_payload = {
                "agent_id": aid,
                "agent_name": agent_name,
                "task_title": task_title,
                "task_id": resolved_task_id or "",
                "task_file": task_rel,
                "error": err_text,
            }
            await emit(project_id, "agent.task.failed", failed_payload)
            _append_project_activity(
                project_id=project_id,
                actor_type="project_agent",
                actor_id=aid,
                actor_label=agent_name,
                event_type="agent.task.failed",
                summary=f"{agent_name} failed a delegated task",
                payload=failed_payload,
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="agent.task.failed",
                text=f"{aid}: {err_text}",
            )
            return

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
        action_items_raw = parsed_report.get("actions") or []
        agent_perms = permissions_by_agent.get(str(aid).strip()) or {}
        if bool(agent_perms.get("can_write_files")):
            agent_output_allow_paths = _normalize_permission_write_paths(
                agent_perms.get("write_paths") or [],
                fallback=[USER_OUTPUTS_DIRNAME, f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}"],
            )
        else:
            agent_output_allow_paths = []
        write_result = _apply_project_file_writes(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            writes=output_files_raw if isinstance(output_files_raw, list) else [],
            default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
            allow_paths=agent_output_allow_paths,
        )
        saved_files = write_result.get("saved") or []
        skipped_files = write_result.get("skipped") or []
        action_result = _apply_project_actions(
            owner_user_id=str(row["user_id"]),
            project_id=project_id,
            project_root=str(row["project_root"] or ""),
            actions=action_items_raw if isinstance(action_items_raw, list) else [],
            allow_paths=agent_output_allow_paths,
            actor_type="project_agent",
            actor_id=aid,
            actor_label=f"agent:{aid}",
        )
        applied_actions = action_result.get("applied") or []
        skipped_actions = action_result.get("skipped") or []
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
            followup_res = await _project_chat(
                row,
                connection_api_key,
                followup_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=None,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution",
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
                followup_actions = parsed_followup.get("actions") or []
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
                    allow_paths=agent_output_allow_paths,
                )
                followup_saved = followup_write_result.get("saved") or []
                followup_skipped = followup_write_result.get("skipped") or []
                followup_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=followup_actions if isinstance(followup_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                followup_applied_actions = followup_action_result.get("applied") or []
                followup_skipped_actions = followup_action_result.get("skipped") or []
                if followup_saved:
                    saved_files.extend(followup_saved)
                if followup_skipped:
                    skipped_files.extend(followup_skipped)
                if followup_applied_actions:
                    applied_actions.extend(followup_applied_actions)
                if followup_skipped_actions:
                    skipped_actions.extend(followup_skipped_actions)
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
            rescue_res = await _project_chat(
                row,
                connection_api_key,
                rescue_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=None,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution",
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
                rescue_actions = parsed_rescue.get("actions") or []
                if not rescue_writes:
                    rescue_writes = _extract_artifacts_from_fenced_code(rescue_text)
                rescue_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=rescue_writes if isinstance(rescue_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                    allow_paths=agent_output_allow_paths,
                )
                rescue_saved = rescue_write_result.get("saved") or []
                rescue_skipped = rescue_write_result.get("skipped") or []
                rescue_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=rescue_actions if isinstance(rescue_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                rescue_applied_actions = rescue_action_result.get("applied") or []
                rescue_skipped_actions = rescue_action_result.get("skipped") or []
                if rescue_saved:
                    saved_files.extend(rescue_saved)
                if rescue_skipped:
                    skipped_files.extend(rescue_skipped)
                if rescue_applied_actions:
                    applied_actions.extend(rescue_applied_actions)
                if rescue_skipped_actions:
                    skipped_actions.extend(rescue_skipped_actions)
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
                allow_paths=agent_output_allow_paths,
            )
            fallback_saved = fallback_write_result.get("saved") or []
            fallback_skipped = fallback_write_result.get("skipped") or []
            if fallback_saved:
                saved_files.extend(fallback_saved)
                skipped_files.append("No explicit output_files from agent; saved fallback markdown deliverable.")
            if fallback_skipped:
                skipped_files.extend(fallback_skipped)

        if not requires_user_input and not _applied_actions_include_kind(applied_actions, "create_task"):
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "No sub-task cards detected yet. Requesting detailed task breakdown for the progress map.",
                },
            )
            subtask_prompt = (
                "Hivee still has no detailed sub-task cards for your assigned work.\n"
                "Return JSON object only.\n"
                f"Create 2-6 concrete sub-task cards via `create_task` actions for agent `{aid}` so the progress map can track your work.\n"
                "Each task must include: `ref`, `title`, `description`, `assignee_agent_id`, `status`, `priority`, "
                "`weight_pct`, `instructions`, `input`, `process`, `output`, `from_agent`, and `handover_to`.\n"
                "Use `add_task_dependency` actions with `task_ref` and `depends_on_task_ref` when the steps are sequential.\n"
                "Base the breakdown on your approved sub-plan or the assigned task. Keep `chat_update` short and mention that the progress map was expanded.\n"
                "Do not repeat the full deliverable unless needed; focus on task breakdown and dependency structure."
            )
            subtask_res = await _project_chat(
                row,
                connection_api_key,
                subtask_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=None,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution",
            )
            if subtask_res.get("ok"):
                stp, stc, _ = _extract_usage_counts(subtask_res)
                if stp <= 0:
                    stp = _estimate_tokens_from_text(subtask_prompt)
                if stc <= 0:
                    stc = _estimate_tokens_from_text(subtask_res.get("text"))
                _update_project_usage_metrics(project_id, prompt_tokens=stp, completion_tokens=stc)
                subtask_text = str(subtask_res.get("text") or "").strip()
                parsed_subtask = _extract_agent_report_payload(subtask_text)
                subtask_chat = str(parsed_subtask.get("chat_update") or "").strip()
                subtask_writes = parsed_subtask.get("output_files") or []
                subtask_actions = parsed_subtask.get("actions") or []
                if not report_notes:
                    report_notes = str(parsed_subtask.get("notes") or "").strip()
                requires_user_input = requires_user_input or bool(parsed_subtask.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_subtask.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_subtask.get("resume_hint") or "").strip()
                subtask_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=subtask_writes if isinstance(subtask_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                    allow_paths=agent_output_allow_paths,
                )
                subtask_saved = subtask_write_result.get("saved") or []
                subtask_skipped = subtask_write_result.get("skipped") or []
                subtask_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=subtask_actions if isinstance(subtask_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                subtask_applied = subtask_action_result.get("applied") or []
                subtask_skipped_actions = subtask_action_result.get("skipped") or []
                if subtask_saved:
                    saved_files.extend(subtask_saved)
                if subtask_skipped:
                    skipped_files.extend(subtask_skipped)
                if subtask_applied:
                    applied_actions.extend(subtask_applied)
                if subtask_skipped_actions:
                    skipped_actions.extend(subtask_skipped_actions)
                if subtask_chat:
                    chat_update = subtask_chat
                if subtask_text:
                    report_text = (report_text + "\n\n[SUBTASK FOLLOW-UP]\n" + subtask_text).strip()
                for note in _summarize_ws_frames(subtask_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_actions.append(
                    "subtask follow-up failed: "
                    + detail_to_text(subtask_res.get("error") or subtask_res.get("details") or "unknown")
                )

        if not saved_files and not _applied_actions_have_project_work(applied_actions) and not requires_user_input:
            await emit(
                project_id,
                "agent.task.live",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "note": "Agent returned chat only. Retrying once with strict execution contract.",
                },
            )
            strict_prompt = (
                "Your previous execution response did not produce any project artifact, task mutation, or progress update.\n"
                "Hivee cannot count chat-only text as execution work.\n"
                "Return JSON object only and include at least one of:\n"
                "- `output_files` with a concrete artifact under Outputs/;\n"
                "- `actions` with create_task/update_task/write_file/update_execution;\n"
                "- `requires_user_input=true` with a precise blocker and resume_hint.\n"
                f"Agent: `{aid}` ({agent_name})\n"
                f"Assigned task:\n{task_text[:3000]}\n"
            )
            strict_res = await _project_chat(
                row,
                connection_api_key,
                strict_prompt,
                agent_id=aid,
                session_key=f"{project_id}:agent:{aid}",
                timeout_sec=180,
                user_id=str(row["user_id"] or ""),
                from_agent_id="hivee",
                from_label="Hivee System",
                context_type="task_execution_contract",
            )
            sp, sc, _ = _extract_usage_counts(strict_res)
            if sp <= 0:
                sp = _estimate_tokens_from_text(strict_prompt)
            if sc <= 0:
                sc = _estimate_tokens_from_text(strict_res.get("text"))
            _update_project_usage_metrics(project_id, prompt_tokens=sp, completion_tokens=sc)
            if strict_res.get("ok"):
                strict_text = str(strict_res.get("text") or "").strip()
                parsed_strict = _extract_agent_report_payload(strict_text)
                strict_chat = str(parsed_strict.get("chat_update") or "").strip()
                strict_writes = parsed_strict.get("output_files") or []
                strict_actions = parsed_strict.get("actions") or []
                strict_write_result = _apply_project_file_writes(
                    owner_user_id=str(row["user_id"]),
                    project_root=str(row["project_root"] or ""),
                    writes=strict_writes if isinstance(strict_writes, list) else [],
                    default_prefix=f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}",
                    allow_paths=agent_output_allow_paths,
                )
                strict_saved = strict_write_result.get("saved") or []
                strict_skipped = strict_write_result.get("skipped") or []
                strict_action_result = _apply_project_actions(
                    owner_user_id=str(row["user_id"]),
                    project_id=project_id,
                    project_root=str(row["project_root"] or ""),
                    actions=strict_actions if isinstance(strict_actions, list) else [],
                    allow_paths=agent_output_allow_paths,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=f"agent:{aid}",
                )
                strict_applied = strict_action_result.get("applied") or []
                strict_skipped_actions = strict_action_result.get("skipped") or []
                if strict_saved:
                    saved_files.extend(strict_saved)
                if strict_skipped:
                    skipped_files.extend(strict_skipped)
                if strict_applied:
                    applied_actions.extend(strict_applied)
                if strict_skipped_actions:
                    skipped_actions.extend(strict_skipped_actions)
                requires_user_input = requires_user_input or bool(parsed_strict.get("requires_user_input"))
                if not pause_reason:
                    pause_reason = str(parsed_strict.get("pause_reason") or "").strip()
                if not resume_hint:
                    resume_hint = str(parsed_strict.get("resume_hint") or "").strip()
                if not report_notes:
                    report_notes = str(parsed_strict.get("notes") or "").strip()
                if strict_chat:
                    chat_update = strict_chat
                if strict_text:
                    report_text = (report_text + "\n\n[EXECUTION CONTRACT RETRY]\n" + strict_text).strip()
                for note in _summarize_ws_frames(strict_res.get("frames"), limit=6):
                    await emit(project_id, "agent.task.live", {"agent_id": aid, "agent_name": agent_name, "note": note})
            else:
                skipped_actions.append(
                    "execution contract retry failed: "
                    + detail_to_text(strict_res.get("error") or strict_res.get("details") or "unknown")
                )

        if not saved_files and not _applied_actions_have_project_work(applied_actions) and not requires_user_input:
            reason = (
                f"{agent_name} returned chat-only output after an execution retry. "
                "Hivee paused instead of keeping the project falsely running."
            )
            _set_project_execution_state(project_id, status=EXEC_STATUS_PAUSED, progress_pct=max(10, _read_project_execution_state(project_id)[1]))
            _refresh_project_documents(project_id)
            payload = {
                "status": EXEC_STATUS_PAUSED,
                "progress_pct": _read_project_execution_state(project_id)[1],
                "agent_id": aid,
                "agent_name": agent_name,
                "reason": reason,
                "resume_hint": "Press Resume to retry this execution step after checking the agent response contract.",
            }
            await emit(project_id, "project.execution.waiting_for_artifact", payload)
            _append_project_activity(
                project_id=project_id,
                actor_type="project_agent",
                actor_id=aid,
                actor_label=agent_name,
                event_type="project.execution.waiting_for_artifact",
                summary="Execution paused because agent produced no structured work",
                payload=payload,
            )
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="execution.waiting_for_artifact",
                text=reason,
                payload={"agent_id": aid, "task_title": task_title, "skipped_actions": skipped_actions[:10]},
            )
            return

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
            # Create a blocked task card so user sees the issue clearly
            issue_title = f"[BLOCKED] {agent_name} — {pause_reason[:120]}"
            issue_desc = (
                f"Agent `{aid}` ({role}) is blocked and needs user input.\n\n"
                f"**Blocker:** {pause_reason}\n\n"
                f"**Resume hint:** {resume_hint or 'No hint provided.'}\n\n"
                f"**Last chat update:** {chat_update[:600]}"
            )
            issue_action_result = _apply_project_actions(
                owner_user_id=str(row["user_id"]),
                project_id=project_id,
                project_root=str(row["project_root"] or ""),
                actions=[{
                    "type": "create_task",
                    "title": issue_title,
                    "description": issue_desc[:TASK_DESCRIPTION_MAX_CHARS],
                    "assignee_agent_id": aid,
                    "status": "blocked",
                    "priority": "urgent",
                }],
                allow_paths=None,
                actor_type="project_agent",
                actor_id=str(aid),
                actor_label=f"agent:{aid}",
            )
            await _emit_project_action_results(project_id, issue_action_result.get("applied") or [])
        else:
            chat_update = _ensure_chat_handoff_mentions(chat_update, assigned_mentions_map.get(aid) or [])

        auto_chat_message = None
        has_explicit_chat_action = any(
            _normalize_agent_action_kind(
                (item or {}).get("type")
                or (item or {}).get("method")
                or (item or {}).get("action")
                or (item or {}).get("name")
            ) == "post_chat_message"
            for item in applied_actions
            if isinstance(item, dict)
        )
        if not has_explicit_chat_action:
            chat_conn = db()
            try:
                auto_chat_message = _create_project_chat_message(
                    chat_conn,
                    project_id=project_id,
                    author_type="project_agent",
                    author_id=aid,
                    author_label=agent_name,
                    text=chat_update,
                    metadata={
                        "source": "delegation.agent_task",
                        "output_file": f"{USER_OUTPUTS_DIRNAME}/{_safe_agent_filename(aid)}-latest.md",
                        "requires_user_input": bool(pause_decision.get("pause")),
                    },
                )
                chat_conn.commit()
            finally:
                chat_conn.close()
        if isinstance(auto_chat_message, dict):
            await _emit_project_chat_message_payload(project_id, auto_chat_message)

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
        pct = min(95, 40 + int(((grp_idx + 1) / max(1, len(parallel_groups))) * 53))
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
        await _emit_project_action_results(project_id, applied_actions)
        reported_payload = {
            "agent_id": aid,
            "agent_name": agent_name,
            "task_title": task_title,
            "task_id": resolved_task_id or "",
            "task_file": task_rel,
            "text": chat_update[:1200],
            "output_file": f"{USER_OUTPUTS_DIRNAME}/{report_file.name}",
            "saved_files": saved_files[:20],
            "skipped_files": skipped_files[:10],
            "applied_actions": applied_actions[:20],
            "skipped_actions": skipped_actions[:10],
            "requires_user_input": bool(pause_decision.get("pause")),
            "pause_reason": pause_reason[:500],
            "resume_hint": resume_hint[:300],
        }
        await emit(project_id, "agent.task.reported", reported_payload)
        _append_project_activity(
            project_id=project_id,
            actor_type="project_agent",
            actor_id=aid,
            actor_label=agent_name,
            event_type="agent.task.reported",
            summary=f"{agent_name}: {chat_update[:900]}",
            payload=reported_payload,
        )
        if applied_actions:
            await emit(
                project_id,
                "agent.task.actions_applied",
                {
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "applied_actions": applied_actions[:20],
                    "skipped_actions": skipped_actions[:10],
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
                "applied_actions": applied_actions[:20],
                "skipped_actions": skipped_actions[:10],
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
                pause_payload = {
                    "status": EXEC_STATUS_PAUSED,
                    "progress_pct": pause_pct,
                    "agent_id": aid,
                    "agent_name": agent_name,
                    "reason": summary[:900],
                    "resume_hint": (resume_hint or "Reply with required input, then say CONTINUE or press Resume.")[:300],
                }
                await emit(project_id, "project.execution.auto_paused", pause_payload)
                _append_project_activity(
                    project_id=project_id,
                    actor_type="project_agent",
                    actor_id=aid,
                    actor_label=agent_name,
                    event_type="project.execution.auto_paused",
                    summary="Execution auto-paused awaiting review/input",
                    payload=pause_payload,
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

    # ── Parallel group runner ─────────────────────────────────────────────────
    for grp_idx, grp in enumerate(parallel_groups):
        state, _ = _read_project_execution_state(project_id)
        if state == EXEC_STATUS_STOPPED:
            _refresh_project_documents(project_id)
            _append_project_daily_log(
                owner_user_id=str(row["user_id"]),
                project_root=str(row["project_root"] or ""),
                kind="delegation.stopped",
                text="Delegation run stopped by user.",
            )
            await emit(project_id, "project.delegation.stopped", {
                "processed_agents": processed_agents,
                "failed_agents": failed_agents,
                "total_agents": len(agent_order),
            })
            return
        await asyncio.gather(*[_run_one_agent(aid, grp_idx) for aid in grp])
        state_after_group, _ = _read_project_execution_state(project_id)
        if state_after_group in {EXEC_STATUS_PAUSED, EXEC_STATUS_STOPPED}:
            _refresh_project_documents(project_id)
            return

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
            pause_payload = {
                "status": EXEC_STATUS_PAUSED,
                "progress_pct": pause_pct,
                "agent_id": primary_agent_id,
                "agent_name": primary_agent_name,
                "reason": summary[:900],
                "resume_hint": hint[:300],
            }
            await emit(project_id, "project.execution.auto_paused", pause_payload)
            _append_project_activity(
                project_id=project_id,
                actor_type="project_agent",
                actor_id=primary_agent_id,
                actor_label=primary_agent_name,
                event_type="project.execution.auto_paused",
                summary="Execution auto-paused awaiting review/input",
                payload=pause_payload,
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
    quoted_project_id = url_quote(project_id, safe="")
    project_files_api_link = f"/api/projects/{project_id}/files"
    outputs_folder_api_link = f"/api/projects/{project_id}/files?path={url_quote(USER_OUTPUTS_DIRNAME, safe='')}"
    project_files_link = f"/?project={quoted_project_id}&project_pane=folder"
    outputs_folder_link = (
        f"/?project={quoted_project_id}&project_pane=folder&project_path={url_quote(USER_OUTPUTS_DIRNAME, safe='')}"
    )
    latest_output_rel = _latest_file_relative_path(outputs_dir, project_dir)
    latest_preview_api_link = (
        f"/api/projects/{project_id}/preview/{_encode_rel_path_for_url_path(latest_output_rel)}"
        if latest_output_rel
        else ""
    )
    latest_preview_link = ""
    if latest_output_rel:
        preview_rel = _clean_relative_project_path(latest_output_rel)
        preview_parent = USER_OUTPUTS_DIRNAME
        if preview_rel and "/" in preview_rel:
            preview_parent = preview_rel.rsplit("/", 1)[0]
        latest_preview_link = (
            f"/?project={quoted_project_id}"
            f"&project_pane=folder"
            f"&project_path={url_quote(preview_parent, safe='')}"
            f"&project_preview={url_quote(preview_rel, safe='')}"
        )
    owner_notice_parts = [
        f"@owner project `{str(row['title'] or project_id)}` is completed.",
        f"Open project files: {project_files_link}",
        f"Outputs folder: {outputs_folder_link}",
    ]
    if latest_preview_link:
        owner_notice_parts.append(f"Latest file preview: {latest_preview_link}")
    primary_done_update = " ".join(owner_notice_parts).strip()
    primary_done_payload = {
        "agent_id": primary_agent_id or "primary",
        "agent_name": primary_agent_name,
        "text": primary_done_update[:1200],
        "project_files_link": project_files_link,
        "outputs_folder_link": outputs_folder_link,
        "latest_preview_link": latest_preview_link,
        "project_files_api_link": project_files_api_link,
        "outputs_folder_api_link": outputs_folder_api_link,
        "latest_preview_api_link": latest_preview_api_link,
    }
    await emit(project_id, "agent.primary.update", primary_done_payload)
    _append_project_activity(
        project_id=project_id,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=primary_agent_name,
        event_type="agent.primary.update",
        summary="Primary agent submitted an update",
        payload=primary_done_payload,
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
            "project_files_api_link": project_files_api_link,
            "outputs_folder_api_link": outputs_folder_api_link,
            "latest_preview_api_link": latest_preview_api_link,
        },
    )
    ready_payload = {
        "agents": assigned_count,
        "processed_agents": processed_agents,
        "failed_agents": failed_agents,
        "notes": str(payload.get("notes") or "")[:1000],
        "project_files_link": project_files_link,
        "outputs_folder_link": outputs_folder_link,
        "latest_preview_link": latest_preview_link,
        "project_files_api_link": project_files_api_link,
        "outputs_folder_api_link": outputs_folder_api_link,
        "latest_preview_api_link": latest_preview_api_link,
        "owner_message": primary_done_update[:1200],
    }
    await emit(project_id, "project.delegation.ready", ready_payload)
    _append_project_activity(
        project_id=project_id,
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=primary_agent_name,
        event_type="project.delegation.ready",
        summary="Delegation ready",
        payload=ready_payload,
    )


async def _onboard_agents_into_project(
    project_id: str,
    added_agent_ids: List[str],
    all_agent_ids: List[str],
) -> None:
    """Called when agents are added/changed on an already-approved project.
    Primary agent reviews the roster + current progress and assigns tasks to new agents."""
    conn = db()
    row = conn.execute(
        """
        SELECT p.id, p.user_id, p.title, p.brief, p.goal, p.setup_json, p.project_root, p.connection_id,
               p.plan_text, p.plan_status, p.execution_status, p.progress_pct,
               p.backend_mode, p.connector_id,
               c.base_url, c.api_key, c.api_key_secret_id, cp.main_agent_id
        FROM projects p
        LEFT JOIN openclaw_connections c ON c.id = p.connection_id
        LEFT JOIN connection_policies cp ON cp.connection_id = p.connection_id AND cp.user_id = p.user_id
        WHERE p.id = ?
        """,
        (project_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    connection_api_key = _resolve_connection_api_key_from_row(conn, user_id=str(row["user_id"]), row=row)
    role_rows = _project_agent_rows(conn, project_id)
    conn.close()

    if not role_rows:
        return

    primary_agent_id = None
    for r in role_rows:
        if bool(r.get("is_primary")):
            primary_agent_id = str(r.get("agent_id") or "").strip() or None
            break
    if not primary_agent_id:
        primary_agent_id = str(row["main_agent_id"] or "").strip() or None
    if not primary_agent_id:
        return

    exec_status = str(row["execution_status"] or "idle")
    progress_pct = int(row["progress_pct"] or 0)
    hivee_api_base = _get_hivee_api_base(project_id)
    agent_token = _issue_agent_session_token(project_id, primary_agent_id)

    added_lines = "\n".join(
        f"- {aid}" + (
            " — " + str(next((r.get("role") or "" for r in role_rows if str(r.get("agent_id") or "") == aid), "")).strip()
            if next((r.get("role") or "" for r in role_rows if str(r.get("agent_id") or "") == aid), "").strip()
            else ""
        )
        for aid in added_agent_ids
    ) or "- (config update on existing agents)"

    task = (
        f"New or updated agents have been added to the project:\n{added_lines}\n\n"
        f"Current execution progress: {exec_status} at {progress_pct}%.\n\n"
        f"Your task:\n"
        f"1. Read plan.md and state.md to understand current progress.\n"
        f"2. Determine what work remains and what tasks fit the new agent(s).\n"
        f"3. Create task cards for them using create_task actions.\n"
        f"4. Notify each new agent via post_chat_message with @mention so they receive their assignment.\n"
        f"5. Update execution progress with update_execution if needed.\n"
        f"6. Post a summary to @owner via post_chat_message.\n"
    )
    instruction = _build_fundamentals_session_prompt(
        task=task,
        project_id=project_id,
        agent_id=primary_agent_id,
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )

    await emit(project_id, "project.agents.onboarding_started", {
        "added": added_agent_ids,
        "primary_agent_id": primary_agent_id,
        "progress_pct": progress_pct,
    })
    _append_project_daily_log(
        owner_user_id=str(row["user_id"]),
        project_root=str(row["project_root"] or ""),
        kind="agents.onboarding",
        text=f"Primary agent onboarding new agents: {', '.join(added_agent_ids) or 'config update'}",
    )

    res = await _project_chat(
        row,
        connection_api_key,
        instruction,
        agent_id=primary_agent_id,
        session_key=f"{project_id}:onboard",
        timeout_sec=None,
        user_id=str(row["user_id"] or ""),
        from_agent_id="hivee",
        from_label="Hivee System",
        context_type="delegation",
    )

    if not res.get("ok"):
        _append_project_daily_log(
            owner_user_id=str(row["user_id"]),
            project_root=str(row["project_root"] or ""),
            kind="agents.onboarding_failed",
            text=detail_to_text(res.get("error") or res.get("details"))[:1200],
        )
        await emit(project_id, "project.agents.onboarding_failed", {
            "error": detail_to_text(res.get("error") or res.get("details"))[:500],
        })
        return

    raw_text = str(res.get("text") or "").strip()
    parsed = _extract_agent_report_payload(raw_text)
    write_payload = parsed.get("output_files") or []
    action_payload = parsed.get("actions") or []

    owner_user_id = str(row["user_id"] or "")
    project_root = str(row["project_root"] or "")

    _apply_project_file_writes(
        owner_user_id=owner_user_id,
        project_root=project_root,
        writes=write_payload if isinstance(write_payload, list) else [],
        default_prefix=f"{USER_OUTPUTS_DIRNAME}/onboarding",
        allow_paths=["*"],
    )
    onboarding_action_result = _apply_project_actions(
        owner_user_id=owner_user_id,
        project_id=project_id,
        project_root=project_root,
        actions=action_payload if isinstance(action_payload, list) else [],
        allow_paths=["*"],
        actor_type="project_agent",
        actor_id=primary_agent_id,
        actor_label=f"agent:{primary_agent_id}",
    )
    await _emit_project_action_results(project_id, onboarding_action_result.get("applied") or [])

    _refresh_project_documents(project_id)
    _append_project_daily_log(
        owner_user_id=owner_user_id,
        project_root=project_root,
        kind="agents.onboarding_done",
        text=f"Primary agent completed onboarding for: {', '.join(added_agent_ids) or 'config update'}",
    )
    await emit(project_id, "project.agents.onboarding_done", {
        "added": added_agent_ids,
        "primary_agent_id": primary_agent_id,
    })


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

__all__ = [name for name in globals() if not name.startswith('__')]

