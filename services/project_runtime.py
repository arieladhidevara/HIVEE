from hivee_shared import *

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

__all__ = [name for name in globals() if not name.startswith('__')]
