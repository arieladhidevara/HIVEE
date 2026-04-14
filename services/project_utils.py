import base64

from core.workspace_paths import *
from core.security_auth import _new_agent_access_token, _hash_access_token

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

def _protocol_markdown() -> str:
    return r"""# Hivee Agent Protocol
> Hivee universal rules. Apply to EVERY project, EVERY session. Never override or ignore these rules.
> This file is READ-ONLY — system-managed. Do not write to it.

---

## 1. Mandatory Response Format

**Every single response MUST be valid JSON in this exact shape:**
```json
{
  "chat_update": "Short human-readable status. ALWAYS required.",
  "output_files": [],
  "actions": [],
  "requires_user_input": false,
  "pause_reason": "",
  "resume_hint": "",
  "notes": ""
}
```

### Field Rules
| Field | Type | Required | Description |
|---|---|---|---|
| `chat_update` | string | **YES, always** | What you did / status / handoff message |
| `output_files` | array | No | Files to write to project storage |
| `actions` | array | No | Explicit Hivee mutations (tasks, chat, files) |
| `requires_user_input` | boolean | No | Set true if you are blocked waiting for owner |
| `pause_reason` | string | When blocked | Clear description of what you need from owner |
| `resume_hint` | string | When blocked | What owner should say/do to unblock you |
| `notes` | string | No | Internal notes for your own record |

**If you cannot produce valid JSON, wrap your text in a fallback:**
```json
{"chat_update": "FALLBACK: <your message here>", "output_files": [], "actions": []}
```

---

## 2. Chat & Mention Rules (STRICTLY ENFORCED)

These rules exist so users and other agents can always follow project progress by reading the chat.

### Required mentions for each situation:
| Situation | Required chat_update content |
|---|---|
| Starting a task | `"Starting [task name]. @owner notified."` |
| Completing a task | `"Completed [task name]. Output at Outputs/[file]. Handing off to @next_agent_id."` |
| Blocked / waiting | `"Blocked on [reason]. @owner input needed. Pausing until resolved."` |
| Handoff ready | `"@target_agent_id your work is ready. Handoff doc: Outputs/handoffs/[your_id]→[target_id].md"` |
| Long task progress | Post intermediate `post_chat_message` actions at each major milestone |
| Raising issue | `"Issue: [description]. Tagging @owner and affected @agent_id."` |
| Decision made | `"Decision: [what]. Logged to decisions.md."` |

### Rules:
- **Every time you take an action → include an `@mention` in `chat_update`.**
- **Never go silent.** If you are working on something long, post a progress update every major step.
- **Always mention `@owner` when:** you are blocked, you have finished your phase, or a decision affects the project scope.
- Use **exact agent IDs** from `agents.md` in mentions (e.g., `@dev`, `@qa`, `@planner`).
- Use `@owner` to address the human project owner.
- Mentions in `chat_update` are **not** routed automatically — to actively ping an agent, use an `actions: post_chat_message` entry.

### Posting to chat via actions:
```json
{
  "type": "post_chat_message",
  "text": "@dev your subtask is ready. Handoff at Outputs/handoffs/planner→dev.md",
  "mentions": ["dev"]
}
```

---

## 3. Output Files — Writing Deliverables

Use `output_files` for any content you produce:
```json
{
  "path": "Outputs/{your_agent_id}/{filename}.md",
  "content": "...",
  "append": false
}
```
- Always write to paths under `Outputs/{your_agent_id}/` unless told otherwise in `scope.md`.
- For handoff files: `Outputs/handoffs/{your_id}→{target_id}.md`
- Append to decisions log: `{"path": "decisions.md", "content": "\n---\n...", "append": true}`

### CRITICAL: File content rules
- **`content` must be the FINISHED document** in its target format. For `.md` files: write proper Markdown. For `.py` files: write real Python code.
- **NEVER put JSON, a chat message, or a summary inside `content`.** The content field is what gets saved verbatim to disk.
- **WRONG:** `"content": "{\"chat_update\": \"I wrote the plan...\"}"`
- **RIGHT:** `"content": "# Project Plan\n\n## Milestone 1\n..."`
- `chat_update` is for **chat only** — a short human-readable status. Deliverable content belongs in `output_files[].content`.

---

## 4. Actions — Hivee Mutation API

Use `actions` array for structured Hivee mutations:

### File operations:
```json
{"type": "write_file",   "path": "Outputs/agent/file.md", "content": "..."}
{"type": "append_file",  "path": "decisions.md",          "content": "\n---\n..."}
{"type": "delete_file",  "path": "Outputs/agent/old.md"}
{"type": "move_file",    "src":  "Outputs/agent/a.md",    "dst": "Outputs/agent/b.md"}
```

### Task operations:
```json
{
  "type": "create_task",
  "title": "...",
  "description": "...",
  "assignee_agent_id": "agent_id",
  "status": "todo",
  "priority": "high",
  "weight_pct": 20,
  "instructions": "What the agent must accomplish",
  "input": "What data/files the agent receives to start",
  "process": "High-level steps the agent should follow",
  "output": "Files/artifacts the agent must produce",
  "from_agent": "delegating_agent_id",
  "handover_to": "next_agent_id_or_owner"
}
{"type": "update_task", "task_id": "task_xxx", "status": "done"}
{"type": "delete_task", "task_id": "task_xxx"}
```

**weight_pct rules:**
- Every task MUST have a `weight_pct` (integer, 0–100).
- All task weights for the project MUST sum to ~100.
- When a task status is set to `done`, its `weight_pct` is automatically added to the project progress bar.

### Chat:
```json
{"type": "post_chat_message", "text": "@dev handoff ready.", "mentions": ["dev"]}
```

### Execution control:
```json
{"type": "update_execution", "status": "running",  "progress_pct": 40, "summary": "Phase 2 started"}
{"type": "update_execution", "status": "completed", "progress_pct": 100, "summary": "All done"}
{"type": "update_execution", "status": "paused",    "progress_pct": 60, "summary": "Waiting for owner input"}
```

---

## 5. Sub-Plan Protocol (REQUIRED before execution)

When you receive a high-level task assignment, you MUST write a sub-plan before executing:

### Step 1 — Write your sub-plan
Include in `output_files` (path: `agents/{your_agent_id}-subplan.md`):
- Approach and methodology
- Step-by-step breakdown of sub-tasks you will create
- Timeline estimate per sub-task
- Expected deliverable files
- Risks, assumptions, dependencies on other agents

### Step 2 — Request primary agent approval via chat
```json
{
  "type": "post_chat_message",
  "text": "@primary_agent_id here is my sub-plan for [task title]. Read agents/{your_id}-subplan.md. Please approve or provide feedback.",
  "mentions": ["primary_agent_id"]
}
```
Set `requires_user_input: true` and `pause_reason: "Waiting for @primary_agent_id sub-plan approval."`.

### Step 3 — After approval
Once the primary agent approves via chat:
1. Create detailed sub-task cards via `create_task` actions (one per sub-task in your plan).
   - Each sub-task MUST include all structured fields: `instructions`, `input`, `process`, `output`,
     `from_agent`, `handover_to`, `weight_pct`. Weights across your sub-tasks should sum to your
     parent task's `weight_pct` allocation.
2. Update `update_execution` to reflect your actual progress.
3. Begin execution and report progress at each sub-task.
4. When a sub-task is done, place output files in your assigned `Outputs/` folder.
5. When ALL your tasks are done, copy final deliverables into `FINAL/` (top-level project folder).
   This is the handoff point to the owner — only polished, complete work goes in `FINAL/`.

### Primary agent: reviewing sub-plans
When an agent @mentions you with a sub-plan request:
- Review it against the overall project plan, budget, and timeline.
- Post your decision to chat: `@agent_id Sub-plan approved. Proceed.` OR `@agent_id Changes needed: [specific feedback].`
- Include `"approved": true` or `"approved": false` in your JSON response.

---

## 6. Handoff Protocol

Before triggering another agent to start work:
1. **Write a handoff file:**
   ```
   Outputs/handoffs/{your_agent_id}→{target_agent_id}.md
   ```
   Include: what you finished, list of output files, what the target must do next, any context they need.
2. **Post to chat:**
   ```
   "@target_agent_id handoff ready. Read Outputs/handoffs/{your_id}→{target_id}.md before starting."
   ```
3. **Create or update a task** if there is a tracked task card for this work.
4. The **receiving agent MUST read the handoff file** before starting. It contains the full context.

---

## 7. Blocked / Needs User Input

When you cannot continue without human input:
```json
{
  "chat_update": "Blocked on [reason]. @owner input needed. Pausing.",
  "requires_user_input": true,
  "pause_reason": "Need the API key for XYZ service before I can proceed.",
  "resume_hint": "Reply with the API key or add it to credentials.md."
}
```
Hivee will automatically create a BLOCKED task card visible to the owner in the UI.

---

## 8. File Access (Read via HTTP)

All project files are readable via HTTP using your agent credentials:
```
GET {hivee_api_base}/files/{file_path}
X-Project-Agent-Id: <your_hivee_agent_id>
X-Project-Agent-Token: <your_hivee_project_token>
```

### Key files to read at session start:
| File URL | Read when |
|---|---|
| `{hivee_api_base}/files/fundamentals.md` | **Every session, first thing** |
| `{hivee_api_base}/files/protocol.md` | First session or when in doubt |
| `{hivee_api_base}/files/scope.md` | First session — confirms your permissions |
| `{hivee_api_base}/files/context.md` | Every session — living project understanding |
| `{hivee_api_base}/files/plan.md` | When starting work — approved execution plan |
| `{hivee_api_base}/files/delegation.md` | When starting work — your assigned tasks |
| `{hivee_api_base}/files/agents.md` | When you need to address another agent |
| `{hivee_api_base}/files/state.md` | When checking overall project progress |
| `{hivee_api_base}/files/decisions.md` | When reviewing past decisions |
| `{hivee_api_base}/files/setup-chat.md` | First session — original project requirements |

---

## 9. Absolute Rules (Never Violate)

1. **Always return valid JSON.** No exceptions.
2. **Always populate `chat_update`.** Even a brief status is required.
3. **Never store final deliverables only on your local runtime.** Push everything via `output_files` or `actions: write_file`.
4. **Never assume missing information silently.** Ask via `requires_user_input` or state assumptions in `chat_update`.
5. **Never edit system-managed files:** `agents.md`, `state.md`, `fundamentals.md`, `protocol.md`, `scope.md`. Exception: primary agent MAY write `plan.md`, `delegation.md`, and `progress_map.json` when explicitly assigned to do so by Hivee.
6. **Never write outside your assigned paths** (defined in `scope.md`).
7. **Never skip `@mentions`** when handing off work or when blocked.
8. **Never hallucinate agent IDs.** Read `agents.md` and use exact IDs.
9. **Always place final deliverables in `FINAL/`.** When your work is done and ready for the owner, copy the polished output files to the `FINAL/` folder. Only complete, verified work belongs there.
10. **Always set `weight_pct` on every task you create.** All task weights in the project must sum to ~100. This drives the progress bar automatically when tasks are marked done.
""".strip() + "\n"


def _fundamentals_markdown(
    *,
    project_id: str,
    title: str,
    phase: str,
    plan_status: str,
    execution_status: str,
    hivee_api_base: str,
    role_rows: Optional[List[Dict[str, Any]]] = None,
) -> str:
    agents_lines = []
    if role_rows:
        for row in role_rows:
            aid = str(row.get("agent_id") or "").strip()
            name = str(row.get("agent_name") or aid).strip()
            role = str(row.get("role") or "").strip() or "Contributor"
            primary = bool(row.get("is_primary"))
            marker = " **(primary/orchestrator)**" if primary else ""
            agents_lines.append(f"| `@{aid}` | {name}{marker} | {role} |")
    agents_table = "\n".join(agents_lines) if agents_lines else "| *(none yet)* | — | — |"

    base = hivee_api_base.rstrip("/")
    return f"""# Project Fundamentals
> **READ THIS FIRST.** Auto-generated by Hivee. Do not edit manually — it is overwritten on project events.
> Every new session starts here. Read top-to-bottom before doing anything.

---

## Project Identity
| Field | Value |
|---|---|
| **Project** | {title} |
| **Project ID** | `{project_id}` |
| **Current Phase** | `{phase}` |
| **Plan Status** | `{plan_status}` |
| **Execution Status** | `{execution_status}` |
| **API Base** | `{base}` |

---

## Your Session Context
You receive these in your prompt:
1. **hivee_agent_id** — your agent ID for this project
2. **hivee_project_token** — a short-lived token for this session only
3. **This file URL** — `{base}/files/fundamentals.md`
4. **Your task** — what you must do this session

**Always authenticate ALL Hivee API requests with both headers:**
```
X-Project-Agent-Id: <your_hivee_agent_id>
X-Project-Agent-Token: <your_hivee_project_token>
```
Do NOT use `Authorization: Bearer` — it will return 401.

---

## Mandatory First Steps (Every Session)

**Do NOT skip any of these:**

1. **Read `protocol.md`** — universal rules you MUST follow
   `GET {base}/files/protocol.md`

2. **Read `scope.md`** — your exact permissions for this project
   `GET {base}/files/scope.md`

3. **Read `context.md`** — living project understanding (requirements, constraints, decisions)
   `GET {base}/files/context.md`

4. **Read `delegation.md`** — your assigned tasks and handoff conditions
   `GET {base}/files/delegation.md`

5. **Read `agents.md`** — all agents, their @mention IDs, and their roles
   `GET {base}/files/agents.md`

6. **Check `state.md`** — current project progress and any open blockers
   `GET {base}/files/state.md`

---

## Complete File Index

| File | URL | Purpose | Editable by agents? |
|---|---|---|---|
| `protocol.md` | `{base}/files/protocol.md` | Universal rules, response format, mention/handoff protocol | No — system-managed |
| `scope.md` | `{base}/files/scope.md` | Your permissions: writable paths, allowed actions | No — system-managed |
| `fundamentals.md` | `{base}/files/fundamentals.md` | **This file** — entry point, project snapshot | No — system-managed |
| `context.md` | `{base}/files/context.md` | Full project understanding, requirements, constraints | Yes — update via `write_file` action |
| `plan.md` | `{base}/files/plan.md` | Approved execution plan (created by primary agent, approved by owner) | No — owner approves, system-managed |
| `delegation.md` | `{base}/files/delegation.md` | Task assignments per agent with handoff triggers | No — system-managed |
| `agents.md` | `{base}/files/agents.md` | Agent roster: IDs, roles, session key patterns | No — system-managed |
| `state.md` | `{base}/files/state.md` | Live project status, progress %, active blockers | No — system-managed |
| `credentials.md` | `{base}/files/credentials.md` | Pointers to external credentials (values NOT stored here) | No — owner-managed |
| `setup-chat.md` | `{base}/files/setup-chat.md` | Original setup conversation (immutable reference) | No — immutable |
| `decisions.md` | `{base}/files/decisions.md` | Append-only decisions log | Yes — append only via `append_file` |
| `Project Info/` | `{base}/files/Project Info/` | Legacy project info directory | Read-only for agents |
| `Outputs/` | `{base}/files/Outputs/` | All agent work outputs | Yes — write under `Outputs/<your_agent_id>/` |
| `Outputs/handoffs/` | `{base}/files/Outputs/handoffs/` | Handoff files between agents | Yes — write `from→to.md` files |
| `reference/` | `{base}/files/reference/` | User-uploaded reference documents | Read-only for agents |

---

## Agents in This Project

| @mention | Name | Role |
|---|---|---|
{agents_table}

Full details (session keys, connector info): `GET {base}/files/agents.md`

---

## Quick API Reference

### Read a file
```
GET {base}/files/<path>
X-Project-Agent-Id: <your_hivee_agent_id>
X-Project-Agent-Token: <your_hivee_project_token>
```

### Write/mutate (via agent-ops)
```
POST {base}/agent-ops
X-Project-Agent-Id: <your_hivee_agent_id>
X-Project-Agent-Token: <your_hivee_project_token>
Content-Type: application/json

{{"ops": [
  {{"type": "write_file", "path": "Outputs/<agent_id>/output.md", "content": "..."}},
  {{"type": "post_chat_message", "text": "@owner work complete.", "mentions": ["owner"]}},
  {{"type": "create_task", "title": "...", "status": "todo", "priority": "medium"}}
]}}
```

### All valid action types
`write_file` · `append_file` · `delete_file` · `move_file` · `create_dir`
`post_chat_message` · `create_task` · `update_task` · `delete_task` · `update_execution`

---

## What To Do If You're Unsure

1. Re-read `protocol.md` — the answer is usually there
2. Re-read `context.md` — check requirements and constraints
3. Post to chat with `@owner` using `requires_user_input: true` — never guess silently
""".strip() + "\n"


def _state_markdown(
    *,
    phase: str,
    plan_status: str,
    execution_status: str,
    progress_pct: int = 0,
    agents: Optional[List[Dict[str, Any]]] = None,
    pending_inputs: Optional[List[str]] = None,
    updated_at: Optional[int] = None,
    hivee_api_base: str = "",
) -> str:
    import time as _time
    ts = updated_at or int(_time.time())
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    base = (hivee_api_base or "").rstrip("/")

    agent_rows = []
    if agents:
        for a in agents:
            aid = str(a.get("agent_id") or "").strip()
            name = str(a.get("agent_name") or aid).strip()
            primary = bool(a.get("is_primary"))
            role = str(a.get("role") or "").strip() or "Contributor"
            agent_rows.append(f"| `@{aid}` | {name} | {role} | {'Yes' if primary else 'No'} |")
    agent_table = "\n".join(agent_rows) if agent_rows else "| *(none yet)* | — | — | — |"

    inputs_list = "\n".join(f"- {i}" for i in (pending_inputs or [])) or "- None."

    # Phase descriptions
    phase_desc = {
        "setup": "Owner is answering setup questions. No agents active yet.",
        "planning": "Primary agent is generating the project plan.",
        "execution": "Agents are executing delegated tasks.",
        "completed": "All tasks completed. Project finished.",
        "paused": "Project paused — waiting for owner input.",
    }.get(str(phase or "").lower(), f"Phase: {phase}")

    exec_desc = {
        "idle": "No tasks running.",
        "running": "Agents are actively working.",
        "paused": "Execution paused — blocked on user input.",
        "stopped": "Execution stopped by owner.",
        "completed": "All tasks done.",
    }.get(str(execution_status or "").lower(), execution_status)

    return f"""# Project State
> Maintained by Hivee. Do not edit manually.
> Last updated: **{dt}**

---

## Current Status

| Field | Value |
|---|---|
| **Phase** | `{phase}` — {phase_desc} |
| **Plan** | `{plan_status}` |
| **Execution** | `{execution_status}` — {exec_desc} |
| **Overall Progress** | **{progress_pct}%** |

---

## Active Agents

| @mention | Name | Role | Primary? |
|---|---|---|---|
{agent_table}

Full agent details (session keys, how to address): `GET {base}/files/agents.md`

---

## Pending Owner Input

{inputs_list}

If you are blocked and need owner input, set `requires_user_input: true` in your response.
Hivee will create a BLOCKED task card visible in the owner's UI.

---

## Related Files

| What | URL |
|---|---|
| Execution plan | `GET {base}/files/plan.md` |
| Task assignments | `GET {base}/files/delegation.md` |
| Agent roster | `GET {base}/files/agents.md` |
| Project context | `GET {base}/files/context.md` |
| Decisions log | `GET {base}/files/decisions.md` |
""".strip() + "\n"


def _agents_markdown(
    role_rows: Optional[List[Dict[str, Any]]] = None,
    *,
    project_id: str = "",
    hivee_api_base: str = "",
) -> str:
    base = (hivee_api_base or "").rstrip("/")
    lines = [
        "# Agent Roster",
        "> Maintained by Hivee. Do not edit manually — overwritten when agents join or leave.",
        "> Use exact `@agent_id` values from this file in mentions. Never guess agent IDs.",
        "",
    ]
    if not role_rows:
        lines += [
            "No agents invited yet.",
            "",
            "## How Agents Join",
            f"The project owner sends an invitation from the Hivee UI. Once accepted, this file is updated.",
        ]
        return "\n".join(lines).strip() + "\n"

    # Summary table
    lines += ["## Quick Reference", "", "| @mention | Name | Role | Primary? |", "|---|---|---|---|"]
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        name = str(row.get("agent_name") or aid).strip()
        role = str(row.get("role") or "").strip() or "Contributor"
        primary = bool(row.get("is_primary"))
        lines.append(f"| `@{aid}` | {name} | {role} | {'Yes' if primary else 'No'} |")
    lines.append("")

    # Detailed section per agent
    lines.append("---")
    lines.append("")
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        name = str(row.get("agent_name") or aid).strip()
        role = str(row.get("role") or "").strip() or "Contributor"
        primary = bool(row.get("is_primary"))
        source = str(row.get("source_type") or "").strip()
        lines += [
            f"## {name}{' (Primary / Orchestrator)' if primary else ''}",
            f"- **Agent ID (use in @mentions):** `@{aid}`",
            f"- **Role:** {role}",
            f"- **Primary agent:** {'Yes — orchestrates the project and delegates to other agents' if primary else 'No — executes delegated tasks'}",
            f"- **Source:** {source or 'invited'}",
        ]
        if base:
            lines += [
                f"- **Session key pattern:** `{project_id}:<phase>` (e.g., `{project_id}:execution`)",
                f"- **Scope file:** `GET {base}/files/scope.md` (each agent receives their own scoped version)",
            ]
        lines += [
            "",
            f"### How to address `@{aid}`",
            f"In `chat_update` or `post_chat_message` action:",
            f'```',
            f'"@{aid} [your message here]"',
            f'```',
            f"In an action entry:",
            f'```json',
            f'{{"type": "post_chat_message", "text": "@{aid} your task is ready.", "mentions": ["{aid}"]}}',
            f'```',
            "",
        ]
    lines += [
        "---",
        "",
        "## Special Mentions",
        "| @mention | Who it reaches |",
        "|---|---|",
        "| `@owner` | The human project owner (shown in UI, not routed to connector) |",
        "| `@all` | All agents (broadcast — each connector receives the message) |",
        "",
        "## Mention Routing",
        "When you include `@agent_id` in a `post_chat_message` action, Hivee automatically:",
        "1. Records the mention in the project chat DB",
        "2. Emits a realtime UI event",
        "3. **Forwards the message to that agent's connector** so the agent receives it as a new task/message",
        "",
        "This means `post_chat_message` with `@mentions` is the correct way to trigger another agent.",
    ]
    return "\n".join(lines).strip() + "\n"


def _scope_markdown(
    *,
    agent_id: str,
    agent_name: str,
    is_primary: bool,
    can_write_files: bool = True,
    write_paths: Optional[List[str]] = None,
    can_post_chat: bool = True,
    can_create_tasks: bool = False,
    can_update_tasks: bool = True,
    hivee_api_base: str = "",
    project_id: str = "",
) -> str:
    base = (hivee_api_base or "").rstrip("/")
    writable = write_paths or [f"Outputs/{agent_id}/", "Outputs/handoffs/"]
    writable_rows = "\n".join(f"| `{p}` | Write allowed |" for p in writable)
    create_tasks_val = "Yes — primary agent can create and assign tasks to any agent" if is_primary else ("Yes" if can_create_tasks else "No — request primary agent to create tasks")
    role_desc = "Primary agent (orchestrator) — you generate the plan, delegate tasks, and coordinate all other agents" if is_primary else "Contributor — you execute tasks delegated to you by the primary agent"

    return f"""# Scope — {agent_name}
> Your personal permission file. Do not edit manually — system-managed.
> **This file is scoped to you (`@{agent_id}`).** Other agents have different scope files.

---

## Your Identity

| Field | Value |
|---|---|
| **Agent ID** | `{agent_id}` |
| **Name** | {agent_name} |
| **Role** | {role_desc} |
| **Project ID** | `{project_id}` |

---

## What You Are Allowed To Do

### File Access
| Action | Allowed? |
|---|---|
| Read any project file | **Yes** — `GET {base}/files/<path>` |
| Write files | {"Yes — see paths below" if can_write_files else "No"} |
| Append to `decisions.md` | **Yes** — use `append_file` action only |

### Writable Paths
| Path | Permission |
|---|---|
{writable_rows}

### Project Mutations
| Action | Allowed? |
|---|---|
| `post_chat_message` (with @mentions) | {"Yes" if can_post_chat else "No"} |
| `create_task` | {create_tasks_val} |
| `update_task` (your assigned tasks) | {"Yes" if can_update_tasks else "No"} |
| `delete_task` | {"Yes — primary agent only" if is_primary else "No"} |
| `update_execution` | {"Yes — primary agent only" if is_primary else "No — report progress via chat instead"} |

---

## What You Are NOT Allowed To Do

- Write to paths not listed above
- Edit system-managed files: `plan.md`, `delegation.md`, `agents.md`, `state.md`, `fundamentals.md`, `protocol.md`, `scope.md`
- {"" if is_primary else "Assign or delegate tasks to other agents (primary agent only)"}
- Approve the project plan (owner only)
- Modify `credentials.md` (owner only)

---

## Your Session Key Pattern

Your sessions use the key: `{project_id}:<phase>`

Examples:
- `{project_id}:planning` — planning phase session
- `{project_id}:execution` — execution phase session
- `{project_id}:mention` — when you receive a @mention from another agent

Hivee may send you messages with `contextType` values:
| contextType | Meaning |
|---|---|
| `plan_generation` | You are asked to create the project plan |
| `delegation` | You are asked to assign tasks to agents |
| `task_execution` | You are asked to execute a specific task |
| `mention` | Another agent @mentioned you in chat |
| `control` | Hivee system message (e.g., setup, approval) |
| `message` | General message |

---

## Quick Action Reference

```json
// Write a file
{{"type": "write_file", "path": "Outputs/{agent_id}/report.md", "content": "..."}}

// Append to decisions log
{{"type": "append_file", "path": "decisions.md", "content": "\\n---\\n**[DATE] Decision by @{agent_id}**\\nWhat: ...\\nWhy: ...\\n"}}

// Post to chat with mention
{{"type": "post_chat_message", "text": "@owner work complete. Output at Outputs/{agent_id}/report.md", "mentions": ["owner"]}}

// Create a task ({"primary only" if is_primary else "primary agent only"})
// All tasks MUST have weight_pct. All task weights in the project must sum to ~100.
{{"type": "create_task", "title": "...", "description": "...", "status": "todo", "priority": "high",
  "assignee_agent_id": "target_agent_id", "weight_pct": 20,
  "instructions": "What must be done", "input": "What is provided", "process": "How to do it",
  "output": "What files/artifacts to produce", "from_agent": "{agent_id}", "handover_to": "next_agent_id"}}

// Update execution status ({"primary only" if is_primary else "primary agent only"})
{{"type": "update_execution", "status": "running", "progress_pct": 50, "summary": "Phase 2 in progress"}}
```

---

## When in Doubt

1. Re-read `GET {base}/files/protocol.md`
2. Re-read `GET {base}/files/context.md`
3. Post to chat: `{{"type": "post_chat_message", "text": "@owner I need clarification on X", "mentions": ["owner"]}}`
   and set `requires_user_input: true` in your response.
""".strip() + "\n"


def _credentials_markdown(*, project_id: str, hivee_api_base: str) -> str:
    base = (hivee_api_base or "").rstrip("/")
    return f"""# Credentials
> Managed by project owner. Do not edit manually.
> **Secret values are NEVER stored in this file.** This file only lists what credentials exist and how to fetch them.

---

## How to Fetch a Secret at Runtime

```
GET {base}/secrets/<secret_name>
X-Project-Agent-Id: <your_hivee_agent_id>
X-Project-Agent-Token: <your_hivee_project_token>
```

The response will contain the decrypted value. **Never log or include secret values in `chat_update` or file outputs.**

---

## Available Credentials

*(No credentials configured yet.)*

If you need a credential that is not listed here:
1. Set `requires_user_input: true` in your response
2. In `pause_reason`, specify: `"Need credential: <name> — <what it's for>"`
3. Mention `@owner` in `chat_update`

The owner will add it via the Hivee UI, and this file will be updated.

---

## Credential Entry Format (owner-managed)

When the owner adds a credential, it appears here as:

```
### <credential_name>
- **Purpose:** <what it is used for>
- **Fetch URL:** GET {base}/secrets/<credential_name>
- **Used by:** @<agent_id>
- **Added:** <date>
```

---

## Security Rules for Agents

1. **Never write secret values into any project file.**
2. **Never include secrets in `chat_update`.**
3. Fetch secrets only when needed, use them, do not cache beyond the session.
4. If a secret fetch fails (401/404), set `requires_user_input: true` and notify `@owner`.

---

## Related

- Agent permissions: `GET {base}/files/scope.md`
- Project context (where secrets are used): `GET {base}/files/context.md`
""".strip() + "\n"


def _decisions_markdown() -> str:
    return """# Decisions Log
> **Append-only.** Never rewrite or delete existing entries.
> Use `actions: append_file` to add new entries. This is the authoritative record of all key decisions.

---

## How to Add an Entry

In your `actions` array:
```json
{
  "type": "append_file",
  "path": "decisions.md",
  "content": "\\n---\\n**[YYYY-MM-DD] Decision by @your_agent_id**\\nWhat was decided: ...\\nWhy: ...\\nImpact on project: ...\\nAlternatives considered: ...\\n"
}
```

Also mention the decision in `chat_update`:
```
"Decision made: [brief summary]. Logged to decisions.md. @owner informed."
```

---

## Entry Template

```
---
**[YYYY-MM-DD HH:MM UTC] Decision by @agent_id**
What was decided: <clear statement of the decision>
Why: <rationale — what drove this choice>
Impact on project: <what changes as a result>
Alternatives considered: <other options that were rejected and why>
Owner approved: <yes / no / pending>
---
```

---

## Log

*(No decisions recorded yet.)*

---

## Types of Decisions to Log

- Architecture or technology choices
- Scope changes (in-scope vs out-of-scope)
- Agent role changes or re-delegation
- Timeline or priority changes
- Any decision that would be confusing without context later
""".strip() + "\n"


def _context_seed_markdown(*, title: str, brief: str, goal: str, hivee_api_base: str = "") -> str:
    base = (hivee_api_base or "").rstrip("/")
    return f"""# Project Context
> **Written and maintained by the primary agent.** This is a living document.
> Update this file whenever significant new understanding, decisions, or constraints emerge.
> Use `actions: write_file` with path `context.md` to update.
> Read this file at the start of every session: `GET {base}/files/context.md`

**Status:** seed — pending primary agent enrichment after reading `setup-chat.md`

---

## Project Overview

**Title:** {title or "—"}

**Brief:**
{brief.strip() or "*(to be filled by primary agent after reading setup-chat.md)*"}

**Goal:**
{goal.strip() or "*(to be filled by primary agent after reading setup-chat.md)*"}

---

## Requirements

*(Primary agent: read `setup-chat.md` and extract all functional and non-functional requirements here)*

### Functional Requirements
- (what the project must do)

### Non-Functional Requirements
- Performance:
- Security:
- Scalability:
- Compatibility:

---

## Constraints

| Type | Constraint |
|---|---|
| Timeline | *(extract from setup-chat.md)* |
| Budget | *(extract from setup-chat.md)* |
| Technology | *(extract from setup-chat.md)* |
| Compliance / Legal | *(extract from setup-chat.md)* |

---

## Scope

**In Scope:**
- *(extract from setup-chat.md)*

**Out of Scope:**
- *(extract from setup-chat.md)*

---

## Assumptions

*(List every assumption made where the user did not provide explicit information)*
- *(assumption 1)*

---

## Open Questions

*(List unresolved questions that need owner input)*
- *(question 1 — mention `@owner` in chat to get answer)*

---

## Team Responsibilities

*(Primary agent: read agents.md and summarize who owns what area of the project)*

| Agent | Responsibility |
|---|---|
| *(read agents.md)* | *(summarize role)* |

---

## Key Decisions

See `decisions.md` for the full log. Summary of major decisions:
- *(add key decision summaries here as they are made)*

---

## Technical Context

*(Architecture, stack, tools, integrations — fill in after reading setup-chat.md)*

---

## How to Update This File

In your `actions` array:
```json
{{"type": "write_file", "path": "context.md", "content": "<full updated content>"}}
```

After updating, post to chat:
```
"Updated context.md with [what changed]. @owner please review if significant scope changes."
```
""".strip() + "\n"


def _setup_chat_markdown(*, transcript_text: str) -> str:
    return f"""# Setup Chat History
> **Immutable.** This is the verbatim original conversation between the owner and the setup agent.
> This file is the authoritative source of the project's original requirements and intent.
> Do not edit. Do not delete. Reference it to understand what the owner originally asked for.

---

## How to Use This File

- **Primary agent:** Read this file first when generating `context.md` and `plan.md`.
  Extract: goals, requirements, constraints, scope, timeline, stack, and any open questions.
- **All agents:** Reference this file when there is ambiguity about the project intent.
- If information here conflicts with a later decision in `decisions.md`, the newer decision takes precedence.

---

## Original Setup Conversation

{transcript_text.strip() or "*(No setup chat history was captured for this project.)*"}
""".strip() + "\n"


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
    lines.extend(["## Project Protocol", f"- See `{PROJECT_PROTOCOL_FILE}` for delegation, mentions, task/issue status updates, and UI sync rules.", ""])
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

def _project_protocol_markdown(*, title: str, brief: str, goal: str) -> str:
    lines = [
        "# Project Protocol",
        "",
        f"Project: {title}",
        "",
        "## Purpose",
        "- Define how agents delegate, communicate, and keep project state synced for UI.",
        "",
        "## Communication",
        "- Use exact invited `agent_id` in mentions: `@agent_id`.",
        "- Every dependency handoff must include `Handoff: @agent_id` in chat_update.",
        "- If blocked by owner approval/input, mention `@owner` and set requires_user_input=true.",
        "",
        "## Delegation Flow",
        "1. Primary agent creates delegation plan and per-agent tasks.",
        "2. Each agent executes scope, writes artifacts via output_files, and uses structured actions for file/task mutations when needed.",
        "3. Next agent continues only after explicit handoff mention.",
        "",
        "## UI Sync Contract (API Source Of Truth)",
        "- Task cards: project task APIs and DB task state (`todo`, `in_progress`, `blocked`, `review`, `done`).",
        "- Issue cards: backend issue/approval signals (failed execution, blocked tasks, pending approvals).",
        "- Progress/status maps: project execution status + progress percentage + task dependency/state data.",
        "- Activity feed/live updates: project activity log and project event stream.",
        "",
        "## Agent Output Contract",
        "- Return JSON with `chat_update`, `output_files`, optional `actions`, optional `notes`, and pause fields.",
        "- Persist every deliverable/handoff artifact through `output_files` using project-relative paths.",
        "- Use `actions` for explicit project mutations like editing real project files, deleting files, moving files, posting team chat messages, or updating task/progress state.",
        "- Do not mark work done if artifacts only exist outside Hivee project files.",
        "",
        "## Hivee Direct Action Surface",
        "- Treat JSON `actions` as the direct Hivee mutation API for this project.",
        "- Storage ops: `write_file`, `append_file`, `upload_file`, `delete_file`, `move_file`, `create_dir`, `delete_dir`.",
        "- Team sync ops: `post_chat_message` with exact `@agent_id` mentions for delegation/handoff.",
        "- Graph/state ops: `create_task`, `update_task`, `delete_task`, dependency ops, blueprint ops, and `update_execution`.",
        "- Successful actions update project DB state and emit realtime UI events automatically.",
        "",
        "## Task And Issue Update Expectations",
        "- Keep chat_update explicit about current status and blockers for downstream UI clarity.",
        "- If blocker/risk appears, state blocker owner and next required action.",
        "",
        "## Writable Roots",
        f"- `{USER_OUTPUTS_DIRNAME}`, `{PROJECT_INFO_DIRNAME}`, `agents`, `logs`.",
        "",
        "## Core Context",
        f"- Brief: {brief.strip() or '-'}",
        f"- Goal: {goal.strip() or '-'}",
        "",
    ]
    return "\n".join(lines).strip() + "\n"


def _artifact_sync_rule_lines(*, project_root: Optional[str] = None) -> List[str]:
    lines = [
        "Artifact sync policy:",
        "- Persist every deliverable and handoff artifact into Hivee project files via `output_files`.",
        "- Use structured `actions` when you must mutate real project files, team chat state, or task/progress state in place.",
        "- Use project-relative paths only (no absolute machine/server paths).",
        "- Do not mark work as done if files exist only on provider/local agent server.",
        "- If external tools/runtime produce artifacts, copy full final content back into `output_files`.",
        f"- Preferred artifact-sync roots: `{USER_OUTPUTS_DIRNAME}`, `{PROJECT_INFO_DIRNAME}`, `agents`, `logs`.",
    ]
    if project_root:
        lines.append(f"- Hivee source-of-truth project root: `{project_root}`.")
    return lines

def _project_context_instruction(
    *,
    title: str,
    brief: str,
    goal: str,
    setup_details: Optional[Dict[str, Any]] = None,
    role_rows: Optional[List[Dict[str, Any]]] = None,
    project_root: Optional[str] = None,
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
    sections.append(f"- Read and follow `{PROJECT_PROTOCOL_FILE}` before planning/delegation/execution updates.")
    if plan_status != PLAN_STATUS_APPROVED:
        sections.append("- Project plan is not approved yet. Only planning/discussion is allowed; do not execute tasks.")
        sections.append(f"- Before planning, read `{PROJECT_INFO_FILE}` and align with setup chat history.")
    else:
        sections.append("- Project plan is approved. Execute within assigned scope and update progress in chat.")
        sections.append("- If execution is blocked by missing user info/approval, pause and ask the owner clearly.")
        sections.append("- For pause points, return JSON with `requires_user_input=true`, `pause_reason`, and optional `resume_hint`.")
        sections.append("- If owner explicitly says SKIP for missing info, make reasonable assumptions and continue execution.")
    sections.extend(_artifact_sync_rule_lines(project_root=project_root))
    sections.append("- When handing off dependencies, mention the related invited agent explicitly using @agent_id.")
    sections.append(
        "- If you generate or modify files, include them in JSON field `output_files` as "
        "[{\"path\":\"...\",\"content\":\"...\",\"append\":false}] and include a human sentence in `chat_update`."
    )
    sections.append(
        "- For explicit project mutations, you may also return JSON `actions` "
        "such as `write_file`, `append_file`, `upload_file`, `delete_file`, `move_file`, `create_dir`, `delete_dir`, "
        "`create_task`, `update_task`, `delete_task`, `add_task_dependency`, `remove_task_dependency`, "
        "`apply_task_blueprint`, `update_execution`, and `post_chat_message`."
    )
    sections.append("- Treat `actions` as Hivee's direct project API: successful file/chat/task/execution actions sync storage, group chat, graph state, and realtime UI automatically.")
    sections.append("- Use `output_files` for synced deliverables, and use `actions` when you must modify real project files, group chat state, or task/progress state.")
    sections.append("- Keep continuity with previous conversation turns in this same project session.")
    return "\n".join(sections)

def _build_project_task_snapshot(project_id: str, *, max_tasks: int = 24) -> str:
    pid = str(project_id or "").strip()
    if not pid:
        return ""
    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT
                t.id,
                t.title,
                t.status,
                t.priority,
                t.assignee_agent_id,
                t.updated_at,
                COUNT(d.depends_on_task_id) AS dep_total,
                SUM(CASE WHEN COALESCE(td.status, '') != ? THEN 1 ELSE 0 END) AS dep_open
            FROM project_tasks t
            LEFT JOIN project_task_dependencies d
                ON d.project_id = t.project_id AND d.task_id = t.id
            LEFT JOIN project_tasks td
                ON td.project_id = d.project_id AND td.id = d.depends_on_task_id
            WHERE t.project_id = ?
            GROUP BY t.id, t.title, t.status, t.priority, t.assignee_agent_id, t.updated_at
            ORDER BY t.updated_at DESC, t.created_at DESC
            LIMIT ?
            """,
            (TASK_STATUS_DONE, pid, max(1, min(int(max_tasks), 60))),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return "Current task map:\n- no tasks yet"
    lines = [
        "Current task map (use exact `task_id` or exact task title in `actions` when mutating tasks):",
    ]
    for row in rows:
        dep_open = max(0, _to_int(row["dep_open"]))
        dep_total = max(0, _to_int(row["dep_total"]))
        dep_part = f" | deps_open={dep_open}/{dep_total}" if dep_total else ""
        assignee = str(row["assignee_agent_id"] or "").strip()
        assignee_part = f" | assignee={assignee}" if assignee else ""
        lines.append(
            f"- [{str(row['id'] or '').strip()}] {str(row['title'] or '').strip() or 'Task'}"
            f" | status={str(row['status'] or '').strip() or TASK_STATUS_TODO}"
            f" | priority={str(row['priority'] or '').strip() or TASK_PRIORITY_MEDIUM}"
            f"{assignee_part}{dep_part}"
        )
    return "\n".join(lines)


def _normalize_project_chat_mentions(raw_mentions: Any, *, fallback_text: str = "") -> List[str]:
    items: List[str] = []
    if isinstance(raw_mentions, list):
        items = [str(item or "").strip() for item in raw_mentions]
    elif isinstance(raw_mentions, tuple):
        items = [str(item or "").strip() for item in raw_mentions]
    elif isinstance(raw_mentions, str):
        items = [raw_mentions.strip()]

    extracted = re.findall(r"@([a-zA-Z0-9._-]+)", str(fallback_text or ""))
    items.extend([str(item or "").strip() for item in extracted])

    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        clean = str(item or "").strip().lstrip("@")
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean[:120])
        if len(out) >= PROJECT_CHAT_MENTION_MAX:
            break
    return out


def _parse_json_dict(raw_value: Any) -> Dict[str, Any]:
    if isinstance(raw_value, dict):
        return dict(raw_value)
    text = str(raw_value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _project_chat_message_payload_from_row(
    row: sqlite3.Row,
    *,
    mentions: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "id": str(row["id"] or ""),
        "project_id": str(row["project_id"] or ""),
        "author_type": str(row["author_type"] or ""),
        "author_id": str(row["author_id"] or "").strip() or None,
        "author_label": str(row["author_label"] or "").strip() or None,
        "text": str(row["text"] or ""),
        "mentions": _normalize_project_chat_mentions(mentions or []),
        "metadata": _parse_json_dict(row["metadata_json"]),
        "created_at": _to_int(row["created_at"]),
    }


def _create_project_chat_message(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    author_type: str,
    author_id: Optional[str],
    author_label: Optional[str],
    text: str,
    metadata: Optional[Dict[str, Any]] = None,
    mentions: Optional[List[str]] = None,
    created_at: Optional[int] = None,
) -> Dict[str, Any]:
    body = str(text or "").strip()
    if not body:
        raise HTTPException(400, "text is required")
    ts = int(time.time()) if created_at is None else int(created_at)
    message_id = new_id("pcm")
    normalized_mentions = _normalize_project_chat_mentions(mentions, fallback_text=body)
    payload = metadata if isinstance(metadata, dict) else {}
    conn.execute(
        """
        INSERT INTO project_chat_messages (
            id, project_id, author_type, author_id, author_label, text, metadata_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            message_id,
            str(project_id or "").strip(),
            str(author_type or "").strip() or "unknown",
            str(author_id or "").strip() or None,
            str(author_label or "").strip() or None,
            body[:PROJECT_CHAT_MESSAGE_MAX_CHARS],
            json.dumps(payload, ensure_ascii=False),
            ts,
        ),
    )
    for mention in normalized_mentions:
        conn.execute(
            """
            INSERT INTO project_chat_mentions (
                id, project_id, message_id, mention_target, created_at
            ) VALUES (?,?,?,?,?)
            """,
            (
                new_id("pcn"),
                str(project_id or "").strip(),
                message_id,
                mention[:120],
                ts,
            ),
        )
    row = conn.execute(
        """
        SELECT id, project_id, author_type, author_id, author_label, text, metadata_json, created_at
        FROM project_chat_messages
        WHERE id = ?
        LIMIT 1
        """,
        (message_id,),
    ).fetchone()
    if not row:
        raise HTTPException(500, "Failed to create project chat message")
    _append_project_activity_log_entry(
        conn,
        project_id=project_id,
        actor_type=str(author_type or "").strip() or "unknown",
        actor_id=str(author_id or "").strip() or None,
        actor_label=str(author_label or "").strip() or None,
        event_type="chat.message",
        summary=body[:220],
        payload={"message_id": message_id, "mentions": normalized_mentions[:PROJECT_CHAT_MENTION_MAX]},
        created_at=ts,
    )
    return _project_chat_message_payload_from_row(row, mentions=normalized_mentions)


async def _dispatch_chat_mention_to_connector(
    project_id: str,
    mention_target: str,
    message_text: str,
    *,
    from_agent_id: str = "hivee",
    from_label: str = "Hivee",
    hivee_api_base: str = "",
) -> None:
    """Forward a @mention to the mentioned agent's connector (fire-and-forget).
    Skips 'owner' mentions (those are for humans, not agent connectors).
    """
    import asyncio
    from services.connector_dispatch import get_project_connector, connector_chat_sync

    target = str(mention_target or "").strip().lstrip("@")
    if not target or target in ("owner", "user"):
        return  # Human-directed mentions don't go to connectors

    connector = get_project_connector(project_id)
    if not connector:
        return  # No connector configured for this project

    connector_id = str(connector.get("id") or "").strip()
    if not connector_id:
        return

    # Build a notification message to deliver to the mentioned agent
    notification = (
        f"[MENTION from @{from_agent_id}]\n\n"
        f"{message_text}\n\n"
        f"---\n"
        f"This message was addressed to you (@{target}) in the project chat. "
        f"Respond via `post_chat_message` action if action is needed."
    )

    try:
        await connector_chat_sync(
            connector_id=connector_id,
            message=notification,
            agent_id=target,
            session_key=f"{project_id}:mention",
            timeout_sec=30,
            from_agent_id=from_agent_id,
            from_label=from_label,
            context_type="mention",
            project_id=project_id,
            hivee_api_base=hivee_api_base,
        )
    except Exception as exc:
        print(f"[mention_dispatch] Failed to dispatch @{target} in project {project_id}: {exc}", flush=True)


def _list_project_chat_messages(
    project_id: str,
    *,
    limit: int = PROJECT_CHAT_MAX_LIMIT,
    before: Optional[int] = None,
    mention_target: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cap = max(1, min(int(limit or PROJECT_CHAT_MAX_LIMIT), PROJECT_CHAT_MAX_LIMIT))
    before_ts = _to_int(before) if before is not None else 0
    mention_filter = str(mention_target or "").strip().lstrip("@").lower()

    conn = db()
    try:
        sql = (
            """
            SELECT pcm.id, pcm.project_id, pcm.author_type, pcm.author_id, pcm.author_label,
                   pcm.text, pcm.metadata_json, pcm.created_at
            FROM project_chat_messages pcm
            """
        )
        params: List[Any] = []
        if mention_filter:
            sql += (
                """
                JOIN project_chat_mentions pcn
                    ON pcn.project_id = pcm.project_id AND pcn.message_id = pcm.id
                """
            )
        sql += " WHERE pcm.project_id = ?"
        params.append(project_id)
        if before_ts > 0:
            sql += " AND pcm.created_at < ?"
            params.append(before_ts)
        if mention_filter:
            sql += " AND LOWER(COALESCE(pcn.mention_target, '')) = ?"
            params.append(mention_filter)
        sql += " ORDER BY pcm.created_at DESC LIMIT ?"
        params.append(cap)
        rows = conn.execute(sql, tuple(params)).fetchall()
        message_ids = [str(row["id"] or "").strip() for row in rows if str(row["id"] or "").strip()]
        mention_map: Dict[str, List[str]] = {}
        if message_ids:
            placeholders = ",".join("?" for _ in message_ids)
            mention_rows = conn.execute(
                f"""
                SELECT message_id, mention_target
                FROM project_chat_mentions
                WHERE project_id = ? AND message_id IN ({placeholders})
                ORDER BY created_at ASC
                """,
                (project_id, *message_ids),
            ).fetchall()
            for mention_row in mention_rows:
                mid = str(mention_row["message_id"] or "").strip()
                target = str(mention_row["mention_target"] or "").strip()
                if not mid or not target:
                    continue
                mention_map.setdefault(mid, [])
                if target not in mention_map[mid]:
                    mention_map[mid].append(target)
        payloads = [
            _project_chat_message_payload_from_row(row, mentions=mention_map.get(str(row["id"] or "").strip(), []))
            for row in reversed(rows)
        ]
        return payloads
    finally:
        conn.close()


def _build_project_chat_snapshot(project_id: str, *, max_messages: int = PROJECT_CHAT_SNAPSHOT_MAX_MESSAGES) -> str:
    messages = _list_project_chat_messages(project_id, limit=max_messages)
    if not messages:
        return "Recent project chat:\n- no chat messages yet"
    lines = ["Recent project chat (use exact @agent_id mentions for handoff):"]
    for item in messages[-max(1, min(int(max_messages), PROJECT_CHAT_SNAPSHOT_MAX_MESSAGES)):]:
        author = str(item.get("author_label") or item.get("author_id") or item.get("author_type") or "actor").strip()
        text = re.sub(r"\s+", " ", str(item.get("text") or "").strip())
        if not text:
            continue
        lines.append(f"- {author}: {text[:220]}")
    return "\n".join(lines)

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
        PROJECT_PROTOCOL_FILE,
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
        "project-protocol.md",
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
    lines.append("- Persist every deliverable and delegation handoff artifact in Hivee project files, not only on agent/provider server.")
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
        "rules": {
            "artifact_sync": {
                "required": True,
                "source_of_truth": project_dir.as_posix(),
                "require_output_files": True,
                "require_project_relative_paths": True,
                "disallow_external_only_storage": True,
                "delegation_handoff_requires_hivee_files": True,
                "preferred_roots": [USER_OUTPUTS_DIRNAME, PROJECT_INFO_DIRNAME, "agents", "logs"],
            }
        },
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
        "artifact_sync_policy": {
            "required": True,
            "source_of_truth": project_dir.as_posix(),
            "require_output_files": True,
            "require_project_relative_paths": True,
            "disallow_external_only_storage": True,
            "delegation_handoff_requires_hivee_files": True,
            "preferred_roots": [USER_OUTPUTS_DIRNAME, PROJECT_INFO_DIRNAME, "agents", "logs"],
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
            project_id=str(row["id"] or ""),
            hivee_api_base=_get_hivee_api_base(str(row["id"] or "")),
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
    project_id: str = "",
    hivee_api_base: str = "",
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
    (project_dir / USER_OUTPUTS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / USER_OUTPUTS_DIRNAME / HANDOFFS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / REFERENCE_DIRNAME).mkdir(parents=True, exist_ok=True)
    (project_dir / "logs").mkdir(parents=True, exist_ok=True)

    details = _normalize_setup_details(setup_details or {})

    # protocol.md — universal rules, always overwrite to keep up to date
    (project_dir / PROTOCOL_FILE).write_text(_protocol_markdown(), encoding="utf-8")

    # context.md — seed only, agent will replace
    context_file = project_dir / CONTEXT_FILE
    if not context_file.exists():
        context_file.write_text(
            _context_seed_markdown(title=title, brief=brief, goal=goal, hivee_api_base=hivee_api_base),
            encoding="utf-8",
        )

    # setup-chat.md — immutable transcript
    explicit_history_text = str(setup_chat_history_text or "").replace("\r", "").strip()
    if not explicit_history_text:
        explicit_history_text = _fallback_setup_chat_history_text(details)
    (project_dir / SETUP_CHAT_MD_FILE).write_text(
        _setup_chat_markdown(transcript_text=explicit_history_text), encoding="utf-8"
    )

    # decisions.md — append-only log, seed only
    decisions_file = project_dir / DECISIONS_MD_FILE
    if not decisions_file.exists():
        decisions_file.write_text(_decisions_markdown(), encoding="utf-8")

    # credentials.md — seed only (no secrets yet)
    creds_file = project_dir / CREDENTIALS_FILE
    if not creds_file.exists():
        creds_file.write_text(
            _credentials_markdown(project_id=project_id, hivee_api_base=hivee_api_base),
            encoding="utf-8",
        )

    # state.md — Hivee maintains, seed here
    state_file = project_dir / STATE_FILE
    if not state_file.exists():
        state_file.write_text(
            _state_markdown(
                phase="setup",
                plan_status=PLAN_STATUS_PENDING,
                execution_status=EXEC_STATUS_IDLE,
                progress_pct=0,
                hivee_api_base=hivee_api_base,
            ),
            encoding="utf-8",
        )

    # agents.md — seed, will be rewritten by _write_project_agents_file
    agents_file = project_dir / AGENTS_FILE
    if not agents_file.exists():
        agents_file.write_text(
            _agents_markdown(project_id=project_id, hivee_api_base=hivee_api_base),
            encoding="utf-8",
        )

    # Legacy Project Info dir files — keep for backward compat with existing sessions
    readme = info_dir / "README.md"
    readme.write_text(
        _project_readme_markdown(title=title, brief=brief, goal=goal, setup_details=details),
        encoding="utf-8",
    )
    (info_dir / "brief.md").write_text(
        _project_brief_markdown(brief=brief, setup_details=details), encoding="utf-8"
    )
    (info_dir / "goal.md").write_text(goal.strip() + "\n", encoding="utf-8")
    (info_dir / "project-setup.md").write_text(_setup_details_markdown(details), encoding="utf-8")
    protocol_legacy = info_dir / "project-protocol.md"
    if not protocol_legacy.exists():
        protocol_legacy.write_text(_protocol_markdown(), encoding="utf-8")

    # Legacy setup chat history
    history_file = info_dir / "setup-chat-history.txt"
    history_compat_file = info_dir / "SETUP-CHAT.txt"
    payload = explicit_history_text.strip() + "\n" if explicit_history_text else "No setup chat history captured.\n"
    history_file.write_text(payload, encoding="utf-8")
    history_compat_file.write_text(payload, encoding="utf-8")

    # Legacy PROJECT-INFO.MD seed
    project_info = info_dir / "PROJECT-INFO.MD"
    if not project_info.exists():
        project_info.write_text(_seed_project_info_markdown(title=title, brief=brief, goal=goal), encoding="utf-8")

    # Meta files
    history_meta = meta_dir / Path(PROJECT_HISTORY_FILE).name
    if not history_meta.exists():
        history_meta.touch()
    decisions_meta = meta_dir / Path(PROJECT_DECISIONS_FILE).name
    if not decisions_meta.exists():
        decisions_meta.touch()
    handoff_meta = meta_dir / Path(PROJECT_HANDOFF_FILE).name
    if not handoff_meta.exists():
        handoff_meta.write_text("# Handoff\n\nNo handoff summary generated yet.\n", encoding="utf-8")

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
    project_root: Optional[str] = None,
    project_info_excerpt: str = "",
) -> str:
    context = _project_context_instruction(
        title=title,
        brief=brief,
        goal=goal,
        setup_details=setup_details,
        role_rows=role_rows,
        project_root=project_root,
        plan_status=PLAN_STATUS_PENDING,
    )
    roster = _agent_roster_markdown(role_rows)
    return (
        f"{context}\n\n"
        f"{roster}\n\n"
        "Task for primary agent:\n"
        f"1) Read `{PROJECT_INFO_FILE}` and `{PROJECT_PROTOCOL_FILE}` first and align every plan detail with them.\n"
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
    project_id: str,
    agent_id: str,
    role_rows: List[Dict[str, Any]],
    agent_token: str,
    hivee_api_base: str,
) -> str:
    roster = []
    for row in role_rows:
        aid = str(row.get("agent_id") or "").strip()
        name = str(row.get("agent_name") or aid).strip()
        role = str(row.get("role") or "").strip()
        primary = bool(row.get("is_primary"))
        if aid:
            roster.append({"agent_id": aid, "agent_name": name, "role": role, "is_primary": primary})

    primary_id = next((r["agent_id"] for r in roster if r.get("is_primary")), agent_id)
    task = (
        "Plan has been approved by user. Your job now:\n\n"
        "1. Read `plan.md` from Hivee storage to get the full approved plan.\n"
        "2. Read `agents.md` to get the exact `@agent_id` for each team member.\n"
        "3. Assign a `weight_pct` (integer 0–100) to EVERY task you create. All weights across ALL tasks\n"
        "   for the project MUST sum to exactly 100. Larger/harder tasks get higher weight.\n"
        "   This drives the project progress bar — when a task is marked done its weight is automatically\n"
        "   added to the project progress percentage.\n"
        "4. Build a progress map and save it as `progress_map.json` via output_files. Format:\n"
        "   {\n"
        "     \"nodes\": [{\"id\": \"task_ref\", \"label\": \"...\", \"agent\": \"agent_id\",\n"
        "                 \"weight_pct\": 20, \"depends_on\": [\"other_ref\"]}],\n"
        "     \"groups\": [[\"ref_a\", \"ref_b\"], [\"ref_c\"]]\n"
        "   }\n"
        "5. Break the plan into HIGH-LEVEL task cards — ONE per agent (or per major phase per agent).\n"
        "   Each task card MUST include ALL of these structured fields:\n"
        "   - `instructions`: What this agent must accomplish (2-4 sentences)\n"
        "   - `input`: What data/files/context this agent receives to start\n"
        "   - `process`: High-level steps the agent should follow\n"
        "   - `output`: Exact files/artifacts this agent must produce\n"
        "   - `from_agent`: agent_id of who delegates this task (you, the primary)\n"
        "   - `handover_to`: agent_id who receives the output next (or 'owner' if final)\n"
        "   - `weight_pct`: This task's share of total project progress (0–100, all must sum to 100)\n"
        "   All outputs MUST be placed in the agent's assigned output folder.\n"
        "   FINAL deliverables (ready for the user) must also be copied into `FINAL/`.\n"
        "6. Decide execution order — parallel vs sequential:\n"
        "   - Group agents that can work simultaneously into the SAME parallel group.\n"
        "   - Agents that depend on a prior group go in a later group.\n"
        "   - Include `parallel_groups` as a list of lists:\n"
        "     e.g. [[\"agent_a\", \"agent_b\"], [\"agent_c\"], [\"agent_d\"]]\n"
        "     Group 0 runs first (all in parallel), then group 1, etc.\n"
        "7. Save `delegation.md` via output_files — include the parallel/sequential plan.\n"
        "8. Create ONE task card (create_task) per agent with all structured fields above.\n"
        "9. Post ONE chat message per assigned agent @mentioning them with their scope.\n"
        "10. Post a @owner summary: task count, weight distribution, parallel groups.\n\n"
        "Return JSON:\n"
        "{\n"
        "  \"chat_update\": \"Summary for @owner...\",\n"
        "  \"parallel_groups\": [[\"agent_id_1\", \"agent_id_2\"], [\"agent_id_3\"]],\n"
        "  \"output_files\": [\n"
        "    {\"path\": \"delegation.md\", \"content\": \"...\", \"append\": false},\n"
        "    {\"path\": \"progress_map.json\", \"content\": \"{...}\", \"append\": false}\n"
        "  ],\n"
        "  \"actions\": [\n"
        "    {\n"
        "      \"type\": \"create_task\",\n"
        "      \"title\": \"...\",\n"
        "      \"description\": \"...\",\n"
        "      \"assignee_agent_id\": \"agent_id\",\n"
        "      \"status\": \"todo\",\n"
        "      \"priority\": \"high\",\n"
        "      \"weight_pct\": 25,\n"
        "      \"instructions\": \"...\",\n"
        "      \"input\": \"...\",\n"
        "      \"process\": \"...\",\n"
        "      \"output\": \"...\",\n"
        "      \"from_agent\": \"primary_agent_id\",\n"
        "      \"handover_to\": \"next_agent_id_or_owner\"\n"
        "    },\n"
        "    {\"type\": \"post_chat_message\", \"text\": \"@agent_id you are assigned [scope]. "
        "Write your detailed sub-plan expanding each sub-task with instructions/input/process/output, "
        f"then @mention @{primary_id} for approval before starting.\", \"mentions\": [\"agent_id\"]}},\n"
        "    ...\n"
        "  ]\n"
        "}\n\n"
        f"Invited agents: {json.dumps(roster, ensure_ascii=False)}"
    )
    return _build_fundamentals_session_prompt(
        task=task,
        project_id=project_id,
        agent_id=agent_id,
        agent_token=agent_token,
        hivee_api_base=hivee_api_base,
    )

def _project_agent_rows(conn: sqlite3.Connection, project_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT agent_id, agent_name, is_primary, role, source_type FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
        (project_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _project_agent_rows_from_id(project_id: str) -> List[Dict[str, Any]]:
    conn = db()
    try:
        return _project_agent_rows(conn, project_id)
    finally:
        conn.close()

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
    if "artifact sync rule" not in normalized.lower():
        normalized = (
            normalized
            + "\n\nArtifact Sync Rule: persist every deliverable and handoff artifact in Hivee project files via output_files; do not keep final-only copies on provider/local runtime server."
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

def _project_lifecycle_stage(
    *,
    invited_agents_count: int,
    primary_agent_id: Optional[str],
    plan_status: Any,
) -> str:
    invited = max(0, int(invited_agents_count or 0))
    has_primary = bool(str(primary_agent_id or "").strip())
    normalized_plan = _coerce_plan_status(plan_status)
    if invited <= 0 or not has_primary:
        return "draft"
    if normalized_plan == PLAN_STATUS_APPROVED:
        return "active"
    return "planning"

def _project_root_ready(owner_user_id: str, project_root: str) -> bool:
    try:
        project_dir = _resolve_owner_project_dir(owner_user_id, project_root).resolve()
    except Exception:
        return False
    try:
        return project_dir.exists() and project_dir.is_dir()
    except Exception:
        return False

def _project_readiness_snapshot(
    *,
    owner_user_id: str,
    project_id: str,
    project_root: str,
    plan_status: Any,
    execution_status: Any = EXEC_STATUS_IDLE,
    role_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    normalized_rows: List[Dict[str, Any]] = []
    if role_rows is None:
        conn = db()
        raw_rows = conn.execute(
            "SELECT agent_id, agent_name, is_primary, role FROM project_agents WHERE project_id = ? ORDER BY is_primary DESC, agent_name ASC",
            (project_id,),
        ).fetchall()
        conn.close()
        normalized_rows = [dict(r) for r in raw_rows]
    else:
        normalized_rows = [dict(r) for r in role_rows]

    invited_agents_count = len(normalized_rows)
    primary_row = next((r for r in normalized_rows if _coerce_bool(r.get("is_primary"))), None)
    primary_agent_id = str((primary_row or {}).get("agent_id") or "").strip() or None
    plan_approved = _coerce_plan_status(plan_status) == PLAN_STATUS_APPROVED
    normalized_execution_status = _coerce_execution_status(execution_status)
    execution_ready_for_run = normalized_execution_status in {EXEC_STATUS_IDLE, EXEC_STATUS_STOPPED}
    execution_cta = "Execution is currently running. Wait until completion, or pause/stop first."
    if normalized_execution_status == EXEC_STATUS_PAUSED:
        execution_cta = "Execution is paused. Resume or stop this run before starting a new run."
    elif normalized_execution_status == EXEC_STATUS_COMPLETED:
        execution_cta = "Project run is already completed."
    elif normalized_execution_status == EXEC_STATUS_STOPPED:
        execution_cta = "Execution was stopped and can be started again."
    elif normalized_execution_status == EXEC_STATUS_IDLE:
        execution_cta = "Execution has not started yet."
    root_ready = _project_root_ready(owner_user_id, project_root)
    stage = _project_lifecycle_stage(
        invited_agents_count=invited_agents_count,
        primary_agent_id=primary_agent_id,
        plan_status=plan_status,
    )

    checks: List[Dict[str, Any]] = [
        {
            "key": "invited_agents",
            "label": "At least one project agent is invited",
            "ok": invited_agents_count > 0,
            "required": True,
            "cta": "Open Manage Agents and invite at least one agent.",
        },
        {
            "key": "primary_agent",
            "label": "Primary agent is assigned",
            "ok": bool(primary_agent_id),
            "required": True,
            "cta": "Open Manage Agents and set one primary agent.",
        },
        {
            "key": "plan_approved",
            "label": "Project plan is approved",
            "ok": plan_approved,
            "required": True,
            "cta": "Open Project Plan and click Approve Plan.",
        },
        {
            "key": "project_root",
            "label": "Project folder exists and is accessible",
            "ok": root_ready,
            "required": True,
            "cta": "Project folder is missing. Recreate project or restore the project root.",
        },
        {
            "key": "execution_state",
            "label": "Execution is idle/stopped (safe to start run)",
            "ok": execution_ready_for_run,
            "required": True,
            "cta": execution_cta,
        },
    ]
    can_run = all(bool(c.get("ok")) for c in checks if bool(c.get("required", True)))
    can_chat_project = invited_agents_count > 0 and bool(primary_agent_id)
    missing = [c for c in checks if bool(c.get("required", True)) and not bool(c.get("ok"))]
    if can_run:
        summary = "Ready to run."
    elif missing:
        summary = str(missing[0].get("cta") or missing[0].get("label") or "Project is not ready.").strip()
    else:
        summary = "Project is not ready."

    return {
        "project_id": project_id,
        "stage": stage,
        "can_chat_project": bool(can_chat_project),
        "can_run": bool(can_run),
        "invited_agents_count": invited_agents_count,
        "primary_agent_id": primary_agent_id,
        "checks": checks,
        "summary": summary[:400],
    }

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
    try:
        _append_project_activity_event(
            owner_user_id=owner_user_id,
            project_root=project_root,
            kind=kind,
            text=compact_text,
            payload=payload,
        )
    except Exception:
        pass

def _append_project_activity_event(
    *,
    owner_user_id: str,
    project_root: str,
    kind: str,
    text: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    user_id = str(owner_user_id or "").strip()
    root = str(project_root or "").strip()
    if not user_id or not root:
        return

    event_type = str(kind or "").strip()[:120] or "project.event"
    body = str(text or "").strip()
    payload_obj = payload if isinstance(payload, dict) else {}
    summary = body[:1000] if body else event_type

    actor_type = "system"
    actor_id = None
    actor_label = None
    agent_id = str(payload_obj.get("agent_id") or "").strip()
    user_actor_id = str(payload_obj.get("user_id") or payload_obj.get("actor_user_id") or "").strip()
    if agent_id:
        actor_type = "project_agent"
        actor_id = agent_id
        actor_label = str(payload_obj.get("agent_name") or agent_id).strip()[:180] or None
    elif user_actor_id:
        actor_type = "user"
        actor_id = user_actor_id
        actor_label = str(payload_obj.get("user_email") or payload_obj.get("actor_label") or "").strip()[:180] or None
    else:
        actor_label = str(payload_obj.get("actor") or payload_obj.get("source") or "").strip()[:180] or None

    conn = db()
    try:
        row = conn.execute(
            "SELECT id FROM projects WHERE user_id = ? AND project_root = ? LIMIT 1",
            (user_id, root),
        ).fetchone()
        if not row:
            return
        project_id = str(row["id"])
        conn.execute(
            """
            INSERT INTO project_activity_log (
                id, project_id, actor_type, actor_id, actor_label, event_type, summary, payload_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                new_id("act"),
                project_id,
                actor_type,
                actor_id,
                actor_label,
                event_type,
                summary,
                json.dumps(payload_obj, ensure_ascii=False),
                int(time.time()),
            ),
        )
        conn.commit()
    finally:
        conn.close()

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

def _normalize_agent_action_kind(raw_kind: Any) -> str:
    key = str(raw_kind or "").strip().lower().replace("-", "_").replace(" ", "_")
    alias_map = {
        "write": "write_file",
        "writefile": "write_file",
        "file_write": "write_file",
        "file.write": "write_file",
        "create_file": "write_file",
        "replace_file": "write_file",
        "append": "append_file",
        "appendfile": "append_file",
        "file_append": "append_file",
        "file.append": "append_file",
        "upload": "upload_file",
        "uploadfile": "upload_file",
        "file_upload": "upload_file",
        "file.upload": "upload_file",
        "delete": "delete_file",
        "remove": "delete_file",
        "deletefile": "delete_file",
        "remove_file": "delete_file",
        "file_delete": "delete_file",
        "file.delete": "delete_file",
        "move": "move_file",
        "rename": "move_file",
        "movefile": "move_file",
        "rename_file": "move_file",
        "file_move": "move_file",
        "file.move": "move_file",
        "mkdir": "create_dir",
        "make_dir": "create_dir",
        "create_dir": "create_dir",
        "directory_create": "create_dir",
        "dir.create": "create_dir",
        "rmdir": "delete_dir",
        "remove_dir": "delete_dir",
        "delete_dir": "delete_dir",
        "directory_delete": "delete_dir",
        "dir.delete": "delete_dir",
        "create_task": "create_task",
        "task_create": "create_task",
        "task.create": "create_task",
        "update_task": "update_task",
        "task_update": "update_task",
        "task.update": "update_task",
        "delete_task": "delete_task",
        "remove_task": "delete_task",
        "task_delete": "delete_task",
        "task.delete": "delete_task",
        "add_dependency": "add_task_dependency",
        "add_task_dependency": "add_task_dependency",
        "task_dependency_add": "add_task_dependency",
        "task.dependency.add": "add_task_dependency",
        "remove_dependency": "remove_task_dependency",
        "remove_task_dependency": "remove_task_dependency",
        "task_dependency_remove": "remove_task_dependency",
        "task.dependency.remove": "remove_task_dependency",
        "apply_blueprint": "apply_task_blueprint",
        "apply_task_blueprint": "apply_task_blueprint",
        "task_blueprint_apply": "apply_task_blueprint",
        "task.blueprint.apply": "apply_task_blueprint",
        "update_execution": "update_execution",
        "set_execution": "update_execution",
        "set_execution_state": "update_execution",
        "execution_update": "update_execution",
        "execution.update": "update_execution",
        "update_progress": "update_execution",
        "set_progress": "update_execution",
        "progress_update": "update_execution",
        "progress.update": "update_execution",
        "post_chat_message": "post_chat_message",
        "send_chat_message": "post_chat_message",
        "chat_message": "post_chat_message",
        "chat_send": "post_chat_message",
        "chat.send": "post_chat_message",
        "message_post": "post_chat_message",
        "message.post": "post_chat_message",
    }
    return alias_map.get(key, key)

def _normalize_agent_action_items(raw: Any) -> List[Dict[str, Any]]:
    items: List[Any] = []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        nested = raw.get("items")
        if isinstance(nested, list):
            items = nested
        else:
            items = [raw]
    out: List[Dict[str, Any]] = []
    for item in items[:MAX_AGENT_FILE_WRITES]:
        if not isinstance(item, dict):
            continue
        kind = _normalize_agent_action_kind(
            item.get("type")
            or item.get("method")
            or item.get("action")
            or item.get("name")
        )
        if not kind:
            continue
        params = item.get("params")
        payload: Dict[str, Any] = dict(params) if isinstance(params, dict) else {}
        for key, value in item.items():
            if key in {"type", "method", "action", "name", "params"}:
                continue
            payload[key] = value
        payload["type"] = kind
        out.append(payload)
    return out

def _extract_agent_report_payload(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    parsed = _extract_json_object(raw) or {}
    chat_update = raw
    output_files: List[Dict[str, Any]] = []
    actions: List[Dict[str, Any]] = []
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
        actions.extend(_normalize_agent_action_items(parsed.get("actions")))
        if not actions:
            actions.extend(_normalize_agent_action_items(parsed.get("methods")))
        if not actions:
            actions.extend(_normalize_agent_action_items(parsed.get("operations")))
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
        "actions": actions[:MAX_AGENT_FILE_WRITES],
        "notes": notes[:2000],
        "requires_user_input": bool(requires_user_input),
        "pause_reason": pause_reason[:1200],
        "resume_hint": resume_hint[:500],
    }

def _normalize_permission_write_paths(raw_paths: Any, *, fallback: Optional[List[str]] = None) -> List[str]:
    candidate_values: List[Any] = []
    if isinstance(raw_paths, list):
        candidate_values = raw_paths
    elif isinstance(raw_paths, tuple):
        candidate_values = list(raw_paths)
    elif isinstance(raw_paths, str):
        text = raw_paths.strip()
        if text:
            parsed: Any = None
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                candidate_values = parsed
            else:
                candidate_values = [text]
    elif raw_paths is not None:
        candidate_values = [raw_paths]

    cleaned: List[str] = []
    seen: set[str] = set()
    for item in candidate_values:
        raw_item = str(item or "").strip()
        if raw_item in {"*", "all", "project", "project_root"}:
            if "*" not in seen:
                seen.add("*")
                cleaned.append("*")
            continue
        rel = _clean_relative_project_path(raw_item)
        if not rel:
            continue
        rel = _remap_legacy_project_doc_rel_path(rel)
        if rel.lower() == LEGACY_OUTPUTS_DIRNAME:
            rel = USER_OUTPUTS_DIRNAME
        elif _rel_path_startswith(rel, LEGACY_OUTPUTS_DIRNAME):
            suffix = rel[len(LEGACY_OUTPUTS_DIRNAME) :].lstrip("/\\")
            rel = _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
        path_parts = [p for p in Path(rel).parts if p not in {"", "."}]
        if any(p == ".." for p in path_parts):
            continue
        low = rel.lower()
        if low in seen:
            continue
        seen.add(low)
        cleaned.append(rel)
        if len(cleaned) >= 40:
            break

    if cleaned:
        return cleaned
    if fallback is not None:
        return _normalize_permission_write_paths(fallback, fallback=None)
    return []

def _apply_project_file_writes(
    *,
    owner_user_id: str,
    project_root: str,
    writes: List[Dict[str, Any]],
    default_prefix: str = f"{USER_OUTPUTS_DIRNAME}/generated",
    allow_paths: Optional[List[str]] = None,
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
    normalized_allow_paths: Optional[List[str]] = None
    if allow_paths is not None:
        normalized_allow_paths = []
        allow_seen: set[str] = set()
        for raw_allow in allow_paths:
            raw_allow_text = str(raw_allow or "").strip()
            if raw_allow_text in {"*", "all", "project", "project_root"}:
                normalized_allow_paths = None
                allow_seen = {"*"}
                break
            allow_rel = _clean_relative_project_path(str(raw_allow or ""))
            if not allow_rel:
                continue
            allow_rel = _remap_legacy_project_doc_rel_path(allow_rel)
            if allow_rel.lower() == LEGACY_OUTPUTS_DIRNAME:
                allow_rel = USER_OUTPUTS_DIRNAME
            elif _rel_path_startswith(allow_rel, LEGACY_OUTPUTS_DIRNAME):
                suffix = allow_rel[len(LEGACY_OUTPUTS_DIRNAME):].lstrip("/\\")
                allow_rel = _clean_relative_project_path(f"{USER_OUTPUTS_DIRNAME}/{suffix}")
            allow_low = allow_rel.lower()
            if allow_low in allow_seen:
                continue
            allow_seen.add(allow_low)
            normalized_allow_paths.append(allow_rel)
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
        if normalized_allow_paths is not None and not any(_rel_path_startswith(rel, root) for root in normalized_allow_paths):
            skipped.append(f"{rel}: blocked by write path policy")
            continue
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

def _allow_paths_grant_full_project_access(allow_paths: Optional[List[str]]) -> bool:
    if allow_paths is None:
        return True
    normalized = _normalize_permission_write_paths(allow_paths, fallback=[])
    return "*" in normalized

def _project_action_path_allowed(rel_path: str, allow_paths: Optional[List[str]]) -> bool:
    rel = _clean_relative_project_path(rel_path)
    if not rel:
        return False
    if _allow_paths_grant_full_project_access(allow_paths):
        return True
    normalized = _normalize_permission_write_paths(allow_paths, fallback=[])
    if not normalized:
        return False
    return any(_rel_path_startswith(rel, root) for root in normalized)

def _append_project_activity_log_entry(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    actor_type: str,
    actor_id: Optional[str],
    actor_label: Optional[str],
    event_type: str,
    summary: str,
    payload: Optional[Dict[str, Any]] = None,
    created_at: Optional[int] = None,
) -> str:
    event_id = new_id("act")
    ts = int(time.time()) if created_at is None else int(created_at)
    conn.execute(
        """
        INSERT INTO project_activity_log (
            id, project_id, actor_type, actor_id, actor_label, event_type, summary, payload_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            str(event_id),
            str(project_id or "").strip(),
            str(actor_type or "").strip() or "unknown",
            str(actor_id or "").strip() or None,
            str(actor_label or "").strip() or None,
            str(event_type or "").strip() or "event",
            str(summary or "").strip()[:1000] or "-",
            json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
            ts,
        ),
    )
    return str(event_id)

def _coerce_project_action_task_status(raw_status: Any, *, required: bool = False) -> str:
    status = str(raw_status or "").strip().lower()
    if not status:
        if required:
            raise HTTPException(400, "Task status is required")
        return TASK_STATUS_TODO
    if status not in TASK_STATUSES:
        raise HTTPException(400, f"Invalid task status. Allowed: {', '.join(TASK_STATUSES)}")
    return status

def _coerce_project_action_task_priority(raw_priority: Any, *, required: bool = False) -> str:
    priority = str(raw_priority or "").strip().lower()
    if not priority:
        if required:
            raise HTTPException(400, "Task priority is required")
        return TASK_PRIORITY_MEDIUM
    if priority not in TASK_PRIORITIES:
        raise HTTPException(400, f"Invalid task priority. Allowed: {', '.join(TASK_PRIORITIES)}")
    return priority

def _project_action_assert_assignee_exists(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    assignee_agent_id: Optional[str],
) -> Optional[str]:
    aid = str(assignee_agent_id or "").strip()
    if not aid:
        return None
    row = conn.execute(
        "SELECT agent_id FROM project_agents WHERE project_id = ? AND agent_id = ? LIMIT 1",
        (project_id, aid),
    ).fetchone()
    if not row:
        raise HTTPException(400, "assignee_agent_id is not part of this project")
    return aid

def _project_action_open_dependencies(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    task_id: str,
) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT d.depends_on_task_id, t.title, t.status
        FROM project_task_dependencies d
        JOIN project_tasks t
            ON t.id = d.depends_on_task_id AND t.project_id = d.project_id
        WHERE d.project_id = ?
          AND d.task_id = ?
          AND COALESCE(t.status, '') != ?
        ORDER BY t.updated_at DESC
        """,
        (project_id, task_id, TASK_STATUS_DONE),
    ).fetchall()

def _project_action_ensure_status_transition_allowed(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    task_id: str,
    next_status: str,
) -> None:
    target = str(next_status or "").strip().lower()
    if target not in {TASK_STATUS_IN_PROGRESS, TASK_STATUS_REVIEW, TASK_STATUS_DONE}:
        return
    blockers = _project_action_open_dependencies(conn, project_id=project_id, task_id=task_id)
    if not blockers:
        return
    preview = ", ".join(
        [str(row["title"] or row["depends_on_task_id"]).strip()[:80] for row in blockers[:3] if str(row["title"] or row["depends_on_task_id"]).strip()]
    )
    raise HTTPException(409, f"Task has open dependencies: {preview or 'dependency tasks'}. Complete dependencies first.")

def _project_action_would_create_dependency_cycle(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    task_id: str,
    depends_on_task_id: str,
) -> bool:
    origin = str(task_id or "").strip()
    start = str(depends_on_task_id or "").strip()
    if not origin or not start:
        return False
    stack: List[str] = [start]
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current == origin:
            return True
        if current in seen:
            continue
        seen.add(current)
        rows = conn.execute(
            """
            SELECT depends_on_task_id
            FROM project_task_dependencies
            WHERE project_id = ? AND task_id = ?
            """,
            (project_id, current),
        ).fetchall()
        for row in rows:
            nxt = str(row["depends_on_task_id"] or "").strip()
            if nxt and nxt not in seen:
                stack.append(nxt)
    return False

def _resolve_project_action_task_row(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    task_id: Optional[str] = None,
    task_title: Optional[str] = None,
    refs: Optional[Dict[str, str]] = None,
) -> sqlite3.Row:
    candidate_id = str(task_id or "").strip()
    if candidate_id.startswith("$") and refs:
        candidate_id = refs.get(candidate_id[1:], "") or candidate_id
    elif refs and candidate_id in refs:
        candidate_id = refs.get(candidate_id, "") or candidate_id
    if candidate_id:
        row = conn.execute(
            "SELECT * FROM project_tasks WHERE project_id = ? AND id = ? LIMIT 1",
            (project_id, candidate_id),
        ).fetchone()
        if row:
            return row
        raise HTTPException(404, "Task not found")
    title = str(task_title or "").strip()
    if not title:
        raise HTTPException(400, "task_id or task_title is required")
    rows = conn.execute(
        "SELECT * FROM project_tasks WHERE project_id = ? AND lower(trim(title)) = lower(trim(?)) ORDER BY updated_at DESC LIMIT 2",
        (project_id, title),
    ).fetchall()
    if not rows:
        raise HTTPException(404, "Task not found")
    if len(rows) > 1:
        raise HTTPException(409, "Multiple tasks share that title. Use task_id instead.")
    return rows[0]

def _resolve_project_action_rel_path(raw_path: Any) -> str:
    rel = _clean_relative_project_path(str(raw_path or ""))
    rel = _remap_legacy_project_doc_rel_path(rel)
    return rel

def _resolve_project_action_target(
    *,
    owner_user_id: str,
    project_root: str,
    raw_path: Any,
    allow_paths: Optional[List[str]],
    require_exists: bool,
) -> Tuple[str, Path]:
    rel = _resolve_project_action_rel_path(raw_path)
    if not rel:
        raise HTTPException(400, "path is required")
    if not _project_action_path_allowed(rel, allow_paths):
        raise HTTPException(403, f"{rel}: outside allowed project write scope")
    target = _resolve_project_relative_path(
        owner_user_id,
        project_root,
        rel,
        require_exists=require_exists,
        require_dir=False,
    ).resolve()
    return rel, target

def _normalize_project_action_payload(raw_payload: Any) -> Dict[str, Any]:
    if isinstance(raw_payload, dict):
        return dict(raw_payload)
    return {}

def _action_ref_key(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if value.startswith("$"):
        value = value[1:]
    return value

def _apply_project_actions(
    *,
    owner_user_id: str,
    project_id: str,
    project_root: str,
    actions: List[Dict[str, Any]],
    allow_paths: Optional[List[str]] = None,
    actor_type: str = "project_agent",
    actor_id: Optional[str] = None,
    actor_label: Optional[str] = None,
) -> Dict[str, Any]:
    applied: List[Dict[str, Any]] = []
    skipped: List[str] = []
    refs: Dict[str, str] = {}
    if not actions:
        return {"applied": applied, "skipped": skipped}

    conn = db()
    try:
        for idx, raw_action in enumerate(actions[:MAX_AGENT_FILE_WRITES], start=1):
            action = _normalize_project_action_payload(raw_action)
            kind = _normalize_agent_action_kind(action.get("type"))
            if not kind:
                skipped.append(f"action {idx}: missing type")
                continue
            now = int(time.time())
            try:
                if kind in {"write_file", "append_file"}:
                    rel, target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=False,
                    )
                    content = str(action.get("content") or "")
                    if not content:
                        raise HTTPException(400, "content is required")
                    payload_bytes = len(content.encode("utf-8"))
                    if payload_bytes > MAX_AGENT_FILE_BYTES:
                        raise HTTPException(400, f"content exceeds {MAX_AGENT_FILE_BYTES} bytes")
                    if target.exists() and target.is_dir():
                        raise HTTPException(400, "target path is a directory")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    mode = "a" if kind == "append_file" or bool(action.get("append")) else "w"
                    with target.open(mode, encoding="utf-8") as f:
                        f.write(content)
                    applied.append(
                        {
                            "type": kind,
                            "path": rel,
                            "mode": mode,
                            "bytes": payload_bytes,
                            "event": "project.file.written",
                            "event_payload": {
                                "path": rel,
                                "mode": mode,
                                "bytes": payload_bytes,
                                "actor": f"agent:{actor_id or 'unknown'}",
                            },
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="file.write",
                        summary=f"File written: {rel}",
                        payload={"path": rel, "mode": mode, "bytes": payload_bytes, "method": kind},
                        created_at=now,
                    )
                elif kind == "upload_file":
                    rel, target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=False,
                    )
                    content_b64 = str(action.get("content_base64") or action.get("base64") or "").strip()
                    if content_b64:
                        try:
                            payload_bytes = base64.b64decode(content_b64, validate=True)
                        except Exception:
                            raise HTTPException(400, "content_base64 is not valid base64")
                    else:
                        payload_bytes = str(action.get("content") or "").encode("utf-8")
                    if not payload_bytes:
                        raise HTTPException(400, "file payload is empty")
                    if len(payload_bytes) > MAX_AGENT_FILE_BYTES:
                        raise HTTPException(400, f"file exceeds {MAX_AGENT_FILE_BYTES} bytes")
                    if target.exists() and target.is_dir():
                        raise HTTPException(400, "target path is a directory")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("wb") as f:
                        f.write(payload_bytes)
                    applied.append(
                        {
                            "type": kind,
                            "path": rel,
                            "mode": "wb",
                            "bytes": len(payload_bytes),
                            "event": "project.file.written",
                            "event_payload": {
                                "path": rel,
                                "mode": "wb",
                                "bytes": len(payload_bytes),
                                "actor": f"agent:{actor_id or 'unknown'}",
                            },
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="file.write",
                        summary=f"Binary file uploaded: {rel}",
                        payload={"path": rel, "mode": "wb", "bytes": len(payload_bytes), "method": kind},
                        created_at=now,
                    )
                elif kind == "delete_file":
                    rel, target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=True,
                    )
                    if target.is_dir():
                        raise HTTPException(400, "delete_file only supports files")
                    target.unlink(missing_ok=False)
                    applied.append(
                        {
                            "type": kind,
                            "path": rel,
                            "event": "project.file.deleted",
                            "event_payload": {"path": rel, "actor": f"agent:{actor_id or 'unknown'}"},
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="file.delete",
                        summary=f"File deleted: {rel}",
                        payload={"path": rel},
                        created_at=now,
                    )
                elif kind == "move_file":
                    src_rel, src_target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("from_path") or action.get("source_path"),
                        allow_paths=allow_paths,
                        require_exists=True,
                    )
                    dst_rel, dst_target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("to_path") or action.get("destination_path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=False,
                    )
                    if src_target.is_dir():
                        raise HTTPException(400, "move_file only supports files")
                    if dst_target.exists():
                        if dst_target.is_dir():
                            raise HTTPException(400, "destination path is a directory")
                        if not bool(action.get("overwrite")):
                            raise HTTPException(409, "destination file already exists")
                        dst_target.unlink()
                    dst_target.parent.mkdir(parents=True, exist_ok=True)
                    src_target.replace(dst_target)
                    applied.append(
                        {
                            "type": kind,
                            "path": src_rel,
                            "to_path": dst_rel,
                            "event": "project.file.moved",
                            "event_payload": {
                                "path": src_rel,
                                "to_path": dst_rel,
                                "actor": f"agent:{actor_id or 'unknown'}",
                            },
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="file.moved",
                        summary=f"File moved: {src_rel} -> {dst_rel}",
                        payload={"path": src_rel, "to_path": dst_rel},
                        created_at=now,
                    )
                elif kind == "create_dir":
                    rel, target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=False,
                    )
                    if target.exists() and not target.is_dir():
                        raise HTTPException(400, "target path is a file")
                    target.mkdir(parents=True, exist_ok=True)
                    applied.append(
                        {
                            "type": kind,
                            "path": rel,
                            "event": "project.dir.created",
                            "event_payload": {"path": rel, "actor": f"agent:{actor_id or 'unknown'}"},
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="dir.created",
                        summary=f"Directory created: {rel}",
                        payload={"path": rel},
                        created_at=now,
                    )
                elif kind == "delete_dir":
                    rel, target = _resolve_project_action_target(
                        owner_user_id=owner_user_id,
                        project_root=project_root,
                        raw_path=action.get("path") or action.get("target_path"),
                        allow_paths=allow_paths,
                        require_exists=True,
                    )
                    if not target.is_dir():
                        raise HTTPException(400, "delete_dir only supports directories")
                    if bool(action.get("recursive")):
                        shutil.rmtree(target)
                    else:
                        target.rmdir()
                    applied.append(
                        {
                            "type": kind,
                            "path": rel,
                            "recursive": bool(action.get("recursive")),
                            "event": "project.dir.deleted",
                            "event_payload": {
                                "path": rel,
                                "recursive": bool(action.get("recursive")),
                                "actor": f"agent:{actor_id or 'unknown'}",
                            },
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="dir.deleted",
                        summary=f"Directory deleted: {rel}",
                        payload={"path": rel, "recursive": bool(action.get("recursive"))},
                        created_at=now,
                    )
                elif kind == "create_task":
                    title = str(action.get("title") or "").strip()
                    if not title:
                        raise HTTPException(400, "title is required")
                    task_id = new_id("tsk")
                    status_value = _coerce_project_action_task_status(action.get("status"), required=False)
                    priority_value = _coerce_project_action_task_priority(action.get("priority"), required=False)
                    assignee = _project_action_assert_assignee_exists(
                        conn,
                        project_id=project_id,
                        assignee_agent_id=action.get("assignee_agent_id"),
                    )
                    raw_weight = action.get("weight_pct")
                    weight_pct_value = max(0, min(100, int(raw_weight))) if raw_weight is not None else 0
                    metadata = action.get("metadata") if isinstance(action.get("metadata"), dict) else {}
                    # Lift structured card fields into metadata if provided at action top level
                    for _card_field in ("instructions", "input", "process", "output", "from_agent", "handover_to"):
                        if action.get(_card_field) is not None and _card_field not in metadata:
                            metadata[_card_field] = action[_card_field]
                    due_at_value = _to_int(action.get("due_at")) if action.get("due_at") is not None else None
                    conn.execute(
                        """
                        INSERT INTO project_tasks (
                            id, project_id, created_by_user_id, created_by_agent_id,
                            title, description, status, priority, assignee_agent_id,
                            due_at, weight_pct, metadata_json, created_at, updated_at, closed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            task_id,
                            project_id,
                            actor_id if actor_type == "user" else None,
                            actor_id if actor_type == "project_agent" else None,
                            title[:TASK_TITLE_MAX_CHARS],
                            str(action.get("description") or "")[:TASK_DESCRIPTION_MAX_CHARS],
                            status_value,
                            priority_value,
                            assignee,
                            due_at_value,
                            weight_pct_value,
                            json.dumps(metadata, ensure_ascii=False),
                            now,
                            now,
                            now if status_value == TASK_STATUS_DONE else None,
                        ),
                    )
                    ref_key = _action_ref_key(action.get("ref") or action.get("task_ref"))
                    if ref_key:
                        refs[ref_key] = task_id
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="task.created",
                        summary=f"Task created: {title[:120]}",
                        payload={
                            "task_id": task_id,
                            "status": status_value,
                            "priority": priority_value,
                            "assignee_agent_id": assignee,
                            "ref": ref_key or None,
                        },
                        created_at=now,
                    )
                    applied.append(
                        {
                            "type": kind,
                            "task_id": task_id,
                            "title": title[:TASK_TITLE_MAX_CHARS],
                            "ref": ref_key or None,
                            "event": "project.task.created",
                            "event_payload": {"task_id": task_id},
                        }
                    )
                elif kind == "update_task":
                    row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("task_id") or action.get("task_ref"),
                        task_title=action.get("task_title"),
                        refs=refs,
                    )
                    task_id = str(row["id"] or "")
                    updates: List[str] = []
                    params: List[Any] = []
                    changed_fields: List[str] = []
                    if action.get("title") is not None:
                        title_value = str(action.get("title") or "").strip()
                        if not title_value:
                            raise HTTPException(400, "title cannot be empty")
                        updates.append("title = ?")
                        params.append(title_value[:TASK_TITLE_MAX_CHARS])
                        changed_fields.append("title")
                    if action.get("description") is not None:
                        updates.append("description = ?")
                        params.append(str(action.get("description") or "")[:TASK_DESCRIPTION_MAX_CHARS])
                        changed_fields.append("description")
                    _update_task_next_status: Optional[str] = None
                    if action.get("status") is not None:
                        next_status = _coerce_project_action_task_status(action.get("status"), required=True)
                        _project_action_ensure_status_transition_allowed(
                            conn,
                            project_id=project_id,
                            task_id=task_id,
                            next_status=next_status,
                        )
                        _update_task_next_status = next_status
                        updates.append("status = ?")
                        params.append(next_status)
                        changed_fields.append("status")
                        if next_status == TASK_STATUS_DONE:
                            updates.append("closed_at = ?")
                            params.append(now)
                        else:
                            updates.append("closed_at = NULL")
                    if action.get("priority") is not None:
                        updates.append("priority = ?")
                        params.append(_coerce_project_action_task_priority(action.get("priority"), required=True))
                        changed_fields.append("priority")
                    if bool(action.get("clear_assignee")):
                        updates.append("assignee_agent_id = NULL")
                        changed_fields.append("assignee_agent_id")
                    elif action.get("assignee_agent_id") is not None:
                        assignee = _project_action_assert_assignee_exists(
                            conn,
                            project_id=project_id,
                            assignee_agent_id=action.get("assignee_agent_id"),
                        )
                        updates.append("assignee_agent_id = ?")
                        params.append(assignee)
                        changed_fields.append("assignee_agent_id")
                    if bool(action.get("clear_due_at")):
                        updates.append("due_at = NULL")
                        changed_fields.append("due_at")
                    elif action.get("due_at") is not None:
                        updates.append("due_at = ?")
                        params.append(_to_int(action.get("due_at")))
                        changed_fields.append("due_at")
                    if action.get("metadata") is not None:
                        metadata = action.get("metadata") if isinstance(action.get("metadata"), dict) else {}
                        updates.append("metadata_json = ?")
                        params.append(json.dumps(metadata, ensure_ascii=False))
                        changed_fields.append("metadata")
                    if action.get("weight_pct") is not None:
                        raw_w = action.get("weight_pct")
                        new_weight = max(0, min(100, int(raw_w))) if raw_w is not None else 0
                        updates.append("weight_pct = ?")
                        params.append(new_weight)
                        changed_fields.append("weight_pct")
                    if not updates:
                        skipped.append(f"update_task {task_id}: no fields changed")
                        continue
                    updates.append("updated_at = ?")
                    params.append(now)
                    params.append(task_id)
                    conn.execute(f"UPDATE project_tasks SET {', '.join(updates)} WHERE id = ?", tuple(params))
                    # Auto-accumulate weight_pct into project progress when task is marked done
                    if _update_task_next_status == TASK_STATUS_DONE:
                        _task_weight = conn.execute(
                            "SELECT weight_pct FROM project_tasks WHERE id = ?", (task_id,)
                        ).fetchone()
                        if _task_weight and _task_weight["weight_pct"] > 0:
                            _cur_proj = conn.execute(
                                "SELECT progress_pct FROM projects WHERE id = ?", (project_id,)
                            ).fetchone()
                            if _cur_proj:
                                _new_pct = min(100, (_cur_proj["progress_pct"] or 0) + _task_weight["weight_pct"])
                                conn.execute(
                                    "UPDATE projects SET progress_pct = ?, execution_updated_at = ? WHERE id = ?",
                                    (_new_pct, now, project_id),
                                )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="task.updated",
                        summary=f"Task updated: {task_id}",
                        payload={"task_id": task_id, "changed_fields": changed_fields[:20]},
                        created_at=now,
                    )
                    applied.append(
                        {
                            "type": kind,
                            "task_id": task_id,
                            "changed_fields": changed_fields[:20],
                            "event": "project.task.updated",
                            "event_payload": {"task_id": task_id, "changed_fields": changed_fields[:20]},
                        }
                    )
                elif kind == "delete_task":
                    row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("task_id") or action.get("task_ref"),
                        task_title=action.get("task_title"),
                        refs=refs,
                    )
                    task_id = str(row["id"] or "")
                    conn.execute("DELETE FROM project_task_checkouts WHERE task_id = ?", (task_id,))
                    conn.execute("DELETE FROM project_task_comments WHERE task_id = ?", (task_id,))
                    conn.execute(
                        "DELETE FROM project_task_dependencies WHERE project_id = ? AND (task_id = ? OR depends_on_task_id = ?)",
                        (project_id, task_id, task_id),
                    )
                    conn.execute("DELETE FROM project_tasks WHERE id = ?", (task_id,))
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="task.deleted",
                        summary=f"Task deleted: {str(row['title'] or task_id)[:120]}",
                        payload={"task_id": task_id},
                        created_at=now,
                    )
                    applied.append(
                        {
                            "type": kind,
                            "task_id": task_id,
                            "event": "project.task.deleted",
                            "event_payload": {"task_id": task_id},
                        }
                    )
                elif kind == "add_task_dependency":
                    row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("task_id") or action.get("task_ref"),
                        task_title=action.get("task_title"),
                        refs=refs,
                    )
                    dep_row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("depends_on_task_id") or action.get("depends_on_task_ref"),
                        task_title=action.get("depends_on_task_title"),
                        refs=refs,
                    )
                    task_id = str(row["id"] or "")
                    dep_id = str(dep_row["id"] or "")
                    if task_id == dep_id:
                        raise HTTPException(400, "Task cannot depend on itself")
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM project_task_dependencies
                        WHERE project_id = ? AND task_id = ? AND depends_on_task_id = ?
                        LIMIT 1
                        """,
                        (project_id, task_id, dep_id),
                    ).fetchone()
                    if not existing:
                        if _project_action_would_create_dependency_cycle(
                            conn,
                            project_id=project_id,
                            task_id=task_id,
                            depends_on_task_id=dep_id,
                        ):
                            raise HTTPException(409, "Dependency would create a cycle")
                        conn.execute(
                            """
                            INSERT INTO project_task_dependencies (
                                project_id, task_id, depends_on_task_id, created_at
                            ) VALUES (?,?,?,?)
                            """,
                            (project_id, task_id, dep_id, now),
                        )
                        conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
                        _append_project_activity_log_entry(
                            conn,
                            project_id=project_id,
                            actor_type=actor_type,
                            actor_id=actor_id,
                            actor_label=actor_label,
                            event_type="task.dependency.added",
                            summary=f"Task dependency added: {task_id} -> {dep_id}",
                            payload={"task_id": task_id, "depends_on_task_id": dep_id},
                            created_at=now,
                        )
                    applied.append(
                        {
                            "type": kind,
                            "task_id": task_id,
                            "depends_on_task_id": dep_id,
                            "event": "project.task.dependency.added",
                            "event_payload": {"task_id": task_id, "depends_on_task_id": dep_id},
                        }
                    )
                elif kind == "remove_task_dependency":
                    row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("task_id") or action.get("task_ref"),
                        task_title=action.get("task_title"),
                        refs=refs,
                    )
                    dep_row = _resolve_project_action_task_row(
                        conn,
                        project_id=project_id,
                        task_id=action.get("depends_on_task_id") or action.get("depends_on_task_ref"),
                        task_title=action.get("depends_on_task_title"),
                        refs=refs,
                    )
                    task_id = str(row["id"] or "")
                    dep_id = str(dep_row["id"] or "")
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM project_task_dependencies
                        WHERE project_id = ? AND task_id = ? AND depends_on_task_id = ?
                        LIMIT 1
                        """,
                        (project_id, task_id, dep_id),
                    ).fetchone()
                    if not existing:
                        raise HTTPException(404, "Dependency not found")
                    conn.execute(
                        "DELETE FROM project_task_dependencies WHERE project_id = ? AND task_id = ? AND depends_on_task_id = ?",
                        (project_id, task_id, dep_id),
                    )
                    conn.execute("UPDATE project_tasks SET updated_at = ? WHERE id = ?", (now, task_id))
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="task.dependency.removed",
                        summary=f"Task dependency removed: {task_id} -> {dep_id}",
                        payload={"task_id": task_id, "depends_on_task_id": dep_id},
                        created_at=now,
                    )
                    applied.append(
                        {
                            "type": kind,
                            "task_id": task_id,
                            "depends_on_task_id": dep_id,
                            "event": "project.task.dependency.removed",
                            "event_payload": {"task_id": task_id, "depends_on_task_id": dep_id},
                        }
                    )
                elif kind == "apply_task_blueprint":
                    blueprint_id = str(action.get("blueprint_id") or "").strip().lower()
                    blueprint = next(
                        (
                            item
                            for item in TASK_BLUEPRINTS
                            if str(item.get("id") or "").strip().lower() == blueprint_id
                        ),
                        None,
                    )
                    if not blueprint:
                        raise HTTPException(404, "Task blueprint not found")
                    assignee = _project_action_assert_assignee_exists(
                        conn,
                        project_id=project_id,
                        assignee_agent_id=action.get("assignee_agent_id"),
                    )
                    title_prefix = str(action.get("title_prefix") or "").strip()[:80]
                    include_dependencies = bool(action.get("include_dependencies", True))
                    created_task_ids: List[str] = []
                    for spec_idx, spec in enumerate(blueprint.get("tasks") or []):
                        new_task_id = new_id("tsk")
                        base_title = str(spec.get("title") or "Task").strip()[:TASK_TITLE_MAX_CHARS]
                        full_title = f"{title_prefix} {base_title}".strip()[:TASK_TITLE_MAX_CHARS] if title_prefix else base_title
                        priority = _coerce_project_action_task_priority(spec.get("priority"), required=False)
                        metadata = {
                            "blueprint_id": str(blueprint.get("id") or ""),
                            "blueprint_step": spec_idx + 1,
                        }
                        conn.execute(
                            """
                            INSERT INTO project_tasks (
                                id, project_id, created_by_user_id, created_by_agent_id,
                                title, description, status, priority, assignee_agent_id,
                                due_at, weight_pct, metadata_json, created_at, updated_at, closed_at
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                new_task_id,
                                project_id,
                                actor_id if actor_type == "user" else None,
                                actor_id if actor_type == "project_agent" else None,
                                full_title,
                                str(spec.get("description") or "")[:TASK_DESCRIPTION_MAX_CHARS],
                                TASK_STATUS_TODO,
                                priority,
                                assignee,
                                None,
                                0,
                                json.dumps(metadata, ensure_ascii=False),
                                now,
                                now,
                                None,
                            ),
                        )
                        created_task_ids.append(new_task_id)
                    if include_dependencies:
                        for pair in blueprint.get("dependencies") or []:
                            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                                continue
                            task_idx = _to_int(pair[0])
                            dep_idx = _to_int(pair[1])
                            if task_idx < 0 or dep_idx < 0:
                                continue
                            if task_idx >= len(created_task_ids) or dep_idx >= len(created_task_ids):
                                continue
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO project_task_dependencies (
                                    project_id, task_id, depends_on_task_id, created_at
                                ) VALUES (?,?,?,?)
                                """,
                                (project_id, created_task_ids[task_idx], created_task_ids[dep_idx], now),
                            )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="task.blueprint.applied",
                        summary=f"Task blueprint applied: {str(blueprint.get('name') or blueprint.get('id') or 'blueprint')}",
                        payload={
                            "blueprint_id": str(blueprint.get("id") or ""),
                            "created_task_ids": created_task_ids,
                            "include_dependencies": include_dependencies,
                        },
                        created_at=now,
                    )
                    applied.append(
                        {
                            "type": kind,
                            "blueprint_id": str(blueprint.get("id") or ""),
                            "created_task_ids": created_task_ids,
                            "created_count": len(created_task_ids),
                            "event": "project.task.blueprint.applied",
                            "event_payload": {
                                "blueprint_id": str(blueprint.get("id") or ""),
                                "task_ids": created_task_ids,
                                "created_count": len(created_task_ids),
                            },
                        }
                    )
                elif kind == "update_execution":
                    next_status = action.get("status")
                    next_progress = action.get("progress_pct")
                    if next_progress is None and action.get("progress") is not None:
                        next_progress = action.get("progress")
                    if next_status is None and next_progress is None:
                        raise HTTPException(400, "status or progress_pct is required")
                    state = _set_project_execution_state(
                        project_id,
                        status=next_status if next_status is not None else None,
                        progress_pct=_to_int(next_progress) if next_progress is not None else None,
                    )
                    if not state:
                        raise HTTPException(404, "Project not found")
                    summary = str(action.get("summary") or action.get("note") or action.get("text") or "").strip()
                    payload = {
                        "status": str(state.get("status") or ""),
                        "progress_pct": _to_int(state.get("progress_pct")),
                        "updated_at": _to_int(state.get("updated_at")),
                        "summary": summary[:500] or None,
                        "actor": f"agent:{actor_id or 'unknown'}",
                    }
                    applied.append(
                        {
                            "type": kind,
                            "status": payload["status"],
                            "progress_pct": payload["progress_pct"],
                            "summary": summary[:500],
                            "event": "project.execution.updated",
                            "event_payload": payload,
                        }
                    )
                    _append_project_activity_log_entry(
                        conn,
                        project_id=project_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        actor_label=actor_label,
                        event_type="project.execution.updated",
                        summary=summary[:220] or f"Execution updated to {payload['status']}",
                        payload=payload,
                        created_at=now,
                    )
                elif kind == "post_chat_message":
                    body = str(
                        action.get("text")
                        or action.get("body")
                        or action.get("message")
                        or ""
                    ).strip()
                    if not body:
                        raise HTTPException(400, "text is required")
                    metadata = action.get("metadata") if isinstance(action.get("metadata"), dict) else {}
                    message_payload = _create_project_chat_message(
                        conn,
                        project_id=project_id,
                        author_type=actor_type,
                        author_id=actor_id,
                        author_label=actor_label,
                        text=body,
                        metadata=metadata,
                        mentions=action.get("mentions") if isinstance(action.get("mentions"), list) else None,
                        created_at=now,
                    )
                    extra_events = [
                        {
                            "event": "project.chat.mention",
                            "event_payload": {
                                "message_id": str(message_payload.get("id") or ""),
                                "project_id": project_id,
                                "target": mention,
                                "author_type": actor_type,
                                "author_id": actor_id,
                                "author_label": actor_label,
                                "text": str(message_payload.get("text") or "")[:500],
                                "created_at": now,
                            },
                        }
                        for mention in (message_payload.get("mentions") or [])[:PROJECT_CHAT_MENTION_MAX]
                    ]
                    applied.append(
                        {
                            "type": kind,
                            "message_id": str(message_payload.get("id") or ""),
                            "text": str(message_payload.get("text") or ""),
                            "mentions": list(message_payload.get("mentions") or []),
                            "event": "project.chat.message",
                            "event_payload": message_payload,
                            "extra_events": extra_events,
                        }
                    )
                else:
                    skipped.append(f"action {idx}: unsupported type `{kind}`")
                    continue
            except HTTPException as exc:
                skipped.append(f"{kind}: {detail_to_text(exc.detail)}")
            except Exception as exc:
                skipped.append(f"{kind}: {detail_to_text(exc)}")
        conn.commit()
    finally:
        conn.close()
    return {"applied": applied[:MAX_AGENT_FILE_WRITES], "skipped": skipped[:MAX_AGENT_FILE_WRITES]}

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
        "  \"actions\": [{\"type\":\"write_file\",\"path\":\"src/app.js\",\"content\":\"...\"}],\n"
        "  \"notes\": \"optional\",\n"
        "  \"requires_user_input\": false,\n"
        "  \"pause_reason\": \"\",\n"
        "  \"resume_hint\": \"\"\n"
        "}\n"
        "Rules:\n"
        "- If you created or modified files, include all of them in output_files with full content.\n"
        "- Persist deliverables in Hivee project files; do not report done if files exist only on your own server/runtime.\n"
        "- Use project-relative paths only.\n"
        "- If you must mutate existing project files, team chat state, or task/progress state directly, put that in `actions`.\n"
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
        "  \"actions\": [{\"type\":\"write_file\",\"path\":\"src/app.js\",\"content\":\"...\"}],\n"
        "  \"notes\": \"optional\",\n"
        "  \"requires_user_input\": false,\n"
        "  \"pause_reason\": \"\",\n"
        "  \"resume_hint\": \"\"\n"
        "}\n"
        "Hard requirements:\n"
        "- output_files MUST NOT be empty unless requires_user_input=true.\n"
        "- If implementation task, return concrete source files (for websites include at least index.html and style.css).\n"
        f"- If planning/research task, return at least one markdown deliverable file at {fallback_file}.\n"
        "- Persist deliverables in Hivee project files; do not keep final-only copies on provider/local runtime server.\n"
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
        # Reject placeholder/default agents — they are not real agents.
        if "default" in aid:
            return -1000
        text = f"{aid} {name}"
        score = 0
        if aid in {"main", "primary", "core"}:
            score += 120
        if "main" in text:
            score += 80
        if "primary" in text:
            score += 50
        if "core" in text:
            score += 20
        return score

    # Filter out any agent that scores below zero (pure placeholders), then rank.
    eligible = [a for a in agents if _score(a) >= 0]
    if not eligible:
        return None
    ranked = sorted(
        eligible,
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

def _build_fundamentals_session_prompt(
    *,
    task: str,
    project_id: str,
    agent_id: str,
    agent_token: str,
    hivee_api_base: str,
) -> str:
    """
    Minimal session injection: agent identity + token + fundamentals URL + task.
    Agent fetches all context it needs from fundamentals.md on-demand.
    All Hivee API calls must use X-Project-Agent-Id and X-Project-Agent-Token headers.
    """
    fundamentals_url = f"{hivee_api_base.rstrip('/')}/files/{FUNDAMENTALS_FILE}"
    return (
        f"hivee_agent_id: {agent_id}\n"
        f"hivee_project_token: {agent_token}\n"
        f"fundamentals: GET {fundamentals_url}\n"
        f"  Headers: X-Project-Agent-Id: {agent_id}\n"
        f"           X-Project-Agent-Token: {agent_token}\n\n"
        f"All Hivee API requests must include these two headers:\n"
        f"  X-Project-Agent-Id: {agent_id}\n"
        f"  X-Project-Agent-Token: {agent_token}\n\n"
        f"Task:\n{task.strip()}"
    )


def _write_project_agents_file(
    project_dir: Path,
    role_rows: List[Dict[str, Any]],
    *,
    project_id: str = "",
    hivee_api_base: str = "",
) -> None:
    agents_file = project_dir / AGENTS_FILE
    agents_file.write_text(
        _agents_markdown(role_rows, project_id=project_id, hivee_api_base=hivee_api_base),
        encoding="utf-8",
    )


def _write_project_state_file(
    project_dir: Path,
    *,
    phase: str,
    plan_status: str,
    execution_status: str,
    progress_pct: int = 0,
    agents: Optional[List[Dict[str, Any]]] = None,
    pending_inputs: Optional[List[str]] = None,
    hivee_api_base: str = "",
) -> None:
    state_file = project_dir / STATE_FILE
    state_file.write_text(
        _state_markdown(
            phase=phase,
            plan_status=plan_status,
            execution_status=execution_status,
            progress_pct=progress_pct,
            agents=agents,
            pending_inputs=pending_inputs,
            hivee_api_base=hivee_api_base,
        ),
        encoding="utf-8",
    )


def _write_project_fundamentals_file(
    project_dir: Path,
    *,
    project_id: str,
    title: str,
    phase: str,
    plan_status: str,
    execution_status: str,
    hivee_api_base: str,
    role_rows: Optional[List[Dict[str, Any]]] = None,
) -> None:
    fundamentals_file = project_dir / FUNDAMENTALS_FILE
    fundamentals_file.write_text(
        _fundamentals_markdown(
            project_id=project_id,
            title=title,
            phase=phase,
            plan_status=plan_status,
            execution_status=execution_status,
            hivee_api_base=hivee_api_base,
            role_rows=role_rows,
        ),
        encoding="utf-8",
    )


def _write_project_scope_file(
    project_dir: Path,
    *,
    agent_id: str,
    agent_name: str,
    is_primary: bool,
    write_paths: Optional[List[str]] = None,
    hivee_api_base: str = "",
    project_id: str = "",
) -> None:
    scope_file = project_dir / SCOPE_FILE
    scope_file.write_text(
        _scope_markdown(
            agent_id=agent_id,
            agent_name=agent_name,
            is_primary=is_primary,
            write_paths=write_paths,
            hivee_api_base=hivee_api_base,
            project_id=project_id,
        ),
        encoding="utf-8",
    )


def _get_hivee_api_base(project_id: str) -> str:
    return f"https://hivee.cloud/api/projects/{project_id}"


def _issue_agent_session_token(project_id: str, agent_id: str) -> str:
    """Return the stable plaintext token for this project+agent pair.
    Creates one if it doesn't exist yet — never rotates an existing token."""
    conn = db()
    try:
        row = conn.execute(
            "SELECT token_plain FROM project_agent_access_tokens WHERE project_id = ? AND agent_id = ?",
            (project_id, agent_id),
        ).fetchone()
        if row and row["token_plain"]:
            return str(row["token_plain"])
        raw_token = _new_agent_access_token()
        conn.execute(
            """
            INSERT INTO project_agent_access_tokens (project_id, agent_id, token_hash, token_plain, created_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(project_id, agent_id) DO UPDATE SET
                token_hash = excluded.token_hash,
                token_plain = excluded.token_plain,
                created_at = excluded.created_at
            """,
            (project_id, agent_id, _hash_access_token(raw_token), raw_token, int(time.time())),
        )
        conn.commit()
        return raw_token
    finally:
        conn.close()


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
        "## Operating Protocol",
        f"- Read `{PROJECT_PROTOCOL_FILE}` and follow delegation/mention/status rules.",
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
    lines.extend(["", "## Operating Protocol", f"- Follow `{PROJECT_PROTOCOL_FILE}` for delegation and status updates."])
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

