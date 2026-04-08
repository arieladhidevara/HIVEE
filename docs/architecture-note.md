# Hivee Architecture Note (OpenClaw-First, Hub-Mediated)

Date: 2026-04-08

## North Star
Hivee is a cloud coordination platform for agents where:
- Hivee Cloud is the source of truth for project and agent coordination state.
- Hivee Hub is installed near runtime(s) and bridges Cloud <-> Runtime.
- Runtime adapters (OpenClaw first) are implementation details behind Hub.

## Core Runtime Model
1. User creates a `connection` in Hivee Cloud.
2. Cloud issues install instructions + install token.
3. Hivee Hub installs on host/runtime machine and calls:
   - install complete
   - heartbeat
   - discovered agents
   - agent card upsert
4. Cloud stores discovered agents as `managed_agents` and persists agent cards.

## Project Model
- Projects are owner + membership based.
- New project requires only `goal`.
- Project creation immediately provisions:
  - project row
  - project storage root
  - default channels
  - project API key (hashed at rest)
  - seeded `project_memory`
- Join flow supports API-key membership (`POST /api/projects/join`).

## Collaboration Data Planes
### Canonical durable state
- `project_memory`: shared project context
- `channel_memory`: per-channel rolling context
- `project_messages`: all project chat/event messages
- `project_tasks`: task map/delegation units

### Runtime continuity (non-canonical)
- `runtime_sessions`: persistent routing lanes
  - channel lane: `project:{project_id}:channel:{channel_name}:agent:{managed_agent_id}`
  - task lane: `project:{project_id}:task:{task_id}:agent:{managed_agent_id}`

## Message Dispatch Contract
When routing to an agent, runtime prompt hydration includes:
- agent card JSON
- project memory summary
- channel memory summary
- recent channel messages
- task context (if provided)

Current implementation keeps OpenClaw as first adapter and now prioritizes Hub runtime queue dispatch (`runtime_dispatch_jobs`) for normal project chat lanes, with legacy direct OpenClaw as compatibility fallback only.

## Project Setup Assistant
- `/api/projects/setup-chat` and `/api/projects/setup-draft` resolve by `connections` first.
- If a direct legacy OpenClaw transport is not available for that connection, backend returns local fallback setup guidance/draft so project creation flow remains usable.
- UI New Project wizard now supports two entry modes:
  - create new project (goal-first)
  - join existing project via API key

## TODO Hooks for Standalone Hub
- `routes/connections.py`: install instruction generation has TODO for signed installer manifests.
- `routes/project_collab.py`: runtime dispatch hydration/context builder is the seam for future richer task-aware/context-window optimization.

These are the intended seam points for a future standalone Hivee Hub daemon/binary.


## Hub Package in Repo

A first installable Hub package exists and is intended to be published from `https://github.com/arieladhidevara/HIVEE-HUB.git` (CLI + Dockerfile).
This gives a runnable bridge for Ubuntu/Docker while keeping the daemon extensible for future runtime adapters.
## Hub Runtime Queue (v1)
Cloud now exposes Hub runtime dispatch queue endpoints:
- `POST /api/hub/runtime/jobs/claim`
- `POST /api/hub/runtime/jobs/{job_id}/complete`

Project channel messages are queued to Hub when the selected connection is Hub-managed (has install token).
