# Hivee Migration Note (Connection/Hub + Project-Centric Refactor)

Date: 2026-04-08

## Summary
This refactor shifts the product from legacy environment-claim/OpenClaw-bootstrap onboarding toward a Hivee-native model:
- Hivee Cloud owns durable state.
- Users create `connections` first.
- Hivee Hub installs using a time-limited install token.
- Hub heartbeats + discovered agents sync into `managed_agents`.
- Chat is project-channel scoped (`project_messages`), not free-floating global chat.

## Schema Additions / Normalization
`init_db()` now creates/migrates these core entities:
- `connections`
- `project_memberships`
- `project_agent_memberships`
- `project_channels`
- `project_messages`
- `project_memory`
- `channel_memory`
- `runtime_sessions`

Existing entities were normalized with additional fields where needed:
- `projects`: `owner_user_id`, `project_api_key_hash`, `created_via`, `status`, `updated_at`
- `managed_agents`: `runtime_agent_id`, `agent_card_version`, `agent_card_json`, `discovered_at`
- `project_tasks`: `assignee_agent_membership_id`

## Backfill Behavior
On startup migration, the DB backfills:
- legacy `openclaw_connections` into `connections` (`legacy_openclaw_connection_id` mapping)
- owner/member rows into `project_memberships`
- legacy project agent rows into `project_agent_memberships`
- default channels (`main`, `planning`, `artifacts`, `system`)
- baseline `project_memory` and `channel_memory`

## API Surface Changes
### New/Refactored connection + hub APIs
- `POST /api/connections`
- `GET /api/connections`
- `GET /api/connections/{connection_id}`
- `POST /api/connections/{connection_id}/install-token/regenerate`
- `GET /api/connections/{connection_id}/install-instructions`
- `GET /api/connections/{connection_id}/agents`
- `POST /api/connections/{connection_id}/agents/refresh`
- `POST /api/hub/install/complete`
- `POST /api/hub/heartbeat`
- `POST /api/hub/agents/discovered`
- `POST /api/hub/agents/{managed_agent_id}/card`
- `POST /api/hub/runtime/jobs/claim`
- `POST /api/hub/runtime/jobs/{job_id}/complete`
- Project setup assistant endpoints now resolve through the new `connections` model first and use local draft/chat fallback when legacy direct OpenClaw transport is unavailable.

### New project collaboration APIs
- `POST /api/projects/join`
- `GET /api/projects/{project_id}/members`
- `POST /api/projects/{project_id}/agents/attach`
- `GET /api/projects/{project_id}/channels`
- `POST /api/projects/{project_id}/channels`
- `GET /api/projects/{project_id}/messages`
- `POST /api/projects/{project_id}/messages`
- `GET /api/projects/{project_id}/channels/{channel_id}/messages`
- `POST /api/projects/{project_id}/channels/{channel_id}/messages`

## Deprecated Behavior
- Workspace/global OpenClaw proxy chat endpoints are deprecated and return 410:
  - `POST /api/openclaw/{connection_id}/chat`
  - `POST /api/openclaw/{connection_id}/ws-chat`

Legacy A2A/environment claim flows remain in code for compatibility but are no longer the intended onboarding path.

## Frontend Impact (Minimal Styling Churn)
- Setup flow now creates Hivee `connections` and shows Hub install instructions/token.
- Agents section now includes connection selector, token rotation, install instructions, and discovered-agent/card view per connection.
- Projects now support join-by-API-key.
- Chat context is project-only in UI.

## Known Follow-ups
- Strengthen runtime queue reliability (lease renewal/retry/ack dedupe) for Hub dispatch jobs.
- Expand channel/task UI coverage for task-lane routing and deeper memory controls.

A runnable Hub package scaffold is provided and intended to live in `https://github.com/arieladhidevara/HIVEE-HUB.git` (CLI + Docker build path).
