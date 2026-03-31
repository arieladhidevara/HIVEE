# NEW-ACCOUNT-SETUP

Use this playbook when an AI agent is asked to prepare a fresh Hivee environment for owner claim.

## Target App
- Hivee URL: https://hivee.up.railway.app/
- Security policy URL: https://hivee.up.railway.app/new-user/AGENT-SECURITY-RULES.MD

## Important Principle
- Agent bootstrap first, owner claim later.
- Agent must not request owner email/password.
- Owner enters email/password and OpenClaw connection data directly on Hivee claim page.

## Required Inputs
- Agent identifier (example: `dailybot`)
- Optional environment display name (example: `Owner Workspace`)

No owner credentials are required for this flow.

## Bootstrap Flow
1. Create unclaimed environment using Hivee API:
   - `POST /api/a2a/environments/bootstrap`
   - JSON body example:
     - `{"agent_id":"dailybot","display_name":"Owner Workspace"}`
2. Read response and save:
   - `environment_id`
   - `claim_url`
   - `agent_session_token`
   - expiration timestamps
3. Send `claim_url` to owner via private channel.
4. Ask owner to open that link and finish signup or login.
5. On claim page, owner must fill:
   - Hivee email
   - Hivee password
   - OpenClaw base URL
   - OpenClaw API key / token
   - Optional connection name
6. Optionally monitor status:
   - `GET /api/a2a/environments/{environment_id}`
   - Header: `X-A2A-Agent-Session: <agent_session_token>`
7. When status payload shows `environment.claimed = true` (and status `active`):
   - Send user message: `Connected. You can now control me directly from the Hivee UI.`
8. After sending that message, acknowledge handoff:
   - `POST /api/a2a/environments/{environment_id}/handoff/ack`
   - Header: `X-A2A-Agent-Session: <agent_session_token>`
9. Read handoff ACK response and save:
   - `link_token`
   - `link_token_expires_at`
10. Consider setup complete when handoff ACK succeeds and `link_token` is stored.

## Handoff Session Notes
- After claim succeeds, Hivee keeps the bootstrap agent session alive briefly in read-only handoff mode.
- Default handoff window is short (`~5 minutes`) before session expiry.
- Agent should poll quickly, notify user once connected, then call handoff ACK immediately.
- If this environment was previously claimed for the same agent, the old active link token is revoked during the new claim.
- Handoff ACK rotates/replaces any previous active link token for the same `environment_id + agent_id`.
- Store `link_token` only in agent private memory/secrets storage, never in shared chat.

## Reconnect On Future Chats
When owner chats again later (next day, etc):
1. Start runtime session from link token:
   - `POST /api/a2a/agent-links/session/start`
   - JSON: `{"link_token":"<stored_link_token>"}`
2. Save response:
   - `session_token`
   - `session_expires_at`
3. Call Hivee API with:
   - Header: `X-A2A-Agent-Session: <session_token>`
4. If session expires, call `session/start` again with the same stored `link_token`.

## If Claim Link Expires
1. Regenerate claim link:
   - `POST /api/a2a/environments/{environment_id}/claim/start`
   - Header: `X-A2A-Agent-Session: <agent_session_token>`
2. Share the new `claim_url` with owner.

## Safety Rules (Must Follow)
- Never ask owner for Hivee email/password in this flow.
- Never reveal `agent_session_token` or claim code publicly.
- Never post secrets in Hivee chat/docs/files/logs.
- Never guess or fabricate data.

## Completion Message Template
Use this format:

`Environment bootstrap completed. Claim URL sent privately. Waiting for owner claim.`
