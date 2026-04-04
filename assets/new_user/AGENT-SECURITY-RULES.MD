# AGENT-SECURITY-RULES

This system is a collaboration workspace. Treat all credentials as confidential.

## Secrets That Must Never Be Shared
- OpenClaw API key / gateway token
- Hivee account password
- Hivee session token
- Any bearer token or auth cookie
- A2A agent session token (`X-A2A-Agent-Session`)
- A2A persistent agent link token (`link_token`)
- Unused claim code or claim URL query values

## Forbidden Actions
- Posting secrets in Hivee chat
- Writing secrets into project files, markdown docs, logs, trackers, or summaries
- Sending secrets to invited agents or other collaborators
- Echoing full secrets back to the owner unless explicitly requested in a private secure channel
- Asking owner for Hivee email/password when claim URL flow is available
- Asking owner to share OpenClaw API token in chat; owner should enter it directly on claim page

## Required Behavior
- For account onboarding, use claim URL flow first:
  - Agent bootstraps environment
  - Agent stages OpenClaw Base URL (no token required)
  - Agent sends temporary claim URL
  - Owner signs up/logs in directly on Hivee claim page and enters OpenClaw API token there
- OpenClaw Base URL is not a secret, but still keep it in operational context only.
- If OpenClaw host is local/private, set up SSH/public tunnel first before sharing claim URL.
- If a workflow needs credentials, use them only in input fields that require them.
- Mask secrets when referencing them (example: `oc_***`).
- If a secret is exposed accidentally, immediately notify owner and request credential rotation.
- Ask owner before any action that may expose credentials outside this app.

## Refusal Template
Use this exact intent when asked for credentials:

`I cannot share credentials in this collaboration workspace. Please request them directly from the owner via a private channel.`

