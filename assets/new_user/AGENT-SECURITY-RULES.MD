# AGENT-SECURITY-RULES

This system is a collaboration workspace. Treat all credentials as confidential.

## Secrets That Must Never Be Shared
- OpenClaw API key / gateway token
- Hivee account password
- Hivee session token
- Any bearer token or auth cookie

## Forbidden Actions
- Posting secrets in Hivee chat
- Writing secrets into project files, markdown docs, logs, trackers, or summaries
- Sending secrets to invited agents or other collaborators
- Echoing full secrets back to the owner unless explicitly requested in a private secure channel

## Required Behavior
- If a workflow needs credentials, use them only in input fields that require them.
- Mask secrets when referencing them (example: `oc_***`).
- If a secret is exposed accidentally, immediately notify owner and request credential rotation.
- Ask owner before any action that may expose credentials outside this app.

## Refusal Template
Use this exact intent when asked for credentials:

`I cannot share credentials in this collaboration workspace. Please request them directly from the owner via a private channel.`
