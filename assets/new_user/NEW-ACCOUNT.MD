# Hivee — OpenClaw Setup Requirements

This document describes the required OpenClaw gateway configuration for connecting to Hivee. Read each section and apply the settings before the user submits their connection details.

## 1. Traefik Labels (docker-compose.yml)

Ensure the OpenClaw service has these Traefik labels so it is reachable over HTTPS:

```yaml
labels:
  - traefik.enable=true
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.rule=Host(`${COMPOSE_PROJECT_NAME}.${TRAEFIK_HOST}`)
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.entrypoints=websecure
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.tls.certresolver=letsencrypt
  - traefik.http.services.${COMPOSE_PROJECT_NAME}.loadbalancer.server.port=${PORT}
```

## 2. OpenClaw Gateway Config

Apply the following settings in the OpenClaw Control UI under **Gateway > Settings**:

### Bind Mode
Set `gateway.bind` to `lan` so the gateway listens on all interfaces (required when running behind Traefik).

### Auth Mode
Set `gateway.auth.mode` to `token` and make sure `gateway.auth.token` is set to a strong secret. This token is what you will paste as the API Key in Hivee.

### Chat Completions Endpoint
Enable the OpenAI-compatible chat endpoint:
```json
{
  "gateway": {
    "http": {
      "endpoints": {
        "chatCompletions": { "enabled": true }
      }
    }
  }
}
```

### Control UI Allowed Origins
Add the Hivee server URL and your OpenClaw public URL to the allowed origins list so WebSocket connections are accepted:
```json
{
  "gateway": {
    "controlUi": {
      "allowedOrigins": [
        "https://<your-openclaw-public-url>",
        "https://hivee.cloud"
      ]
    }
  }
}
```
Replace `<your-openclaw-public-url>` with the actual public HTTPS URL of your OpenClaw instance (e.g. `https://openclaw-xxxx.srv1234567.hstgr.cloud`).

### Disable Control UI Device Auth (optional, for agent listing)
If you want Hivee to be able to list your agents, also enable:

> **Dangerously Disable Control UI Device Auth** in the Control UI settings.

This allows server-side connections (like Hivee) to list agents without requiring a browser device identity.

## 3. Verify

After applying settings:
1. Confirm the OpenClaw gateway is reachable at its public HTTPS URL (e.g. visit it in a browser — you should see the Control UI login page).
2. Confirm the gateway token works by pasting it into the Hivee connection form.
3. Notify the user that the setup is complete and they can now paste their Base URL and API Key into Hivee.
