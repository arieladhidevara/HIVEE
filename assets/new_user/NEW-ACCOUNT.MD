# Hivee — OpenClaw Setup Requirements

This document explains how to configure an OpenClaw gateway so it can connect to Hivee reliably over HTTPS.

Read each section and apply the settings before submitting your connection details to Hivee.

## 1. Expose OpenClaw over HTTPS

Your OpenClaw instance must be reachable through a public HTTPS URL.

If you are using Traefik, ensure your `docker-compose.yml` includes labels like these on the OpenClaw service:

```yaml
labels:
  - traefik.enable=true
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.rule=Host(`${COMPOSE_PROJECT_NAME}.${TRAEFIK_HOST}`)
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.entrypoints=websecure
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.tls.certresolver=letsencrypt
  - traefik.http.services.${COMPOSE_PROJECT_NAME}.loadbalancer.server.port=${PORT}
```

This allows OpenClaw to be served through HTTPS, which is the recommended setup for both Hivee and the OpenClaw Control UI.

## 2. Configure the OpenClaw Gateway

Apply the following settings in OpenClaw.

### Bind Mode

Set `gateway.bind` to `lan` so the gateway listens on all interfaces.

```json
{
  "gateway": {
    "bind": "lan"
  }
}
```

This is required when OpenClaw is running behind Traefik or another reverse proxy.

### Auth Mode

Set `gateway.auth.mode` to `token` and choose a strong secret token.

```json
{
  "gateway": {
    "auth": {
      "mode": "token",
      "token": "<strong-secret-token>"
    }
  }
}
```

This token is what you will paste into Hivee as the API Key.

### Chat Completions Endpoint

Enable the OpenAI-compatible chat endpoint:

```json
{
  "gateway": {
    "http": {
      "endpoints": {
        "chatCompletions": {
          "enabled": true
        }
      }
    }
  }
}
```

### Control UI Allowed Origins

Add both your public OpenClaw URL and the Hivee URL to the Control UI allowed origins list:

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

Replace `<your-openclaw-public-url>` with your actual public HTTPS OpenClaw URL, for example:

`https://openclaw-xxxx.srv1234567.hstgr.cloud`

This helps OpenClaw accept valid browser and WebSocket connections from the expected origins.

## 3. Recommended Control UI Security Setup

For remote access, the recommended setup is:

- use HTTPS
- configure explicit `allowedOrigins`
- keep Control UI authentication protections enabled unless you have a very specific reason not to

This is the normal and preferred setup.

### About `dangerouslyDisableDeviceAuth`

`dangerouslyDisableDeviceAuth` may allow non-browser or server-side connections to access Control UI functionality without normal browser device identity checks.

This can be useful in special integration scenarios, but it should be treated as an advanced compatibility option, not a default requirement.

Only enable it if:

- you specifically need server-side access to Control UI functionality
- your integration depends on Control UI-level WebSocket access
- you understand the security tradeoff

Example:

```json
{
  "gateway": {
    "controlUi": {
      "dangerouslyDisableDeviceAuth": true
    }
  }
}
```

### Important Note About Managed Wrappers

In some managed deployments or wrapper environments, dangerous or insecure Control UI flags may be removed or overridden automatically during startup.

Because of that, you should not rely on them as your primary setup strategy.

Instead, prefer:

- HTTPS
- correct reverse proxy configuration
- correct `allowedOrigins`

## 4. Important Proxy / Wrapper Requirement

If your OpenClaw instance is accessed through an additional wrapper, proxy UI, or bridge service, that layer must support WebSocket forwarding correctly.

This matters because Hivee and the OpenClaw UI may both use WebSocket connections. A broken bridge can make the two clients interfere with each other even if OpenClaw itself is configured correctly.

A proxy or wrapper must:

- create a separate upstream WebSocket connection per client session
- wait for the upstream socket to be fully open before forwarding messages
- handle close and error events without crashing the wrapper
- avoid sharing a fragile single upstream connection across unrelated clients

If this layer is implemented incorrectly, you may see issues such as:

- Hivee connects but the OpenClaw UI disconnects
- the OpenClaw UI works until Hivee logs in
- connections appear to alternate or break each other
- WebSocket errors during login or agent listing

These are proxy compatibility issues, not a limitation of OpenClaw itself.

## 5. Optional: Agent Listing / Advanced Integrations

Some advanced integrations may need more than the basic chat endpoint.

If you want a server-side integration to access Control UI-level functionality such as agent listing, session inspection, or other Control UI WebSocket features, additional compatibility work may be required.

In those cases:

- verify that the integration is connecting through HTTPS or another secure setup
- verify that the proxy or wrapper supports WebSocket forwarding correctly
- only then consider advanced Control UI auth changes if they are truly necessary

Do not treat Control UI auth bypass settings as the default path for basic Hivee connectivity.

## 6. Verification Checklist

After applying the setup:

1. Open your public HTTPS OpenClaw URL in a browser and confirm the Control UI loads.
2. Confirm the OpenClaw instance is reachable over HTTPS.
3. In Hivee, use:
   - Base URL = your public OpenClaw HTTPS URL
   - API Key = your gateway token
4. Confirm Hivee can connect successfully.
5. Confirm Hivee and the OpenClaw UI can stay connected without disconnecting each other.
6. If one client breaks when the other connects, inspect your reverse proxy or WebSocket wrapper layer.

## 7. Final Notes

For most users, the minimum reliable setup is:

- public HTTPS OpenClaw URL
- `gateway.bind = "lan"`
- token auth enabled
- chat completions endpoint enabled
- `controlUi.allowedOrigins` includes both your public OpenClaw URL and `https://hivee.cloud`

That is the preferred baseline configuration for a stable Hivee connection.
