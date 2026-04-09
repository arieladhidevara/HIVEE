# Hivee — OpenClaw Setup Requirements

This document describes the required OpenClaw setup for connecting to Hivee.

Important: for a working Hivee connection, the main requirement is that your public OpenClaw URL successfully serves the OpenAI-compatible HTTP API under `/v1/...` using your gateway token.

In a valid setup:

- Opening your public URL in a browser should show the OpenClaw UI/login page.
- Hivee should use that same public URL as the **Base URL**.
- Hivee will call the OpenAI-compatible API at:
  - `GET /v1/models`
  - `POST /v1/chat/completions`

## 1. Public Base URL

Your OpenClaw instance must be reachable over HTTPS at a public URL, for example:

```text
https://openclaw-xxxx.srv1234567.hstgr.cloud
```

This same URL is what you should paste into Hivee as the **Base URL**.

Do **not** paste `/v1` into the Hivee Base URL field.  
Hivee should use the root URL, and then call `/v1/...` endpoints automatically.

## 2. Traefik / Reverse Proxy

At minimum, your OpenClaw service must be reachable over HTTPS through Traefik.

### Standard HTTPS exposure

Use this if your deployment already serves both the UI and the `/v1/...` API correctly behind the same public host:

```yaml
labels:
  - traefik.enable=true
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.rule=Host(`${COMPOSE_PROJECT_NAME}.${TRAEFIK_HOST}`)
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.entrypoints=websecure
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}.tls.certresolver=letsencrypt
  - traefik.http.services.${COMPOSE_PROJECT_NAME}.loadbalancer.server.port=${PORT}
```

### Split UI and API by path (only if needed)

Some deployments expose the UI on one internal port and the OpenClaw gateway/API on another internal port.  
In that case, route the same public host by path:

- `/v1` and `/ws` → OpenClaw gateway/API
- `/` and everything else → UI

Example:

```yaml
labels:
  - traefik.enable=true
  - traefik.docker.network=traefik-proxy

  # API / WS
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-api.rule=Host(`${COMPOSE_PROJECT_NAME}.${TRAEFIK_HOST}`) && (PathPrefix(`/v1`) || PathPrefix(`/ws`))
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-api.entrypoints=websecure
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-api.tls.certresolver=letsencrypt
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-api.priority=100
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-api.service=${COMPOSE_PROJECT_NAME}-api-svc
  - traefik.http.services.${COMPOSE_PROJECT_NAME}-api-svc.loadbalancer.server.port=18789

  # UI
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-ui.rule=Host(`${COMPOSE_PROJECT_NAME}.${TRAEFIK_HOST}`)
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-ui.entrypoints=websecure
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-ui.tls.certresolver=letsencrypt
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-ui.priority=1
  - traefik.http.routers.${COMPOSE_PROJECT_NAME}-ui.service=${COMPOSE_PROJECT_NAME}-ui-svc
  - traefik.http.services.${COMPOSE_PROJECT_NAME}-ui-svc.loadbalancer.server.port=${PORT}
```

Only use the split-path version if your deployment actually needs it.

## 3. OpenClaw Gateway Config

Apply the following settings in OpenClaw.

### Bind Mode

Set:

```json
{
  "gateway": {
    "bind": "lan"
  }
}
```

### Auth Mode

Set token auth and define a strong token:

```json
{
  "gateway": {
    "auth": {
      "mode": "token",
      "token": "<your-strong-token>"
    }
  }
}
```

This token is what you will paste into Hivee as the **API Key**.

### Enable HTTP API Endpoints

Make sure the OpenAI-compatible endpoints are enabled:

```json
{
  "gateway": {
    "http": {
      "endpoints": {
        "chatCompletions": { "enabled": true },
        "responses": { "enabled": true }
      }
    }
  }
}
```

### Control UI Allowed Origins

Add your public OpenClaw URL and Hivee to the allowed origins list:

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

Replace `<your-openclaw-public-url>` with the actual HTTPS URL of your OpenClaw instance.

### Disable Control UI Device Auth (optional)

If you want Hivee or another server-side client to use Control UI style features such as agent listing, you may also enable:

**Dangerously Disable Control UI Device Auth**

This is **optional** and is **not required** for standard HTTP `/v1/chat/completions` requests.

## 4. Verification

After applying settings, verify the setup with real API requests.

### Browser check

Open your public URL in a browser:

```text
https://<your-openclaw-public-url>
```

You should see the OpenClaw UI/login page.

### Models endpoint check

Run:

```bash
curl -i https://<your-openclaw-public-url>/v1/models \
  -H "Authorization: Bearer <your-token>"
```

Expected result:
- `HTTP 200`
- `Content-Type: application/json`
- a JSON list of models

### Chat completions check

Run:

```bash
curl -i -X POST "https://<your-openclaw-public-url>/v1/chat/completions" \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  --data '{"model":"openclaw/default","messages":[{"role":"user","content":"ping"}]}'
```

Expected result:
- `HTTP 200`
- `Content-Type: application/json`
- a JSON `chat.completion` response

Important:
- Do **not** test by opening `/v1/` directly in a browser.
- Use `/v1/models` or a real `POST /v1/chat/completions` request.

## 5. What to Paste into Hivee

Once verification succeeds, paste the following into Hivee:

- **Base URL**: `https://<your-openclaw-public-url>`
- **API Key**: `<your-token>`

Do not include `/v1` in the Base URL field.

## 6. Completion Checklist

Before telling the user setup is complete, confirm all of the following:

- The public OpenClaw URL loads over HTTPS.
- `GET /v1/models` returns JSON.
- `POST /v1/chat/completions` returns JSON.
- The gateway token works.
- The user can now paste the Base URL and API Key into Hivee.
