# Hivee Hub Install Guide

Date: 2026-04-08

## Prerequisites

- Sudah punya akun Hivee + session login.
- Sudah create Connection dari UI Hivee (dapat `connection_id` + `install_token`).
- Runtime OpenClaw sudah reachable dari mesin Hub.

## 1) Ubuntu/Linux Install

Install package Hub dari repo terpisah (`HIVEE-HUB`):

```bash
python3 -m pip install --upgrade "git+https://github.com/arieladhidevara/HIVEE-HUB.git"
```

Jalankan Hub:

```bash
hivee-hub connect \
  --cloud-url "https://hivee.cloud" \
  --connection-id "<connection_id>" \
  --install-token "<install_token>" \
  --runtime openclaw \
  --openclaw-base-url "<openclaw_base_url>" \
  --openclaw-api-key "<openclaw_api_key>"
```

## 2) Docker Install

Build image lokal dari repo `HIVEE-HUB`:

```bash
git clone https://github.com/arieladhidevara/HIVEE-HUB.git
cd HIVEE-HUB
docker build -t hivee-hub:local .
```

Run container:

```bash
docker run -d --name hivee-hub --restart unless-stopped \
  -e HIVEE_CLOUD_URL="https://hivee.cloud" \
  -e HIVEE_CONNECTION_ID="<connection_id>" \
  -e HIVEE_INSTALL_TOKEN="<install_token>" \
  -e HIVEE_RUNTIME_TYPE="openclaw" \
  -e OPENCLAW_BASE_URL="<openclaw_base_url>" \
  -e OPENCLAW_API_KEY="<openclaw_api_key>" \
  hivee-hub:local
```

Fallback agent list kalau OpenClaw listing belum ready:

```bash
-e HIVEE_RUNTIME_AGENT_IDS="planner-alpha,builder-beta"
```

## 3) Verify Hub Connection

Expected backend calls from Hub:
- `POST /api/hub/install/complete`
- `POST /api/hub/heartbeat`
- `POST /api/hub/agents/discovered`
- `POST /api/hub/agents/{managed_agent_id}/card`
- `POST /api/hub/runtime/jobs/claim`
- `POST /api/hub/runtime/jobs/{job_id}/complete`

Verify from Hivee UI/API:
- Connection `hub_status` jadi `online`
- `last_heartbeat_at` bergerak
- Managed agents muncul di connection
- Agent card tersimpan

## 4) Runtime Notes

- `OPENCLAW_BASE_URL` harus reachable dari host/container Hub.
- Untuk Docker di Linux, kalau runtime ada di host yang sama, gunakan URL/runtime networking yang sesuai (host network atau published endpoint).
- `hivee-hub run` membaca env vars; `hivee-hub connect` memakai argumen langsung.