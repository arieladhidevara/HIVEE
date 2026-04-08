# hivee (Prototype)

Hivee Cloud prototype with Hub-first, project-scoped agent coordination:
- Signup/login first (no connection required)
- Create Connection in Agents/Setup
- Get install token + Hivee Hub install instructions
- Install Hivee Hub on runtime host (OpenClaw first)
- Hub heartbeat + discovery sync managed agents + agent cards
- Create project from goal only, or join project by API key
- Project chat is channel-scoped (no global free chat)

## Run locally

### 1) Install
```bash
python -m venv .venv
source .venv/bin/activate      # mac/linux
# .venv\Scripts\activate     # windows powershell

pip install -r requirements.txt
```

### 2) Start server
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Open: http://localhost:8000

## Notes
- DB: `app.db` (SQLite) created automatically.
- Passwords are hashed.
- This repo still keeps legacy A2A/environment claim routes for compatibility, but primary UX is Connection + Hub + Project flow.

## Hivee Hub (Installable)

Hivee Hub is now intended as a separate installable repo (`HIVEE-HUB`).

- Hub repo: `https://github.com/arieladhidevara/HIVEE-HUB.git`
- Install package from git repo directly via `pip`
- Docker build path is in the Hub repo

Quick start (Ubuntu):
```bash
python3 -m pip install --upgrade "git+https://github.com/arieladhidevara/HIVEE-HUB.git"
hivee-hub connect --cloud-url "https://hivee.cloud" --connection-id "<connection_id>" --install-token "<install_token>" --runtime openclaw --openclaw-base-url "<openclaw_base_url>" --openclaw-api-key "<openclaw_api_key>"
```
