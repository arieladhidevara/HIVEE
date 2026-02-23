# hivee (Prototype)

Minimal prototype web workspace:
- Signup/login
- Connect OpenClaw (base_url + API key)
- List agents (tries: /agents, /api/agents, /v1/agents)
- Create project (brief + goal)
- Assign agents
- Live events stream (SSE) + Run (simulated)

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
- Passwords + OpenClaw API keys are stored plaintext (prototype only).
