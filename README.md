# Hivee — Collaborative AI Agent Orchestration Platform

## Overview

**Hivee** is a web-based workspace for orchestrating AI agents around shared project goals. It enables seamless collaboration between humans and autonomous agents by providing a unified interface for:

- **Project Management** — Define goals, track progress, manage tasks
- **Agent Discovery & Connection** — Connect to OpenClaw-compatible Hubs and discover available agents
- **Work Delegation** — Automatically route tasks to agents based on their roles and capabilities
- **Real-Time Monitoring** — Watch live execution with SSE streaming, task progress, and execution metrics
- **External Collaboration** — Invite external agents from other Hubs to join projects via secure invitations
- **Inbox & Notifications** — Receive and manage project invitations and collaboration requests

## Tech Stack

- **Backend**: FastAPI (Python) with async/await for real-time event streaming
- **Frontend**: Single-page application (SPA) with vanilla JavaScript, HTML, CSS
- **Database**: SQLite (production uses more robust setup)
- **Authentication**: Session tokens + OAuth (Google, GitHub)
- **Real-Time**: Server-Sent Events (SSE) for live project updates
- **Deployment**: Docker containerized, runs on Railway or any UNIX server via uvicorn

## Running Locally

### Prerequisites

- Python 3.9+
- Virtual environment (recommended)

### 1. Install Dependencies

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
# .venv\Scripts\activate       # Windows PowerShell

# Install requirements
pip install -r requirements.txt
```

### 2. Start the Development Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 in your browser.

## Project Structure

```
hivee/
├── main.py                 # FastAPI app, route registration, SPA fallback
├── hivee_shared.py         # Shared imports and globals
├── requirements.txt        # Python dependencies
├── routes/                 # API endpoint handlers
│   ├── auth_account.py     # Authentication, signup, login, session management
│   ├── a2a.py              # Agent-to-agent communication
│   ├── openclaw.py         # OpenClaw Hub integration
│   ├── workspace.py        # User workspace and environment management
│   ├── projects.py         # Project CRUD, execution, monitoring
│   ├── tasks.py            # Task management and delegation
│   └── connectors.py       # Hub connection management
├── core/                   # Core business logic
│   ├── session_*.py        # Session and permission management
│   ├── security_auth.py    # Password hashing, OAuth
│   └── ...                 # Other core utilities
├── services/               # External service integrations
├── static/                 # Frontend assets
│   ├── index.html          # Main SPA (serves /{username} routes)
│   ├── docs.html           # Documentation page (/docs)
│   ├── app.js              # SPA JavaScript (view switching, API calls)
│   ├── styles.css          # All CSS styling
│   ├── logo.png            # Hivee branding
│   └── logos/              # Third-party service logos
├── assets/                 # Static resources (e.g., new user setup)
└── app.db                  # SQLite database (auto-created)
```

## Key Concepts

### Projects

A project is a container for collaborative work with a specific goal:
- **Title**: Name of the project
- **Goal**: Objective to accomplish
- **Assigned Agents**: Internal and external agents working on the project
- **Tasks**: Discrete work items broken down from the goal
- **Execution State**: Plan, running, completed, failed, etc.
- **Lifecycle**: Create → Assign → Define Tasks → Run → Monitor → Review

### Agents

An agent is an autonomous AI entity that performs work:
- **Role**: Planner (breaks down goals), Builder (implements), Reviewer (validates), or Owner (project owner)
- **Location**: Lives on a remote Hub (OpenClaw-compatible service)
- **Discovery**: Automatically discovered when you connect a Hub
- **Capabilities**: Can execute tasks, generate code, provide analysis, etc.
- **Communication**: Uses the project's real-time event stream to report progress

### Hubs (Connections)

A Hub is a remote OpenClaw-compatible service that hosts agents:
- **Base URL**: HTTPS endpoint of the Hub service
- **Authentication**: API key/token for access
- **Agent Listing**: Hivee discovers all available agents on the Hub
- **Execution**: Tasks are sent to the Hub for agents to execute
- **Health**: Monitored continuously; project can't run if primary Hub is down

### Delegation

The process of assigning work from a goal to agents:
1. User defines a project with a goal
2. User assigns agents (planners, builders, reviewers)
3. Planner agent receives the goal and creates a detailed plan
4. Plan is broken into tasks and distributed to builders
5. Builders execute tasks, report progress in real-time
6. User monitors via the Live view and can request changes
7. Results are compiled and saved to project files

### Invitations & Collaboration

External agents from other Hubs can be invited to projects:
- **Invite URL**: Generated per agent per project, includes invite code
- **Acceptance**: Agent owner accepts via Inbox, confirming their Hub connection
- **Permissions**: External agents get specific roles (planner, builder, etc.)
- **Isolation**: External agents can only see their assigned project, not your workspace

## API Overview

### Authentication

```bash
# Sign up
curl -X POST http://localhost:8000/api/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com", "password":"SecurePass123!"}'

# Login
curl -X POST http://localhost:8000/api/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com", "password":"SecurePass123!"}'

# All subsequent requests include Authorization header:
# Authorization: Bearer <session_token>
```

### Project Operations

```bash
# List projects
curl -H "Authorization: Bearer <token>" \
  http://localhost:8000/api/projects

# Create project
curl -X POST http://localhost:8000/api/projects \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"title":"Build API", "goal":"Create REST API for user management"}'

# Run project
curl -X POST http://localhost:8000/api/projects/{project_id}/run \
  -H "Authorization: Bearer <token>"

# Stream live events
curl -H "Authorization: Bearer <token>" \
  http://localhost:8000/api/projects/{project_id}/events
```

### Hub Management

```bash
# List connected Hubs
curl -H "Authorization: Bearer <token>" \
  http://localhost:8000/api/me/environments

# Add Hub connection
curl -X POST http://localhost:8000/api/connectors \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "openclaw_base_url": "https://hub.example.com",
    "openclaw_api_key": "sk_...",
    "name": "Company Hub"
  }'
```

### Invitations

```bash
# List pending invitations
curl -H "Authorization: Bearer <token>" \
  http://localhost:8000/api/me/inbox/invites

# Accept invitation
curl -X POST http://localhost:8000/api/me/inbox/invites/{invite_id}/accept \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"connection_id":"...","agent_id":"..."}'
```

## Routing & Frontend

Hivee uses a single-page application (SPA) with two main views:

- **Auth View** (`#view_auth`) — Signup, login, project invitations, claim flows
- **Home View** (`#view_home`) — Main workspace with nav, projects, settings

### Route Handling

- `/` → Serves `static/index.html` (auth page or redirected to home)
- `/docs` → Serves `static/docs.html` (documentation)
- `/api/*` → FastAPI route handlers (protected by session middleware)
- `/{username}` → SPA fallback, serves `static/index.html`
- `/{username}/{rest:path}` → SPA fallback for deep links

The SPA uses in-memory view switching (no hash routing) — the frontend maintains state via JavaScript, localStorage for persistence.

## Development Notes

### Database

SQLite database (`app.db`) is auto-created on first run. Schema includes tables for:
- `users` — User accounts and authentication
- `sessions` — Active session tokens
- `projects` — Project metadata
- `tasks` — Task breakdown and assignments
- `project_agents` — Agents assigned to projects
- `project_external_agent_invites` — Collaboration invitations
- `connectors` — Hub connections
- `oauth_identities` — OAuth provider links

### Security Considerations

⚠️ **This is a prototype.** For production:
- Passwords and API keys should use proper hashing and encryption (currently plaintext in demo)
- Use HTTPS only
- Implement CSRF protection on all state-changing endpoints
- Add rate limiting on auth endpoints
- Rotate OAuth tokens periodically
- Use environment variables for secrets (not committed to repo)

### Real-Time Events

Projects stream execution updates via Server-Sent Events:

```javascript
const eventSource = new EventSource(`/api/projects/${projectId}/events`, {
  headers: { Authorization: `Bearer ${token}` }
});

eventSource.addEventListener('project.task.completed', (e) => {
  const data = JSON.parse(e.data);
  console.log('Task completed:', data);
});
```

### Extending the Application

To add new features:
1. **Backend**: Add routes in `routes/` and core logic in `core/`
2. **Frontend**: Modify `static/app.js` for new views/interactions, update `static/styles.css`
3. **Database**: Add migrations or schema changes in route handlers (for prototype)

## Deployment

### Docker

```bash
docker build -t hivee:latest .
docker run -p 8000:8000 hivee:latest
```

### Railway

Deploy via `railway.json`:

```bash
railway up
```

### Manual Server

```bash
# On a UNIX server with Python 3.9+
git clone <repo>
cd hivee
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

Use a process manager (systemd, supervisor) to keep the app running.

## Documentation

Full user documentation is available at `/docs` (when running the app). This includes:
- Getting started guide
- Project and agent management
- Delegation workflow
- Hub connection setup
- Invitation and collaboration guide
- API reference

## Contributing

To contribute improvements:
1. Create a feature branch
2. Make changes
3. Test locally with `python -m pytest` (if tests exist)
4. Submit a pull request with a clear description

## Support & Feedback

For issues, questions, or feature requests:
- Check the [documentation](/docs) first
- Review existing issues in the repository
- Open a new issue with a clear description of the problem

## License

[License information to be added]

---

**Hivee** — Orchestrate. Delegate. Achieve.
