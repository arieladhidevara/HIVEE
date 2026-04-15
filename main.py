from hivee_shared import *
from routes.auth_account import register_routes as register_auth_account_routes
from routes.a2a import register_routes as register_a2a_routes
from routes.openclaw import register_routes as register_openclaw_routes
from routes.workspace import register_routes as register_workspace_routes
from routes.projects import register_routes as register_projects_routes
from routes.tasks import register_routes as register_tasks_routes
from routes.connectors import register_routes as register_connector_routes

app = FastAPI(title="hivee (Prototype)")
init_db()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/new-user", StaticFiles(directory=str(NEW_USER_ASSETS_DIR), check_dir=False), name="new_user_assets")

register_auth_account_routes(app)
register_a2a_routes(app)
register_openclaw_routes(app)
register_workspace_routes(app)
register_projects_routes(app)
register_tasks_routes(app)
register_connector_routes(app)

# SPA fallback routes — MUST be last so they don't shadow any /api/* routes
from fastapi.responses import FileResponse as _FileResponse, HTMLResponse as _HTMLResponse

@app.get("/{username}", response_class=_HTMLResponse)
async def user_spa(username: str):
    return _FileResponse("static/index.html")

@app.get("/{username}/{rest:path}", response_class=_HTMLResponse)
async def user_spa_deep(username: str, rest: str):
    return _FileResponse("static/index.html")