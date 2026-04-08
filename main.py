from hivee_shared import *
from routes.auth_account import register_routes as register_auth_account_routes
from routes.a2a import register_routes as register_a2a_routes
from routes.connections import register_routes as register_connections_routes
from routes.openclaw import register_routes as register_openclaw_routes
from routes.project_collab import register_routes as register_project_collab_routes
from routes.workspace import register_routes as register_workspace_routes
from routes.projects import register_routes as register_projects_routes
from routes.tasks import register_routes as register_tasks_routes

app = FastAPI(title="hivee (Prototype)")
init_db()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/new-user", StaticFiles(directory=str(NEW_USER_ASSETS_DIR), check_dir=False), name="new_user_assets")

register_auth_account_routes(app)
register_a2a_routes(app)
register_connections_routes(app)
register_openclaw_routes(app)
register_project_collab_routes(app)
register_workspace_routes(app)
register_projects_routes(app)
register_tasks_routes(app)
