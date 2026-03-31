from hivee_shared import *

def register_routes(app: FastAPI) -> None:
    @app.get("/api/workspace/tree", response_model=WorkspaceTreeOut)
    async def get_workspace_tree(request: Request):
        user_id = get_session_user(request)
        _ensure_user_workspace(user_id)
        workspace_root = _user_home_dir(user_id).resolve()
        workspace_root.mkdir(parents=True, exist_ok=True)
        if not _path_within(workspace_root, _user_home_dir(user_id)):
            raise HTTPException(500, "Workspace root is outside user home")
        return WorkspaceTreeOut(
            workspace_root=workspace_root.as_posix(),
            tree=_render_tree(workspace_root),
        )
    
    @app.get("/api/workspace/files", response_model=WorkspaceFilesOut)
    async def list_workspace_files(request: Request, path: str = ""):
        user_id = get_session_user(request)
        workspace_root, target = _resolve_workspace_relative_path(
            user_id,
            path,
            require_exists=True,
            require_dir=True,
        )
        current_rel = ""
        if target != workspace_root:
            current_rel = target.relative_to(workspace_root).as_posix()
        parent_rel: Optional[str] = None
        if current_rel:
            parent_rel = str(Path(current_rel).parent).replace("\\", "/")
            if parent_rel == ".":
                parent_rel = ""
    
        entries: List[ProjectFileEntryOut] = []
        for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            rel = child.relative_to(workspace_root).as_posix()
            stat = child.stat()
            entries.append(
                ProjectFileEntryOut(
                    name=child.name,
                    path=rel,
                    kind="dir" if child.is_dir() else "file",
                    size=None if child.is_dir() else int(stat.st_size),
                    modified_at=int(stat.st_mtime),
                )
            )
    
        return WorkspaceFilesOut(
            workspace_root=workspace_root.as_posix(),
            current_path=current_rel,
            parent_path=parent_rel,
            entries=entries,
        )
    
    @app.get("/api/workspace/files/content", response_model=WorkspaceFileContentOut)
    async def read_workspace_file(request: Request, path: str):
        user_id = get_session_user(request)
        workspace_root, target = _resolve_workspace_relative_path(
            user_id,
            path,
            require_exists=True,
            require_dir=False,
        )
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        data = target.read_bytes()
        size = len(data)
        truncated = size > MAX_FILE_PREVIEW_BYTES
        if truncated:
            data = data[:MAX_FILE_PREVIEW_BYTES]
        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError:
            content = data.decode("utf-8", errors="replace")
        rel = target.relative_to(workspace_root).as_posix()
        return WorkspaceFileContentOut(
            workspace_root=workspace_root.as_posix(),
            path=rel,
            size=size,
            truncated=truncated,
            content=content,
        )
    
    @app.get("/api/workspace/files/raw")
    async def read_workspace_file_raw(request: Request, path: str):
        user_id = get_session_user(request)
        _, target = _resolve_workspace_relative_path(
            user_id,
            path,
            require_exists=True,
            require_dir=False,
        )
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        guessed, _ = mimetypes.guess_type(target.name)
        media_type = guessed or "application/octet-stream"
        return FileResponse(str(target), media_type=media_type)
    
    @app.get("/api/workspace/preview/{path:path}")
    async def preview_workspace_file(request: Request, path: str):
        user_id = get_session_user(request)
        _, target = _resolve_workspace_relative_path(
            user_id,
            path,
            require_exists=True,
            require_dir=False,
        )
        if target.is_dir():
            raise HTTPException(400, "Path is a directory")
        guessed, _ = mimetypes.guess_type(target.name)
        media_type = guessed or "application/octet-stream"
        return FileResponse(str(target), media_type=media_type)
    
