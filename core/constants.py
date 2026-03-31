import asyncio
import hashlib
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote as url_quote, urlencode, urlparse, urlunparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

DB_PATH = "app.db"
HIVEE_ROOT = "HIVEE"
HIVEE_TEMPLATES_ROOT = f"{HIVEE_ROOT}/TEMPLATES"
AGENTS_ROOT_DIRNAME = "AGENTS"
AGENT_CARD_FILENAME = "agent-card.json"
MANAGED_AGENT_CARD_VERSION = "1.0"
MANAGED_AGENT_MEMORY_SCOPES = ("working", "project", "long_term")
NEW_USER_ASSETS_DIR = Path("assets") / "new_user"
SERVER_WORKSPACES_DIR = Path("server_workspaces")
MAX_TEMPLATE_FILE_BYTES = 96_000
MAX_TEMPLATE_FILES = 100
MAX_TEMPLATE_PAYLOAD_BYTES = 250_000
MAX_SETUP_TEMPLATE_PROMPT_CHARS = 2_400
MAX_SETUP_QUESTION_ITEMS = 12
MAX_SETUP_QUESTION_CHARS = 220
MAX_SETUP_TRANSCRIPT_ITEMS = 16
MAX_SETUP_TRANSCRIPT_ITEM_CHARS = 280
MAX_SETUP_TRANSCRIPT_CHARS = 6_000
MAX_WORKSPACE_TREE_CHARS = 8000
MAX_TREE_ENTRIES = 400
MAX_FILE_PREVIEW_BYTES = 120_000
MAX_AGENT_FILE_WRITES = 40
MAX_AGENT_FILE_BYTES = 300_000
MAX_PROJECT_CONTEXT_TREE_CHARS = 2_800
MAX_PROJECT_CONTEXT_FILE_CHARS = 2_400
MAX_PROJECT_CONTEXT_TOTAL_CHARS = 12_000
MAX_PROJECT_CONTEXT_FILES = 8
SAFE_PROVIDER_MAX_OUTPUT_TOKENS = 8_192
PROJECT_INFO_DIRNAME = "Project Info"
USER_OUTPUTS_DIRNAME = "Outputs"
LEGACY_OUTPUTS_DIRNAME = "outputs"
SETUP_CHAT_HISTORY_FILE = f"{PROJECT_INFO_DIRNAME}/setup-chat-history.txt"
SETUP_CHAT_HISTORY_COMPAT_FILE = f"{PROJECT_INFO_DIRNAME}/SETUP-CHAT.txt"
PROJECT_INFO_FILE = f"{PROJECT_INFO_DIRNAME}/PROJECT-INFO.MD"
PROJECT_DELEGATION_FILE = f"{PROJECT_INFO_DIRNAME}/PROJECT-DELEGATION.MD"
OVERVIEW_FILE = f"{PROJECT_INFO_DIRNAME}/overview.md"
PROJECT_PLAN_FILE = f"{PROJECT_INFO_DIRNAME}/project-plan.md"
USAGE_FILE = f"{PROJECT_INFO_DIRNAME}/usage.md"
TRACKER_FILE = f"{PROJECT_INFO_DIRNAME}/tracker.md"
README_FILE = f"{PROJECT_INFO_DIRNAME}/README.md"
BRIEF_FILE = f"{PROJECT_INFO_DIRNAME}/brief.md"
GOAL_FILE = f"{PROJECT_INFO_DIRNAME}/goal.md"
PROJECT_SETUP_FILE = f"{PROJECT_INFO_DIRNAME}/project-setup.md"
CHAT_HIVEE_FILE = f"{PROJECT_INFO_DIRNAME}/chat-hivee.md"
PROJECT_META_DIRNAME = "Project Meta"
PROJECT_CARD_FILE = f"{PROJECT_META_DIRNAME}/project-card.json"
PROJECT_MEMORY_FILE = f"{PROJECT_META_DIRNAME}/project-memory.json"
PROJECT_HISTORY_FILE = f"{PROJECT_META_DIRNAME}/project-history.jsonl"
PROJECT_CHECKPOINTS_DIR = f"{PROJECT_META_DIRNAME}/checkpoints"
PROJECT_POLICIES_FILE = f"{PROJECT_META_DIRNAME}/project-policies.json"
PROJECT_METRICS_FILE = f"{PROJECT_META_DIRNAME}/project-metrics.json"
PROJECT_RISKS_FILE = f"{PROJECT_META_DIRNAME}/project-risks.json"
PROJECT_DECISIONS_FILE = f"{PROJECT_META_DIRNAME}/project-decisions.jsonl"
PROJECT_HANDOFF_FILE = f"{PROJECT_META_DIRNAME}/handoff.md"
PLAN_STATUS_PENDING = "pending"
PLAN_STATUS_GENERATING = "generating"
PLAN_STATUS_AWAITING_APPROVAL = "awaiting_approval"
PLAN_STATUS_APPROVED = "approved"
PLAN_STATUS_FAILED = "failed"
EXEC_STATUS_IDLE = "idle"
EXEC_STATUS_RUNNING = "running"
EXEC_STATUS_PAUSED = "paused"
EXEC_STATUS_STOPPED = "stopped"
EXEC_STATUS_COMPLETED = "completed"
SESSION_COOKIE_NAME = "hivee_session"
SESSION_COOKIE_MAX_AGE_SEC = 60 * 60 * 24 * 30
ENV_STATUS_PENDING_BOOTSTRAP = "pending_bootstrap"
ENV_STATUS_PENDING_CLAIM = "pending_claim"
ENV_STATUS_ACTIVE = "active"
ENV_STATUS_SUSPENDED = "suspended"
ENV_STATUS_ARCHIVED = "archived"
ENV_CLAIM_CODE_TTL_SEC = 60 * 15
ENV_AGENT_SESSION_TTL_SEC = 60 * 60 * 24
ENV_AGENT_HANDOFF_TTL_SEC = 60 * 5
ENV_AGENT_RUNTIME_SESSION_TTL_SEC = 60 * 15
ENV_AGENT_LINK_TOKEN_TTL_SEC = 60 * 60 * 24 * 180
ENV_AGENT_SESSION_HEADER = "X-A2A-Agent-Session"
ENV_AGENT_ID_HEADER = "X-A2A-Agent-Id"
ENV_AGENT_SESSION_STATUS_ACTIVE = "active"
ENV_AGENT_SESSION_STATUS_HANDOFF_PENDING = "handoff_pending"
ENV_AGENT_SESSION_STATUS_REVOKED = "revoked"
ENV_AGENT_SESSION_STATUS_EXPIRED = "expired"
ENV_AGENT_LINK_STATUS_ACTIVE = "active"
ENV_AGENT_LINK_STATUS_REVOKED = "revoked"
ENV_AGENT_LINK_STATUS_EXPIRED = "expired"
PASSWORD_MIN_LENGTH = 10
PASSWORD_HASH_PREFIX = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 260_000
OAUTH_STATE_TTL_SEC = 60 * 10
OAUTH_PROVIDERS: Dict[str, Dict[str, str]] = {
    "google": {
        "display": "Google",
        "client_id_env": "GOOGLE_OAUTH_CLIENT_ID",
        "client_secret_env": "GOOGLE_OAUTH_CLIENT_SECRET",
        "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scope": "openid email profile",
    },
    "github": {
        "display": "GitHub",
        "client_id_env": "GITHUB_OAUTH_CLIENT_ID",
        "client_secret_env": "GITHUB_OAUTH_CLIENT_SECRET",
        "authorize_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scope": "read:user user:email",
    },
}
DEFAULT_PROJECT_SETUP_MD = """# PROJECT-SETUP
Ask these questions one by one before creating a project:
1. What is the project name?
2. What is the main goal?
3. Who are the target users?
4. What are the key constraints (time, budget, tech, compliance)?
5. What is in-scope and out-of-scope?
6. What is the deadline or milestone cadence?
7. What tools or stack are required?
8. What output should be produced first?
After collecting answers, summarize and ask for confirmation before starting the project workspace.
"""


__all__ = [name for name in globals() if not name.startswith('__')]
