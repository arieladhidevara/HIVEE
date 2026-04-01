let sessionToken = null;
let activeConnectionId = null;
let preferredConnectionId = null;
let selectedProjectId = null;
let selectedProjectData = null;
let selectedProjectPlan = null;
let selectedProjectReadiness = null;
let selectedPrimaryAgentId = null;
let selectedAssignedAgents = [];
let currentAgents = [];
let streamAbort = null;

let connectionsCache = [];
let projectsCache = [];
let connectionHealthy = false;
let currentAccountProfile = null;

let activeNavTab = "projects";
let activeProjectPane = "info";
let activeAuthMethod = "hooman";
let workspacePolicy = {
  workspace_root: "HIVEE",
  templates_root: "HIVEE/TEMPLATES",
  main_agent_id: "",
  main_agent_name: "",
  bootstrap_status: "unknown",
  workspace_tree: "",
};
let workspaceTreeText = "";
let projectTreeText = "";
let wizardMode = "chat";
let wizardChatBooted = false;
let wizardSetupSessionKey = "";
let wizardChatPending = false;
let wizardTranscript = [];
let wizardDraft = null;
let wizardSuggestedRoles = new Map();
let wizardExternalInvites = [];
let wizardExternalMemberships = [];
let wizardAgentPermissions = [];
let wizardProjectAgents = [];
let wizardLatestExternalInvite = null;
let runtimePollHandle = null;
let projectFilesCurrentPath = "";
let projectFilePreviewPath = "";
let projectFilesPayload = null;
let workspaceFilesCurrentPath = "";
let workspaceFilesPayload = null;
let projectStreamConnected = false;
let projectRefreshTimer = null;
let livePreviewPath = "";
let livePreviewBlobUrl = "";
let projectFilePreviewBlobUrl = "";
let workspaceFilePreviewBlobUrl = "";
let livePreviewReqSeq = 0;

let chatAgents = [];
let chatAliasMap = new Map();
let chatById = new Map();
let chatAutocompleteItems = [];
let chatAutocompleteIndex = 0;
let chatContextMode = "workspace";
const DEFAULT_OWNER_FILES_PATH = "";
const SESSION_TOKEN_KEY = "hivee_session_token_v2";
const AGENT_SETUP_DOC_PATH = "/new-user/NEW-ACCOUNT-SETUP.MD";
const AGENT_SECURITY_DOC_PATH = "/new-user/AGENT-SECURITY-RULES.MD";
const CLAIM_ENV_PARAM = "claim_env_id";
const CLAIM_CODE_PARAM = "claim_code";
const PROJECT_INVITE_PARAM = "project_invite";
const PROJECT_INVITE_CODE_PARAM = "project_invite_code";
const PROJECT_DEEPLINK_PROJECT_PARAM = "project";
const PROJECT_DEEPLINK_PANE_PARAM = "project_pane";
const PROJECT_DEEPLINK_PATH_PARAM = "project_path";
const PROJECT_DEEPLINK_PREVIEW_PARAM = "project_preview";
const OAUTH_ERROR_PARAM = "oauth_error";
const PASSWORD_POLICY_MIN_LENGTH = 10;
const SUMMARY_AGENT_DEFAULT_AVATAR = "/static/default-agent-avatar.svg";
const AGENT_MASCOT_PATH = "/static/mascot.svg";
const AGENT_COLOR_STORAGE_KEY = "hivee_agent_colors_v1";
const AGENT_COLOR_SWATCHES = [
  "#F97316",
  "#22D3EE",
  "#3B82F6",
  "#EF4444",
  "#FACC15",
  "#14B8A6",
  "#F59E0B",
  "#10B981",
  "#EAB308",
  "#38BDF8",
];
const AGENT_MASCOT_TEMPLATE = String.raw`<?xml version="1.0" encoding="UTF-8"?><svg id="Layer_2" data-name="Layer 2" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 274.94 276.67"><defs><style>.cls-1{fill:#fff}.cls-2{fill:#fee612}</style></defs><g id="Layer_1-2" data-name="Layer 1"><path class="cls-2" d="M228.34,100.05c4.05,12.55,5.86,27.55,1.64,40.25-5.97,5.91-16.03,10.38-24.04,13.08-18.4,6.18-37.42,9.21-56.82,10.03-22.82.97-44.56-.73-66.75-6.02-10.9-2.6-29.5-8.9-37.31-17.08-5.8-14.26-1.16-36.87,5.36-50.58,10.94-23.01,30.12-41.12,54.75-49.24-3.73-10.52-9.76-22.26-18.95-28.83-3.07,6.05-10.13,8.15-15.22,4.27-3.41-2.59-4.89-7.12-2.67-10.92,1.78-3.05,5.12-4.7,8.7-4.95,4.1-.28,8.1,1.22,11.59,3.41,12.16,7.63,19.84,21.52,24.13,34.94,17.07-3.98,31.34-3.9,48.59-.11,3.54-10.38,8.36-19.82,15.66-27.8,6.17-6.74,18.15-14.57,26.71-8.1,3.98,3,4.36,8.15,1.29,12-2.07,2.59-5.35,3.8-8.62,3.32-5.82-.84-7.44-6.01-8.01-5.93-1.13.15-6.04,5.33-7.09,6.58-5.46,6.48-9.19,13.92-11.77,22.12,10.23,3.29,19.05,8.26,27.29,14.72,14.73,11.56,25.77,26.93,31.55,44.82ZM114.86,100.53c.31-7.34-4.01-14.16-10.51-17.05s-14.13-1.4-19.21,3.34-6.7,12.06-4.47,18.72c1.97,5.88,6.96,10.02,12.4,11.33,6.34,1.53,12.96-.46,17.1-5.09,2.81-3.14,4.51-6.78,4.69-11.25ZM191.56,89.13c-6.16-8.57-18.37-9.52-26.13-2.52-6.22,5.61-7.43,14.95-3.15,22.03,5.34,8.83,16.48,11.3,24.88,5.8s10.55-16.76,4.4-25.31Z"/><path class="cls-2" d="M43.78,164.38c8.88,5.81,18.67,8.88,28.75,11.97,15.33,4.7,30.9,7.36,46.88,8.59,7.81.6,27.76.63,35.43-.02,10.64-.91,20.93-2.19,31.37-4.4,15.84-3.36,34.93-9.35,48.51-18.8.3,20.9-7.88,41.78-21.45,58.35-24.73,30.2-66.1,41.1-103.35,31.15-22.76-6.08-42.06-20.02-54.86-39.73-9.47-14.58-15.4-31.87-15.14-49.64l3.86,2.53Z"/><path class="cls-2" d="M266.23,152.45c-7.97,7.02-18.02,8.11-26.49,1.43-.87-5.86-2.4-11.25-5.11-16.35l2.31-10.77c8.28-6.18,19.04-8.44,28.9-4.75,5.88,2.2,9.52,7.45,9.08,13.92-.44,6.5-3.82,12.24-8.68,16.52Z"/><path class="cls-2" d="M34.96,153.65c-7.06,6.2-16.89,6.1-24.26.57-4.4-3.3-7.81-7.61-9.66-12.86-.72-2.05-.9-4.13-1.01-6.3-.59-11,10.75-15.67,20.06-14.75,6.46.64,12.43,2.57,17.74,6.5l1.61,8.43c.13.68.78,2.22.43,2.86-1.99,3.69-3.36,7.56-4.15,11.64l-.75,3.91Z"/><path class="cls-2" d="M182.37,269.4c-3.29,5.77-9.94,8.58-16.12,6.68-7.64-2.35-10.55-9.98-10.13-17.43,10.66-1.88,20.47-4.84,30-9.92l-.55,8.82c-.26,4.1-1.08,8.15-3.2,11.86Z"/><path class="cls-2" d="M101.63,276.3c-8.77-2.01-11.96-10.53-12.53-18.71l-.6-8.67c9.69,4.79,19.2,8.04,29.79,9.68,0,1.95,0,3.79-.23,5.7-.98,8.11-8.42,13.84-16.44,12.01Z"/><path class="cls-2" d="M257.99,179.85c-3.59,3.93-8.95,5.07-14,3.95-2.32-.57-4.33-1.73-5.68-3.68,1.51-7.28,1.93-14.21,1.62-21.95,4.52,2.38,9.01,3.23,13.84,4.12,3.79.7,6.58,4.48,7.15,8.03.34,3.67-.39,6.75-2.93,9.53Z"/><path class="cls-2" d="M36.22,180.61c-4.02,4.85-13.32,4.83-18.35.2-2.91-2.68-4.67-6.24-3.92-10.36.58-3.17,2.85-7.29,6.63-8.08,4.85-1.02,9.4-1.62,14.1-4.14l-.12,5.68c-.12,5.6.88,11.03,1.67,16.71Z"/><path d="M114.86,100.53c-.19,4.47-1.88,8.11-4.69,11.25-4.14,4.63-10.77,6.62-17.1,5.09-5.43-1.31-10.42-5.45-12.4-11.33-2.23-6.65-.58-14.01,4.47-18.72s12.6-6.28,19.21-3.34,10.82,9.71,10.51,17.05ZM101.82,99.14c2.9-.35,4.71-2.44,5.11-4.61.5-2.71-.84-5.07-2.91-6.21-2.31-1.28-5.08-.95-6.9,1.06-1.61,1.78-1.92,4.4-.84,6.67.82,1.74,3.04,3.38,5.54,3.08Z"/><path d="M191.56,89.13c6.15,8.56,3.95,19.84-4.4,25.31s-19.54,3.03-24.88-5.8c-4.28-7.08-3.07-16.42,3.15-22.03,7.76-7,19.97-6.06,26.13,2.52ZM184.93,97.74c2.78-2.36,2.44-6.6-.06-8.7-2.63-2.2-6.73-1.96-8.67,1.01-1.66,2.53-1.06,5.89,1.13,7.75,2.06,1.74,5.19,1.99,7.6-.06Z"/><path class="cls-1" d="M101.82,99.14c-2.5.3-4.72-1.34-5.54-3.08-1.07-2.27-.77-4.89.84-6.67,1.82-2.01,4.59-2.34,6.9-1.06,2.06,1.14,3.41,3.5,2.91,6.21-.4,2.17-2.21,4.26-5.11,4.61Z"/><path class="cls-1" d="M184.93,97.74c-2.41,2.04-5.55,1.79-7.6.06-2.19-1.85-2.79-5.21-1.13-7.75,1.95-2.97,6.05-3.21,8.67-1.01s2.84,6.34.06,8.7Z"/></g></svg>`;
const CLAIM_SOCIAL_CTX_KEY = "hivee_claim_social_ctx_v1";
let claimAuthContext = {
  active: false,
  environmentId: "",
  code: "",
};
let claimSessionState = {
  connected: false,
  email: "",
  providers: [],
};
let projectInviteContext = {
  active: false,
  token: "",
  inviteCode: "",
  info: null,
  connections: [],
};
let projectInviteManagedAgents = [];
let projectInviteAgentSelections = new Map();
let projectDeepLinkContext = {
  active: false,
  projectId: "",
  pane: "",
  path: "",
  previewPath: "",
};
let summaryAgents = [];
let summaryAgentsLoading = false;
let summaryAgentsError = "";
let oauthProvidersState = new Map();
let agentColorAssignments = {};
const agentMascotUriCache = new Map();

function readStoredSessionToken() {
  try {
    return String(localStorage.getItem(SESSION_TOKEN_KEY) || "").trim() || null;
  } catch {
    return null;
  }
}

function persistSessionToken(token) {
  const clean = String(token || "").trim();
  try {
    if (!clean) {
      localStorage.removeItem(SESSION_TOKEN_KEY);
      return;
    }
    localStorage.setItem(SESSION_TOKEN_KEY, clean);
  } catch {}
}

function isHexColor(value) {
  return /^#[0-9a-f]{6}$/i.test(String(value || "").trim());
}

function readStoredAgentColors() {
  try {
    const raw = localStorage.getItem(AGENT_COLOR_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return {};
    const out = {};
    for (const [k, v] of Object.entries(parsed)) {
      const id = String(k || "").trim();
      const hex = String(v || "").trim();
      if (!id || !isHexColor(hex)) continue;
      out[id] = hex.toUpperCase();
    }
    return out;
  } catch {
    return {};
  }
}

function persistAgentColors() {
  try {
    localStorage.setItem(AGENT_COLOR_STORAGE_KEY, JSON.stringify(agentColorAssignments || {}));
  } catch {}
}

function hashText(value) {
  const base = String(value || "");
  let hash = 0;
  for (let i = 0; i < base.length; i++) {
    hash = ((hash << 5) - hash + base.charCodeAt(i)) | 0;
  }
  return hash;
}

function colorHexForAgent(agentId) {
  const id = String(agentId || "").trim() || "agent";
  const remembered = String(agentColorAssignments[id] || "").trim();
  if (isHexColor(remembered)) return remembered.toUpperCase();
  const pick = AGENT_COLOR_SWATCHES[Math.abs(hashText(id)) % AGENT_COLOR_SWATCHES.length] || "#F97316";
  const normalized = String(pick || "#F97316").toUpperCase();
  agentColorAssignments[id] = normalized;
  persistAgentColors();
  return normalized;
}

function hexToRgba(hex, alpha = 1) {
  const clean = String(hex || "").replace("#", "");
  if (!/^[0-9a-f]{6}$/i.test(clean)) return `rgba(249, 115, 22, ${alpha})`;
  const r = parseInt(clean.slice(0, 2), 16);
  const g = parseInt(clean.slice(2, 4), 16);
  const b = parseInt(clean.slice(4, 6), 16);
  const a = Number.isFinite(Number(alpha)) ? Math.max(0, Math.min(Number(alpha), 1)) : 1;
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}

function mascotDataUriForColor(hex) {
  const cleanHex = isHexColor(hex) ? hex.toUpperCase() : "#F97316";
  if (agentMascotUriCache.has(cleanHex)) return agentMascotUriCache.get(cleanHex);
  const svg = AGENT_MASCOT_TEMPLATE.replace(/#fee612/gi, cleanHex);
  const uri = `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
  agentMascotUriCache.set(cleanHex, uri);
  return uri;
}

function createAgentAvatarImg(agentId, alt = "Agent mascot") {
  const img = document.createElement("img");
  const hex = colorHexForAgent(agentId);
  img.src = mascotDataUriForColor(hex);
  img.alt = alt;
  img.loading = "lazy";
  img.decoding = "async";
  return img;
}

agentColorAssignments = readStoredAgentColors();

function $(id) { return document.getElementById(id); }

function authHeaders() {
  if (claimAuthContext.active) return {};
  if (!sessionToken) sessionToken = readStoredSessionToken();
  if (!sessionToken) return {};
  return { Authorization: `Bearer ${sessionToken}` };
}

function clearAuthSession() {
  persistSessionToken(null);
  sessionToken = null;
  activeConnectionId = null;
  preferredConnectionId = null;
  selectedProjectId = null;
  selectedProjectData = null;
  selectedProjectPlan = null;
  selectedProjectReadiness = null;
  selectedPrimaryAgentId = null;
  selectedAssignedAgents = [];
  chatContextMode = "workspace";
}

function setMessage(id, text, tone = "") {
  const el = $(id);
  if (!el) return;
  el.textContent = text || "";
  el.classList.remove("error", "ok");
  if (tone) el.classList.add(tone);
}

function parseClaimAuthFromUrl() {
  const params = new URLSearchParams(window.location.search || "");
  const envId = String(params.get(CLAIM_ENV_PARAM) || "").trim();
  const code = String(params.get(CLAIM_CODE_PARAM) || "").trim();
  claimAuthContext = {
    active: Boolean(envId && code),
    environmentId: envId,
    code,
  };
}

function clearClaimParamsFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete(CLAIM_ENV_PARAM);
  url.searchParams.delete(CLAIM_CODE_PARAM);
  const next = url.pathname + (url.search ? url.search : "") + (url.hash ? url.hash : "");
  window.history.replaceState({}, "", next);
}

function parseProjectInviteFromUrl() {
  const params = new URLSearchParams(window.location.search || "");
  const token = String(params.get(PROJECT_INVITE_PARAM) || "").trim();
  const inviteCode = String(params.get(PROJECT_INVITE_CODE_PARAM) || "").trim().toUpperCase();
  projectInviteContext = {
    active: Boolean(token),
    token,
    inviteCode,
    info: null,
    connections: [],
  };
}

function clearProjectInviteParamFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete(PROJECT_INVITE_PARAM);
  url.searchParams.delete(PROJECT_INVITE_CODE_PARAM);
  const next = url.pathname + (url.search ? url.search : "") + (url.hash ? url.hash : "");
  window.history.replaceState({}, "", next);
}

function clearProjectInviteContext({ clearUrl = false } = {}) {
  projectInviteContext = {
    active: false,
    token: "",
    inviteCode: "",
    info: null,
    connections: [],
  };
  projectInviteManagedAgents = [];
  projectInviteAgentSelections = new Map();
  closeProjectInviteAgentModal({ reset: true });
  if (clearUrl) clearProjectInviteParamFromUrl();
  renderProjectInviteUI();
}

function normalizeProjectPaneForLink(pane) {
  const raw = String(pane || "").trim().toLowerCase();
  if (["info", "folder", "live", "usage", "tracker"].includes(raw)) return raw;
  return "folder";
}

function parseProjectDeepLinkFromUrl() {
  const params = new URLSearchParams(window.location.search || "");
  const projectId = String(params.get(PROJECT_DEEPLINK_PROJECT_PARAM) || "").trim();
  const pane = normalizeProjectPaneForLink(params.get(PROJECT_DEEPLINK_PANE_PARAM));
  const path = String(params.get(PROJECT_DEEPLINK_PATH_PARAM) || "").trim();
  const previewPath = String(params.get(PROJECT_DEEPLINK_PREVIEW_PARAM) || "").trim();
  projectDeepLinkContext = {
    active: Boolean(projectId),
    projectId,
    pane,
    path,
    previewPath,
  };
}

function clearProjectDeepLinkParamsFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete(PROJECT_DEEPLINK_PROJECT_PARAM);
  url.searchParams.delete(PROJECT_DEEPLINK_PANE_PARAM);
  url.searchParams.delete(PROJECT_DEEPLINK_PATH_PARAM);
  url.searchParams.delete(PROJECT_DEEPLINK_PREVIEW_PARAM);
  const next = url.pathname + (url.search ? url.search : "") + (url.hash ? url.hash : "");
  window.history.replaceState({}, "", next);
}

function clearProjectDeepLinkContext({ clearUrl = false } = {}) {
  projectDeepLinkContext = {
    active: false,
    projectId: "",
    pane: "",
    path: "",
    previewPath: "",
  };
  if (clearUrl) clearProjectDeepLinkParamsFromUrl();
}

function buildProjectDeepLink(projectId, { pane = "folder", path = "", previewPath = "" } = {}) {
  const pid = String(projectId || "").trim();
  if (!pid) return "";
  const params = new URLSearchParams();
  params.set(PROJECT_DEEPLINK_PROJECT_PARAM, pid);
  params.set(PROJECT_DEEPLINK_PANE_PARAM, normalizeProjectPaneForLink(pane));
  const cleanPath = normalizePath(path);
  if (cleanPath) params.set(PROJECT_DEEPLINK_PATH_PARAM, cleanPath);
  const cleanPreviewPath = normalizePath(previewPath);
  if (cleanPreviewPath) params.set(PROJECT_DEEPLINK_PREVIEW_PARAM, cleanPreviewPath);
  return `${window.location.pathname || "/"}?${params.toString()}`;
}

async function applyProjectDeepLinkContext() {
  if (!projectDeepLinkContext.active) return false;
  const targetProjectId = String(projectDeepLinkContext.projectId || "").trim();
  if (!targetProjectId) {
    clearProjectDeepLinkContext({ clearUrl: true });
    return false;
  }

  const hasTarget = projectsCache.some((p) => String(p?.id || "").trim() === targetProjectId);
  if (!hasTarget) {
    clearProjectDeepLinkContext({ clearUrl: true });
    return false;
  }

  if (selectedProjectId !== targetProjectId) {
    await selectProject(targetProjectId);
  }

  setNavTab("projects");
  const pane = normalizeProjectPaneForLink(projectDeepLinkContext.pane || "folder");
  setProjectPane(pane);

  if (pane === "folder") {
    const folderPath = normalizePath(projectDeepLinkContext.path || "");
    await loadProjectFiles(folderPath).catch(() => {});
    const previewPath = normalizePath(projectDeepLinkContext.previewPath || "");
    if (previewPath) {
      await openProjectFile(previewPath).catch(() => {});
    }
  } else if (pane === "live") {
    const previewPath = normalizePath(projectDeepLinkContext.previewPath || "");
    if (previewPath) {
      livePreviewPath = normalizeOutputPath(previewPath);
      await renderLivePreview(livePreviewPath, { force: true }).catch(() => {});
    }
  }

  clearProjectDeepLinkContext({ clearUrl: true });
  return true;
}
function readOauthErrorFromUrl() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    return String(params.get(OAUTH_ERROR_PARAM) || "").trim();
  } catch {
    return "";
  }
}

function clearOauthErrorParamFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete(OAUTH_ERROR_PARAM);
  const next = url.pathname + (url.search ? url.search : "") + (url.hash ? url.hash : "");
  window.history.replaceState({}, "", next);
}

function readClaimSocialContext() {
  try {
    const raw = sessionStorage.getItem(CLAIM_SOCIAL_CTX_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeClaimSocialContext(payload) {
  try {
    sessionStorage.setItem(CLAIM_SOCIAL_CTX_KEY, JSON.stringify(payload || {}));
  } catch {}
}

function clearClaimSocialContext() {
  try {
    sessionStorage.removeItem(CLAIM_SOCIAL_CTX_KEY);
  } catch {}
}

function setClaimSessionState(next) {
  claimSessionState = {
    connected: Boolean(next?.connected),
    email: String(next?.email || "").trim(),
    providers: Array.isArray(next?.providers) ? next.providers.map((p) => String(p || "").trim()).filter(Boolean) : [],
  };
}

function applyClaimConnectionFromContext(ctx) {
  const base = String(ctx?.openclaw_base_url || "").trim();
  const key = String(ctx?.openclaw_api_key || "").trim();
  const name = String(ctx?.openclaw_name || "").trim();
  if (!base && !key && !name) return;
  for (const prefix of ["li", "su"]) {
    const baseEl = $(`${prefix}_oc_base`);
    const keyEl = $(`${prefix}_oc_key`);
    const nameEl = $(`${prefix}_oc_name`);
    if (baseEl && base) baseEl.value = base;
    if (keyEl && key) keyEl.value = key;
    if (nameEl && name) nameEl.value = name;
  }
}

function claimConnectionForContext() {
  const loginPayload = claimConnectionPayload("li");
  const signupPayload = claimConnectionPayload("su");
  const hasComplete = (p) => Boolean(String(p?.openclaw_base_url || "").trim() && String(p?.openclaw_api_key || "").trim());
  const hasAny = (p) => Boolean(String(p?.openclaw_base_url || "").trim() || String(p?.openclaw_api_key || "").trim() || String(p?.openclaw_name || "").trim());
  if (hasComplete(loginPayload)) return loginPayload;
  if (hasComplete(signupPayload)) return signupPayload;
  if (hasAny(loginPayload)) return loginPayload;
  if (hasAny(signupPayload)) return signupPayload;
  return {
    openclaw_base_url: "",
    openclaw_api_key: "",
    openclaw_name: null,
  };
}

function validateOptionalClaimConnectionPayload(payload) {
  const base = String(payload?.openclaw_base_url || "").trim();
  const key = String(payload?.openclaw_api_key || "").trim();
  if (!base && !key) return;
  if (!base || !key) {
    throw new Error("Enter both OpenClaw Base URL and API key together, or leave both empty.");
  }
  validateClaimConnectionPayload(payload);
}

function setClaimCredentialRequired(enabled) {
  const ids = ["li_email", "li_pass", "su_email", "su_pass", "su_pass_confirm"];
  for (const id of ids) {
    const el = $(id);
    if (!el) continue;
    if (enabled) {
      el.setAttribute("required", "required");
    } else {
      el.removeAttribute("required");
    }
  }
}

async function loadClaimSessionState() {
  if (!claimAuthContext.active) {
    setClaimSessionState({ connected: false, email: "", providers: [] });
    return;
  }
  try {
    const me = await api("/api/me");
    let providers = [];
    try {
      const linked = await api("/api/me/oauth-providers");
      providers = Array.isArray(linked?.providers) ? linked.providers : [];
    } catch {}
    setClaimSessionState({
      connected: true,
      email: String(me?.email || "").trim(),
      providers,
    });
  } catch {
    setClaimSessionState({ connected: false, email: "", providers: [] });
  }
}

function oauthDisplayName(provider) {
  const key = String(provider || "").trim().toLowerCase();
  if (key === "google") return "Google";
  if (key === "github") return "GitHub";
  return key || "OAuth";
}

function applyOAuthProvidersUI() {
  const buttons = Array.from(document.querySelectorAll("[data-oauth-provider]"));
  let configuredCount = 0;
  for (const btn of buttons) {
    const provider = String(btn.dataset.oauthProvider || "").trim().toLowerCase();
    const configured = Boolean(oauthProvidersState.get(provider));
    if (configured) configuredCount += 1;
    btn.disabled = !configured;
    btn.classList.toggle("is-disabled", !configured);
    const label = String(btn.dataset.oauthLabel || oauthDisplayName(provider));
    btn.title = configured ? `Continue with ${label}` : `${label} login is not configured yet`;
  }
  const hint = $("social_auth_hint");
  if (!hint) return;
  if (!buttons.length) {
    hint.textContent = "";
    return;
  }
  if (configuredCount === 0) {
    hint.textContent = "Social login is not enabled on this server.";
    return;
  }
  if (configuredCount < buttons.length) {
    hint.textContent = claimAuthContext.active
      ? "Some providers are not enabled yet. You can sign in with social auth first or fill OpenClaw details first."
      : "Some providers are not enabled yet.";
    return;
  }
  if (!claimAuthContext.active) {
    hint.textContent = "";
    return;
  }
  hint.textContent = claimSessionState.connected
    ? "Session is connected. Enter OpenClaw Base URL + API key, then click Claim Environment."
    : "You can sign in with social auth first, then continue by entering OpenClaw Base URL + API key.";
}

async function loadOAuthProviders() {
  oauthProvidersState = new Map();
  try {
    const res = await api("/api/oauth/providers");
    const providers = Array.isArray(res?.providers) ? res.providers : [];
    for (const item of providers) {
      const key = String(item?.provider || "").trim().toLowerCase();
      if (!key) continue;
      oauthProvidersState.set(key, Boolean(item?.configured));
    }
  } catch {
    for (const btn of document.querySelectorAll("[data-oauth-provider]")) {
      const key = String(btn.dataset.oauthProvider || "").trim().toLowerCase();
      if (!key) continue;
      oauthProvidersState.set(key, false);
    }
  }
  applyOAuthProvidersUI();
}

function applyClaimAuthUI() {
  const notice = $("claim_notice");
  const methodRow = document.querySelector("#view_auth .method-row");
  const methodAgent = $("method_agent");
  const btnLogin = $("btn_login");
  const btnSignup = $("btn_signup");
  const socialAuthBlock = $("social_auth_block");
  const switchRow = $("auth_switch_row");
  const loginForm = $("form_login");
  const signupForm = $("form_signup");
  const claimCard = $("claim_connected_card");
  const claimIdentity = $("claim_connected_identity");
  const claimProvider = $("claim_connected_provider");
  const claimOnlyBlocks = document.querySelectorAll(".claim-only");
  const credentialBlocks = document.querySelectorAll(".credential-only");
  if (!claimAuthContext.active) {
    const inviteSignedInMode = Boolean(projectInviteContext.active && sessionToken);
    setClaimSessionState({ connected: false, email: "", providers: [] });
    if (notice) {
      notice.classList.add("hidden");
      notice.textContent = "";
    }
    if (claimCard) claimCard.classList.add("hidden");
    if (methodAgent) {
      methodAgent.disabled = false;
      methodAgent.classList.remove("hidden");
    }
    if (methodRow) methodRow.classList.toggle("hidden", inviteSignedInMode);
    if (switchRow) switchRow.classList.toggle("hidden", inviteSignedInMode);
    if (loginForm) loginForm.classList.toggle("hidden", inviteSignedInMode);
    if (signupForm) signupForm.classList.add("hidden");
    $("tab_login")?.classList.add("active");
    $("tab_signup")?.classList.remove("active");
    if (btnLogin) btnLogin.textContent = "Continue";
    if (btnSignup) btnSignup.textContent = "Create Account";
    setClaimCredentialRequired(true);
    credentialBlocks.forEach((el) => el.classList.remove("hidden"));
    if (socialAuthBlock) socialAuthBlock.classList.toggle("hidden", inviteSignedInMode);
    applyOAuthProvidersUI();
    claimOnlyBlocks.forEach((el) => el.classList.add("hidden"));
    renderProjectInviteUI();
    return;
  }

  activeAuthMethod = "hooman";
  if (methodAgent) {
    methodAgent.disabled = true;
    methodAgent.classList.add("hidden");
  }
  if (methodRow) methodRow.classList.remove("hidden");
  if (notice) {
    const shortEnv = claimAuthContext.environmentId.length > 18
      ? claimAuthContext.environmentId.slice(0, 18) + "..."
      : claimAuthContext.environmentId;
    notice.textContent = claimSessionState.connected
      ? `Claim link detected for environment ${shortEnv}. Session connected, complete claim below.`
      : `Claim link detected for environment ${shortEnv}. Login or sign up to claim it.`;
    notice.classList.remove("hidden");
  }
  if (claimSessionState.connected) {
    if (claimCard) claimCard.classList.remove("hidden");
    if (claimIdentity) claimIdentity.textContent = claimSessionState.email || "Connected account";
    if (claimProvider) {
      claimProvider.textContent = claimSessionState.providers.length
        ? `OAuth linked: ${claimSessionState.providers.join(", ")}`
        : "OAuth session active.";
    }
    if (switchRow) switchRow.classList.add("hidden");
    if (loginForm) loginForm.classList.remove("hidden");
    if (signupForm) signupForm.classList.add("hidden");
    $("tab_login")?.classList.add("active");
    $("tab_signup")?.classList.remove("active");
    setClaimCredentialRequired(false);
    credentialBlocks.forEach((el) => el.classList.add("hidden"));
    if (btnLogin) btnLogin.textContent = "Claim Environment";
    if (btnSignup) btnSignup.textContent = "Create Account and Claim";
  } else {
    if (claimCard) claimCard.classList.add("hidden");
    if (switchRow) switchRow.classList.remove("hidden");
    setClaimCredentialRequired(true);
    credentialBlocks.forEach((el) => el.classList.remove("hidden"));
    if (btnLogin) btnLogin.textContent = "Login and Claim";
    if (btnSignup) btnSignup.textContent = "Create Account and Claim";
  }
  if (socialAuthBlock) socialAuthBlock.classList.remove("hidden");
  applyOAuthProvidersUI();
  claimOnlyBlocks.forEach((el) => el.classList.remove("hidden"));
  renderProjectInviteUI();
}
function inviteStatusLabel(status) {
  const normalized = String(status || "pending").trim().toLowerCase();
  if (normalized === "pending") return "Pending";
  if (normalized === "accepted") return "Accepted";
  if (normalized === "expired") return "Expired";
  if (normalized === "revoked") return "Revoked";
  return normalized || "unknown";
}

function renderProjectInviteUI() {
  const notice = $("project_invite_notice");
  const card = $("project_invite_card");
  const title = $("project_invite_title");
  const meta = $("project_invite_meta");
  const loggedIn = $("project_invite_logged_in");
  const setupBtn = $("btn_open_project_setup_doc");
  const docBtn = $("btn_open_project_invite_doc");
  const linksHint = $("project_invite_links_hint");
  const codeInput = $("invite_portal_code");
  const codeHint = $("project_invite_code_hint");
  const connectionSelect = $("invite_connection_id");
  const agentIdInput = $("invite_agent_id");
  const agentNameInput = $("invite_agent_name");
  const acceptBtn = $("btn_accept_project_invite");

  if (!notice || !card) return;

  if (!projectInviteContext.active) {
    notice.classList.add("hidden");
    notice.textContent = "";
    card.classList.add("hidden");
    if (loggedIn) loggedIn.textContent = "";
    if (docBtn) {
      docBtn.disabled = true;
      docBtn.dataset.url = "";
    }
    if (setupBtn) {
      setupBtn.disabled = true;
      setupBtn.dataset.url = "";
    }
    if (connectionSelect) connectionSelect.innerHTML = "";
    if (codeInput) codeInput.value = "";
    if (codeHint) codeHint.textContent = "";
    return;
  }

  const info = projectInviteContext.info || {};
  const status = String(info.status || "pending").toLowerCase();
  const hasSession = Boolean(sessionToken);
  const sessionIdentity = String(currentAccountProfile?.email || "").trim();
  const canAccept = Boolean(info.can_accept);
  const requiresInviteCode = Boolean(info.requires_invite_code);

  const codeFromUrl = String(projectInviteContext.inviteCode || "").trim().toUpperCase();

  notice.classList.remove("hidden");
  if (!info.project_id) {
    notice.textContent = "Invitation link detected. Loading details...";
  } else if (status !== "pending") {
    if (status === "expired") {
      notice.textContent = "This invitation is expired. Ask the project owner to generate a new link.";
    } else if (status === "accepted") {
      notice.textContent = "This invitation has already been accepted.";
    } else if (status === "revoked") {
      notice.textContent = "This invitation was revoked by the project owner.";
    } else {
      notice.textContent = `Invitation status: ${inviteStatusLabel(status)}.`;
    }
  } else if (!hasSession) {
    notice.textContent = "Log in or sign up to accept this invitation.";
  } else if (requiresInviteCode && !codeFromUrl && !String(codeInput?.value || "").trim()) {
    notice.textContent = "You are signed in. Enter invite code from the invitation document, then continue.";
  } else {
    notice.textContent = "You are signed in. Choose connection, then continue to agent selection.";
  }

  const inviteDocUrl = toAbsoluteAppUrl(
    info.invitation_doc_url
    || (`/api/projects/invites/${encodeURIComponent(projectInviteContext.token || "")}/Project-Invitation.md`)
  );
  const setupDocUrl = toAbsoluteAppUrl(info.setup_doc_url || AGENT_SETUP_DOC_PATH);

  if (docBtn) {
    docBtn.dataset.url = inviteDocUrl || "";
    docBtn.disabled = !Boolean(inviteDocUrl);
  }
  if (setupBtn) {
    setupBtn.dataset.url = setupDocUrl || "";
    setupBtn.disabled = !Boolean(setupDocUrl);
  }

  if (!hasSession) {
    card.classList.add("hidden");
    if (loggedIn) loggedIn.textContent = "";
    if (acceptBtn) acceptBtn.disabled = true;
    return;
  }

  card.classList.remove("hidden");
  if (loggedIn) {
    if (sessionIdentity) {
      loggedIn.textContent = `Logged in as ${sessionIdentity}`;
    } else {
      loggedIn.textContent = "Signed in. Loading account identity...";
    }
  }
  if (title) title.textContent = String(info.project_title || "Accept Invitation");
  if (meta) {
    const parts = [];
    if (info.project_id) parts.push(`Project: ${info.project_id}`);
    if (info.target_email_masked) parts.push(`Target: ${info.target_email_masked}`);
    if (info.requested_agent_id || info.requested_agent_name) {
      parts.push(`Requested Agent: ${String(info.requested_agent_name || info.requested_agent_id || "").trim()}`);
    }
    if (info.role) parts.push(`Role: ${String(info.role || "").trim()}`);
    parts.push(`Status: ${inviteStatusLabel(status)}`);
    if (info.expires_at) parts.push(`Expires: ${formatTs(info.expires_at)}`);
    meta.textContent = parts.join(" | ");
  }
  if (linksHint) {
    linksHint.textContent = canAccept
      ? "Open docs if you need details, then continue with connection and agent selection."
      : "Open docs if needed. This invitation is not currently accept-able.";
  }

  if (codeInput) {
    if (!String(codeInput.value || "").trim() && codeFromUrl) {
      codeInput.value = codeFromUrl;
    }
    codeInput.disabled = !hasSession || !canAccept;
  }
  if (codeHint) {
    const hint = String(info.invite_code_hint || "").trim();
    codeHint.textContent = hint
      ? `Invite code hint: ${hint}`
      : (requiresInviteCode ? "Invite code is required for this invitation." : "");
  }

  if (connectionSelect) {
    const previous = String(connectionSelect.value || "").trim();
    connectionSelect.innerHTML = "";
    const rows = Array.isArray(projectInviteContext.connections) ? projectInviteContext.connections : [];
    if (!rows.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = hasSession
        ? "No OpenClaw connection found for this account"
        : "Login first to load your OpenClaw connections";
      connectionSelect.appendChild(opt);
    } else {
      for (const item of rows) {
        const id = String(item?.id || "").trim();
        if (!id) continue;
        const opt = document.createElement("option");
        opt.value = id;
        const name = String(item?.name || "").trim();
        const base = String(item?.base_url || "").trim();
        opt.textContent = name ? `${name} (${id})` : `${id}${base ? ` - ${base}` : ""}`;
        connectionSelect.appendChild(opt);
      }
      if (previous && rows.some((x) => String(x?.id || "") === previous)) {
        connectionSelect.value = previous;
      }
    }
    connectionSelect.disabled = !hasSession || !canAccept || !Array.isArray(projectInviteContext.connections) || !projectInviteContext.connections.length;
  }

  if (agentIdInput) {
    const suggestedId = String(info.requested_agent_id || "").trim();
    const idLockedByInvite = Boolean(suggestedId);
    if (idLockedByInvite) {
      agentIdInput.value = suggestedId;
    } else if (!String(agentIdInput.value || "").trim() && suggestedId) {
      agentIdInput.value = suggestedId;
    }
    agentIdInput.disabled = !hasSession || !canAccept || idLockedByInvite;
  }
  if (agentNameInput) {
    const suggestedName = String(info.requested_agent_name || "").trim();
    if (!String(agentNameInput.value || "").trim() && suggestedName) {
      agentNameInput.value = suggestedName;
    }
    agentNameInput.disabled = !hasSession || !canAccept;
  }

  if (acceptBtn) {
    const hasConnection = Boolean(String(connectionSelect?.value || "").trim());
    const codeValue = String(codeInput?.value || "").trim();
    const hasRequiredCode = !requiresInviteCode || Boolean(codeValue);
    acceptBtn.disabled = !hasSession || !canAccept || !hasConnection || !hasRequiredCode;
  }
}

async function loadProjectInviteConnections() {
  if (!projectInviteContext.active) return [];
  if (!sessionToken) {
    projectInviteContext.connections = [];
    renderProjectInviteUI();
    return [];
  }
  try {
    const rows = await api("/api/openclaw/connections");
    projectInviteContext.connections = Array.isArray(rows) ? rows : [];
  } catch {
    projectInviteContext.connections = [];
  }
  renderProjectInviteUI();
  return projectInviteContext.connections;
}

async function initializeProjectInviteContext({ refreshConnections = true, autoAccept = false } = {}) {
  if (!projectInviteContext.active) {
    renderProjectInviteUI();
    return null;
  }
  setMessage("project_invite_msg", "");
  const tokenQuoted = encodeURIComponent(projectInviteContext.token);
  const info = await api(`/api/projects/invites/${tokenQuoted}`);
  projectInviteContext.info = info || null;
  if (sessionToken) {
    await loadAccountProfile({ silent: true }).catch(() => {});
  }
  if (refreshConnections) {
    await loadProjectInviteConnections().catch(() => {});
  } else {
    renderProjectInviteUI();
  }
  if (autoAccept && sessionToken) {
    await tryAutoAcceptProjectInvite().catch(() => false);
  }
  return projectInviteContext.info;
}
function closeProjectInviteAgentModal({ reset = true } = {}) {
  $("project_invite_agent_modal")?.classList.add("hidden");
  setMessage("project_invite_agent_modal_msg", "");
  if (!reset) return;
  projectInviteManagedAgents = [];
  projectInviteAgentSelections = new Map();
  const picker = $("project_invite_agent_picker");
  if (picker) picker.innerHTML = "";
  const meta = $("project_invite_agent_modal_meta");
  if (meta) meta.textContent = "";
}

async function loadProjectInviteManagedAgents(connectionId) {
  const cid = String(connectionId || "").trim();
  if (!cid) return [];
  const q = `?connection_id=${encodeURIComponent(cid)}`;
  const res = await api(`/api/a2a/agents${q}`);
  const rows = Array.isArray(res?.agents) ? res.agents : [];
  projectInviteManagedAgents = rows
    .map((item) => {
      const id = String(item?.agent_id || item?.id || "").trim();
      const name = String(item?.agent_name || item?.name || id).trim() || id;
      const status = String(item?.status || "").trim();
      const connId = String(item?.connection_id || cid).trim() || cid;
      if (!id) return null;
      return { id, name, status, connection_id: connId };
    })
    .filter(Boolean);
  return projectInviteManagedAgents;
}

function renderProjectInviteAgentPicker() {
  const picker = $("project_invite_agent_picker");
  if (!picker) return;
  picker.innerHTML = "";

  const info = projectInviteContext.info || {};
  const inviteRole = String(info?.role || "").trim();
  const lockedAgentId = String(info?.requested_agent_id || "").trim();
  const lockedAgentName = String(info?.requested_agent_name || "").trim();

  const managed = Array.isArray(projectInviteManagedAgents) ? projectInviteManagedAgents : [];
  const displayRows = [...managed];
  for (const selected of projectInviteAgentSelections.values()) {
    const sid = String(selected?.agent_id || "").trim();
    if (!sid) continue;
    if (displayRows.some((row) => String(row?.id || "").trim() === sid)) continue;
    displayRows.push({
      id: sid,
      name: String(selected?.agent_name || sid).trim() || sid,
      status: "manual",
      connection_id: String($("invite_connection_id")?.value || "").trim(),
      manual: true,
    });
  }

  if (!displayRows.length) {
    const empty = document.createElement("div");
    empty.className = "helper";
    empty.textContent = "No managed agents found on this connection. Fill Agent ID manually in the invite card first.";
    picker.appendChild(empty);
    return;
  }

  for (const row of displayRows) {
    const agentId = String(row?.id || "").trim();
    if (!agentId) continue;
    const isLocked = Boolean(lockedAgentId && agentId === lockedAgentId);
    const selected = projectInviteAgentSelections.get(agentId) || null;
    const isChecked = Boolean(selected || isLocked);
    const roleValue = String(selected?.role || inviteRole || "").trim();

    const item = document.createElement("article");
    item.className = "agent-pick";

    const check = document.createElement("input");
    check.type = "checkbox";
    check.checked = isChecked;
    check.disabled = isLocked;

    const labelWrap = document.createElement("div");
    const strong = document.createElement("strong");
    strong.textContent = `${String(row?.name || agentId)} (${agentId})`;
    const meta = document.createElement("span");
    meta.className = "small";
    const infoBits = [];
    if (row?.status) infoBits.push(`status: ${String(row.status)}`);
    if (isLocked) infoBits.push("locked by invite");
    if (row?.manual) infoBits.push("manual");
    meta.textContent = infoBits.join(" | ") || "managed agent";
    labelWrap.appendChild(strong);
    labelWrap.appendChild(meta);

    const source = document.createElement("span");
    source.className = "small";
    source.textContent = row?.manual ? "custom" : "managed";

    const roleInput = document.createElement("input");
    roleInput.type = "text";
    roleInput.dataset.agentRole = "1";
    roleInput.placeholder = "Role in project";
    roleInput.value = roleValue;
    roleInput.disabled = !isChecked;

    if (isChecked) {
      projectInviteAgentSelections.set(agentId, {
        agent_id: agentId,
        agent_name: String(row?.name || selected?.agent_name || agentId).trim() || agentId,
        role: roleValue,
        locked: isLocked,
      });
    } else if (!isLocked) {
      projectInviteAgentSelections.delete(agentId);
    }

    check.addEventListener("change", () => {
      if (check.checked) {
        projectInviteAgentSelections.set(agentId, {
          agent_id: agentId,
          agent_name: String(row?.name || agentId).trim() || agentId,
          role: String(roleInput.value || "").trim(),
          locked: isLocked,
        });
        roleInput.disabled = false;
      } else if (!isLocked) {
        projectInviteAgentSelections.delete(agentId);
        roleInput.disabled = true;
      }
      setMessage("project_invite_agent_modal_msg", "");
    });

    roleInput.addEventListener("input", () => {
      if (!projectInviteAgentSelections.has(agentId)) return;
      const prev = projectInviteAgentSelections.get(agentId) || {};
      projectInviteAgentSelections.set(agentId, {
        ...prev,
        role: String(roleInput.value || "").trim(),
      });
    });

    item.appendChild(check);
    item.appendChild(labelWrap);
    item.appendChild(source);
    item.appendChild(roleInput);
    picker.appendChild(item);
  }
}

function collectProjectInviteSelectedAgents({ fallbackToManual = true } = {}) {
  const selected = [...projectInviteAgentSelections.values()]
    .map((item) => ({
      agent_id: String(item?.agent_id || "").trim(),
      agent_name: String(item?.agent_name || "").trim(),
      role: String(item?.role || "").trim(),
    }))
    .filter((item) => Boolean(item.agent_id));

  if (selected.length || !fallbackToManual) return selected;

  const info = projectInviteContext.info || {};
  const fallbackId = String($("invite_agent_id")?.value || info?.requested_agent_id || "").trim();
  if (!fallbackId) return [];
  return [
    {
      agent_id: fallbackId,
      agent_name: String($("invite_agent_name")?.value || info?.requested_agent_name || fallbackId).trim() || fallbackId,
      role: String(info?.role || "").trim(),
    },
  ];
}

async function openProjectInviteAgentModal() {
  if (!projectInviteContext.active) throw new Error("No active project invite.");
  if (!sessionToken) throw new Error("Login or sign up first before accepting invite.");

  const connectionId = String($("invite_connection_id")?.value || "").trim();
  if (!connectionId) throw new Error("Choose one of your OpenClaw connections first.");

  const info = projectInviteContext.info || {};
  const requiresCode = Boolean(info?.requires_invite_code);
  const inviteCode = String($("invite_portal_code")?.value || projectInviteContext.inviteCode || "").trim().toUpperCase();
  if (requiresCode && !inviteCode) {
    throw new Error("Invite code is required. Open Project-Invitation.md to get the code.");
  }

  await loadProjectInviteManagedAgents(connectionId);
  const lockId = String(info?.requested_agent_id || "").trim();
  const lockName = String(info?.requested_agent_name || lockId).trim() || lockId;
  const inviteRole = String(info?.role || "").trim();
  const typedId = String($("invite_agent_id")?.value || "").trim();
  const typedName = String($("invite_agent_name")?.value || typedId).trim() || typedId;

  projectInviteAgentSelections = new Map();
  if (lockId) {
    const lockedManaged = projectInviteManagedAgents.find((x) => String(x?.id || "").trim() === lockId);
    projectInviteAgentSelections.set(lockId, {
      agent_id: lockId,
      agent_name: String(lockedManaged?.name || lockName || lockId).trim() || lockId,
      role: inviteRole,
      locked: true,
    });
  } else if (typedId) {
    const typedManaged = projectInviteManagedAgents.find((x) => String(x?.id || "").trim() === typedId);
    projectInviteAgentSelections.set(typedId, {
      agent_id: typedId,
      agent_name: String(typedManaged?.name || typedName || typedId).trim() || typedId,
      role: inviteRole,
      locked: false,
    });
  } else if (projectInviteManagedAgents.length) {
    const first = projectInviteManagedAgents[0];
    projectInviteAgentSelections.set(String(first.id), {
      agent_id: String(first.id),
      agent_name: String(first.name || first.id).trim() || String(first.id),
      role: inviteRole,
      locked: false,
    });
  }

  const activeConn = projectInviteContext.connections.find((c) => String(c?.id || "").trim() === connectionId);
  const metaText = [
    `Project: ${String(info?.project_title || info?.project_id || "External Project")}`,
    `Connection: ${String(activeConn?.name || connectionId)}`,
    lockId ? `Locked Agent: ${lockName} (${lockId})` : "Locked Agent: none",
  ].join(" | ");
  const meta = $("project_invite_agent_modal_meta");
  if (meta) meta.textContent = metaText;

  renderProjectInviteAgentPicker();
  setMessage("project_invite_agent_modal_msg", "");
  $("project_invite_agent_modal")?.classList.remove("hidden");
}

async function acceptProjectInviteFromUI({ connectionId: overrideConnectionId = "", selectedAgents = null } = {}) {
  if (!projectInviteContext.active) throw new Error("No active project invite.");
  if (!sessionToken) throw new Error("Login or sign up first before accepting invite.");

  const connectionId = String(overrideConnectionId || $("invite_connection_id")?.value || "").trim();
  if (!connectionId) throw new Error("Choose one of your OpenClaw connections first.");

  const info = projectInviteContext.info || {};
  const requiresCode = Boolean(info?.requires_invite_code);
  const inviteCode = String($("invite_portal_code")?.value || projectInviteContext.inviteCode || "").trim().toUpperCase();
  if (requiresCode && !inviteCode) {
    throw new Error("Invite code is required. Open Project-Invitation.md to get the code.");
  }

  const payload = {
    connection_id: connectionId,
  };
  if (inviteCode) payload.invite_code = inviteCode;

  const selected = Array.isArray(selectedAgents) ? selectedAgents : null;
  if (selected && selected.length) {
    const normalized = [];
    const seen = new Set();
    for (const item of selected) {
      const agentId = String(item?.agent_id || item?.agentId || "").trim();
      if (!agentId || seen.has(agentId)) continue;
      seen.add(agentId);
      normalized.push({
        agent_id: agentId,
        agent_name: String(item?.agent_name || item?.agentName || "").trim() || undefined,
        role: String(item?.role || "").trim() || undefined,
      });
    }
    if (!normalized.length) {
      throw new Error("Pick at least one agent to join this project.");
    }
    payload.selected_agents = normalized;
    const lockedAgentId = String(info?.requested_agent_id || "").trim();
    const primary = (lockedAgentId && normalized.find((x) => String(x.agent_id || "") === lockedAgentId)) || normalized[0];
    payload.agent_id = String(primary?.agent_id || "").trim();
    if (primary?.agent_name) payload.agent_name = String(primary.agent_name).trim();
  } else {
    const agentId = String($("invite_agent_id")?.value || "").trim();
    const agentName = String($("invite_agent_name")?.value || "").trim();
    if (agentId) payload.agent_id = agentId;
    if (agentName) payload.agent_name = agentName;
  }

  const tokenQuoted = encodeURIComponent(projectInviteContext.token);
  const res = await api(`/api/projects/invites/${tokenQuoted}/accept`, "POST", payload);
  const acceptedProjectId = String(res?.project_id || "").trim();
  const acceptedConnectionId = String(res?.accepted_connection_id || connectionId).trim();
  if (acceptedConnectionId) preferredConnectionId = acceptedConnectionId;
  setMessage("project_invite_msg", "Invite accepted. Opening project...", "ok");
  closeProjectInviteAgentModal({ reset: true });
  clearProjectInviteContext({ clearUrl: true });
  await fetchInitial({ preferredProjectId: acceptedProjectId || null });
}

async function confirmProjectInviteFromModal() {
  const selected = collectProjectInviteSelectedAgents({ fallbackToManual: true });
  if (!selected.length) {
    throw new Error("Pick at least one agent to join this project.");
  }
  const connectionId = String($("invite_connection_id")?.value || "").trim();
  await acceptProjectInviteFromUI({ connectionId, selectedAgents: selected });
}

function ignoreProjectInviteFromUI() {
  clearProjectInviteContext({ clearUrl: true });
  setMessage("project_invite_msg", "Invite ignored. You can reopen invite link anytime.", "ok");
  if (sessionToken) {
    fetchInitial({ preferredProjectId: projectDeepLinkContext.projectId || null }).catch(() => setView("auth"));
  }
}

function claimConnectionPayload(prefix) {
  return {
    openclaw_base_url: $(`${prefix}_oc_base`)?.value?.trim() || "",
    openclaw_api_key: $(`${prefix}_oc_key`)?.value || "",
    openclaw_name: $(`${prefix}_oc_name`)?.value?.trim() || null,
  };
}

function validateClaimConnectionPayload(payload) {
  const base = String(payload?.openclaw_base_url || "").trim();
  const key = String(payload?.openclaw_api_key || "").trim();
  if (!base) throw new Error("OpenClaw base URL is required for claim.");
  if (!/^https?:\/\//i.test(base)) throw new Error("OpenClaw base URL must start with http:// or https://");
  if (!key) throw new Error("OpenClaw API key/token is required for claim.");
}

function validatePasswordStrength(password, label = "Password") {
  const value = String(password || "");
  if (value.length < PASSWORD_POLICY_MIN_LENGTH) {
    throw new Error(`${label} must be at least ${PASSWORD_POLICY_MIN_LENGTH} characters.`);
  }
  if (!/[a-z]/.test(value)) throw new Error(`${label} must include at least one lowercase letter.`);
  if (!/[A-Z]/.test(value)) throw new Error(`${label} must include at least one uppercase letter.`);
  if (!/[0-9]/.test(value)) throw new Error(`${label} must include at least one number.`);
  if (!/[^A-Za-z0-9]/.test(value)) throw new Error(`${label} must include at least one symbol.`);
}

function detailToText(detail) {
  if (detail == null) return "";
  if (typeof detail === "string") return detail;
  if (typeof detail === "number" || typeof detail === "boolean") return String(detail);
  if (Array.isArray(detail)) return detail.map((x) => detailToText(x)).filter(Boolean).join(" | ");
  if (typeof detail === "object") {
    const preferred = [detail.message, detail.error, detail.detail, detail.reason].map((x) => detailToText(x)).filter(Boolean);
    if (preferred.length) return preferred.join(" | ");
    return JSON.stringify(detail);
  }
  return String(detail);
}

function showUiError(targetId, err) {
  const msg = detailToText(err?.message || err || "Unknown error");
  setMessage(targetId, msg, "error");
  appendChatMessage("system", msg, "error");
}

function setView(name) {
  ["auth", "setup", "home"].forEach((v) => {
    const node = $("view_" + v);
    if (node) node.classList.toggle("active", v === name);
  });
  if (name === "home") clearChatIfFresh();
}

async function api(path, method = "GET", body = null) {
  const opts = { method, headers: { ...authHeaders() } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const ct = res.headers.get("content-type") || "";
  const data = ct.includes("application/json") ? await res.json() : await res.text();
  if (!res.ok) {
    if (res.status === 401) {
      clearAuthSession();
      setView("auth");
    }
    const msg = detailToText(data?.detail ?? data?.message ?? data);
    throw new Error(msg);
  }
  return data;
}

function formatTs(ts) {
  return new Date(ts * 1000).toLocaleString();
}

function shortText(text, max = 90) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return "No description";
  if (raw.length <= max) return raw;
  return raw.slice(0, max - 1).trimEnd() + "...";
}

function applyConnectionStatus() {
  const connected = Boolean(activeConnectionId && connectionHealthy);

  const dot = $("top_status_dot");
  const text = $("top_status_text");
  if (dot) {
    dot.classList.toggle("connected", connected);
    dot.classList.toggle("disconnected", !connected);
  }
  if (text) text.textContent = connected ? "Connected" : "Disconnected";

  const pill = $("config_conn_status");
  if (pill) {
    pill.textContent = connected ? "Connected" : "Disconnected";
    pill.classList.toggle("connected", connected);
    pill.classList.toggle("disconnected", !connected);
  }
}

function renderConfigConnectionDetails() {
  const conn = connectionsCache.find((x) => x.id === activeConnectionId) || null;
  const nameEl = $("config_conn_name");
  const urlEl = $("config_conn_url");
  if (nameEl) nameEl.textContent = conn?.name || "OpenClaw";
  if (urlEl) urlEl.textContent = conn?.base_url || "-";
  setMessage("config_msg", conn ? "Connection loaded." : "No connection selected.");
  applyConnectionStatus();
}

function renderAccountProfile(profile = null) {
  currentAccountProfile = profile || null;
  const emailEl = $("acct_email");
  const idEl = $("acct_user_id");
  const joinedEl = $("acct_created_at");
  const workspaceEl = $("acct_workspace_root");
  const projectsEl = $("acct_projects_count");
  const connEl = $("acct_connections_count");
  if (!emailEl || !idEl || !joinedEl || !workspaceEl || !projectsEl || !connEl) return;

  if (!profile) {
    emailEl.textContent = "-";
    idEl.textContent = "-";
    joinedEl.textContent = "-";
    workspaceEl.textContent = "-";
    projectsEl.textContent = "0";
    connEl.textContent = "0";
    return;
  }

  emailEl.textContent = String(profile.email || "-");
  idEl.textContent = String(profile.id || "-");
  joinedEl.textContent = profile.created_at ? formatTs(profile.created_at) : "-";
  workspaceEl.textContent = String(profile.workspace_root || "-");
  projectsEl.textContent = Number(profile.projects_count || 0).toLocaleString();
  connEl.textContent = Number(profile.connections_count || 0).toLocaleString();
}

async function loadAccountProfile({ silent = false } = {}) {
  const profile = await api("/api/me");
  renderAccountProfile(profile);
  if (!silent) setMessage("account_msg", "Account loaded.", "ok");
  return profile;
}

async function changeAccountPassword(ev) {
  ev.preventDefault();
  setMessage("account_msg", "");
  const currentPassword = $("acct_current_pass")?.value || "";
  const newPassword = $("acct_new_pass")?.value || "";
  const confirmPassword = $("acct_confirm_pass")?.value || "";
  if (!currentPassword || !newPassword || !confirmPassword) {
    throw new Error("Fill all password fields.");
  }
  validatePasswordStrength(newPassword, "New password");
  if (newPassword !== confirmPassword) {
    throw new Error("New password confirmation does not match.");
  }
  await api("/api/me/password", "POST", {
    current_password: currentPassword,
    new_password: newPassword,
  });
  $("form_change_password")?.reset();
  setMessage("account_msg", "Password updated successfully.", "ok");
}

function resetClientSessionState(authMessage) {
  if (streamAbort) {
    streamAbort.abort();
    streamAbort = null;
  }
  projectStreamConnected = false;
  if (projectRefreshTimer) {
    clearTimeout(projectRefreshTimer);
    projectRefreshTimer = null;
  }
  if (runtimePollHandle) {
    clearInterval(runtimePollHandle);
    runtimePollHandle = null;
  }
  clearAuthSession();
  renderConnections([]);
  showEmptyProject();
  renderAccountProfile(null);
  setView("auth");
  if (authMessage) setMessage("auth_msg", authMessage, "ok");
}

async function logoutUser() {
  try {
    await api("/api/logout", "POST");
  } catch {}
  resetClientSessionState("Logged out.");
}

async function deleteAccount(ev) {
  ev.preventDefault();
  setMessage("account_msg", "");
  const currentPassword = $("acct_delete_pass")?.value || "";
  const confirmText = String($("acct_delete_confirm")?.value || "").trim().toUpperCase();
  if (!currentPassword) throw new Error("Enter current password.");
  if (confirmText !== "DELETE") throw new Error("Type DELETE to confirm account deletion.");

  const ok = window.confirm(
    "Delete account permanently? This removes all projects, connections, and workspace files."
  );
  if (!ok) return;

  await api("/api/me/delete", "POST", {
    current_password: currentPassword,
    confirm_text: confirmText,
  });
  $("form_delete_account")?.reset();
  $("form_change_password")?.reset();
  resetClientSessionState("Account deleted.");
}

function applyWorkspacePolicy(policy) {
  workspacePolicy = {
    workspace_root: "HIVEE",
    templates_root: "HIVEE/TEMPLATES",
    main_agent_id: "",
    main_agent_name: "",
    bootstrap_status: "unknown",
    workspace_tree: "",
    ...policy,
  };
  workspaceTreeText = String(workspacePolicy?.workspace_tree || "").trim();
  renderFolderBrowsers();
  syncChatContextControls();
  updateChatProjectName();
}

async function loadConnectionPolicy(connectionId) {
  if (!connectionId) {
    applyWorkspacePolicy(null);
    return null;
  }
  const policy = await api(`/api/openclaw/${connectionId}/policy`);
  applyWorkspacePolicy(policy);
  return policy;
}

async function ensureWorkspaceForActiveConnection({ silent = false } = {}) {
  if (!activeConnectionId) {
    applyWorkspacePolicy(null);
    return null;
  }

  if (!silent) setMessage("config_msg", "Ensuring HIVEE workspace...", "");
  const bootstrap = await api(`/api/openclaw/${activeConnectionId}/bootstrap`, "POST");
  const policy = await loadConnectionPolicy(activeConnectionId).catch(() => null);
  if (!silent) setMessage("config_msg", "HIVEE workspace ready.", "ok");
  return { bootstrap, policy };
}

async function loadWorkspaceTree() {
  const data = await api("/api/workspace/tree");
  workspaceTreeText = String(data?.tree || "").trim();
  if (data?.workspace_root) workspacePolicy.workspace_root = data.workspace_root;
  renderFolderBrowsers();
  return data;
}

async function loadProjectWorkspaceTree(projectId) {
  if (!projectId) {
    projectTreeText = "";
    renderFolderBrowsers();
    return null;
  }
  const data = await api(`/api/projects/${projectId}/workspace/tree`);
  projectTreeText = String(data?.tree || "").trim();
  if (selectedProjectData && data?.project_root) {
    selectedProjectData.project_root = data.project_root;
  }
  renderFolderBrowsers();
  return data;
}

function renderConnections(connections) {
  connectionsCache = connections || [];
  const sel = $("home_connections");
  if (sel) {
    sel.innerHTML = "";
    for (const c of connectionsCache) {
      const opt = document.createElement("option");
      opt.value = c.id;
      opt.textContent = `${c.name || "OpenClaw"} - ${c.base_url}`;
      sel.appendChild(opt);
    }
  }

  if (!connectionsCache.length) {
    activeConnectionId = null;
    preferredConnectionId = null;
    connectionHealthy = false;
    renderConfigConnectionDetails();
    return;
  }

  const hasPreferredConnection = Boolean(
    preferredConnectionId
    && connectionsCache.some((c) => c.id === preferredConnectionId)
  );
  if (hasPreferredConnection) {
    activeConnectionId = preferredConnectionId;
    preferredConnectionId = null;
  } else if (!activeConnectionId || !connectionsCache.some((c) => c.id === activeConnectionId)) {
    activeConnectionId = connectionsCache[0].id;
  }
  if (sel) sel.value = activeConnectionId;
  renderConfigConnectionDetails();
}

function projectNeedsApproval(project) {
  const plan = String(project?.plan_status || "").trim().toLowerCase();
  const exec = String(project?.execution_status || "").trim().toLowerCase();
  return plan === "awaiting_approval" || exec === "paused";
}

function renderProjects(projects) {
  projectsCache = projects || [];
  const box = $("projects_list");
  if (!box) return;
  box.innerHTML = "";

  if (!projectsCache.length) {
    box.innerHTML = '<p class="helper">No projects yet. Create your first project.</p>';
    renderWorkspaceUsage();
    return;
  }

  for (const p of projectsCache) {
    const needsApproval = projectNeedsApproval(p);
    const el = document.createElement("button");
    el.type = "button";
    el.className = "project-item" + (p.id === selectedProjectId ? " active" : "") + (needsApproval ? " needs-approval" : "");

    const titleRow = document.createElement("div");
    titleRow.className = "project-item-title-row";

    const title = document.createElement("strong");
    title.textContent = p.title;
    titleRow.appendChild(title);

    if (needsApproval) {
      const dot = document.createElement("span");
      dot.className = "project-item-approval-dot";
      dot.title = "Needs approval";
      titleRow.appendChild(dot);
    }

    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = formatTs(p.created_at);

    el.appendChild(titleRow);
    el.appendChild(meta);

    el.onclick = () => {
      if (activeNavTab !== "projects") setNavTab("projects");
      selectProject(p.id).catch((e) => showUiError("chat_hint", e));
    };
    box.appendChild(el);
  }

  // Also render horizontal cards in the center empty state grid
  const grid = $("project_cards_grid");
  if (grid) {
    grid.innerHTML = "";
    if (!projectsCache.length) {
      grid.innerHTML = '<p class="helper" style="padding:8px 0">No projects yet. Click New Project to get started.</p>';
    } else {
      for (const p of projectsCache) {
        const card = document.createElement("button");
        card.type = "button";
        card.className = "project-card";
        const needsApproval = projectNeedsApproval(p);
        card.innerHTML = `<strong>${p.title}</strong><div class="meta">${formatTs(p.created_at)}${needsApproval ? ' Â· Needs approval' : ''}</div>`;
        card.onclick = () => selectProject(p.id).catch((e) => showUiError("chat_hint", e));
        grid.appendChild(card);
      }
    }
  }
  renderWorkspaceUsage();
}

function showEmptyProject() {
  const empty = $("project_empty");
  const details = $("project_details");
  if (empty) empty.classList.remove("hidden");
  if (details) details.classList.add("hidden");
  selectedProjectId = null;
  selectedProjectData = null;
  selectedProjectPlan = null;
  selectedProjectReadiness = null;
  selectedPrimaryAgentId = null;
  selectedAssignedAgents = [];
  chatContextMode = "workspace";
  projectTreeText = "";
  projectFilesCurrentPath = "";
  workspaceFilesCurrentPath = "";
  workspaceFilesPayload = null;
  projectFilePreviewPath = "";
  projectFilesPayload = null;
  releaseProjectFilePreviewBlob();
  releaseWorkspaceFilePreviewBlob();
  livePreviewPath = "";
  livePreviewReqSeq += 1;
  clearLivePreview("No project selected.");
  if (streamAbort) {
    streamAbort.abort();
    streamAbort = null;
  }
  projectStreamConnected = false;
  if (projectRefreshTimer) {
    clearTimeout(projectRefreshTimer);
    projectRefreshTimer = null;
  }
  if (runtimePollHandle) {
    clearInterval(runtimePollHandle);
    runtimePollHandle = null;
  }
  const tracker = $("events");
  if (tracker) tracker.innerHTML = "";
  const live = $("overview_live_updates");
  if (live) live.innerHTML = "";
  syncPrimaryNavState();
  syncProjectContextSidebar();
  syncChatContextControls();
  updateChatProjectName();
  renderFolderBrowsers();
  renderProjectUsage();
  renderWorkspaceUsage();
  renderProjectPlanInfo();
  renderProjectFiles(null);
  renderWorkspaceFiles(null);
  renderLiveStatus();
  syncProjectHeadbar();
  syncWorkspaceSectionTitle();
}

function showProjectDetails() {
  const empty = $("project_empty");
  const details = $("project_details");
  if (empty) empty.classList.add("hidden");
  if (details) details.classList.remove("hidden");
  syncProjectContextSidebar();
  syncProjectHeadbar();
  syncWorkspaceSectionTitle();
}

function getActiveNavLabel() {
  const activeBtn = document.querySelector("[data-nav-tab].active");
  const text = activeBtn?.querySelector("span")?.textContent || activeBtn?.textContent || "Workspace";
  return String(text).trim();
}

function syncWorkspaceSectionTitle() {
  const title = $("workspace_section_title");
  if (!title) return;
  // When inside a project show its name as the big heading
  if (activeNavTab === "projects" && selectedProjectData) {
    title.textContent = selectedProjectData.title;
  } else {
    title.textContent = getActiveNavLabel();
  }
}

function syncPrimaryNavState() {
  const nav = document.querySelector(".primary-nav");
  const projectsBtn = document.querySelector('[data-nav-tab="projects"]');
  const inProjects = activeNavTab === "projects";
  if (nav) nav.classList.toggle("projects-mode", inProjects);
  if (projectsBtn) {
    projectsBtn.classList.toggle("project-selected", inProjects && Boolean(selectedProjectId));
  }
}

function syncProjectContextSidebar() {
  const context = $("context_sidebar");
  if (!context) return;
  const shouldShow = activeNavTab === "projects" && Boolean(selectedProjectId);
  context.classList.toggle("hidden", !shouldShow);
}

function syncProjectHeadbar() {
  const bar = $("project_headbar");
  const title = $("detail_title");
  const titleText = $("detail_title_text");
  const titleApproval = $("detail_title_approval_badge");
  const subline = $("detail_subline");
  const runBtn = $("btn_run");
  const deleteBtn = $("btn_delete_project");
  if (!bar || !title || !subline || !runBtn || !deleteBtn) return;

  if (activeNavTab !== "projects") {
    bar.classList.add("hidden");
    return;
  }

  if (selectedProjectData) {
    // Title shows current pane; section title (above) shows project name
    const paneLabels = { live: "Live", info: "Overview", folder: "Files", usage: "Usage", tracker: "Tracker" };
    const paneLabel = paneLabels[activeProjectPane] || String(activeProjectPane);
    if (titleText) titleText.textContent = paneLabel;
    else title.textContent = paneLabel;

    if (titleApproval) {
      titleApproval.classList.toggle("hidden", !projectNeedsApproval(selectedProjectData));
    }

    const createdAt = selectedProjectData.created_at ? formatTs(selectedProjectData.created_at) : "-";
    const stage = projectStageLabel(selectedProjectReadiness?.stage || "draft");
    subline.textContent = `${String(selectedProjectData.title || "").trim()} â€” Stage ${stage} â€” ${createdAt}`;
    const canRun = Boolean(selectedProjectReadiness?.can_run);
    runBtn.classList.remove("hidden");
    runBtn.disabled = !canRun;
    runBtn.textContent = canRun ? "Run" : "Run (Locked)";
    runBtn.title = canRun
      ? "Start project run"
      : detailToText(selectedProjectReadiness?.summary || "Complete readiness checklist first.");
    deleteBtn.classList.remove("hidden");
  } else {
    if (titleText) titleText.textContent = "";
    if (titleApproval) titleApproval.classList.add("hidden");
    bar.classList.add("hidden");
    runBtn.classList.add("hidden");
    runBtn.disabled = true;
    runBtn.textContent = "Run";
    deleteBtn.classList.add("hidden");
    return;
  }

  bar.classList.remove("hidden");
}

function addEvent(kind, payload) {
  const box = $("events");
  if (!box) return;
  const row = document.createElement("div");
  row.className = "event";
  row.innerHTML = `<div class="meta">${new Date().toLocaleTimeString()} - ${kind}</div><div class="text"></div>`;
  row.querySelector(".text").textContent = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  box.appendChild(row);
  box.scrollTop = box.scrollHeight;
}

function addLiveUpdate(kind, payload) {
  const box = $("overview_live_updates");
  if (!box) return;
  const row = document.createElement("div");
  row.className = "event";
  row.innerHTML = `<div class="meta">${new Date().toLocaleTimeString()} - ${kind}</div><div class="text"></div>`;
  row.querySelector(".text").textContent = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  box.appendChild(row);
  box.scrollTop = box.scrollHeight;
}

function eventSummary(kind, payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const name = data.agent_name || data.agent_id || "agent";
  if (kind === "project.delegation.started") return "Primary agent started delegation.";
  if (kind === "project.delegation.ready") return `Delegation ready. Reports: ${Number(data.processed_agents || 0)} success, ${Number(data.failed_agents || 0)} failed.`;
  if (kind === "project.delegation.failed") return `Delegation failed: ${detailToText(data.error || payload)}`;
  if (kind === "project.delegation.stopped") return "Delegation stopped by user.";
  if (kind === "agent.primary.update") return `Primary update: ${shortText(data.text || "", 180)}`;
  if (kind === "agent.task.assigned") return `${name} assigned task.`;
  if (kind === "agent.task.started") return `${name} started task.`;
  if (kind === "agent.task.reported") return `${name} reported update.`;
  if (kind === "agent.task.failed") return `${name} failed: ${shortText(data.error || "", 180)}`;
  if (kind === "agent.task.live") return `${name}: ${shortText(data.note || "", 180)}`;
  if (kind === "project.file.written") return `${data.actor || "actor"} wrote ${data.path || "file"}.`;
  if (kind === "project.execution.auto_paused") return `Execution paused: ${shortText(data.reason || "Waiting for user input.", 180)}`;
  if (kind === "project.execution.resumed_after_pause") return "Execution resumed after approval.";
  if (kind.startsWith("project.execution.")) return `Execution ${kind.split(".").pop()}: ${data.status || "-"}.`;
  if (kind === "run.started") return "Run started.";
  if (kind === "run.completed") return "Run completed.";
  if (kind === "run.stopped") return "Run stopped.";
  if (kind === "project.plan.ready") return "Plan ready. Waiting for approval.";
  if (kind === "project.external_agent.joined") return `${name} joined this project${data.role ? ` as ${data.role}` : ""}.`;
  return "";
}

function eventChatMessage(kind, payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const name = data.agent_name || data.agent_id || "agent";
  if (kind === "project.delegation.started") return { role: "agent", agentId: data.agent_id || "primary", text: "I am delegating tasks to invited agents now.", meta: "primary agent" };
  if (kind === "agent.primary.update") {
    const raw = String(data.text || "").trim();
    const looksStructured =
      raw.startsWith("{") ||
      raw.startsWith("```") ||
      raw.includes("\"agent_tasks\"") ||
      raw.includes("\"project_md\"");
    if (looksStructured) {
      return {
        role: "agent",
        agentId: data.agent_id || "primary",
        text: "I finished delegation setup, assigned tasks to invited agents, and I will hand off work with @mentions in chat.",
        meta: `${name}`,
      };
    }
    return { role: "agent", agentId: data.agent_id || "primary", text: raw || "I have a new project update.", meta: `${name}` };
  }
  if (kind === "agent.task.assigned") {
    const mentions = Array.isArray(data.mentions) && data.mentions.length ? ` I will sync with ${data.mentions.map((x) => `@${x}`).join(", ")}.` : "";
    return { role: "agent", agentId: data.agent_id || name, text: `Hey team, I got my task as ${data.role || "contributor"} and I am starting now.${mentions}`, meta: `${name}` };
  }
  if (kind === "agent.task.started") return { role: "agent", agentId: data.agent_id || name, text: "I started working on my assigned task.", meta: `${name}` };
  if (kind === "agent.task.reported") return { role: "agent", agentId: data.agent_id || name, text: data.text || "I finished this step and shared my output.", meta: `${name}` };
  if (kind === "agent.chat.update") return { role: "agent", agentId: data.agent_id || name, text: data.text || "Update posted.", meta: `${name}` };
  if (kind === "project.external_agent.joined") {
    const roleText = String(data.role || "").trim();
    const roleSuffix = roleText ? ` as ${roleText}` : "";
    return {
      role: "system",
      text: `${name} joined the project${roleSuffix}.`,
      meta: "membership",
    };
  }
  if (kind === "project.execution.auto_paused") {
    const reason = detailToText(data.reason || "Execution paused. Waiting for your approval/input.");
    const hint = detailToText(data.resume_hint || "Reply with required info, then say CONTINUE or press Resume.");
    return { role: "system", text: `${reason}\n${hint}`, meta: "approval required" };
  }
  if (kind === "project.execution.pause") return { role: "system", text: detailToText(data.summary || "Execution paused."), meta: "workflow" };
  if (kind === "project.execution.resume" || kind === "project.execution.resumed_after_pause") {
    return { role: "system", text: detailToText(data.summary || "Execution resumed."), meta: "workflow" };
  }
  if (kind === "project.execution.stop") return { role: "system", text: detailToText(data.summary || "Execution stopped."), meta: "workflow" };
  if (kind === "agent.task.failed") return { role: "system", text: `${name} hit an error: ${detailToText(data.error || payload)}`, meta: "live - task failed" };
  if (kind === "project.delegation.ready") {
    const ownerMessage = String(data.owner_message || "").trim();
    if (ownerMessage) {
      return null;
    }
    const outputsLink = String(data.outputs_folder_link || "").trim();
    const linkSuffix = outputsLink ? ` Open outputs: ${outputsLink}` : "";
    return {
      role: "agent",
      agentId: data.agent_id || "primary",
      text: `@owner delegation is done. ${Number(data.processed_agents || 0)} agents already reported.${linkSuffix}`,
      meta: "primary agent",
    };
  }
  return null;
}

function scheduleProjectDataRefresh() {
  if (!selectedProjectId) return;
  if (projectRefreshTimer) return;
  projectRefreshTimer = setTimeout(() => {
    projectRefreshTimer = null;
    if (!selectedProjectId) return;
    refreshSelectedProjectData().catch(() => {});
    loadProjectFiles(projectFilesCurrentPath || "").catch(() => {});
    if (activeNavTab === "files") {
      loadWorkspaceFiles(workspaceFilesCurrentPath || DEFAULT_OWNER_FILES_PATH).catch(() => {});
    }
  }, 320);
}

function handleProjectEvent(kind, payload) {
  addEvent(kind, payload);
  addLiveUpdate(kind, payload);
  const livePath = captureLiveOutputPath(payload);
  if (livePath) {
    livePreviewPath = livePath;
    if (activeProjectPane === "live") {
      renderLivePreview(livePreviewPath, { force: true }).catch(() => {});
    } else {
      setLivePreviewMeta(`Latest output: ${livePreviewPath}`);
    }
  }
  const chat = eventChatMessage(kind, payload);
  if (chat?.text) appendChatMessage(chat.role || "assistant", chat.text, chat.meta || `live - ${kind}`, { agentId: chat.agentId || chat.agent_id || "" });
  if (kind === "project.execution.auto_paused") {
    setMessage("chat_hint", detailToText(payload?.reason || "Execution paused. Waiting for approval/input."), "error");
  } else if (kind === "project.execution.resume" || kind === "project.execution.resumed_after_pause") {
    setMessage("chat_hint", "Execution resumed.", "ok");
  }
  if (selectedProjectId && /^(project\.|run\.|agent\.)/.test(String(kind))) {
    scheduleProjectDataRefresh();
  }
}

function colorForAgent(agentId) {
  const hex = colorHexForAgent(agentId);
  return {
    hex,
    bg: hexToRgba(hex, 0.11),
    border: hexToRgba(hex, 0.5),
  };
}

function parseMessageLinks(text) {
  const raw = String(text || "");
  const re = /((?:https?:\/\/|\/api\/|\/\?)[^\s<>"']+)/g;
  const parts = [];
  let cursor = 0;
  let match;
  while ((match = re.exec(raw)) !== null) {
    const start = match.index;
    const end = start + match[0].length;
    if (start > cursor) {
      parts.push({ type: "text", value: raw.slice(cursor, start) });
    }
    let linkText = match[0];
    let suffix = "";
    while (/[),.;!?]$/.test(linkText)) {
      suffix = linkText.slice(-1) + suffix;
      linkText = linkText.slice(0, -1);
    }
    if (linkText) {
      parts.push({ type: "link", value: linkText });
    }
    if (suffix) {
      parts.push({ type: "text", value: suffix });
    }
    cursor = end;
  }
  if (cursor < raw.length) {
    parts.push({ type: "text", value: raw.slice(cursor) });
  }
  return parts.length ? parts : [{ type: "text", value: raw }];
}

function _projectLinkLabel(target) {
  const pane = normalizeProjectPaneForLink(target?.pane || "folder");
  const path = normalizePath(target?.path || "");
  const previewPath = normalizePath(target?.previewPath || "");
  if (previewPath) return "Open latest file in Project Space";
  if (pane === "folder" && path) return `Open ${path} in Project Space`;
  if (pane === "live") return "Open live preview in Project Space";
  if (pane === "folder") return "Open project files in Project Space";
  return "Open project in Project Space";
}

function _parseProjectDeepLinkTarget(rawLink) {
  try {
    const parsed = new URL(String(rawLink || ""), window.location.origin);
    if (parsed.origin !== window.location.origin) return null;
    const projectId = String(parsed.searchParams.get(PROJECT_DEEPLINK_PROJECT_PARAM) || "").trim();
    if (!projectId) return null;
    const pane = normalizeProjectPaneForLink(parsed.searchParams.get(PROJECT_DEEPLINK_PANE_PARAM));
    const path = normalizePath(parsed.searchParams.get(PROJECT_DEEPLINK_PATH_PARAM) || "");
    const previewPath = normalizePath(parsed.searchParams.get(PROJECT_DEEPLINK_PREVIEW_PARAM) || "");
    return {
      projectId,
      pane,
      path,
      previewPath,
      label: _projectLinkLabel({ pane, path, previewPath }),
    };
  } catch {
    return null;
  }
}

function _parseProjectApiLinkTarget(rawLink) {
  try {
    const parsed = new URL(String(rawLink || ""), window.location.origin);
    if (parsed.origin !== window.location.origin) return null;
    const pathname = String(parsed.pathname || "");
    const filesMatch = pathname.match(/^\/api\/projects\/([^/]+)\/files$/i);
    if (filesMatch) {
      const projectId = decodeURIComponent(filesMatch[1] || "").trim();
      if (!projectId) return null;
      const path = normalizePath(parsed.searchParams.get("path") || "");
      return {
        projectId,
        pane: "folder",
        path,
        previewPath: "",
        label: _projectLinkLabel({ pane: "folder", path }),
      };
    }

    const previewMatch = pathname.match(/^\/api\/projects\/([^/]+)\/preview\/(.+)$/i);
    if (previewMatch) {
      const projectId = decodeURIComponent(previewMatch[1] || "").trim();
      const previewPath = normalizePath(decodeURIComponent(previewMatch[2] || ""));
      if (!projectId || !previewPath) return null;
      const parentPath = previewPath.includes("/") ? previewPath.slice(0, previewPath.lastIndexOf("/")) : "";
      return {
        projectId,
        pane: "folder",
        path: normalizePath(parentPath),
        previewPath,
        label: _projectLinkLabel({ pane: "folder", path: parentPath, previewPath }),
      };
    }
    return null;
  } catch {
    return null;
  }
}

function parseProjectSpaceLinkTarget(rawLink) {
  return _parseProjectDeepLinkTarget(rawLink) || _parseProjectApiLinkTarget(rawLink);
}

async function openProjectSpaceFromLinkTarget(target) {
  const projectId = String(target?.projectId || "").trim();
  if (!projectId) return;

  if (selectedProjectId !== projectId) {
    await selectProject(projectId);
  }
  if (activeNavTab !== "projects") {
    setNavTab("projects");
  }

  const pane = normalizeProjectPaneForLink(target?.pane || "folder");
  setProjectPane(pane);

  if (pane === "folder") {
    const path = normalizePath(target?.path || "");
    await loadProjectFiles(path).catch(() => {});
    const previewPath = normalizePath(target?.previewPath || "");
    if (previewPath) {
      await openProjectFile(previewPath).catch(() => {});
    }
  } else if (pane === "live") {
    const previewPath = normalizePath(target?.previewPath || "");
    if (previewPath) {
      livePreviewPath = normalizeOutputPath(previewPath);
      await renderLivePreview(livePreviewPath, { force: true }).catch(() => {});
    }
  }
}

function renderChatBubbleContent(bubble, text) {
  bubble.textContent = "";
  const parts = parseMessageLinks(text);
  for (const part of parts) {
    if (part.type !== "link") {
      bubble.appendChild(document.createTextNode(part.value));
      continue;
    }

    const internalTarget = parseProjectSpaceLinkTarget(part.value);
    const a = document.createElement("a");

    if (internalTarget) {
      const href = buildProjectDeepLink(internalTarget.projectId, {
        pane: internalTarget.pane,
        path: internalTarget.path,
        previewPath: internalTarget.previewPath,
      });
      a.href = href || part.value;
      a.textContent = internalTarget.label || part.value;
      a.target = "_self";
      a.addEventListener("click", (ev) => {
        ev.preventDefault();
        openProjectSpaceFromLinkTarget(internalTarget)
          .catch((e) => setMessage("chat_hint", detailToText(e?.message || e), "error"));
      });
      bubble.appendChild(a);
      continue;
    }

    a.href = part.value;
    a.textContent = part.value;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    bubble.appendChild(a);
  }
}

function appendChatMessage(role, text, meta = "", opts = {}) {
  const box = $("chat_messages");
  if (!box) return;
  const stickToBottom = (box.scrollHeight - box.scrollTop - box.clientHeight) < 36;
  const node = document.createElement("div");
  node.className = `chat-msg ${role}`;
  const safeMeta = meta || `${role} - ${new Date().toLocaleTimeString()}`;
  node.innerHTML = `<div class="chat-meta">${safeMeta}</div><div class="chat-bubble"></div>`;
  const bubble = node.querySelector(".chat-bubble");
  renderChatBubbleContent(bubble, text);
  const agentId = String(opts?.agentId || "").trim();
  if (role === "agent" && agentId) {
    const palette = colorForAgent(agentId);
    bubble.style.background = palette.bg;
    bubble.style.border = `1px solid ${palette.border}`;
  }
  // Approval messages get gradient outline
  if (String(meta).toLowerCase().includes("approval")) {
    node.classList.add("needs-approval");
  }
  box.appendChild(node);
  if (stickToBottom) box.scrollTop = box.scrollHeight;
}

function clearChatIfFresh() {
  const box = $("chat_messages");
  if (!box) return;
  if (!box.children.length) {
    appendChatMessage("system", "Connected. Mention agent using @alias.", "ready");
  }
}

function normalizeChatContextMode(mode) {
  return String(mode || "").trim().toLowerCase() === "project" ? "project" : "workspace";
}

function activeChatContextMode() {
  const requested = normalizeChatContextMode(chatContextMode);
  if (requested === "project" && selectedProjectId) return "project";
  return "workspace";
}

function workspaceMainChatAgent() {
  const id = String(workspacePolicy?.main_agent_id || "").trim();
  if (!id) return null;
  const name = String(workspacePolicy?.main_agent_name || id).trim() || id;
  return {
    id,
    name,
    role: "owner-main",
    is_primary: true,
  };
}

function syncChatContextControls() {
  const select = $("chat_context_mode");
  const note = $("chat_context_note");
  const hasProject = Boolean(selectedProjectId);
  const effectiveMode = activeChatContextMode();
  chatContextMode = effectiveMode;

  if (select) {
    const projectOption = select.querySelector('option[value="project"]');
    if (projectOption) projectOption.disabled = !hasProject;
    if (select.value !== effectiveMode) select.value = effectiveMode;
  }

  if (note) {
    note.textContent = effectiveMode === "project"
      ? "Project context active: invited project agents available."
      : "Workspace context active: only your main user agent is available.";
  }

  const input = $("chat_input");
  if (input) {
    input.placeholder = effectiveMode === "project"
      ? "Type message... example: @dailybot make recap"
      : "Type workspace message... example: review my workspace status";
  }
}

function setChatContextMode(mode, { silent = false } = {}) {
  chatContextMode = normalizeChatContextMode(mode);
  syncChatContextControls();
  updateChatProjectName();
  loadChatAgents().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  if (!silent) {
    setMessage(
      "chat_hint",
      activeChatContextMode() === "project"
        ? "Project context active."
        : "Workspace context active (main user agent only).",
      "ok"
    );
  }
}

function updateChatProjectName() {
  const el = $("chat_project_name");
  if (!el) return;
  const modeLabel = activeChatContextMode() === "project" ? "Project" : "Workspace";
  if (!selectedProjectData) {
    el.textContent = `Project: none | Context: ${modeLabel}`;
    return;
  }
  el.textContent = `Project: ${selectedProjectData.title} | Context: ${modeLabel}`;
}

function planStatusLabel(status) {
  const s = String(status || "").toLowerCase();
  if (s === "approved") return "Approved";
  if (s === "awaiting_approval") return "Waiting Approval";
  if (s === "generating") return "Generating";
  if (s === "failed") return "Failed";
  return "Pending";
}

function projectStageLabel(stage) {
  const s = String(stage || "").toLowerCase();
  if (s === "active") return "Active";
  if (s === "planning") return "Planning";
  return "Draft";
}

function executionStatusLabel(status) {
  const s = String(status || "").toLowerCase();
  if (s === "running") return "Running";
  if (s === "paused") return "Paused";
  if (s === "stopped") return "Stopped";
  if (s === "completed") return "Completed";
  return "Idle";
}

function clampProgress(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function formatBytes(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "-";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function normalizePath(path) {
  return String(path || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
}

function encodePathForUrlPath(path) {
  const clean = normalizePath(path);
  if (!clean) return "";
  return clean.split("/").map((part) => encodeURIComponent(part)).join("/");
}

function buildProjectPreviewUrl(path) {
  const rel = encodePathForUrlPath(path);
  if (!rel || !selectedProjectId) return "";
  return `/api/projects/${encodeURIComponent(selectedProjectId)}/preview/${rel}`;
}

function buildWorkspacePreviewUrl(path) {
  const rel = encodePathForUrlPath(path);
  if (!rel) return "";
  return `/api/workspace/preview/${rel}`;
}

function normalizeOutputPath(path) {
  const clean = normalizePath(path);
  if (!clean) return "";
  if (/^outputs(\/|$)/i.test(clean)) return clean.replace(/^outputs/i, "Outputs");
  return clean;
}

function isOutputsPath(path) {
  return /^Outputs(\/|$)/i.test(normalizeOutputPath(path));
}

function fileExtension(path) {
  const clean = normalizePath(path);
  const base = clean.split("/").pop() || "";
  const idx = base.lastIndexOf(".");
  if (idx < 0) return "";
  return base.slice(idx + 1).toLowerCase();
}

function releaseLivePreviewBlob() {
  if (livePreviewBlobUrl) {
    URL.revokeObjectURL(livePreviewBlobUrl);
    livePreviewBlobUrl = "";
  }
}

function releaseProjectFilePreviewBlob() {
  if (projectFilePreviewBlobUrl) {
    URL.revokeObjectURL(projectFilePreviewBlobUrl);
    projectFilePreviewBlobUrl = "";
  }
}

function releaseWorkspaceFilePreviewBlob() {
  if (workspaceFilePreviewBlobUrl) {
    URL.revokeObjectURL(workspaceFilePreviewBlobUrl);
    workspaceFilePreviewBlobUrl = "";
  }
}

function releaseScopedFilePreviewBlob(scope) {
  if (scope === "project") {
    releaseProjectFilePreviewBlob();
  } else {
    releaseWorkspaceFilePreviewBlob();
  }
}

function setScopedFilePreviewBlob(scope, objectUrl) {
  if (scope === "project") {
    projectFilePreviewBlobUrl = objectUrl;
  } else {
    workspaceFilePreviewBlobUrl = objectUrl;
  }
}

function resetFilePreview(containerId, message = "Select a file to preview.") {
  const preview = $(containerId);
  if (!preview) return;
  preview.innerHTML = "";
  const helper = document.createElement("p");
  helper.className = "helper";
  helper.textContent = message;
  preview.appendChild(helper);
}

function mountFilePreviewNode(containerId, node, metaText = "") {
  const preview = $(containerId);
  if (!preview) return;
  preview.innerHTML = "";
  if (metaText) {
    const meta = document.createElement("div");
    meta.className = "file-preview-meta";
    meta.textContent = metaText;
    preview.appendChild(meta);
  }
  preview.appendChild(node);
}

function renderTextFilePreview(containerId, data, fallbackPath = "") {
  const pathText = String(data?.path || fallbackPath || "-");
  const sizeText = formatBytes(data?.size);
  const pre = document.createElement("pre");
  pre.className = "file-preview-pre";
  const body = [];
  if (data?.content) body.push(String(data.content));
  if (data?.truncated) body.push("\n(Preview truncated)");
  if (!body.length) body.push("(empty file)");
  pre.textContent = body.join("\n").trimEnd();
  mountFilePreviewNode(
    containerId,
    pre,
    `Path: ${pathText} | Size: ${sizeText}${data?.truncated ? " | Truncated" : ""}`,
  );
}

function renderUnsupportedFilePreview(containerId, path, contentType = "", size = 0) {
  const msg = document.createElement("p");
  msg.className = "helper";
  msg.textContent = `Preview for this file type is not supported yet (${contentType || "binary"}).`;
  mountFilePreviewNode(
    containerId,
    msg,
    `Path: ${path || "-"} | Size: ${formatBytes(size)}`,
  );
}

function renderDirectHtmlPreview(containerId, scope, path, size = null) {
  const directUrl = scope === "project"
    ? buildProjectPreviewUrl(path)
    : buildWorkspacePreviewUrl(path);
  if (!directUrl) {
    renderUnsupportedFilePreview(containerId, path, "text/html", Number(size) || 0);
    return;
  }
  releaseScopedFilePreviewBlob(scope);
  const frame = document.createElement("iframe");
  frame.src = directUrl;
  frame.title = path || "preview";
  frame.sandbox = "allow-same-origin allow-scripts allow-forms";
  const sizeText = Number.isFinite(Number(size)) ? formatBytes(Number(size)) : "-";
  mountFilePreviewNode(
    containerId,
    frame,
    `Path: ${path || "-"} | Size: ${sizeText} | Type: HTML`,
  );
}

function renderRawFilePreview(containerId, scope, path, raw) {
  const kind = classifyLivePreview(path, raw.contentType);
  const supported = new Set(["image", "pdf", "html", "video"]);
  if (!supported.has(kind)) {
    releaseScopedFilePreviewBlob(scope);
    renderUnsupportedFilePreview(containerId, path, raw.contentType, raw?.blob?.size || 0);
    return;
  }

  if (kind === "html") {
    renderDirectHtmlPreview(containerId, scope, path, raw?.blob?.size);
    return;
  }

  const objectUrl = URL.createObjectURL(raw.blob);
  releaseScopedFilePreviewBlob(scope);
  setScopedFilePreviewBlob(scope, objectUrl);

  let node = null;
  if (kind === "image") {
    const img = document.createElement("img");
    img.src = objectUrl;
    img.alt = path || "image";
    node = img;
  } else if (kind === "video") {
    const video = document.createElement("video");
    video.src = objectUrl;
    video.controls = true;
    video.preload = "metadata";
    node = video;
  } else {
    const frame = document.createElement("iframe");
    frame.src = objectUrl;
    frame.title = path || "preview";
    node = frame;
  }

  mountFilePreviewNode(
    containerId,
    node,
    `Path: ${path || "-"} | Size: ${formatBytes(raw.blob.size)} | Type: ${kind.toUpperCase()}`,
  );
}

function setLivePreviewMeta(text) {
  const meta = $("live_preview_meta");
  if (!meta) return;
  meta.textContent = text || "Latest artifact preview from invited agents.";
}

function clearLivePreview(reason = "No output file rendered yet.") {
  const mount = $("live_preview_mount");
  const empty = $("live_preview_empty");
  releaseLivePreviewBlob();
  if (mount) mount.innerHTML = "";
  if (empty) {
    empty.textContent = reason;
    empty.classList.remove("hidden");
  }
  setLivePreviewMeta(reason);
}

function mountLivePreviewNode(node) {
  const mount = $("live_preview_mount");
  const empty = $("live_preview_empty");
  if (!mount) return;
  mount.innerHTML = "";
  mount.appendChild(node);
  if (empty) empty.classList.add("hidden");
}

function renderLiveStatus() {
  const dot = $("live_status_dot");
  const text = $("live_status_text");
  if (!dot || !text) return;

  if (!selectedProjectData) {
    dot.classList.remove("approved", "completed");
    dot.classList.add("idle");
    text.textContent = "Idle";
    text.classList.remove("connected", "disconnected");
    text.classList.add("neutral");
    return;
  }

  const planApproved = String(selectedProjectData.plan_status || "").toLowerCase() === "approved";
  const exec = String(selectedProjectData.execution_status || "").toLowerCase();
  const done = exec === "completed" || exec === "stopped";
  dot.classList.remove("approved", "completed", "idle");
  text.classList.remove("neutral", "connected", "disconnected");
  if (done) {
    dot.classList.add("completed");
    text.textContent = "Finished";
    text.classList.add("disconnected");
    return;
  }
  if (planApproved) {
    dot.classList.add("approved");
    text.textContent = "Active";
    text.classList.add("connected");
    return;
  }
  dot.classList.add("idle");
  text.textContent = "Idle";
  text.classList.add("neutral");
}

function classifyLivePreview(path, contentType = "") {
  const ext = fileExtension(path);
  const ct = String(contentType || "").toLowerCase();
  const imageExts = new Set(["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp"]);
  const videoExts = new Set(["mp4", "webm", "ogg", "mov", "m4v", "avi", "mkv"]);
  const textExts = new Set([
    "txt", "md", "markdown", "json", "yml", "yaml", "xml", "csv", "log",
    "js", "ts", "tsx", "jsx", "py", "go", "java", "rb", "php", "sql", "sh", "ps1",
    "css", "scss", "sass", "html", "htm", "ini",
  ]);
  if (ct.includes("application/pdf") || ext === "pdf") return "pdf";
  if (ct.startsWith("image/") || imageExts.has(ext)) return "image";
  if (ct.startsWith("video/") || videoExts.has(ext)) return "video";
  if (ct.includes("text/html") || ext === "html" || ext === "htm") return "html";
  if (ct.startsWith("text/") || ct.includes("json") || ct.includes("xml") || textExts.has(ext)) return "text";
  return "binary";
}

async function fetchProjectFileBlob(path) {
  const q = encodeURIComponent(path || "");
  const res = await fetch(`/api/projects/${selectedProjectId}/files/raw?path=${q}`, {
    headers: { ...authHeaders() },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(detailToText(text || `Failed to load file: ${res.status}`));
  }
  return {
    blob: await res.blob(),
    contentType: String(res.headers.get("content-type") || ""),
  };
}

async function fetchWorkspaceFileBlob(path) {
  const q = encodeURIComponent(path || "");
  const res = await fetch(`/api/workspace/files/raw?path=${q}`, {
    headers: { ...authHeaders() },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(detailToText(text || `Failed to load file: ${res.status}`));
  }
  return {
    blob: await res.blob(),
    contentType: String(res.headers.get("content-type") || ""),
  };
}

async function renderLivePreview(path, { force = false } = {}) {
  if (!selectedProjectId) {
    clearLivePreview("Select a project first.");
    return;
  }
  const rel = normalizeOutputPath(path);
  if (!rel) {
    clearLivePreview("No output file rendered yet.");
    return;
  }
  if (!force && livePreviewPath === rel && activeProjectPane !== "live") return;
  livePreviewPath = rel;
  const reqId = ++livePreviewReqSeq;
  setLivePreviewMeta(`Rendering ${rel}...`);

  const kindByPath = classifyLivePreview(rel, "");
  if (kindByPath === "text") {
    const data = await api(`/api/projects/${selectedProjectId}/files/content?path=${encodeURIComponent(rel)}`);
    if (reqId !== livePreviewReqSeq) return;
    releaseLivePreviewBlob();
    const pre = document.createElement("pre");
    pre.textContent = data.truncated ? `${data.content}\n\n(Preview truncated)` : (data.content || "(empty file)");
    mountLivePreviewNode(pre);
    setLivePreviewMeta(`Live: ${data.path} | ${formatBytes(data.size)} | ${new Date().toLocaleTimeString()}`);
    return;
  }
  if (kindByPath === "html") {
    if (reqId !== livePreviewReqSeq) return;
    releaseLivePreviewBlob();
    const frame = document.createElement("iframe");
    frame.src = buildProjectPreviewUrl(rel);
    frame.title = rel;
    frame.sandbox = "allow-same-origin allow-scripts allow-forms";
    mountLivePreviewNode(frame);
    setLivePreviewMeta(`Live: ${rel} | HTML | ${new Date().toLocaleTimeString()}`);
    return;
  }

  const raw = await fetchProjectFileBlob(rel);
  if (reqId !== livePreviewReqSeq) return;
  const kind = classifyLivePreview(rel, raw.contentType);
  if (kind === "html") {
    releaseLivePreviewBlob();
    const frame = document.createElement("iframe");
    frame.src = buildProjectPreviewUrl(rel);
    frame.title = rel;
    frame.sandbox = "allow-same-origin allow-scripts allow-forms";
    mountLivePreviewNode(frame);
    setLivePreviewMeta(`Live: ${rel} | HTML | ${new Date().toLocaleTimeString()}`);
    return;
  }

  const objectUrl = URL.createObjectURL(raw.blob);
  releaseLivePreviewBlob();
  livePreviewBlobUrl = objectUrl;

  if (kind === "image") {
    const img = document.createElement("img");
    img.src = objectUrl;
    img.alt = rel;
    mountLivePreviewNode(img);
  } else if (kind === "video") {
    const video = document.createElement("video");
    video.src = objectUrl;
    video.controls = true;
    video.preload = "metadata";
    mountLivePreviewNode(video);
  } else if (kind === "pdf") {
    const frame = document.createElement("iframe");
    frame.src = objectUrl;
    frame.title = rel;
    mountLivePreviewNode(frame);
  } else {
    const wrap = document.createElement("div");
    wrap.className = "helper";
    wrap.textContent = `Preview for this file type is not supported yet (${raw.contentType || "binary"}).`;
    mountLivePreviewNode(wrap);
  }
  setLivePreviewMeta(`Live: ${rel} | ${formatBytes(raw.blob.size)} | ${new Date().toLocaleTimeString()}`);
}

function captureLiveOutputPath(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const direct = normalizeOutputPath(data.path || data.output_file || "");
  if (isOutputsPath(direct)) return direct;
  if (Array.isArray(data.saved_files)) {
    const saved = data.saved_files
      .map((x) => normalizeOutputPath(x?.path || ""))
      .filter((x) => isOutputsPath(x));
    if (saved.length) return saved[saved.length - 1];
  }
  return "";
}

async function findLatestOutputArtifact(path = "Outputs", depth = 0) {
  if (!selectedProjectId || depth > 4) return null;
  const payload = await api(`/api/projects/${selectedProjectId}/files?path=${encodeURIComponent(path)}`);
  let best = null;
  const dirs = [];
  for (const item of payload.entries || []) {
    if (item.kind === "file") {
      const ts = Number(item.modified_at || 0);
      if (!best || ts > Number(best.modified_at || 0)) best = item;
    } else if (item.kind === "dir") {
      dirs.push(item.path);
    }
  }
  for (const dir of dirs.slice(0, 24)) {
    const child = await findLatestOutputArtifact(dir, depth + 1);
    if (child && (!best || Number(child.modified_at || 0) > Number(best.modified_at || 0))) {
      best = child;
    }
  }
  return best;
}

async function loadLatestLiveArtifact({ render = true } = {}) {
  if (!selectedProjectId) {
    clearLivePreview("No project selected.");
    return null;
  }
  let latest = null;
  try {
    latest = await findLatestOutputArtifact("Outputs", 0);
  } catch {
    latest = await findLatestOutputArtifact("outputs", 0).catch(() => null);
  }
  if (!latest?.path) {
    if (render && activeProjectPane === "live") clearLivePreview("No outputs in Outputs folder yet.");
    return null;
  }
  livePreviewPath = normalizeOutputPath(latest.path);
  if (render && activeProjectPane === "live") {
    await renderLivePreview(livePreviewPath, { force: true });
  } else {
    setLivePreviewMeta(`Latest output: ${livePreviewPath}`);
  }
  return livePreviewPath;
}

function renderProjectExecutionInfo() {
  const progressLabel = $("detail_progress_label");
  const progressFill = $("detail_progress_fill");
  const statusEl = $("detail_execution_status");
  const pauseBtn = $("btn_pause_project");
  const stopBtn = $("btn_stop_project");
  if (!progressLabel || !progressFill || !statusEl || !pauseBtn || !stopBtn) return;

  if (!selectedProjectData) {
    progressLabel.textContent = "0%";
    statusEl.textContent = "Status: idle";
    progressFill.style.width = "0%";
    pauseBtn.disabled = true;
    stopBtn.disabled = true;
    pauseBtn.textContent = "Pause";
    return;
  }

  const status = String(selectedProjectData.execution_status || "idle").toLowerCase();
  const pct = clampProgress(selectedProjectData.progress_pct || 0);
  progressLabel.textContent = `${pct}%`;
  statusEl.textContent = `Status: ${executionStatusLabel(status)}`;
  progressFill.style.width = `${pct}%`;

  pauseBtn.textContent = status === "paused" ? "Resume" : "Pause";
  pauseBtn.disabled = status === "stopped" || status === "completed" || status === "idle";
  stopBtn.disabled = status === "stopped" || status === "completed" || status === "idle";
  renderLiveStatus();
}

function updateProjectPlanActionButtons({ status = "", text = "" } = {}) {
  const refreshBtn = $("btn_refresh_plan");
  const regenerateBtn = $("btn_regenerate_plan");
  const approveBtn = $("btn_approve_plan");
  if (!refreshBtn || !regenerateBtn || !approveBtn) return;

  const buttons = [refreshBtn, regenerateBtn, approveBtn];
  for (const btn of buttons) {
    btn.classList.add("hidden");
    btn.disabled = !selectedProjectData;
    btn.dataset.planAction = "";
  }
  if (!selectedProjectData) return;

  const planStatus = String(status || "").trim().toLowerCase() || "pending";
  const execStatus = String(selectedProjectData.execution_status || "").trim().toLowerCase() || "idle";
  const hasPlanText = Boolean(String(text || "").trim());

  let target = null;
  let action = "";
  let label = "";

  if (execStatus === "paused") {
    target = approveBtn;
    action = "resume";
    label = "Resume Project";
  } else if (planStatus === "generating") {
    target = refreshBtn;
    action = "refresh";
    label = "Refresh Plan";
  } else if (!hasPlanText || planStatus === "failed") {
    target = regenerateBtn;
    action = "regenerate";
    label = "Regenerate Plan";
  } else if (planStatus === "awaiting_approval" || planStatus === "pending") {
    target = approveBtn;
    action = "approve";
    label = "Approve Plan";
  } else {
    target = refreshBtn;
    action = "refresh";
    label = "Refresh Plan";
  }

  target.classList.remove("hidden");
  target.disabled = false;
  target.dataset.planAction = action;
  target.textContent = label;
}

function renderProjectPlanInfo() {
  const stageEl = $("detail_stage");
  const statusEl = $("detail_plan_status");
  const updatedEl = $("detail_plan_updated");
  const readinessEl = $("detail_readiness");
  const textEl = $("detail_plan_text");
  if (!statusEl || !updatedEl || !textEl) return;

  if (!selectedProjectData) {
    if (stageEl) stageEl.textContent = "Lifecycle: -";
    statusEl.textContent = "Status: -";
    updatedEl.textContent = "";
    if (readinessEl) readinessEl.innerHTML = "";
    textEl.textContent = "No project selected.";
    updateProjectPlanActionButtons({ status: "", text: "" });
    renderProjectExecutionInfo();
    renderLiveStatus();
    return;
  }

  const status = selectedProjectPlan?.status || selectedProjectData.plan_status || "pending";
  const text = selectedProjectPlan?.text || selectedProjectData.plan_text || "";
  const updatedAt = selectedProjectPlan?.updated_at || selectedProjectData.plan_updated_at || null;
  const readiness = selectedProjectReadiness;
  statusEl.textContent = `Status: ${planStatusLabel(status)}`;
  updatedEl.textContent = updatedAt ? `Updated: ${formatTs(updatedAt)}` : "";
  if (stageEl) {
    const stage = readiness?.stage || "draft";
    stageEl.textContent = `Lifecycle: ${projectStageLabel(stage)}`;
  }
  if (readinessEl) {
    readinessEl.innerHTML = "";
    const checks = Array.isArray(readiness?.checks) ? readiness.checks : [];
    if (!checks.length) {
      const row = document.createElement("div");
      row.className = "readiness-item pending";
      row.textContent = "Readiness: checking project setup...";
      readinessEl.appendChild(row);
    } else {
      for (const check of checks) {
        const row = document.createElement("div");
        row.className = `readiness-item ${check?.ok ? "ok" : "pending"}`;
        const prefix = check?.ok ? "Ready" : "Missing";
        const label = detailToText(check?.label || check?.key || "check");
        const cta = (!check?.ok && check?.cta) ? ` | ${detailToText(check.cta)}` : "";
        row.textContent = `${prefix}: ${label}${cta}`;
        readinessEl.appendChild(row);
      }
    }
  }
  textEl.textContent = text || "Primary agent has not published a plan yet.";
  updateProjectPlanActionButtons({ status, text });
  if (selectedProjectData) selectedProjectData.plan_status = status;
  renderProjectExecutionInfo();
  renderLiveStatus();
  syncProjectHeadbar();
}

async function loadProjectReadiness(projectId) {
  if (!projectId) {
    selectedProjectReadiness = null;
    renderProjectPlanInfo();
    return null;
  }
  const readiness = await api(`/api/projects/${projectId}/readiness`);
  selectedProjectReadiness = readiness;
  renderProjectPlanInfo();
  return readiness;
}

async function loadProjectPlan(projectId) {
  if (!projectId) {
    selectedProjectPlan = null;
    renderProjectPlanInfo();
    return null;
  }
  const plan = await api(`/api/projects/${projectId}/plan`);
  selectedProjectPlan = plan;
  if (selectedProjectData) {
    selectedProjectData.plan_status = plan.status;
    selectedProjectData.plan_text = plan.text;
    selectedProjectData.plan_updated_at = plan.updated_at;
    selectedProjectData.plan_approved_at = plan.approved_at;
  }
  renderProjectPlanInfo();
  return plan;
}

async function approveProjectPlan() {
  if (!selectedProjectId) throw new Error("Select project first");
  const plan = await api(`/api/projects/${selectedProjectId}/plan/approve`, "POST", { approve: true });
  selectedProjectPlan = plan;
  await refreshSelectedProjectData().catch(() => {});
  await loadProjectReadiness(selectedProjectId).catch(() => {});
  renderProjectPlanInfo();
  setMessage("chat_hint", "Plan approved. Delegation task started.", "ok");
  addEvent("project.plan.approved", { project_id: selectedProjectId });
}

async function regenerateProjectPlan() {
  if (!selectedProjectId) throw new Error("Select project first");
  const plan = await api(`/api/projects/${selectedProjectId}/plan/regenerate`, "POST");
  selectedProjectPlan = plan;
  await refreshSelectedProjectData().catch(() => {});
  await loadProjectReadiness(selectedProjectId).catch(() => {});
  renderProjectPlanInfo();
  setMessage("chat_hint", "Regenerating project plan...", "ok");
  addEvent("project.plan.regenerate_requested", { project_id: selectedProjectId });
}

async function controlProjectExecution(action) {
  if (!selectedProjectId) throw new Error("Select project first");
  const payload = { action };
  const res = await api(`/api/projects/${selectedProjectId}/execution/control`, "POST", payload);
  if (selectedProjectData) {
    selectedProjectData.execution_status = res.status;
    selectedProjectData.progress_pct = res.progress_pct;
    selectedProjectData.execution_updated_at = res.updated_at;
  }
  renderProjectPlanInfo();
  const verb = action === "resume" ? "resumed" : `${action}ed`;
  setMessage("chat_hint", `Project ${verb}.`, "ok");
  addEvent(`project.execution.${action}`, { project_id: selectedProjectId, status: res.status, progress_pct: res.progress_pct });
}

async function refreshSelectedProjectData() {
  if (!selectedProjectId) return null;
  const latest = await api(`/api/projects/${selectedProjectId}`);
  selectedProjectData = latest;
  const cacheIdx = projectsCache.findIndex((p) => String(p?.id || "") === String(latest?.id || ""));
  if (cacheIdx >= 0) {
    projectsCache[cacheIdx] = { ...projectsCache[cacheIdx], ...latest };
    renderProjects(projectsCache);
  }
  if (selectedProjectPlan) {
    selectedProjectPlan.status = latest.plan_status || selectedProjectPlan.status;
    selectedProjectPlan.text = latest.plan_text || selectedProjectPlan.text;
    selectedProjectPlan.updated_at = latest.plan_updated_at || selectedProjectPlan.updated_at;
    selectedProjectPlan.approved_at = latest.plan_approved_at || selectedProjectPlan.approved_at;
  }
  renderProjectUsage();
  renderProjectPlanInfo();
  renderLiveStatus();
  syncProjectHeadbar();
  return latest;
}

function restartRuntimePoll() {
  if (runtimePollHandle) {
    clearInterval(runtimePollHandle);
    runtimePollHandle = null;
  }
  if (!selectedProjectId) return;
  runtimePollHandle = setInterval(() => {
    refreshSelectedProjectData().catch(() => {});
  }, 8000);
}

function normalizeAlias(input) {
  return String(input || "")
    .trim()
    .replace(/^@+/, "")
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9._-]/g, "");
}

function buildAliases(agent) {
  const aliases = new Set();
  const idAlias = normalizeAlias(agent.id);
  const nameAlias = normalizeAlias(agent.name);
  const compactNameAlias = normalizeAlias(String(agent.name || "").replace(/\s+/g, ""));
  if (idAlias) aliases.add(idAlias);
  if (nameAlias) aliases.add(nameAlias);
  if (compactNameAlias) aliases.add(compactNameAlias);
  return [...aliases];
}

function hideMentionAutocomplete() {
  const box = $("chat_autocomplete");
  if (!box) return;
  box.classList.add("hidden");
  box.innerHTML = "";
  chatAutocompleteItems = [];
  chatAutocompleteIndex = 0;
}

function renderMentionAutocomplete(items) {
  const box = $("chat_autocomplete");
  if (!box) return;
  chatAutocompleteItems = items;
  chatAutocompleteIndex = 0;
  if (!items.length) {
    hideMentionAutocomplete();
    return;
  }
  box.innerHTML = "";
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "mention-item" + (i === 0 ? " active" : "");
    btn.dataset.alias = item.alias;
    btn.textContent = `@${item.alias} - ${item.agent.name}`;
    btn.onclick = () => {
      applyMention(item.alias);
      hideMentionAutocomplete();
    };
    box.appendChild(btn);
  }
  box.classList.remove("hidden");
}

function activeMentionButton() {
  const box = $("chat_autocomplete");
  if (!box || box.classList.contains("hidden")) return null;
  return box.querySelector(".mention-item.active") || box.querySelector(".mention-item");
}

function shiftMentionSelection(step) {
  if (!chatAutocompleteItems.length) return;
  chatAutocompleteIndex += step;
  if (chatAutocompleteIndex < 0) chatAutocompleteIndex = chatAutocompleteItems.length - 1;
  if (chatAutocompleteIndex >= chatAutocompleteItems.length) chatAutocompleteIndex = 0;
  const box = $("chat_autocomplete");
  if (!box) return;
  const items = [...box.querySelectorAll(".mention-item")];
  items.forEach((x, idx) => x.classList.toggle("active", idx === chatAutocompleteIndex));
}

function mentionQueryState() {
  const input = $("chat_input");
  if (!input) return null;
  const pos = input.selectionStart ?? input.value.length;
  const prefix = input.value.slice(0, pos);
  const match = prefix.match(/(^|[\s(])@([a-zA-Z0-9._-]{0,32})$/);
  if (!match) return null;
  return {
    query: normalizeAlias(match[2]),
    cursor: pos,
    aliasStart: pos - match[2].length - 1,
  };
}

function refreshMentionAutocomplete() {
  const q = mentionQueryState();
  if (!q) {
    hideMentionAutocomplete();
    return;
  }
  const aliases = [...chatAliasMap.entries()].map(([alias, agent]) => ({ alias, agent }));
  const filtered = aliases
    .filter((x) => !q.query || x.alias.startsWith(q.query))
    .slice(0, 8);
  renderMentionAutocomplete(filtered);
}

function applyMention(alias) {
  const input = $("chat_input");
  if (!input) return;

  const state = mentionQueryState();
  if (!state) {
    const token = `@${alias}`;
    input.value = input.value.trim() ? `${input.value.trim()} ${token} ` : `${token} `;
    input.focus();
    return;
  }

  const before = input.value.slice(0, state.aliasStart);
  const after = input.value.slice(state.cursor);
  const token = `@${alias} `;
  input.value = before + token + after;
  const pos = (before + token).length;
  input.selectionStart = pos;
  input.selectionEnd = pos;
  input.focus();
}

function renderChatAgents(agents) {
  const list = $("chat_agent_list");
  if (list) list.innerHTML = "";
  chatAliasMap = new Map();
  chatById = new Map();

  for (const agent of agents) {
    chatById.set(agent.id, agent);
    const aliases = buildAliases(agent);
    for (const alias of aliases) {
      if (!chatAliasMap.has(alias)) chatAliasMap.set(alias, agent);
    }

    if (list) {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "agent-mention-chip";
      const mentionAlias = aliases[0] || normalizeAlias(agent.id);
      chip.title = `${agent.name} (${agent.id})`;
      const palette = colorForAgent(agent.id);
      chip.style.setProperty("--agent-color", palette.hex);
      chip.style.borderColor = palette.border;
      chip.style.background = hexToRgba(palette.hex, 0.08);

      const avatar = createAgentAvatarImg(agent.id, `${agent.name || agent.id} mascot`);
      avatar.className = "agent-mention-avatar";
      avatar.onerror = () => {
        avatar.src = AGENT_MASCOT_PATH || SUMMARY_AGENT_DEFAULT_AVATAR;
      };

      const label = document.createElement("span");
      label.textContent = `@${mentionAlias}`;

      chip.appendChild(avatar);
      chip.appendChild(label);
      chip.onclick = () => applyMention(mentionAlias);
      list.appendChild(chip);
    }
  }

  hideMentionAutocomplete();
  setMessage("chat_hint", agents.length ? `Loaded ${agents.length} agents.` : "No agents available.");
}

async function loadChatAgents() {
  syncChatContextControls();
  const contextMode = activeChatContextMode();

  if (contextMode === "project") {
    const scoped = (selectedAssignedAgents || []).map((a) => ({ id: a.id, name: a.name, role: a.role || "", is_primary: Boolean(a.is_primary) }));
    chatAgents = scoped;
    renderChatAgents(scoped);
    connectionHealthy = Boolean(activeConnectionId);
    applyConnectionStatus();
    if (scoped.length) {
      setMessage("chat_hint", `Project context: ${scoped.length} invited agents available for mention.`, "ok");
    } else {
      setMessage("chat_hint", "Project context active, but no invited agents yet. Open Manage Agents to assign one.", "error");
    }
    return;
  }

  if (!activeConnectionId) {
    chatAgents = [];
    connectionHealthy = false;
    renderChatAgents([]);
    applyConnectionStatus();
    return;
  }

  let mainAgent = workspaceMainChatAgent();
  if (!mainAgent) {
    await loadConnectionPolicy(activeConnectionId).catch(() => null);
    mainAgent = workspaceMainChatAgent();
  }

  if (!mainAgent) {
    chatAgents = [];
    renderChatAgents([]);
    connectionHealthy = Boolean(activeConnectionId);
    applyConnectionStatus();
    setMessage("chat_hint", "Workspace context requires a configured main user agent. Re-bootstrap OpenClaw connection.", "error");
    return;
  }

  chatAgents = [mainAgent];
  renderChatAgents(chatAgents);
  connectionHealthy = true;
  applyConnectionStatus();
  setMessage("chat_hint", "Workspace context: chatting with your main user agent only.", "ok");
}

function parseMention(rawMessage) {
  const mentionMatches = [...rawMessage.matchAll(/@([a-zA-Z0-9._-]+)/g)];
  let matchedAgent = null;
  let unknownAlias = null;
  let cleaned = rawMessage;

  for (const match of mentionMatches) {
    const original = match[0];
    const alias = normalizeAlias(match[1]);
    const mapped = chatAliasMap.get(alias);
    if (mapped && !matchedAgent) {
      matchedAgent = mapped;
      cleaned = cleaned.replace(original, "").trim();
    } else if (!mapped && !unknownAlias) {
      unknownAlias = alias;
    }
  }

  return { matchedAgent, unknownAlias, cleaned };
}

function resolveChatTarget(rawMessage) {
  const parsed = parseMention(rawMessage);
  let chosen = parsed.matchedAgent;
  const contextMode = activeChatContextMode();
  if (!chosen && contextMode === "project" && selectedPrimaryAgentId) {
    chosen = chatById.get(selectedPrimaryAgentId) || null;
  }
  if (!chosen && contextMode === "workspace") {
    const mainAgent = workspaceMainChatAgent();
    if (mainAgent) {
      chosen = chatById.get(mainAgent.id) || mainAgent;
    }
  }
  return {
    agent: chosen,
    unknownAlias: parsed.unknownAlias,
    message: parsed.cleaned || rawMessage,
  };
}

async function sendChatPrototype() {
  const input = $("chat_input");
  if (!input) return;
  const raw = input.value.trim();
  if (!activeConnectionId) throw new Error("OpenClaw connection not selected");
  if (!raw) throw new Error("Type message first");

  syncChatContextControls();
  const contextMode = activeChatContextMode();
  const usingProjectContext = contextMode === "project" && Boolean(selectedProjectId);
  if (usingProjectContext) {
    if (!selectedProjectReadiness || selectedProjectReadiness.project_id !== selectedProjectId) {
      await loadProjectReadiness(selectedProjectId).catch(() => {});
    }
    if (!selectedProjectReadiness?.can_chat_project) {
      throw new Error("Project context is not ready. Open Manage Agents, invite at least one agent, and set a primary agent.");
    }
  }

  const resolved = resolveChatTarget(raw);
  if (resolved.unknownAlias) {
    const msg = `Unknown mention @${resolved.unknownAlias}.`;
    setMessage("chat_hint", msg, "error");
    throw new Error(msg);
  }
  if (contextMode === "workspace" && !resolved.agent) {
    throw new Error("Workspace context requires a configured main user agent.");
  }

  const targetName = resolved.agent
    ? `${resolved.agent.name} (${resolved.agent.id})`
    : (usingProjectContext ? "project auto route" : "main workspace agent");
  appendChatMessage("user", resolved.message, `you -> ${targetName}`);

  const payload = {
    message: resolved.message,
    agent_id: resolved.agent ? resolved.agent.id : null,
    context_mode: usingProjectContext ? "project" : "workspace",
    session_key: usingProjectContext ? selectedProjectId : "main",
    timeout_sec: 25,
  };

  input.value = "";
  hideMentionAutocomplete();
  $("btn_chat_send").disabled = true;

  try {
    const res = await api(`/api/openclaw/${activeConnectionId}/ws-chat`, "POST", payload);
    const shown = res.text || detailToText(res.frames) || "(no text response yet)";
    const workspaceAgentId = workspaceMainChatAgent()?.id || "";
    const fallbackAgentId = usingProjectContext ? (selectedPrimaryAgentId || "") : workspaceAgentId;
    const resolvedAgentId = String(res.resolved_agent_id || resolved.agent?.id || fallbackAgentId).trim();
    const resolvedAgent = chatById.get(resolvedAgentId);
    const canInlineReply = !usingProjectContext || !projectStreamConnected;
    if (canInlineReply) {
      const role = usingProjectContext ? "agent" : "assistant";
      const meta = usingProjectContext
        ? (resolvedAgent ? resolvedAgent.name : (resolvedAgentId || "agent"))
        : `${res.transport || "ws"} via ${res.path || "gateway"}`;
      appendChatMessage(role, shown, meta, { agentId: resolvedAgentId });
    }
    setMessage("chat_hint", "Message delivered.", "ok");
    addEvent("chat.reply", { path: res.path, text: shown, context_mode: payload.context_mode });
    if (usingProjectContext) {
      await refreshSelectedProjectData().catch(() => {});
      await loadProjectFiles(projectFilesCurrentPath || "").catch(() => {});
    }
  } catch (e) {
    const msg = detailToText(e?.message || e);
    appendChatMessage("system", msg, "delivery error");
    setMessage("chat_hint", msg, "error");
    throw e;
  } finally {
    $("btn_chat_send").disabled = false;
  }
}

function renderFolderBrowsers() {
  const workspaceRoot = workspacePolicy?.workspace_root || "HIVEE";
  const templatesRoot = workspacePolicy?.templates_root || `${workspaceRoot}/TEMPLATES`;
  const workspaceTree = String(workspaceTreeText || workspacePolicy?.workspace_tree || "").trim();
  const folderRoot = workspaceTree || [
    `${workspaceRoot}/`,
    `  ${templatesRoot.replace(`${workspaceRoot}/`, "")}/`,
    "    PROJECT-SETUP.MD",
    "  PROJECTS/",
  ].join("\n");

  const globalFolder = $("folder_browser");
  if (globalFolder) globalFolder.textContent = folderRoot;
}

function fileKindIconSvg(kind, name = "") {
  if (kind === "dir") {
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 7h7l2 2h9v8H3z"/><path d="M3 11h18"/></svg>';
  }
  const ext = fileExtension(name);
  if (["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp"].includes(ext)) {
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M8 15l3-3 2 2 3-3 2 4"/><circle cx="9" cy="9" r="1.5"/></svg>';
  }
  if (ext === "pdf") {
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3h7l5 5v13H7z"/><path d="M14 3v5h5"/><path d="M9 15h6M9 18h5"/></svg>';
  }
  if (ext === "html" || ext === "htm") {
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 4h14l-1 14-6 2-6-2z"/><path d="M8 8l-1 1 1 1M16 8l1 1-1 1"/></svg>';
  }
  return '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3h7l5 5v13H7z"/><path d="M14 3v5h5"/></svg>';
}

function renderProjectFiles(payload) {
  const crumbs = $("project_files_breadcrumbs");
  const list = $("project_files_list");
  const preview = $("project_file_preview");
  if (!crumbs || !list || !preview) return;

  if (!selectedProjectData) {
    crumbs.innerHTML = "";
    list.innerHTML = '<p class="helper">No project selected.</p>';
    releaseProjectFilePreviewBlob();
    resetFilePreview("project_file_preview", "Select a file to preview.");
    return;
  }

  if (!payload) {
    list.innerHTML = '<p class="helper">Loading files...</p>';
    return;
  }

  projectFilesPayload = payload;
  projectFilesCurrentPath = payload.current_path || "";
  crumbs.innerHTML = "";
  const rootBtn = document.createElement("button");
  rootBtn.type = "button";
  rootBtn.className = "crumb";
  rootBtn.textContent = "ROOT";
  rootBtn.onclick = () => loadProjectFiles("").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  crumbs.appendChild(rootBtn);

  if (projectFilesCurrentPath) {
    const parts = projectFilesCurrentPath.split("/").filter(Boolean);
    let partial = "";
    for (const part of parts) {
      partial = partial ? `${partial}/${part}` : part;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "crumb";
      btn.textContent = part;
      const target = partial;
      btn.onclick = () => loadProjectFiles(target).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      crumbs.appendChild(btn);
    }
  }

  list.innerHTML = "";
  if (payload.parent_path != null) {
    const up = document.createElement("button");
    up.type = "button";
    up.className = "file-row dir";
    up.innerHTML = `<span class="name"><span class="file-kind-icon">${fileKindIconSvg("dir")}</span><span class="file-label">..</span></span><span class="meta">Parent</span>`;
    up.onclick = () => loadProjectFiles(payload.parent_path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    list.appendChild(up);
  }

  const entries = Array.isArray(payload.entries) ? payload.entries : [];
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.className = "helper";
    empty.textContent = "Folder is empty.";
    list.appendChild(empty);
    return;
  }

  for (const item of entries) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `file-row ${item.kind === "dir" ? "dir" : "file"}`;
    const left = document.createElement("span");
    left.className = "name";
    const icon = document.createElement("span");
    icon.className = "file-kind-icon";
    icon.innerHTML = fileKindIconSvg(item.kind, item.name || item.path || "");
    const label = document.createElement("span");
    label.className = "file-label";
    label.textContent = item.name || item.path;
    left.appendChild(icon);
    left.appendChild(label);
    const right = document.createElement("span");
    right.className = "meta";
    if (item.kind === "dir") {
      right.textContent = "Folder";
    } else {
      right.textContent = formatBytes(item.size);
      if (item.modified_at) {
        right.textContent += ` | ${new Date(item.modified_at * 1000).toLocaleString()}`;
      }
    }
    row.appendChild(left);
    row.appendChild(right);
    if (item.kind === "dir") {
      row.onclick = () => loadProjectFiles(item.path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    } else {
      row.onclick = () => openProjectFile(item.path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    }
    list.appendChild(row);
  }
}

function renderWorkspaceFiles(payload) {
  const hint = $("workspace_files_hint");
  const crumbs = $("workspace_files_breadcrumbs");
  const list = $("workspace_files_list");
  const preview = $("workspace_file_preview");
  if (!hint || !crumbs || !list || !preview) return;

  if (!payload) {
    hint.textContent = "Loading workspace files...";
    crumbs.innerHTML = "";
    list.innerHTML = '<p class="helper">Loading files...</p>';
    releaseWorkspaceFilePreviewBlob();
    resetFilePreview("workspace_file_preview", "Select a file to preview.");
    return;
  }

  workspaceFilesPayload = payload;
  workspaceFilesCurrentPath = payload.current_path || "";
  const workspaceRoot = String(payload.workspace_root || "").trim() || "HIVEE";
  hint.textContent = `Workspace root: ${workspaceRoot}. Open PROJECTS folder to access all project directories.`;
  crumbs.innerHTML = "";
  const ownerRootLabel = workspaceRoot
    .trim()
    .split("/")
    .filter(Boolean)
    .pop() || "OWNER_HOME";
  const rootBtn = document.createElement("button");
  rootBtn.type = "button";
  rootBtn.className = "crumb";
  rootBtn.textContent = ownerRootLabel;
  rootBtn.onclick = () => loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  crumbs.appendChild(rootBtn);

  if (workspaceFilesCurrentPath) {
    const parts = workspaceFilesCurrentPath.split("/").filter(Boolean);
    let partial = "";
    for (const part of parts) {
      partial = partial ? `${partial}/${part}` : part;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "crumb";
      btn.textContent = part;
      const target = partial;
      btn.onclick = () => loadWorkspaceFiles(target).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      crumbs.appendChild(btn);
    }
  }

  list.innerHTML = "";
  if (payload.parent_path != null) {
    const up = document.createElement("button");
    up.type = "button";
    up.className = "file-row dir";
    up.innerHTML = `<span class="name"><span class="file-kind-icon">${fileKindIconSvg("dir")}</span><span class="file-label">..</span></span><span class="meta">Parent</span>`;
    up.onclick = () => loadWorkspaceFiles(payload.parent_path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    list.appendChild(up);
  }

  const entries = Array.isArray(payload.entries) ? payload.entries : [];
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.className = "helper";
    empty.textContent = "Folder is empty.";
    list.appendChild(empty);
    return;
  }

  for (const item of entries) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `file-row ${item.kind === "dir" ? "dir" : "file"}`;
    const left = document.createElement("span");
    left.className = "name";
    const icon = document.createElement("span");
    icon.className = "file-kind-icon";
    icon.innerHTML = fileKindIconSvg(item.kind, item.name || item.path || "");
    const label = document.createElement("span");
    label.className = "file-label";
    label.textContent = item.name || item.path;
    left.appendChild(icon);
    left.appendChild(label);
    const right = document.createElement("span");
    right.className = "meta";
    if (item.kind === "dir") {
      right.textContent = "Folder";
    } else {
      right.textContent = formatBytes(item.size);
      if (item.modified_at) {
        right.textContent += ` | ${new Date(item.modified_at * 1000).toLocaleString()}`;
      }
    }
    row.appendChild(left);
    row.appendChild(right);
    if (item.kind === "dir") {
      row.onclick = () => loadWorkspaceFiles(item.path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    } else {
      row.onclick = () => openWorkspaceFile(item.path || "").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    }
    list.appendChild(row);
  }
}

async function loadProjectFiles(path = "") {
  if (!selectedProjectId) {
    renderProjectFiles(null);
    return null;
  }
  const q = encodeURIComponent(path || "");
  const payload = await api(`/api/projects/${selectedProjectId}/files?path=${q}`);
  renderProjectFiles(payload);
  return payload;
}

async function loadWorkspaceFiles(path = "") {
  const normalized = String(path || "").trim();
  const effectivePath = normalized || DEFAULT_OWNER_FILES_PATH;
  const q = encodeURIComponent(effectivePath);
  const payload = await api(`/api/workspace/files?path=${q}`);
  renderWorkspaceFiles(payload);
  return payload;
}

async function openProjectFile(path = "") {
  if (!selectedProjectId) return null;
  const rel = String(path || "").trim();
  const q = encodeURIComponent(rel);
  projectFilePreviewPath = rel;
  const kindByPath = classifyLivePreview(rel, "");
  if (kindByPath === "text") {
    const data = await api(`/api/projects/${selectedProjectId}/files/content?path=${q}`);
    releaseProjectFilePreviewBlob();
    renderTextFilePreview("project_file_preview", data, rel);
    return data;
  }
  if (kindByPath === "html") {
    renderDirectHtmlPreview("project_file_preview", "project", rel);
    return { kind: "html", path: rel };
  }
  const raw = await fetchProjectFileBlob(rel);
  const detected = classifyLivePreview(rel, raw.contentType);
  if (detected === "text") {
    const data = await api(`/api/projects/${selectedProjectId}/files/content?path=${q}`);
    releaseProjectFilePreviewBlob();
    renderTextFilePreview("project_file_preview", data, rel);
    return data;
  }
  renderRawFilePreview("project_file_preview", "project", rel, raw);
  return raw;
}

async function openWorkspaceFile(path = "") {
  const rel = String(path || "").trim();
  const q = encodeURIComponent(rel);
  const kindByPath = classifyLivePreview(rel, "");
  if (kindByPath === "text") {
    const data = await api(`/api/workspace/files/content?path=${q}`);
    releaseWorkspaceFilePreviewBlob();
    renderTextFilePreview("workspace_file_preview", data, rel);
    return data;
  }
  if (kindByPath === "html") {
    renderDirectHtmlPreview("workspace_file_preview", "workspace", rel);
    return { kind: "html", path: rel };
  }
  const raw = await fetchWorkspaceFileBlob(rel);
  const detected = classifyLivePreview(rel, raw.contentType);
  if (detected === "text") {
    const data = await api(`/api/workspace/files/content?path=${q}`);
    releaseWorkspaceFilePreviewBlob();
    renderTextFilePreview("workspace_file_preview", data, rel);
    return data;
  }
  renderRawFilePreview("workspace_file_preview", "workspace", rel, raw);
  return raw;
}

function renderProjectUsage() {
  const box = $("project_usage_box");
  if (!box) return;
  if (!selectedProjectData) {
    box.textContent = "No usage stats yet.";
    return;
  }
  const primary = selectedAssignedAgents.find((a) => a.is_primary);
  const promptTokens = Math.max(0, Number(selectedProjectData.usage_prompt_tokens || 0));
  const completionTokens = Math.max(0, Number(selectedProjectData.usage_completion_tokens || 0));
  const totalTokens = Math.max(0, Number(selectedProjectData.usage_total_tokens || 0));
  const promptRatePerM = 0.5;
  const completionRatePerM = 1.5;
  const promptCost = (promptTokens / 1_000_000) * promptRatePerM;
  const completionCost = (completionTokens / 1_000_000) * completionRatePerM;
  const totalCost = promptCost + completionCost;
  const safeTotal = Math.max(1, totalTokens);
  const promptPct = totalTokens <= 0 ? 0 : Math.max(3, Math.round((promptTokens / safeTotal) * 100));
  const completionPct = totalTokens <= 0 ? 0 : Math.max(3, Math.round((completionTokens / safeTotal) * 100));
  const totalPct = totalTokens <= 0 ? 0 : 100;
  box.innerHTML = `
    <div class="usage-kpi-grid">
      <div class="usage-kpi"><span class="label">Prompt Tokens</span><span class="value">${promptTokens.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Completion Tokens</span><span class="value">${completionTokens.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Total Tokens</span><span class="value">${totalTokens.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Estimated Cost</span><span class="value">$${totalCost.toFixed(4)}</span></div>
    </div>
    <div class="usage-chart">
      <div class="usage-bar-row">
        <span>Prompt</span>
        <div class="usage-bar-track"><div class="usage-bar-fill prompt" style="width:${promptPct}%"></div></div>
        <strong>$${promptCost.toFixed(4)}</strong>
      </div>
      <div class="usage-bar-row">
        <span>Completion</span>
        <div class="usage-bar-track"><div class="usage-bar-fill completion" style="width:${completionPct}%"></div></div>
        <strong>$${completionCost.toFixed(4)}</strong>
      </div>
      <div class="usage-bar-row">
        <span>Total</span>
        <div class="usage-bar-track"><div class="usage-bar-fill total" style="width:${totalPct}%"></div></div>
        <strong>${selectedProjectData.usage_updated_at ? formatTs(selectedProjectData.usage_updated_at) : "-"}</strong>
      </div>
    </div>
    <div class="usage-footnote">
      Project: ${selectedProjectData.title} | Connection: ${selectedProjectData.connection_id} | Assigned agents: ${selectedAssignedAgents.length} | Primary: ${primary ? primary.name : "Not set"}.
      Cost is an estimate using a generic token rate and may differ from provider billing.
    </div>
  `;
}

function renderWorkspaceUsage() {
  const box = $("workspace_usage_box");
  if (!box) return;

  const rows = Array.isArray(projectsCache) ? projectsCache : [];
  if (!rows.length) {
    box.textContent = "No project usage data yet.";
    return;
  }

  let promptTokens = 0;
  let completionTokens = 0;
  let totalTokens = 0;
  let latestUpdateTs = 0;
  const byProject = [];

  for (const p of rows) {
    const title = String(p?.title || p?.id || "Project").trim();
    const prompt = Math.max(0, Number(p?.usage_prompt_tokens || 0));
    const completion = Math.max(0, Number(p?.usage_completion_tokens || 0));
    const total = Math.max(0, Number(p?.usage_total_tokens || (prompt + completion)));
    const updatedAt = Math.max(0, Number(p?.usage_updated_at || 0));
    promptTokens += prompt;
    completionTokens += completion;
    totalTokens += total;
    latestUpdateTs = Math.max(latestUpdateTs, updatedAt);
    byProject.push({ title, total });
  }

  const promptRatePerM = 0.5;
  const completionRatePerM = 1.5;
  const promptCost = (promptTokens / 1_000_000) * promptRatePerM;
  const completionCost = (completionTokens / 1_000_000) * completionRatePerM;
  const totalCost = promptCost + completionCost;
  const safeTotal = Math.max(1, totalTokens);
  const promptPct = totalTokens <= 0 ? 0 : Math.max(3, Math.round((promptTokens / safeTotal) * 100));
  const completionPct = totalTokens <= 0 ? 0 : Math.max(3, Math.round((completionTokens / safeTotal) * 100));
  const topProjects = byProject
    .filter((x) => Number(x.total || 0) > 0)
    .sort((a, b) => Number(b.total || 0) - Number(a.total || 0))
    .slice(0, 8);

  const topHtml = topProjects.length
    ? topProjects
      .map((x) => `
        <div class="usage-project-row">
          <span class="name" title="${x.title}">${x.title}</span>
          <span class="tokens">${Number(x.total || 0).toLocaleString()} tokens</span>
          <span class="cost">$${(((Number(x.total || 0) * 0.5) / 1_000_000)).toFixed(4)}</span>
        </div>
      `)
      .join("")
    : '<p class="helper">No tokens recorded yet across projects.</p>';

  box.innerHTML = `
    <div class="usage-kpi-grid">
      <div class="usage-kpi"><span class="label">Projects</span><span class="value">${rows.length.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Prompt Tokens</span><span class="value">${promptTokens.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Completion Tokens</span><span class="value">${completionTokens.toLocaleString()}</span></div>
      <div class="usage-kpi"><span class="label">Total Tokens</span><span class="value">${totalTokens.toLocaleString()}</span></div>
    </div>
    <div class="usage-chart">
      <div class="usage-bar-row">
        <span>Prompt</span>
        <div class="usage-bar-track"><div class="usage-bar-fill prompt" style="width:${promptPct}%"></div></div>
        <strong>$${promptCost.toFixed(4)}</strong>
      </div>
      <div class="usage-bar-row">
        <span>Completion</span>
        <div class="usage-bar-track"><div class="usage-bar-fill completion" style="width:${completionPct}%"></div></div>
        <strong>$${completionCost.toFixed(4)}</strong>
      </div>
      <div class="usage-bar-row">
        <span>Total</span>
        <div class="usage-bar-track"><div class="usage-bar-fill total" style="width:${totalTokens > 0 ? 100 : 0}%"></div></div>
        <strong>$${totalCost.toFixed(4)}</strong>
      </div>
    </div>
    <div class="usage-project-list">
      ${topHtml}
    </div>
    <div class="usage-footnote">
      Aggregated usage from all projects${latestUpdateTs ? ` | Last update: ${formatTs(latestUpdateTs)}` : ""}.
      Cost is an estimate using a generic token rate and may differ from provider billing.
    </div>
  `;
}

function _formatSummaryCapabilityLabel(raw) {
  return String(raw || "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function _extractSummaryAgentCapabilities(card) {
  const tags = [];
  const seen = new Set();
  const capObj = card && typeof card.capabilities === "object" ? card.capabilities : {};
  for (const [key, value] of Object.entries(capObj)) {
    const enabled = value === true || (typeof value === "number" && value > 0) || (value && typeof value === "object");
    if (!enabled) continue;
    const label = _formatSummaryCapabilityLabel(key);
    const low = label.toLowerCase();
    if (!label || seen.has(low)) continue;
    seen.add(low);
    tags.push(label);
  }

  const skills = Array.isArray(card?.skills) ? card.skills : [];
  for (const skill of skills) {
    const skillName = String(skill?.name || skill?.id || "").trim();
    if (!skillName) continue;
    const label = `Skill: ${skillName}`;
    const low = label.toLowerCase();
    if (seen.has(low)) continue;
    seen.add(low);
    tags.push(label);
    if (tags.length >= 8) break;
  }

  return tags.slice(0, 8);
}

function renderSummaryAgents() {
  const list = $("summary_agents_list");
  const msg = $("summary_agents_msg");
  if (!list || !msg) return;

  list.innerHTML = "";
  msg.classList.remove("error", "ok");

  if (summaryAgentsLoading) {
    msg.textContent = "Loading agents...";
    return;
  }

  if (summaryAgentsError) {
    msg.textContent = summaryAgentsError;
    msg.classList.add("error");
    return;
  }

  if (!summaryAgents.length) {
    msg.textContent = "No managed agents found yet. Bootstrap OpenClaw first.";
    return;
  }

  msg.textContent = `${summaryAgents.length} agents loaded.`;

  for (const agent of summaryAgents) {
    const card = document.createElement("article");
    card.className = "summary-agent-card";
    const palette = colorForAgent(agent.agent_id);

    const avatarWrap = document.createElement("div");
    avatarWrap.className = "summary-agent-avatar";
    avatarWrap.style.borderColor = palette.border;
    avatarWrap.style.background = hexToRgba(palette.hex, 0.08);
    const avatar = createAgentAvatarImg(agent.agent_id, "Agent mascot");
    avatar.onerror = () => {
      avatar.src = AGENT_MASCOT_PATH || SUMMARY_AGENT_DEFAULT_AVATAR;
    };
    avatarWrap.appendChild(avatar);

    const body = document.createElement("div");
    body.className = "summary-agent-body";

    const heading = document.createElement("div");
    heading.className = "summary-agent-heading";
    const name = document.createElement("strong");
    name.textContent = String(agent.agent_name || agent.agent_id || "Agent");
    const status = document.createElement("span");
    status.className = "summary-agent-status";
    status.textContent = String(agent.status || "active");
    heading.appendChild(name);
    heading.appendChild(status);

    const meta = document.createElement("p");
    meta.className = "summary-agent-meta";
    const idPart = String(agent.agent_id || "").trim();
    const connPart = String(agent.connection_id || "").trim();
    meta.textContent = `ID: ${idPart || "-"} | Connection: ${connPart || "-"}`;

    const caps = document.createElement("div");
    caps.className = "summary-agent-capabilities";
    const capList = Array.isArray(agent.capabilities) ? agent.capabilities : [];
    if (!capList.length) {
      const empty = document.createElement("span");
      empty.className = "chip";
      empty.textContent = "No capability metadata";
      caps.appendChild(empty);
    } else {
      for (const cap of capList) {
        const chip = document.createElement("span");
        chip.className = "chip summary-cap-chip";
        chip.textContent = cap;
        caps.appendChild(chip);
      }
    }

    body.appendChild(heading);
    body.appendChild(meta);
    body.appendChild(caps);

    card.appendChild(avatarWrap);
    card.appendChild(body);
    list.appendChild(card);
  }
}

async function loadSummaryAgents({ force = false } = {}) {
  if (summaryAgentsLoading && !force) return;
  if (!sessionToken) {
    summaryAgents = [];
    summaryAgentsError = "";
    renderSummaryAgents();
    return;
  }

  summaryAgentsLoading = true;
  summaryAgentsError = "";
  renderSummaryAgents();

  try {
    const listed = await api("/api/a2a/agents");
    const items = Array.isArray(listed?.agents) ? listed.agents : [];

    const enriched = await Promise.all(items.map(async (agent) => {
      const agentId = String(agent?.agent_id || "").trim();
      const connectionId = String(agent?.connection_id || "").trim();
      let card = {};
      if (agentId) {
        const q = connectionId ? `?connection_id=${encodeURIComponent(connectionId)}` : "";
        try {
          const detail = await api(`/api/a2a/agents/${encodeURIComponent(agentId)}/card${q}`);
          card = (detail && typeof detail.card === "object" && detail.card) ? detail.card : {};
        } catch {
          card = {};
        }
      }
      return {
        agent_id: agentId,
        agent_name: String(agent?.agent_name || agentId || "agent").trim(),
        connection_id: connectionId,
        status: String(agent?.status || "active").trim() || "active",
        capabilities: _extractSummaryAgentCapabilities(card),
      };
    }));

    summaryAgents = enriched.filter((agent) => Boolean(agent.agent_id));
    summaryAgentsError = "";
  } catch (e) {
    summaryAgents = [];
    summaryAgentsError = detailToText(e?.message || e);
  } finally {
    summaryAgentsLoading = false;
    renderSummaryAgents();
    renderWizardOwnerAgents();
  }
}
function setProjectPane(pane) {
  activeProjectPane = pane;
  for (const btn of document.querySelectorAll("[data-project-pane]")) {
    btn.classList.toggle("active", btn.dataset.projectPane === pane);
  }
  const map = {
    live: "project_panel_live",
    info: "project_panel_info",
    folder: "project_panel_folder",
    usage: "project_panel_usage",
    tracker: "project_panel_tracker",
  };
  for (const [key, id] of Object.entries(map)) {
    const node = $(id);
    if (!node) continue;
    node.classList.toggle("hidden", key !== pane);
  }
  if (pane === "folder" && selectedProjectId) {
    loadProjectFiles(projectFilesCurrentPath || "").catch(() => {});
  }
  if (pane === "live" && selectedProjectId) {
    if (livePreviewPath) {
      renderLivePreview(livePreviewPath, { force: true }).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    } else {
      loadLatestLiveArtifact({ render: true }).catch(() => clearLivePreview("No outputs in Outputs folder yet."));
    }
  }
  syncProjectHeadbar();
}

function setNavTab(tab) {
  activeNavTab = tab;
  for (const btn of document.querySelectorAll("[data-nav-tab]")) {
    btn.classList.toggle("active", btn.dataset.navTab === tab);
  }

  for (const pane of document.querySelectorAll("[data-center-tab]")) {
    pane.classList.toggle("active", pane.dataset.centerTab === tab);
    pane.classList.toggle("hidden", pane.dataset.centerTab !== tab);
  }

  syncProjectContextSidebar();
  syncPrimaryNavState();
  syncWorkspaceSectionTitle();
  syncProjectHeadbar();

  if (tab === "files") {
    renderFolderBrowsers();
    loadWorkspaceFiles(workspaceFilesCurrentPath || DEFAULT_OWNER_FILES_PATH).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  }
  if (tab === "usage") {
    renderWorkspaceUsage();
  }
  if (tab === "agents") {
    loadSummaryAgents().catch(() => {});
  }
  if (tab === "config") renderConfigConnectionDetails();
  if (tab === "account") {
    loadAccountProfile({ silent: true }).catch((e) => setMessage("account_msg", detailToText(e), "error"));
  }
}

async function selectProject(projectId) {
  selectedProjectId = projectId;
  projectStreamConnected = false;
  livePreviewReqSeq += 1;
  livePreviewPath = "";
  clearLivePreview("Loading latest output...");
  if (projectRefreshTimer) {
    clearTimeout(projectRefreshTimer);
    projectRefreshTimer = null;
  }
  projectTreeText = "";
  projectFilesCurrentPath = "";
  projectFilePreviewPath = "";
  projectFilesPayload = null;
  releaseProjectFilePreviewBlob();
  const tracker = $("events");
  if (tracker) tracker.innerHTML = "";
  const live = $("overview_live_updates");
  if (live) live.innerHTML = "";
  const [project, assigned] = await Promise.all([
    api(`/api/projects/${projectId}`),
    api(`/api/projects/${projectId}/agents`).catch(() => ({ agents: [], primary_agent: null })),
  ]);

  selectedProjectData = project;
  selectedProjectPlan = {
    project_id: projectId,
    status: project.plan_status || "pending",
    text: project.plan_text || "",
    updated_at: project.plan_updated_at || null,
    approved_at: project.plan_approved_at || null,
  };
  selectedAssignedAgents = assigned.agents || [];
  selectedPrimaryAgentId = assigned?.primary_agent?.id || null;
  selectedProjectReadiness = null;
  chatContextMode = "project";
  showProjectDetails();
  syncPrimaryNavState();
  syncChatContextControls();
  updateChatProjectName();
  syncProjectHeadbar();

  $("detail_brief").textContent = "Brief: " + project.brief;
  $("detail_goal").textContent = "Goal: " + project.goal;

  const chips = $("detail_agents");
  chips.innerHTML = "";
  if (!selectedAssignedAgents.length) {
    chips.innerHTML = '<span class="chip">No agents assigned</span>';
  } else {
    for (const a of selectedAssignedAgents) {
      const chip = document.createElement("span");
      chip.className = "chip agent-chip-inline" + (a.is_primary ? " primary" : "");
      const palette = colorForAgent(a.id);
      chip.style.borderColor = palette.border;
      chip.style.background = hexToRgba(palette.hex, a.is_primary ? 0.16 : 0.09);

      const avatar = createAgentAvatarImg(a.id, `${a.name || a.id} mascot`);
      avatar.className = "agent-inline-avatar";
      avatar.onerror = () => {
        avatar.src = AGENT_MASCOT_PATH || SUMMARY_AGENT_DEFAULT_AVATAR;
      };

      const roleText = a.role ? ` - ${a.role}` : "";
      const label = document.createElement("span");
      label.textContent = a.is_primary ? `${a.name} (Primary)${roleText}` : `${a.name}${roleText}`;

      chip.appendChild(avatar);
      chip.appendChild(label);
      chips.appendChild(chip);
    }
  }

  renderFolderBrowsers();
  renderProjectUsage();
  renderProjectPlanInfo();
  renderProjectFiles(projectFilesPayload);
  renderLiveStatus();
  await loadProjectWorkspaceTree(projectId).catch(() => {});
  await loadProjectPlan(projectId).catch(() => {});
  await loadProjectReadiness(projectId).catch(() => {});
  await loadProjectFiles("").catch(() => {});
  await loadLatestLiveArtifact({ render: activeProjectPane === "live" }).catch(() => {});
  await loadChatAgents().catch(() => {});
  restartRuntimePoll();
  subscribeEvents().catch((e) => addEvent("error", detailToText(e)));

  const projects = await api("/api/projects");
  renderProjects(projects);
}

async function loadAgentsForWizard() {
  if (!activeConnectionId) throw new Error("Connect OpenClaw first");
  const res = await api(`/api/openclaw/${activeConnectionId}/agents`);
  const rawAgents = Array.isArray(res?.agents) ? res.agents : [];
  currentAgents = rawAgents
    .map((item) => {
      const source = item && typeof item === "object" ? item : {};
      const raw = (source.raw && typeof source.raw === "object") ? source.raw : source;
      const id = String(source.id || source.agent_id || source.name || raw.id || raw.agent_id || "").trim();
      const name = String(source.name || source.title || raw.name || id).trim() || id;
      if (!id) return null;
      return { id, name, raw };
    })
    .filter(Boolean);
  renderWizardOwnerAgents();
}

function _formatAgentSpecialization(agent) {
  const source = agent && typeof agent === "object" ? agent : {};
  const raw = source.raw && typeof source.raw === "object" ? source.raw : source;
  const summary = summaryAgents.find((s) => String(s.agent_id || "").trim() === String(source.id || "").trim());
  if (summary && Array.isArray(summary.capabilities) && summary.capabilities.length) {
    return summary.capabilities.slice(0, 2).join(" | ");
  }

  const direct = [
    raw.specialty,
    raw.specialization,
    raw.role,
    raw.description,
    raw.summary,
  ]
    .map((v) => String(v || "").trim())
    .find(Boolean);
  if (direct) return direct.slice(0, 180);

  const skills = Array.isArray(raw.skills) ? raw.skills : [];
  const skillNames = skills
    .map((s) => String(s?.name || s?.id || "").trim())
    .filter(Boolean)
    .slice(0, 2);
  if (skillNames.length) return skillNames.join(" | ");

  const tags = Array.isArray(raw.tags) ? raw.tags.map((v) => String(v || "").trim()).filter(Boolean).slice(0, 2) : [];
  if (tags.length) return tags.join(" | ");

  return "General collaborator";
}

function wizardEffectiveProjectAgents() {
  const base = Array.isArray(wizardProjectAgents) && wizardProjectAgents.length
    ? wizardProjectAgents
    : (Array.isArray(selectedAssignedAgents) ? selectedAssignedAgents : []);
  return base.map((a) => ({
    id: String(a?.id || "").trim(),
    name: String(a?.name || a?.agent_name || a?.id || "").trim(),
    role: String(a?.role || "").trim(),
    is_primary: Boolean(a?.is_primary),
    source_type: String(a?.source_type || "owner").trim() || "owner",
    permissions: a?.permissions || {},
  })).filter((a) => Boolean(a.id));
}

function wizardOwnerAgents() {
  return wizardEffectiveProjectAgents().filter((a) => a.source_type === "owner");
}

function buildOwnerAgentsPayload(ownerAgents, primaryAgentId = null) {
  const list = Array.isArray(ownerAgents) ? ownerAgents : [];
  return {
    agent_ids: list.map((a) => String(a.id || "").trim()),
    agent_names: list.map((a) => String(a.name || a.id || "").trim()),
    agent_roles: list.map((a) => String(a.role || "").trim()),
    primary_agent_id: String(primaryAgentId || list.find((a) => a.is_primary)?.id || list[0]?.id || "").trim() || null,
  };
}

async function saveOwnerAgentsToProject(ownerAgents, primaryAgentId, successMessage = "Project agents updated.") {
  if (!selectedProjectId) throw new Error("Choose project first");
  if (!Array.isArray(ownerAgents) || !ownerAgents.length) {
    throw new Error("At least one owner agent is required in project.");
  }
  const payload = buildOwnerAgentsPayload(ownerAgents, primaryAgentId);
  await api(`/api/projects/${selectedProjectId}/agents`, "POST", payload);
  await selectProject(selectedProjectId);
  await loadChatAgents().catch(() => {});
  await refreshWizardExternalAccess({ silent: true }).catch(() => {});
  renderWizardOwnerAgents();
  setMessage("wizard_external_msg", successMessage, "ok");
}

async function inviteOwnerAgentToProject(agent) {
  const id = String(agent?.id || "").trim();
  if (!id) throw new Error("Invalid agent");
  const existing = wizardOwnerAgents();
  if (existing.some((a) => a.id === id)) {
    setMessage("wizard_external_msg", `${agent.name || id} is already in project.`, "ok");
    return;
  }
  const next = [...existing, {
    id,
    name: String(agent?.name || id).trim() || id,
    role: resolveSuggestedRole(agent) || "",
    is_primary: false,
    source_type: "owner",
  }];
  const primary = selectedPrimaryAgentId && next.some((a) => a.id === selectedPrimaryAgentId)
    ? selectedPrimaryAgentId
    : next[0].id;
  await saveOwnerAgentsToProject(next, primary, `${agent.name || id} invited to project.`);
}

async function updateOwnerAgentRole(agentId, roleValue, { setPrimary = false } = {}) {
  const id = String(agentId || "").trim();
  if (!id) throw new Error("agent_id is required");
  const owners = wizardOwnerAgents();
  const target = owners.find((a) => a.id === id);
  if (!target) throw new Error("Owner agent not found in project");
  const next = owners.map((a) => (a.id === id ? { ...a, role: String(roleValue || "").trim() } : a));
  const primary = setPrimary
    ? id
    : (selectedPrimaryAgentId && next.some((a) => a.id === selectedPrimaryAgentId) ? selectedPrimaryAgentId : next[0].id);
  await saveOwnerAgentsToProject(next, primary, setPrimary ? `Primary agent set: ${id}` : `Role updated: ${id}`);
}

async function removeOwnerAgentFromProject(agentId) {
  const id = String(agentId || "").trim();
  if (!id) throw new Error("agent_id is required");
  const owners = wizardOwnerAgents();
  if (!owners.some((a) => a.id === id)) return;
  const next = owners.filter((a) => a.id !== id);
  if (!next.length) {
    throw new Error("Project must keep at least one owner agent.");
  }
  const primary = selectedPrimaryAgentId === id ? next[0].id : (selectedPrimaryAgentId || next[0].id);
  await saveOwnerAgentsToProject(next, primary, `Removed owner agent: ${id}`);
}

async function removeWizardProjectAgent(agent) {
  const id = String(agent?.id || "").trim();
  if (!id) throw new Error("agent_id is required");
  const sourceType = String(agent?.source_type || "owner").trim() || "owner";
  if (sourceType === "external") {
    const membership = wizardExternalMemberships.find((m) => String(m?.agent_id || "").trim() === id && String(m?.status || "").toLowerCase() === "active");
    if (!membership?.id) throw new Error("External membership record not found.");
    await revokeWizardExternalMembership(membership.id);
    await selectProject(selectedProjectId);
    await refreshWizardExternalAccess({ silent: true }).catch(() => {});
    renderWizardOwnerAgents();
    return;
  }
  await removeOwnerAgentFromProject(id);
}

function renderWizardOwnerAgents() {
  const box = $("wizard_owner_agents");
  if (!box) return;
  box.innerHTML = "";

  const assigned = new Set(wizardEffectiveProjectAgents().map((a) => a.id));
  const available = (Array.isArray(currentAgents) ? currentAgents : []).filter((a) => a && a.id && !assigned.has(a.id));
  if (!available.length) {
    const empty = document.createElement("div");
    empty.className = "helper";
    empty.textContent = "No available owner agents to invite.";
    box.appendChild(empty);
    return;
  }

  for (const agent of available) {
    const row = document.createElement("article");
    row.className = "owner-agent-row";

    const avatarWrap = document.createElement("div");
    avatarWrap.className = "wizard-agent-avatar";
    const palette = colorForAgent(agent.id);
    avatarWrap.style.borderColor = palette.border;
    avatarWrap.style.background = hexToRgba(palette.hex, 0.08);
    avatarWrap.appendChild(createAgentAvatarImg(agent.id, `${agent.name || agent.id} mascot`));

    const body = document.createElement("div");
    const title = document.createElement("div");
    title.className = "title";
    title.textContent = `${agent.name || agent.id} (${agent.id})`;
    const meta = document.createElement("p");
    meta.className = "meta";
    const suggested = resolveSuggestedRole(agent);
    const specialization = _formatAgentSpecialization(agent);
    meta.textContent = `Specialization: ${specialization}${suggested ? ` | Suggested role: ${suggested}` : ""}`;
    body.appendChild(title);
    body.appendChild(meta);

    const inviteBtn = document.createElement("button");
    inviteBtn.type = "button";
    inviteBtn.textContent = "Invite";
    inviteBtn.addEventListener("click", () => {
      inviteOwnerAgentToProject(agent).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
    });

    row.appendChild(avatarWrap);
    row.appendChild(body);
    row.appendChild(inviteBtn);
    box.appendChild(row);
  }
}

function openWizard(newProject = true) {
  $("project_wizard").classList.remove("hidden");
  setMessage("wizard_msg", "");
  if (newProject) {
    $("wizard_title").textContent = "New Project";
    $("wizard_step_mode").classList.remove("hidden");
    $("wizard_step_chat").classList.remove("hidden");
    $("wizard_step_project").classList.add("hidden");
    $("wizard_step_agents").classList.add("hidden");
    $("wizard_footer_actions")?.classList.remove("hidden");
    $("wizard_external_access")?.classList.add("hidden");
    setMessage("wizard_external_msg", "");
    $("form_project").reset();
    wizardMode = "chat";
    wizardChatBooted = false;
    wizardChatPending = false;
    wizardSetupSessionKey = `new-project-${Date.now().toString(36)}`;
    wizardTranscript = [];
    wizardDraft = null;
    wizardSuggestedRoles = new Map();
    wizardProjectAgents = [];
    wizardAgentPermissions = [];
    currentAgents = [];
    const log = $("wizard_chat_log");
    if (log) log.innerHTML = "";
    const chatInput = $("wizard_chat_input");
    if (chatInput) chatInput.value = "";
    $("btn_mode_chat")?.classList.add("active");
    $("btn_mode_manual")?.classList.remove("active");
    setWizardMode("chat");
    sendWizardSetupChat({ autoStart: true }).catch((e) => setMessage("wizard_msg", detailToText(e), "error"));
  } else {
    $("wizard_title").textContent = "Manage Agents";
    $("wizard_step_mode").classList.add("hidden");
    $("wizard_step_chat").classList.add("hidden");
    $("wizard_step_project").classList.add("hidden");
    $("wizard_step_agents").classList.remove("hidden");
    $("wizard_footer_actions")?.classList.add("hidden");
    $("wizard_external_access")?.classList.remove("hidden");
    (async () => {
      await loadSummaryAgents().catch(() => {});
      await refreshWizardExternalAccess({ silent: true });
      await loadAgentsForWizard();
    })().catch((e) => setMessage("wizard_msg", detailToText(e), "error"));
  }
}

function closeWizard() {
  $("project_wizard").classList.add("hidden");
  wizardChatPending = false;
}

function appendWizardChatMessage(role, text, meta = "") {
  const log = $("wizard_chat_log");
  if (!log) return;
  const node = document.createElement("div");
  node.className = `wizard-chat-msg ${role}`;
  const m = document.createElement("div");
  m.className = "wizard-chat-meta";
  m.textContent = meta || (role === "user" ? "you" : "setup-agent");
  const bubble = document.createElement("div");
  bubble.className = "wizard-chat-bubble";
  bubble.textContent = text;
  node.appendChild(m);
  node.appendChild(bubble);
  log.appendChild(node);
  log.scrollTop = log.scrollHeight;
}

function pushWizardTranscript(role, text) {
  const clean = String(text || "").trim();
  if (!clean) return;
  wizardTranscript.push({ role, text: clean });
  if (wizardTranscript.length > 80) {
    wizardTranscript = wizardTranscript.slice(-80);
  }
}

function setWizardMode(mode) {
  wizardMode = mode === "manual" ? "manual" : "chat";
  $("btn_mode_chat")?.classList.toggle("active", wizardMode === "chat");
  $("btn_mode_manual")?.classList.toggle("active", wizardMode === "manual");
  $("wizard_step_agents")?.classList.add("hidden");

  if (wizardMode === "manual") {
    $("wizard_title").textContent = "New Project (Manual)";
    $("wizard_step_chat")?.classList.add("hidden");
    $("wizard_step_project")?.classList.remove("hidden");
    const actionBtn = $("btn_wizard_create_now");
    if (actionBtn) actionBtn.textContent = "Create Project Now";
    return;
  }

  $("wizard_title").textContent = "New Project (Chat)";
  $("wizard_step_project")?.classList.add("hidden");
  $("wizard_step_chat")?.classList.remove("hidden");
  const actionBtn = $("btn_wizard_create_now");
  if (actionBtn) actionBtn.textContent = "Create Project Now";
}

async function sendWizardSetupChat({ autoStart = false } = {}) {
  if (!activeConnectionId) throw new Error("OpenClaw connection not selected");
  if (wizardChatPending) return;
  const input = $("wizard_chat_input");
  if (!input) return;

  const typed = input.value.trim();
  const isStart = !wizardChatBooted;
  const msg = isStart
    ? (typed || "Start new project setup and ask the first question.")
    : typed;
  if (!msg) throw new Error("Type your answer first.");
  const hideUserStart = autoStart && isStart && !typed;
  if (!hideUserStart) {
    appendWizardChatMessage("user", msg, "you");
    pushWizardTranscript("user", msg);
  }
  input.value = "";
  wizardChatPending = true;
  $("btn_wizard_chat_send").disabled = true;

  try {
    const res = await api("/api/projects/setup-chat", "POST", {
      connection_id: activeConnectionId,
      message: msg,
      session_key: wizardSetupSessionKey || "new-project",
      timeout_sec: 24,
      start: isStart,
      optimize_tokens: true,
    });
    const text = res.text || detailToText(res.frames) || "(no response)";
    appendWizardChatMessage("agent", text, "setup-agent");
    pushWizardTranscript("assistant", text);
    setMessage("wizard_msg", "Setup chat active.", "ok");
    wizardChatBooted = true;
  } catch (e) {
    appendWizardChatMessage("agent", detailToText(e?.message || e), "setup-agent error");
    throw e;
  } finally {
    wizardChatPending = false;
    $("btn_wizard_chat_send").disabled = false;
  }
}

function getSelectedAgents() {
  const owners = wizardOwnerAgents();
  const payload = buildOwnerAgentsPayload(owners, selectedPrimaryAgentId || owners[0]?.id || null);
  return {
    ids: payload.agent_ids,
    names: payload.agent_names,
    roles: payload.agent_roles,
    primary: payload.primary_agent_id,
  };
}

async function saveAgentSetup() {
  const selected = getSelectedAgents();
  if (!selected.ids.length) throw new Error("Invite at least one owner agent first.");
  const owners = selected.ids.map((id, idx) => ({
    id,
    name: selected.names[idx] || id,
    role: selected.roles[idx] || "",
    is_primary: selected.primary === id,
    source_type: "owner",
  }));
  await saveOwnerAgentsToProject(owners, selected.primary, "Agent setup saved.");
}

function _hoursToInviteTtlSec(rawHours) {
  const n = Number(rawHours);
  if (!Number.isFinite(n)) return 72 * 3600;
  const clampedHours = Math.min(720, Math.max(1, Math.round(n)));
  return clampedHours * 3600;
}

function _parseWritePathsInput(rawText) {
  const parts = String(rawText || "")
    .split(/[,\n]/g)
    .map((s) => String(s || "").trim())
    .filter(Boolean);
  const deduped = [];
  const seen = new Set();
  for (const p of parts) {
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(p);
  }
  return deduped;
}

let wizardProjectInvitationsPreviewUrl = "";

function renderWizardLatestExternalInviteDelivery() {
  const card = $("wizard_external_delivery");
  if (!card) return;
  const inviteUrl = String(wizardLatestExternalInvite?.invite_url || "").trim();
  const portalUrl = String(wizardLatestExternalInvite?.portal_url || "").trim();
  const inviteCode = String(wizardLatestExternalInvite?.invite_code || "").trim();
  const docUrl = String(
    wizardLatestExternalInvite?.project_invitations_preview_url
      || wizardProjectInvitationsPreviewUrl
      || ""
  ).trim();
  const emailSubject = String(wizardLatestExternalInvite?.email_subject || "").trim();
  const emailBody = String(wizardLatestExternalInvite?.email_body || "").trim();
  const emailMailtoUrl = String(wizardLatestExternalInvite?.email_mailto_url || "").trim();

  const hasAny = Boolean(inviteUrl || portalUrl || inviteCode || docUrl || emailSubject || emailBody || emailMailtoUrl);
  card.classList.toggle("hidden", !hasAny);
  if (!hasAny) return;

  const inviteInput = $("ext_latest_invite_url");
  const portalInput = $("ext_latest_portal_url");
  const codeInput = $("ext_latest_invite_code");
  const docInput = $("ext_project_invitations_url");
  const subjectInput = $("ext_latest_email_subject");
  const bodyInput = $("ext_latest_email_body");

  if (inviteInput) inviteInput.value = inviteUrl;
  if (portalInput) portalInput.value = portalUrl;
  if (codeInput) codeInput.value = inviteCode;
  if (docInput) docInput.value = docUrl;
  if (subjectInput) subjectInput.value = emailSubject;
  if (bodyInput) bodyInput.value = emailBody;
}

async function _copyInviteDeliveryText(text, okMsg = "Copied") {
  const value = String(text || "").trim();
  if (!value) throw new Error("Nothing to copy yet.");
  if (!navigator.clipboard?.writeText) throw new Error("Clipboard is not available on this browser.");
  await navigator.clipboard.writeText(value);
  setMessage("wizard_external_msg", okMsg, "ok");
}

function _openInviteDeliveryUrl(url) {
  const value = String(url || "").trim();
  if (!value) throw new Error("URL is empty.");
  window.open(value, "_blank", "noopener");
}

function _latestInviteMailtoUrl() {
  const mailto = String(wizardLatestExternalInvite?.email_mailto_url || "").trim();
  if (mailto) return mailto;
  const subject = encodeURIComponent(String($("ext_latest_email_subject")?.value || "").trim());
  const body = encodeURIComponent(String($("ext_latest_email_body")?.value || "").trim());
  if (!subject && !body) return "";
  return `mailto:?subject=${subject}&body=${body}`;
}

async function tryAutoAcceptProjectInvite() {
  if (!projectInviteContext.active || !sessionToken) return false;
  const info = projectInviteContext.info || {};
  if (!info?.can_accept) return false;
  const connections = Array.isArray(projectInviteContext.connections) ? projectInviteContext.connections : [];
  if (connections.length !== 1) return false;

  const connectionId = String(connections[0]?.id || "").trim();
  if (!connectionId) return false;

  const payload = { connection_id: connectionId };
  const suggestedAgentId = String(info?.requested_agent_id || "").trim();
  const suggestedAgentName = String(info?.requested_agent_name || "").trim();
  const inviteCode = String(projectInviteContext.inviteCode || $("invite_portal_code")?.value || "").trim().toUpperCase();
  if (info?.requires_invite_code && !inviteCode) return false;
  if (suggestedAgentId) payload.agent_id = suggestedAgentId;
  if (suggestedAgentName) payload.agent_name = suggestedAgentName;
  if (inviteCode) payload.invite_code = inviteCode;

  const tokenQuoted = encodeURIComponent(projectInviteContext.token);
  try {
    const res = await api(`/api/projects/invites/${tokenQuoted}/accept`, "POST", payload);
    const acceptedProjectId = String(res?.project_id || "").trim();
    const acceptedConnectionId = String(res?.accepted_connection_id || connectionId).trim();
    if (acceptedConnectionId) preferredConnectionId = acceptedConnectionId;
    setMessage("project_invite_msg", "Invite auto-accepted. Opening project...", "ok");
    clearProjectInviteContext({ clearUrl: true });
    await fetchInitial({ preferredProjectId: acceptedProjectId || null });
    return true;
  } catch (e) {
    setMessage(
      "project_invite_msg",
      `Auto-accept skipped: ${detailToText(e?.message || e)}. You can accept manually below.`,
      "error"
    );
    return false;
  }
}
function renderWizardExternalInvites() {
  const box = $("wizard_external_invites");
  if (!box) return;
  box.innerHTML = "";
  if (!wizardExternalInvites.length) {
    const empty = document.createElement("div");
    empty.className = "helper";
    empty.textContent = "No invites yet.";
    box.appendChild(empty);
    return;
  }

  for (const invite of wizardExternalInvites) {
    const id = String(invite?.id || "").trim();
    if (!id) continue;
    const row = document.createElement("div");
    row.className = "wizard-list-item";

    const title = document.createElement("div");
    title.className = "title";
    const requested = String(invite?.requested_agent_name || invite?.requested_agent_id || "(agent not specified)").trim();
    title.textContent = `${requested} - ${inviteStatusLabel(invite?.status)}`;

    const meta = document.createElement("div");
    meta.className = "meta";
    const metaBits = [];
    if (invite?.target_email) metaBits.push(`target: ${invite.target_email}`);
    if (invite?.role) metaBits.push(`role: ${invite.role}`);
    if (invite?.expires_at) metaBits.push(`expires: ${formatTs(invite.expires_at)}`);
    metaBits.push(`invite_id: ${id}`);
    meta.textContent = metaBits.join(" | ");

    row.appendChild(title);
    row.appendChild(meta);

    const actions = document.createElement("div");
    actions.className = "action-row";
    const inviteDocUrl = String(invite?.invite_doc_preview_url || "").trim();
    if (inviteDocUrl) {
      const openInviteDocBtn = document.createElement("button");
      openInviteDocBtn.type = "button";
      openInviteDocBtn.className = "secondary";
      openInviteDocBtn.textContent = "Open Invite MD";
      openInviteDocBtn.addEventListener("click", () => {
        _openInviteDeliveryUrl(inviteDocUrl);
      });
      actions.appendChild(openInviteDocBtn);
    }

    if (String(invite?.status || "").toLowerCase() === "pending") {
      const revokeBtn = document.createElement("button");
      revokeBtn.type = "button";
      revokeBtn.className = "secondary";
      revokeBtn.textContent = "Revoke";
      revokeBtn.addEventListener("click", () => {
        revokeWizardExternalInvite(id).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
      });
      actions.appendChild(revokeBtn);
    }

    if (actions.childElementCount) {
      row.appendChild(actions);
    }

    box.appendChild(row);
  }
}

function renderWizardExternalMemberships() {
  const box = $("wizard_external_memberships");
  if (!box) return;
  box.innerHTML = "";
  if (!wizardExternalMemberships.length) {
    const empty = document.createElement("div");
    empty.className = "helper";
    empty.textContent = "No external memberships yet.";
    box.appendChild(empty);
    return;
  }

  for (const membership of wizardExternalMemberships) {
    const id = String(membership?.id || "").trim();
    if (!id) continue;
    const row = document.createElement("div");
    row.className = "wizard-list-item";

    const title = document.createElement("div");
    title.className = "title";
    const agentName = String(membership?.agent_name || membership?.agent_id || "external-agent").trim();
    title.textContent = `${agentName} - ${inviteStatusLabel(membership?.status)}`;

    const meta = document.createElement("div");
    meta.className = "meta";
    const metaBits = [];
    if (membership?.role) metaBits.push(`role: ${membership.role}`);
    if (membership?.member_user_id) metaBits.push(`member_user: ${membership.member_user_id}`);
    if (membership?.member_connection_id) metaBits.push(`member_conn: ${membership.member_connection_id}`);
    metaBits.push(`membership_id: ${id}`);
    meta.textContent = metaBits.join(" | ");

    row.appendChild(title);
    row.appendChild(meta);

    if (String(membership?.status || "").toLowerCase() === "active") {
      const actions = document.createElement("div");
      actions.className = "action-row";
      const revokeBtn = document.createElement("button");
      revokeBtn.type = "button";
      revokeBtn.className = "secondary danger";
      revokeBtn.textContent = "Revoke Membership";
      revokeBtn.addEventListener("click", () => {
        revokeWizardExternalMembership(id).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
      });
      actions.appendChild(revokeBtn);
      row.appendChild(actions);
    }

    box.appendChild(row);
  }
}

function renderWizardAgentPermissions() {
  const box = $("wizard_agent_permissions");
  if (!box) return;
  box.innerHTML = "";
  const agents = wizardEffectiveProjectAgents();
  if (!agents.length) {
    const empty = document.createElement("div");
    empty.className = "helper";
    empty.textContent = "No agents in project yet. Invite from Your Agents or External Agents first.";
    box.appendChild(empty);
    return;
  }

  for (const item of agents) {
    const agentId = String(item?.id || item?.agent_id || "").trim();
    if (!agentId) continue;
    const row = document.createElement("article");
    row.className = "wizard-agent-row perm-row";

    const agentName = String(item?.name || item?.agent_name || agentId).trim();
    const sourceType = String(item?.source_type || "owner").trim();
    const perms = (item?.permissions && typeof item.permissions === "object") ? item.permissions : item || {};
    const customBadge = perms?.has_custom ? "custom" : "default";

    const headWrap = document.createElement("div");
    headWrap.className = "wizard-agent-head";
    const avatarWrap = document.createElement("div");
    avatarWrap.className = "wizard-agent-avatar";
    const palette = colorForAgent(agentId);
    avatarWrap.style.borderColor = palette.border;
    avatarWrap.style.background = hexToRgba(palette.hex, 0.08);
    avatarWrap.appendChild(createAgentAvatarImg(agentId, `${agentName} mascot`));

    const headBody = document.createElement("div");
    const titleRow = document.createElement("div");
    titleRow.className = "wizard-agent-title";
    const title = document.createElement("strong");
    title.textContent = `${agentName} (${agentId})`;
    const sourceChip = document.createElement("span");
    sourceChip.className = "chip";
    sourceChip.textContent = sourceType === "external" ? "external" : "owner";
    titleRow.appendChild(title);
    titleRow.appendChild(sourceChip);
    if (item?.is_primary) {
      const primaryChip = document.createElement("span");
      primaryChip.className = "chip primary";
      primaryChip.textContent = "primary";
      titleRow.appendChild(primaryChip);
    }
    const meta = document.createElement("p");
    meta.className = "wizard-agent-meta";
    meta.textContent = `Permission profile: ${customBadge}`;
    headBody.appendChild(titleRow);
    headBody.appendChild(meta);
    headWrap.appendChild(avatarWrap);
    headWrap.appendChild(headBody);

    const roleWrap = document.createElement("div");
    roleWrap.className = "wizard-agent-role";
    const roleLabel = document.createElement("label");
    roleLabel.textContent = "Role";
    const roleInput = document.createElement("input");
    roleInput.type = "text";
    roleInput.value = String(item?.role || "").trim();
    roleInput.placeholder = sourceType === "external" ? "Role from external invite" : "Role in project";
    roleInput.disabled = sourceType !== "owner";
    roleWrap.appendChild(roleLabel);
    roleWrap.appendChild(roleInput);

    const toggles = document.createElement("div");
    toggles.className = "perm-toggles";

    const mkCheck = (label, checked) => {
      const wrap = document.createElement("label");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = Boolean(checked);
      wrap.appendChild(input);
      wrap.appendChild(document.createTextNode(label));
      toggles.appendChild(wrap);
      return input;
    };

    const canChatEl = mkCheck("can_chat_project", perms?.can_chat_project);
    const canReadEl = mkCheck("can_read_files", perms?.can_read_files);
    const canWriteEl = mkCheck("can_write_files", perms?.can_write_files);

    const writePathsLabel = document.createElement("label");
    writePathsLabel.textContent = "write_paths (comma or newline)";
    const writePathsInput = document.createElement("textarea");
    writePathsInput.className = "perm-write-paths";
    writePathsInput.value = Array.isArray(perms?.write_paths) ? perms.write_paths.join("\n") : "";

    const actions = document.createElement("div");
    actions.className = "action-row";
    if (sourceType === "owner") {
      const saveRoleBtn = document.createElement("button");
      saveRoleBtn.type = "button";
      saveRoleBtn.className = "secondary";
      saveRoleBtn.textContent = "Save Role";
      saveRoleBtn.addEventListener("click", () => {
        updateOwnerAgentRole(agentId, roleInput.value, { setPrimary: false })
          .catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
      });

      const primaryBtn = document.createElement("button");
      primaryBtn.type = "button";
      primaryBtn.className = "secondary";
      primaryBtn.textContent = item?.is_primary ? "Primary Agent" : "Set Primary";
      primaryBtn.disabled = Boolean(item?.is_primary);
      primaryBtn.addEventListener("click", () => {
        updateOwnerAgentRole(agentId, roleInput.value, { setPrimary: true })
          .catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
      });

      actions.appendChild(saveRoleBtn);
      actions.appendChild(primaryBtn);
    }

    const saveBtn = document.createElement("button");
    saveBtn.type = "button";
    saveBtn.textContent = "Save Permission";
    saveBtn.addEventListener("click", () => {
      saveWizardAgentPermission(agentId, {
        can_chat_project: Boolean(canChatEl.checked),
        can_read_files: Boolean(canReadEl.checked),
        can_write_files: Boolean(canWriteEl.checked),
        write_paths: _parseWritePathsInput(writePathsInput.value),
      }).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
    });

    const resetBtn = document.createElement("button");
    resetBtn.type = "button";
    resetBtn.className = "secondary";
    resetBtn.textContent = "Reset Default";
    resetBtn.addEventListener("click", () => {
      resetWizardAgentPermission(agentId).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
    });

    actions.appendChild(saveBtn);
    actions.appendChild(resetBtn);

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "secondary danger";
    removeBtn.textContent = sourceType === "external" ? "Remove External" : "Remove Agent";
    removeBtn.addEventListener("click", () => {
      removeWizardProjectAgent(item).catch((e) => setMessage("wizard_external_msg", detailToText(e?.message || e), "error"));
    });
    actions.appendChild(removeBtn);

    row.appendChild(headWrap);
    row.appendChild(roleWrap);
    row.appendChild(toggles);
    row.appendChild(writePathsLabel);
    row.appendChild(writePathsInput);
    row.appendChild(actions);
    box.appendChild(row);
  }
}

async function refreshWizardExternalAccess({ silent = false } = {}) {
  const panel = $("wizard_external_access");
  if (!panel) return;

  if (!selectedProjectId) {
    panel.classList.add("hidden");
    wizardExternalInvites = [];
    wizardExternalMemberships = [];
    wizardAgentPermissions = [];
    wizardProjectAgents = [];
    wizardProjectInvitationsPreviewUrl = "";
    wizardLatestExternalInvite = null;
    renderWizardExternalInvites();
    renderWizardExternalMemberships();
    renderWizardAgentPermissions();
    renderWizardOwnerAgents();
    renderWizardLatestExternalInviteDelivery();
    return;
  }

  panel.classList.remove("hidden");

  const [projectAgentsRes, invitesRes, membershipsRes] = await Promise.all([
    api(`/api/projects/${selectedProjectId}/agents`).catch(() => ({ agents: [], primary_agent: null })),
    api(`/api/projects/${selectedProjectId}/invites/external-agent`).catch(() => ({ invites: [] })),
    api(`/api/projects/${selectedProjectId}/memberships/external-agent`).catch(() => ({ memberships: [] })),
  ]);

  wizardProjectAgents = Array.isArray(projectAgentsRes?.agents) ? projectAgentsRes.agents : [];
  wizardAgentPermissions = wizardProjectAgents.map((item) => ({
    agent_id: item?.id,
    agent_name: item?.name,
    source_type: item?.source_type,
    role: item?.role || "",
    is_primary: Boolean(item?.is_primary),
    permissions: item?.permissions || {},
  }));
  selectedAssignedAgents = wizardProjectAgents;
  selectedPrimaryAgentId = String(projectAgentsRes?.primary_agent?.id || "").trim() || null;
  wizardExternalInvites = Array.isArray(invitesRes?.invites) ? invitesRes.invites : [];
  wizardExternalMemberships = Array.isArray(membershipsRes?.memberships) ? membershipsRes.memberships : [];
  wizardProjectInvitationsPreviewUrl = String(invitesRes?.project_invitations_preview_url || "").trim();

  const latestInviteProjectId = String(wizardLatestExternalInvite?.project_id || "").trim();
  if (wizardLatestExternalInvite && latestInviteProjectId && latestInviteProjectId !== selectedProjectId) {
    wizardLatestExternalInvite = null;
  }

  renderWizardExternalInvites();
  renderWizardExternalMemberships();
  renderWizardAgentPermissions();
  renderWizardOwnerAgents();
  renderWizardLatestExternalInviteDelivery();

  if (!silent) {
    setMessage("wizard_external_msg", "External access data refreshed.", "ok");
  }
}
async function createWizardExternalInvite(ev) {
  if (ev?.preventDefault) ev.preventDefault();
  if (!selectedProjectId) throw new Error("Choose project first");

  const targetEmail = String($("ext_target_email")?.value || "").trim();
  const requestedAgentId = String($("ext_requested_agent_id")?.value || "").trim();
  const requestedAgentName = String($("ext_requested_agent_name")?.value || "").trim();
  const role = String($("ext_role")?.value || "").trim();
  const note = String($("ext_note")?.value || "").trim();
  const expiresHours = String($("ext_expires_hours")?.value || "72").trim();

  const res = await api(`/api/projects/${selectedProjectId}/invites/external-agent`, "POST", {
    target_email: targetEmail || null,
    requested_agent_id: requestedAgentId || null,
    requested_agent_name: requestedAgentName || null,
    role,
    note,
    expires_in_sec: _hoursToInviteTtlSec(expiresHours),
  });

  wizardLatestExternalInvite = res || null;
  if (
    wizardLatestExternalInvite
    && !String(wizardLatestExternalInvite.project_invitations_preview_url || "").trim()
    && wizardProjectInvitationsPreviewUrl
  ) {
    wizardLatestExternalInvite.project_invitations_preview_url = wizardProjectInvitationsPreviewUrl;
  }
  renderWizardLatestExternalInviteDelivery();

  const inviteUrl = String(res?.invite_url || "").trim();
  const portalUrl = String(res?.portal_url || "").trim();
  const inviteCode = String(res?.invite_code || "").trim();
  const docUrl = String(res?.project_invitations_preview_url || "").trim();
  let copied = false;
  if (inviteUrl && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(inviteUrl);
      copied = true;
    } catch {
      copied = false;
    }
  }

  let portalOpened = false;
  if (portalUrl) {
    try {
      _openInviteDeliveryUrl(portalUrl);
      portalOpened = true;
    } catch {
      portalOpened = false;
    }
  }

  const msgBits = ["External invite sent."];
  if (portalOpened) msgBits.push("Receiver portal opened in new tab.");
  if (copied) msgBits.push("Invite URL copied to clipboard.");
  else if (inviteUrl) msgBits.push(`Invite URL: ${inviteUrl}`);
  if (portalUrl && !portalOpened) msgBits.push(`Portal URL: ${portalUrl}`);
  if (inviteCode) msgBits.push(`Invite Code: ${inviteCode}`);
  if (docUrl) msgBits.push(`Project invitations doc: ${docUrl}`);
  if (res?.email_sent_by_primary_agent) msgBits.push("Primary agent sent the invite email.");
  else if (res?.email_delivery_status) msgBits.push(`Email status: ${res.email_delivery_status}`);
  if (res?.email_delivery_error) msgBits.push(`Email issue: ${detailToText(res.email_delivery_error)}`);
  setMessage("wizard_external_msg", msgBits.join(" "), "ok");

  await refreshWizardExternalAccess({ silent: true });
}
async function revokeWizardExternalInvite(inviteId) {
  if (!selectedProjectId) throw new Error("Choose project first");
  const id = String(inviteId || "").trim();
  if (!id) throw new Error("invite_id is required");
  await api(`/api/projects/${selectedProjectId}/invites/external-agent/${encodeURIComponent(id)}/revoke`, "POST");
  setMessage("wizard_external_msg", `Invite revoked: ${id}`, "ok");
  await refreshWizardExternalAccess({ silent: true });
}

async function revokeWizardExternalMembership(membershipId) {
  if (!selectedProjectId) throw new Error("Choose project first");
  const id = String(membershipId || "").trim();
  if (!id) throw new Error("membership_id is required");
  await api(`/api/projects/${selectedProjectId}/memberships/external-agent/${encodeURIComponent(id)}/revoke`, "POST");
  setMessage("wizard_external_msg", `Membership revoked: ${id}`, "ok");
  await refreshWizardExternalAccess({ silent: true });
}

async function saveWizardAgentPermission(agentId, payload) {
  if (!selectedProjectId) throw new Error("Choose project first");
  const id = String(agentId || "").trim();
  if (!id) throw new Error("agent_id is required");
  await api(`/api/projects/${selectedProjectId}/agent-permissions/${encodeURIComponent(id)}`, "POST", payload || {});
  setMessage("wizard_external_msg", `Permission updated: ${id}`, "ok");
  await refreshWizardExternalAccess({ silent: true });
}

async function resetWizardAgentPermission(agentId) {
  if (!selectedProjectId) throw new Error("Choose project first");
  const id = String(agentId || "").trim();
  if (!id) throw new Error("agent_id is required");
  await api(`/api/projects/${selectedProjectId}/agent-permissions/${encodeURIComponent(id)}`, "POST", { reset_to_default: true });
  setMessage("wizard_external_msg", `Permission reset: ${id}`, "ok");
  await refreshWizardExternalAccess({ silent: true });
}

function collectSetupDetailsFromForm() {
  const details = {
    target_users: $("prj_target_users")?.value?.trim() || "",
    constraints: $("prj_constraints")?.value?.trim() || "",
    in_scope: $("prj_in_scope")?.value?.trim() || "",
    out_of_scope: $("prj_out_scope")?.value?.trim() || "",
    milestones: $("prj_milestones")?.value?.trim() || "",
    required_stack: $("prj_required_stack")?.value?.trim() || "",
    first_output: $("prj_first_output")?.value?.trim() || "",
  };
  const compact = {};
  for (const [k, v] of Object.entries(details)) {
    if (v) compact[k] = v;
  }
  return compact;
}

function fillProjectFormFromDraft(draft) {
  if (!draft) return;
  $("prj_title").value = draft.title || "";
  $("prj_brief").value = draft.brief || "";
  $("prj_goal").value = draft.goal || "";
  const d = draft.setup_details || {};
  $("prj_target_users").value = d.target_users || "";
  $("prj_constraints").value = d.constraints || "";
  $("prj_in_scope").value = d.in_scope || "";
  $("prj_out_scope").value = d.out_of_scope || "";
  $("prj_milestones").value = d.milestones || "";
  $("prj_required_stack").value = d.required_stack || "";
  $("prj_first_output").value = d.first_output || "";
}

function cacheSuggestedRolesFromDraft(draft) {
  wizardSuggestedRoles = new Map();
  const list = Array.isArray(draft?.setup_details?.suggested_agents) ? draft.setup_details.suggested_agents : [];
  for (const item of list) {
    if (!item || typeof item !== "object") continue;
    const name = String(item.name || item.id || "").trim().toLowerCase();
    const role = String(item.role || "").trim();
    if (!name || !role) continue;
    wizardSuggestedRoles.set(name, role);
  }
}

function resolveSuggestedRole(agent) {
  const idKey = String(agent.id || "").trim().toLowerCase();
  const nameKey = String(agent.name || "").trim().toLowerCase();
  return wizardSuggestedRoles.get(idKey) || wizardSuggestedRoles.get(nameKey) || "";
}

function buildWizardSetupHistoryText() {
  if (!Array.isArray(wizardTranscript) || !wizardTranscript.length) return "";
  const lines = [];
  for (const item of wizardTranscript.slice(0, 240)) {
    const roleRaw = String(item?.role || "user").trim().toLowerCase();
    const role = roleRaw === "assistant" || roleRaw === "agent"
      ? "ASSISTANT"
      : roleRaw === "system"
        ? "SYSTEM"
        : "USER";
    const text = String(item?.text || item?.content || "").replace(/\r/g, "").trim();
    if (!text) continue;
    lines.push(`${role}: ${text}`);
  }
  return lines.join("\n").slice(0, 120000);
}

async function createProjectRecord({ title, brief, goal, setupDetails, setupChatHistory }) {
  if (!activeConnectionId) throw new Error("OpenClaw connection not selected");
  if (!title || !brief || !goal) throw new Error("title, brief, and goal are required");

  const created = await api("/api/projects", "POST", {
    title,
    brief,
    goal,
    setup_details: setupDetails || {},
    setup_chat_history: String(setupChatHistory || "").slice(0, 120000),
    connection_id: activeConnectionId,
  });

  selectedProjectId = created.id;
  $("wizard_step_chat").classList.add("hidden");
  $("wizard_step_project").classList.add("hidden");
  $("wizard_step_agents").classList.remove("hidden");
  $("wizard_footer_actions")?.classList.add("hidden");
  $("wizard_step_mode")?.classList.add("hidden");
  $("wizard_title").textContent = "Invite Agents";
  setMessage("wizard_msg", "Project created. Invite agents and set roles.", "ok");

  await fetchInitial({ preferredProjectId: created.id });
  await selectProject(created.id);
  await loadSummaryAgents().catch(() => {});
  await refreshWizardExternalAccess({ silent: true }).catch(() => {});
  await loadAgentsForWizard().catch(() => {});
}

async function createProjectFromManual() {
  const title = $("prj_title").value.trim();
  const brief = $("prj_brief").value.trim();
  const goal = $("prj_goal").value.trim();
  if (!title || !brief || !goal) throw new Error("Fill title, brief, and goal");
  const setupDetails = collectSetupDetailsFromForm();
  await createProjectRecord({ title, brief, goal, setupDetails, setupChatHistory: "" });
}

async function createProjectFromChatDraft() {
  if (!wizardTranscript.length) throw new Error("Setup chat is empty. Answer at least one question first.");
  let draft;
  try {
    draft = await api("/api/projects/setup-draft", "POST", {
      connection_id: activeConnectionId,
      transcript: wizardTranscript,
      session_key: wizardSetupSessionKey || "new-project",
      timeout_sec: 20,
      optimize_tokens: true,
    });
  } catch (e) {
    setWizardMode("manual");
    throw new Error(`Could not auto-build project draft from chat. Review fields manually. ${detailToText(e?.message || e)}`);
  }
  wizardDraft = draft;
  cacheSuggestedRolesFromDraft(draft);
  fillProjectFormFromDraft(draft);
  await createProjectRecord({
    title: String(draft.title || "").trim(),
    brief: String(draft.brief || "").trim(),
    goal: String(draft.goal || "").trim(),
    setupDetails: draft.setup_details || {},
    setupChatHistory: buildWizardSetupHistoryText(),
  });
}

async function createProjectNow() {
  if (wizardMode === "manual") {
    await createProjectFromManual();
    return;
  }
  await createProjectFromChatDraft();
}

async function connectOpenClaw(ev) {
  ev.preventDefault();
  setMessage("setup_msg", "");
  const payload = {
    name: $("oc_name").value.trim() || null,
    base_url: $("oc_base").value.trim(),
    api_key: $("oc_key").value.trim(),
  };
  await api("/api/openclaw/connect", "POST", payload);
  setMessage("setup_msg", "Connection saved", "ok");
  await fetchInitial();
}

async function login(ev) {
  ev.preventDefault();
  setMessage("auth_msg", "");
  if (claimAuthContext.active && claimSessionState.connected) {
    const connection = claimConnectionPayload("li");
    validateClaimConnectionPayload(connection);
    const res = await api("/api/a2a/environments/claim/complete", "POST", {
      environment_id: claimAuthContext.environmentId,
      code: claimAuthContext.code,
      mode: "session",
      openclaw_base_url: connection.openclaw_base_url,
      openclaw_api_key: connection.openclaw_api_key,
      openclaw_name: connection.openclaw_name,
    });
    sessionToken = res.token;
    persistSessionToken(sessionToken);
    clearClaimSocialContext();
    setClaimSessionState({ connected: false, email: "", providers: [] });
    setMessage("auth_msg", "Environment claimed.", "ok");
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
    await fetchInitial();
    return;
  }
  const email = $("li_email").value.trim();
  const password = $("li_pass").value;
  const endpoint = claimAuthContext.active ? "/api/a2a/environments/claim/complete" : "/api/login";
  const connection = claimAuthContext.active ? claimConnectionPayload("li") : null;
  if (claimAuthContext.active) validateClaimConnectionPayload(connection);
  const body = claimAuthContext.active
    ? {
      environment_id: claimAuthContext.environmentId,
      code: claimAuthContext.code,
      mode: "login",
      email,
      password,
      openclaw_base_url: connection.openclaw_base_url,
      openclaw_api_key: connection.openclaw_api_key,
      openclaw_name: connection.openclaw_name,
    }
    : {
      email,
      password,
    };
  const res = await api(endpoint, "POST", body);
  sessionToken = res.token;
  persistSessionToken(sessionToken);
  if (claimAuthContext.active) {
    setMessage("auth_msg", "Login success. Environment claimed.", "ok");
    clearClaimSocialContext();
    setClaimSessionState({ connected: false, email: "", providers: [] });
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
  } else {
    setMessage("auth_msg", "Login success", "ok");
  }

  if (projectInviteContext.active) {
    setView("auth");
    await initializeProjectInviteContext({ refreshConnections: true, autoAccept: false }).catch((e) => {
      setMessage("project_invite_msg", detailToText(e?.message || e), "error");
    });
    if (projectInviteContext.active) {
      return;
    }
  }

  await fetchInitial();
}

async function signup(ev) {
  ev.preventDefault();
  setMessage("auth_msg", "");
  if (claimAuthContext.active && claimSessionState.connected) {
    throw new Error("Session already connected. Use Claim Environment button.");
  }
  const email = $("su_email").value.trim();
  const password = $("su_pass").value;
  const confirmPassword = $("su_pass_confirm")?.value || "";
  if (password !== confirmPassword) {
    throw new Error("Password confirmation does not match.");
  }
  validatePasswordStrength(password);
  const endpoint = claimAuthContext.active ? "/api/a2a/environments/claim/complete" : "/api/signup";
  const connection = claimAuthContext.active ? claimConnectionPayload("su") : null;
  if (claimAuthContext.active) validateClaimConnectionPayload(connection);
  const body = claimAuthContext.active
    ? {
      environment_id: claimAuthContext.environmentId,
      code: claimAuthContext.code,
      mode: "signup",
      email,
      password,
      openclaw_base_url: connection.openclaw_base_url,
      openclaw_api_key: connection.openclaw_api_key,
      openclaw_name: connection.openclaw_name,
    }
    : {
      email,
      password,
    };
  const res = await api(endpoint, "POST", body);
  sessionToken = res.token;
  persistSessionToken(sessionToken);
  if (claimAuthContext.active) {
    setMessage("auth_msg", "Account created. Environment claimed.", "ok");
    clearClaimSocialContext();
    setClaimSessionState({ connected: false, email: "", providers: [] });
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
  } else {
    setMessage("auth_msg", "Account created", "ok");
  }

  if (projectInviteContext.active) {
    setView("auth");
    await initializeProjectInviteContext({ refreshConnections: true, autoAccept: false }).catch((e) => {
      setMessage("project_invite_msg", detailToText(e?.message || e), "error");
    });
    if (projectInviteContext.active) {
      return;
    }
  }

  await fetchInitial();
}

async function startOAuth(provider) {
  setMessage("auth_msg", "");
  const providerKey = String(provider || "").trim().toLowerCase();
  if (!providerKey) throw new Error("Invalid OAuth provider.");
  const configured = oauthProvidersState.get(providerKey);
  if (!configured) {
    throw new Error(`${oauthDisplayName(providerKey)} login is not enabled on this server.`);
  }
  if (claimAuthContext.active) {
    const connection = claimConnectionForContext();
    validateOptionalClaimConnectionPayload(connection);
    writeClaimSocialContext({
      environment_id: claimAuthContext.environmentId,
      code: claimAuthContext.code,
      openclaw_base_url: connection.openclaw_base_url,
      openclaw_api_key: connection.openclaw_api_key,
      openclaw_name: connection.openclaw_name,
    });
  } else {
    clearClaimSocialContext();
  }
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}${window.location.hash || ""}`;
  const res = await api(`/api/oauth/${encodeURIComponent(providerKey)}/start`, "POST", {
    next_path: nextPath || "/",
  });
  const authUrl = String(res?.auth_url || "").trim();
  if (!authUrl) throw new Error("OAuth URL was not generated.");
  window.location.assign(authUrl);
}

function shouldClearStoredClaimContext(messageText) {
  const msg = String(messageText || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("invalid claim code")
    || msg.includes("claim code expired")
    || msg.includes("claim code already used")
    || msg.includes("environment already claimed")
    || msg.includes("environment not found")
    || msg.includes("invalid session token")
  );
}

async function tryAutoCompleteClaimFromSocialSession() {
  if (!claimAuthContext.active) return false;
  const saved = readClaimSocialContext();
  if (!saved) return false;
  applyClaimConnectionFromContext(saved);
  const envId = String(saved.environment_id || "").trim();
  const code = String(saved.code || "").trim();
  if (!envId || !code) {
    clearClaimSocialContext();
    return false;
  }
  if (envId !== claimAuthContext.environmentId || code !== claimAuthContext.code) {
    clearClaimSocialContext();
    return false;
  }
  const savedConnection = {
    openclaw_base_url: String(saved.openclaw_base_url || "").trim(),
    openclaw_api_key: String(saved.openclaw_api_key || ""),
    openclaw_name: saved.openclaw_name || null,
  };
  try {
    validateOptionalClaimConnectionPayload(savedConnection);
  } catch (e) {
    setMessage("auth_msg", detailToText(e?.message || e), "error");
    return false;
  }
  if (!savedConnection.openclaw_base_url || !savedConnection.openclaw_api_key) {
    return false;
  }
  try {
    const res = await api("/api/a2a/environments/claim/complete", "POST", {
      environment_id: envId,
      code,
      mode: "session",
      openclaw_base_url: savedConnection.openclaw_base_url,
      openclaw_api_key: savedConnection.openclaw_api_key,
      openclaw_name: savedConnection.openclaw_name,
    });
    sessionToken = res.token;
    persistSessionToken(sessionToken);
    clearClaimSocialContext();
    setClaimSessionState({ connected: false, email: "", providers: [] });
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
    setMessage("auth_msg", "Login social success. Environment claimed.", "ok");
    return true;
  } catch (e) {
    const msg = detailToText(e?.message || e);
    if (shouldClearStoredClaimContext(msg)) {
      clearClaimSocialContext();
    }
    const low = String(msg || "").toLowerCase();
    if (!low.includes("missing authorization") && !low.includes("session is required")) {
      setMessage("auth_msg", msg, "error");
    }
    return false;
  }
}

function setAuthMethod(method) {
  activeAuthMethod = method === "agent" ? "agent" : "hooman";
  const isAgent = activeAuthMethod === "agent";

  $("method_hooman")?.classList.toggle("active", !isAgent);
  $("method_agent")?.classList.toggle("active", isAgent);
  $("hooman_auth")?.classList.toggle("hidden", isAgent);
  $("agent_auth")?.classList.toggle("hidden", !isAgent);
  setMessage("auth_msg", "");
}

function toAbsoluteAppUrl(path) {
  const raw = String(path || "").trim();
  if (!raw) return "";
  try {
    return new URL(raw, window.location.origin).toString();
  } catch {
    return raw;
  }
}

function openProjectInviteDocFromButton(buttonId, emptyMsg = "Link is not available yet.") {
  const btn = $(buttonId);
  const url = String(btn?.dataset?.url || "").trim();
  if (!url) throw new Error(emptyMsg);
  window.open(url, "_blank", "noopener");
}

function refreshAgentGuideUrls() {
  const setupUrl = toAbsoluteAppUrl(AGENT_SETUP_DOC_PATH);
  const securityUrl = toAbsoluteAppUrl(AGENT_SECURITY_DOC_PATH);
  const setupEl = $("agent_login_url");
  const securityEl = $("agent_security_url");
  if (setupEl) setupEl.textContent = setupUrl;
  if (securityEl) {
    securityEl.href = securityUrl;
    securityEl.textContent = securityUrl;
  }
}

async function copyAgentUrl() {
  const url = $("agent_login_url")?.textContent?.trim();
  if (!url) return;

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(url);
      setMessage("auth_msg", "Setup URL copied.", "ok");
      return;
    }
  } catch {}

  setMessage("auth_msg", "Copy not available. Please copy URL manually.", "error");
}

function bindAuthMethods() {
  $("method_hooman").onclick = () => setAuthMethod("hooman");
  $("method_agent").onclick = () => setAuthMethod("agent");
  $("btn_copy_agent_url").onclick = () => copyAgentUrl().catch(() => {});
  $("btn_accept_project_invite")?.addEventListener("click", () => {
    openProjectInviteAgentModal().catch((e) => setMessage("project_invite_msg", detailToText(e?.message || e), "error"));
  });
  $("btn_open_project_invite_doc")?.addEventListener("click", () => {
    try {
      openProjectInviteDocFromButton("btn_open_project_invite_doc", "Invitation document is not available.");
    } catch (e) {
      setMessage("project_invite_msg", detailToText(e?.message || e), "error");
    }
  });
  $("btn_open_project_setup_doc")?.addEventListener("click", () => {
    try {
      openProjectInviteDocFromButton("btn_open_project_setup_doc", "Setup guide is not available.");
    } catch (e) {
      setMessage("project_invite_msg", detailToText(e?.message || e), "error");
    }
  });
  $("btn_invite_open_setup")?.addEventListener("click", () => {
    setView("setup");
  });
  $("btn_ignore_project_invite")?.addEventListener("click", () => {
    ignoreProjectInviteFromUI();
  });
  $("invite_connection_id")?.addEventListener("change", () => {
    closeProjectInviteAgentModal({ reset: true });
    renderProjectInviteUI();
  });
  $("invite_portal_code")?.addEventListener("input", (ev) => {
    const el = ev?.target;
    if (el && typeof el.value === "string") {
      el.value = el.value.toUpperCase();
    }
    renderProjectInviteUI();
  });
  $("btn_confirm_project_invite_accept")?.addEventListener("click", () => {
    confirmProjectInviteFromModal().catch((e) => {
      setMessage("project_invite_agent_modal_msg", detailToText(e?.message || e), "error");
    });
  });
  $("btn_cancel_project_invite_accept")?.addEventListener("click", () => closeProjectInviteAgentModal({ reset: false }));
  $("btn_close_project_invite_agent_modal")?.addEventListener("click", () => closeProjectInviteAgentModal({ reset: false }));
  refreshAgentGuideUrls();
  applyClaimAuthUI();
  renderProjectInviteUI();
  setAuthMethod(activeAuthMethod);
}
function bindTabs() {
  $("tab_login").onclick = () => {
    $("tab_login").classList.add("active");
    $("tab_signup").classList.remove("active");
    $("form_login").classList.remove("hidden");
    $("form_signup").classList.add("hidden");
    setMessage("auth_msg", "");
  };

  $("tab_signup").onclick = () => {
    if (claimAuthContext.active && claimSessionState.connected) return;
    $("tab_signup").classList.add("active");
    $("tab_login").classList.remove("active");
    $("form_signup").classList.remove("hidden");
    $("form_login").classList.add("hidden");
    setMessage("auth_msg", "");
  };
}

async function subscribeEvents() {
  if (!selectedProjectId) throw new Error("Pick project first");
  const projectId = selectedProjectId;

  if (streamAbort) streamAbort.abort();
  projectStreamConnected = false;
  streamAbort = new AbortController();

  const res = await fetch(`/api/projects/${projectId}/events`, {
    headers: { ...authHeaders() },
    signal: streamAbort.signal,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Subscribe failed: ${res.status} ${text}`);
  }

  addEvent("ui.subscribed", { project_id: projectId });
  addLiveUpdate("ui.subscribed", { project_id: projectId });
  projectStreamConnected = true;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const chunks = buffer.split("\n\n");
      buffer = chunks.pop();

      for (const chunk of chunks) {
        const lines = chunk.split("\n");
        let eventName = "message";
        let payload = "";
        for (const line of lines) {
          if (line.startsWith("event:")) eventName = line.slice(6).trim();
          if (line.startsWith("data:")) payload += line.slice(5).trim();
        }
        if (!payload || eventName === "ping" || eventName === "hello") continue;
        if (selectedProjectId !== projectId) continue;
        try {
          const parsed = JSON.parse(payload);
          const kind = parsed.kind || eventName;
          handleProjectEvent(kind, parsed.data || parsed);
        } catch {
          handleProjectEvent(eventName, payload);
        }
      }
    }
    projectStreamConnected = false;
  } catch (e) {
    if (String(e?.name || "").toLowerCase() === "aborterror") {
      projectStreamConnected = false;
      return;
    }
    projectStreamConnected = false;
    throw e;
  } finally {
    if (selectedProjectId !== projectId) {
      projectStreamConnected = false;
    }
  }
}

async function runProject() {
  if (!selectedProjectId) throw new Error("Pick project first");
  if (!selectedProjectReadiness || selectedProjectReadiness.project_id !== selectedProjectId) {
    await loadProjectReadiness(selectedProjectId).catch(() => {});
  }
  if (!selectedProjectReadiness?.can_run) {
    const reason = detailToText(selectedProjectReadiness?.summary || "Project is not ready to run yet.");
    throw new Error(reason);
  }
  await api(`/api/projects/${selectedProjectId}/run`, "POST");
  addEvent("ui.run", "Run started");
  await refreshSelectedProjectData().catch(() => {});
  await loadProjectReadiness(selectedProjectId).catch(() => {});
}

async function deleteSelectedProject() {
  if (!selectedProjectId) throw new Error("Pick project first");
  const projectId = selectedProjectId;
  const projectTitle = String(selectedProjectData?.title || projectId);
  const confirmed = window.confirm(`Delete project \"${projectTitle}\"?\nThis will permanently remove its folder and files.`);
  if (!confirmed) return;

  const res = await api(`/api/projects/${projectId}`, "DELETE");
  addEvent("project.deleted", { project_id: projectId, title: projectTitle, folder_deleted: Boolean(res?.folder_deleted) });
  setMessage("chat_hint", `Project \"${projectTitle}\" deleted.`, "ok");
  if (selectedProjectId === projectId) {
    showEmptyProject();
  }
  await fetchInitial();
}

async function fetchInitial({ preferredProjectId = null } = {}) {
  if (claimAuthContext.active) {
    setView("auth");
    return;
  }
  if (projectInviteContext.active) {
    setView("auth");
    await initializeProjectInviteContext({ refreshConnections: true, autoAccept: false }).catch((e) => {
      setMessage("project_invite_msg", detailToText(e?.message || e), "error");
    });
    if (projectInviteContext.active) {
      return;
    }
  }
  const connections = await api("/api/openclaw/connections");
  if (!connections.length) {
    if (streamAbort) {
      streamAbort.abort();
      streamAbort = null;
    }
    projectStreamConnected = false;
    if (projectRefreshTimer) {
      clearTimeout(projectRefreshTimer);
      projectRefreshTimer = null;
    }
    if (runtimePollHandle) {
      clearInterval(runtimePollHandle);
      runtimePollHandle = null;
    }
    activeConnectionId = null;
    connectionHealthy = false;
    workspaceTreeText = "";
    projectTreeText = "";
    applyWorkspacePolicy(null);
    renderConnections([]);
    setView("setup");
    return;
  }

  renderConnections(connections);
  setView("home");
  await loadAccountProfile({ silent: true }).catch(() => {});
  await ensureWorkspaceForActiveConnection({ silent: true }).catch((e) => {
    setMessage("config_msg", detailToText(e), "error");
  });
  await loadConnectionPolicy(activeConnectionId).catch(() => {});
  await loadWorkspaceTree().catch(() => {});
  await loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch(() => {});

  const projects = await api("/api/projects");
  renderProjects(projects);

  if (projects.length) {
    const deepPreferred = String(projectDeepLinkContext.projectId || "").trim();
    const preferred = String(preferredProjectId || deepPreferred || "").trim();
    const hasPreferred = Boolean(preferred && projects.some((p) => p.id === preferred));
    const hasSelected = Boolean(selectedProjectId && projects.some((p) => p.id === selectedProjectId));
    if (hasPreferred) {
      await selectProject(preferred);
    } else if (hasSelected) {
      await selectProject(selectedProjectId);
    } else {
      showEmptyProject();
      await loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch(() => {});
      await loadChatAgents().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
    }
  } else {
    showEmptyProject();
    await loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch(() => {});
    await loadChatAgents().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  }

  if (projectDeepLinkContext.active) {
    await applyProjectDeepLinkContext().catch(() => {});
  }

  if (activeNavTab === "agents") {
    await loadSummaryAgents({ force: true }).catch(() => {});
  }

  setNavTab(activeNavTab);
  setProjectPane(activeProjectPane);
  renderFolderBrowsers();
  renderProjectUsage();
  renderConfigConnectionDetails();
}

function bindActions() {
  $("form_login").addEventListener("submit", (ev) => login(ev).catch((e) => showUiError("auth_msg", e)));
  $("form_signup").addEventListener("submit", (ev) => signup(ev).catch((e) => showUiError("auth_msg", e)));
  for (const btn of document.querySelectorAll("[data-oauth-provider]")) {
    btn.addEventListener("click", () => {
      const provider = btn.dataset.oauthProvider;
      startOAuth(provider).catch((e) => showUiError("auth_msg", e));
    });
  }
  $("form_connect").addEventListener("submit", (ev) => connectOpenClaw(ev).catch((e) => showUiError("setup_msg", e)));
  $("form_change_password")?.addEventListener("submit", (ev) => changeAccountPassword(ev).catch((e) => showUiError("account_msg", e)));
  $("form_delete_account")?.addEventListener("submit", (ev) => deleteAccount(ev).catch((e) => showUiError("account_msg", e)));
  $("btn_refresh_account")?.addEventListener("click", () =>
    loadAccountProfile({ silent: false }).catch((e) => showUiError("account_msg", e))
  );
  $("btn_logout")?.addEventListener("click", () => logoutUser().catch((e) => showUiError("account_msg", e)));

  $("home_connections").onchange = (ev) => {
    activeConnectionId = ev.target.value;
    connectionHealthy = false;
    workspaceTreeText = "";
    projectTreeText = "";
    applyConnectionStatus();
    renderConfigConnectionDetails();
    ensureWorkspaceForActiveConnection({ silent: false })
      .then(() => loadConnectionPolicy(activeConnectionId).catch(() => null))
      .then(() => loadWorkspaceTree().catch(() => null))
      .then(() => loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch(() => null))
      .catch((e) => setMessage("config_msg", detailToText(e), "error"))
      .finally(() => {
        loadChatAgents().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      });
  };

  $("btn_refresh_home").onclick = () => fetchInitial().catch((e) => setMessage("config_msg", detailToText(e), "error"));
  $("btn_open_connect").onclick = () => setView("setup");

  $("btn_new_project").onclick = () => openWizard(true);
  $("btn_close_wizard").onclick = closeWizard;
  $("btn_mode_chat").onclick = async () => {
    setWizardMode("chat");
    if (!wizardChatBooted && !wizardChatPending) {
      await sendWizardSetupChat({ autoStart: true }).catch((e) => setMessage("wizard_msg", detailToText(e), "error"));
    }
  };
  $("btn_mode_manual").onclick = () => setWizardMode("manual");
  $("btn_wizard_chat_send").onclick = () => sendWizardSetupChat().catch((e) => showUiError("wizard_msg", e));
  $("wizard_chat_input").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      sendWizardSetupChat().catch((e) => showUiError("wizard_msg", e));
    }
  });
  $("btn_wizard_create_now").onclick = () => createProjectNow().catch((e) => showUiError("wizard_msg", e));
  $("btn_manage_agents").onclick = () => {
    if (!selectedProjectId) return;
    openWizard(false);
  };
  $("btn_refresh_plan").onclick = () => {
    if (!selectedProjectId) return;
    loadProjectPlan(selectedProjectId).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  };
  $("btn_regenerate_plan").onclick = () => regenerateProjectPlan().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  $("btn_approve_plan").onclick = () => {
    const mode = String($("btn_approve_plan")?.dataset?.planAction || "approve").trim().toLowerCase();
    if (mode === "resume") {
      controlProjectExecution("resume").catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      return;
    }
    if (mode === "refresh") {
      if (!selectedProjectId) return;
      loadProjectPlan(selectedProjectId).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      return;
    }
    if (mode === "regenerate") {
      regenerateProjectPlan().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
      return;
    }
    approveProjectPlan().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  };
  $("btn_pause_project").onclick = () => {
    const current = String(selectedProjectData?.execution_status || "").toLowerCase();
    const action = current === "paused" ? "resume" : "pause";
    controlProjectExecution(action).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  };
  $("btn_stop_project").onclick = () => controlProjectExecution("stop").catch((e) => setMessage("chat_hint", detailToText(e), "error"));

  $("form_project").addEventListener("submit", (ev) => {
    ev.preventDefault();
    createProjectNow().catch((e) => showUiError("wizard_msg", e));
  });
  $("btn_save_agents")?.addEventListener("click", () => {
    saveAgentSetup().catch((e) => showUiError("wizard_msg", e));
  });
  $("form_external_invite")?.addEventListener("submit", (ev) => {
    createWizardExternalInvite(ev).catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_refresh_external_access")?.addEventListener("click", () => {
    refreshWizardExternalAccess({ silent: false }).catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_copy_latest_invite_url")?.addEventListener("click", () => {
    _copyInviteDeliveryText($("ext_latest_invite_url")?.value, "Invite URL copied.")
      .catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_open_latest_invite_url")?.addEventListener("click", () => {
    try {
      _openInviteDeliveryUrl($("ext_latest_invite_url")?.value);
    } catch (e) {
      showUiError("wizard_external_msg", e);
    }
  });
  $("btn_copy_latest_portal_url")?.addEventListener("click", () => {
    _copyInviteDeliveryText($("ext_latest_portal_url")?.value, "Portal URL copied.")
      .catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_open_latest_portal_url")?.addEventListener("click", () => {
    try {
      _openInviteDeliveryUrl($("ext_latest_portal_url")?.value);
    } catch (e) {
      showUiError("wizard_external_msg", e);
    }
  });
  $("btn_copy_latest_invite_code")?.addEventListener("click", () => {
    _copyInviteDeliveryText($("ext_latest_invite_code")?.value, "Invite code copied.")
      .catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_copy_project_invitations_url")?.addEventListener("click", () => {
    _copyInviteDeliveryText($("ext_project_invitations_url")?.value, "Project invitations URL copied.")
      .catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_open_project_invitations_url")?.addEventListener("click", () => {
    try {
      _openInviteDeliveryUrl($("ext_project_invitations_url")?.value);
    } catch (e) {
      showUiError("wizard_external_msg", e);
    }
  });
  $("btn_copy_latest_email")?.addEventListener("click", () => {
    const subject = String($("ext_latest_email_subject")?.value || "").trim();
    const body = String($("ext_latest_email_body")?.value || "").trim();
    const payload = subject && body ? `Subject: ${subject}\n\n${body}` : (subject || body);
    _copyInviteDeliveryText(payload, "Email draft copied.")
      .catch((e) => showUiError("wizard_external_msg", e));
  });
  $("btn_open_invite_mailto")?.addEventListener("click", () => {
    try {
      const mailto = _latestInviteMailtoUrl();
      if (!mailto) throw new Error("Email draft is empty.");
      window.location.href = mailto;
    } catch (e) {
      showUiError("wizard_external_msg", e);
    }
  });
  $("btn_subscribe").onclick = () => subscribeEvents().catch((e) => addEvent("error", detailToText(e)));
  $("btn_run").onclick = () => runProject().catch((e) => { setMessage("chat_hint", detailToText(e), "error"); addEvent("error", detailToText(e)); });
  $("btn_delete_project").onclick = () => deleteSelectedProject().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  $("btn_clear_events").onclick = () => {
    const evs = $("events");
    if (evs) evs.innerHTML = "";
  };

  $("chat_context_mode")?.addEventListener("change", (ev) => {
    const nextMode = ev?.target?.value || "workspace";
    setChatContextMode(nextMode);
  });
  $("btn_chat_send").onclick = () => sendChatPrototype().catch((e) => showUiError("chat_hint", e));
  $("chat_input").addEventListener("input", () => refreshMentionAutocomplete());
  $("chat_input").addEventListener("click", () => refreshMentionAutocomplete());
  $("chat_input").addEventListener("keydown", (ev) => {
    const hasAutocomplete = !($("chat_autocomplete")?.classList.contains("hidden"));

    if (ev.key === "ArrowDown" && hasAutocomplete) {
      ev.preventDefault();
      shiftMentionSelection(1);
      return;
    }
    if (ev.key === "ArrowUp" && hasAutocomplete) {
      ev.preventDefault();
      shiftMentionSelection(-1);
      return;
    }
    if (ev.key === "Tab" && hasAutocomplete) {
      ev.preventDefault();
      const btn = activeMentionButton();
      if (btn?.dataset.alias) applyMention(btn.dataset.alias);
      hideMentionAutocomplete();
      return;
    }
    if (ev.key === "Escape" && hasAutocomplete) {
      ev.preventDefault();
      hideMentionAutocomplete();
      return;
    }
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      if (hasAutocomplete) {
        const btn = activeMentionButton();
        if (btn?.dataset.alias) applyMention(btn.dataset.alias);
        hideMentionAutocomplete();
        return;
      }
      sendChatPrototype().catch((e) => showUiError("chat_hint", e));
    }
  });

  document.addEventListener("click", (ev) => {
    const box = $("chat_autocomplete");
    const input = $("chat_input");
    if (!box || box.classList.contains("hidden")) return;
    if (box.contains(ev.target) || input.contains(ev.target)) return;
    hideMentionAutocomplete();
  });

  for (const btn of document.querySelectorAll("[data-nav-tab]")) {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.navTab;
      const content = $("nav_content_" + tab);
      if (tab === "projects") {
        setNavTab("projects");
        if (content) content.classList.remove("hidden");
        // Clicking "Projects" while inside a project â†’ back to grid view
        if (selectedProjectId) showEmptyProject();
        return;
      }
      if (activeNavTab !== tab) {
        setNavTab(tab);
        if (content) content.classList.remove("hidden");
        return;
      }
      if (content) content.classList.toggle("hidden");
    });
  }

  for (const btn of document.querySelectorAll("[data-project-pane]")) {
    btn.addEventListener("click", () => setProjectPane(btn.dataset.projectPane));
  }

  // Invite tab toggle (Your Agents / External Agents)
  const tabYours    = $("btn_invite_tab_yours");
  const tabExternal = $("btn_invite_tab_external");
  const panelYours    = $("invite_panel_yours");
  const panelExternal = $("invite_panel_external");
  if (tabYours && tabExternal) {
    tabYours.addEventListener("click", () => {
      tabYours.classList.add("active");
      tabExternal.classList.remove("active");
      if (panelYours)    panelYours.classList.remove("hidden");
      if (panelExternal) panelExternal.classList.add("hidden");
    });
    tabExternal.addEventListener("click", () => {
      tabExternal.classList.add("active");
      tabYours.classList.remove("active");
      if (panelExternal) panelExternal.classList.remove("hidden");
      if (panelYours)    panelYours.classList.add("hidden");
    });
  }

  // Canvas brush aurora on auth view
  const authView = document.getElementById("view_auth");
  const auroraCanvas = document.getElementById("aurora-canvas");
  if (authView && auroraCanvas) {
    const ctx = auroraCanvas.getContext("2d");

    // Offscreen canvas â€” draw all trail here, then blur ONCE onto main canvas
    const offCanvas = document.createElement("canvas");
    const offCtx = offCanvas.getContext("2d");

    let mx = 0, my = 0, lx = 0, ly = 0, auroraMouseActive = false, wasAuthActive = false;
    const brushTrail = [];

    function resizeAurora() {
      const rect = authView.getBoundingClientRect();
      const w = rect.width || window.innerWidth;
      const h = rect.height || window.innerHeight;
      auroraCanvas.width = w; auroraCanvas.height = h;
      offCanvas.width    = w; offCanvas.height    = h;
    }
    resizeAurora();
    window.addEventListener("resize", resizeAurora);

    document.addEventListener("mousemove", (e) => {
      if (!authView.classList.contains("active")) return;
      const rect = authView.getBoundingClientRect();
      mx = e.clientX - rect.left;
      my = e.clientY - rect.top;
      auroraMouseActive = true;
    });

    // Pre-compute Gaussian weights once â€” reused every frame
    const GAUSS_N = 20;
    const gaussStops = Array.from({ length: GAUSS_N + 1 }, (_, si) => {
      const pos = si / GAUSS_N;
      return { pos, w: Math.exp(-4.8 * pos * pos) };
    });

    function tickAurora() {
      requestAnimationFrame(tickAurora);
      const isActive = authView.classList.contains("active");
      if (isActive && !wasAuthActive) { brushTrail.length = 0; lx = mx; ly = my; }
      wasAuthActive = isActive;
      if (!isActive) return;

      const w = auroraCanvas.width, h = auroraCanvas.height;
      lx += (mx - lx) * 0.09;
      ly += (my - ly) * 0.09;

      if (auroraMouseActive) {
        brushTrail.push({ x: lx, y: ly, alpha: 1 });
        if (brushTrail.length > 35) brushTrail.shift();
      }

      // Step 1: draw all trail blobs onto offscreen (no filter)
      offCtx.clearRect(0, 0, w, h);
      const rx = 160, ry = 100;
      for (let i = brushTrail.length - 1; i >= 0; i--) {
        const t = brushTrail[i];
        t.alpha *= 0.955;
        if (t.alpha < 0.015) { brushTrail.splice(i, 1); continue; }
        const grad = offCtx.createRadialGradient(0, 0, 0, 0, 0, ry);
        for (const s of gaussStops) {
          grad.addColorStop(s.pos, `rgba(0,0,0,${(t.alpha * s.w).toFixed(4)})`);
        }
        offCtx.save();
        offCtx.translate(t.x, t.y);
        offCtx.scale(rx / ry, 1);
        offCtx.fillStyle = grad;
        offCtx.beginPath();
        offCtx.arc(0, 0, ry, 0, Math.PI * 2);
        offCtx.fill();
        offCtx.restore();
      }

      // Step 2: black overlay on main canvas
      ctx.globalCompositeOperation = "source-over";
      ctx.fillStyle = "#000";
      ctx.fillRect(0, 0, w, h);

      // Step 3: composite offscreen â†’ main with ONE blur call
      ctx.filter = "blur(10px)";
      ctx.globalCompositeOperation = "destination-out";
      ctx.drawImage(offCanvas, 0, 0);
      ctx.filter = "none";
      ctx.globalCompositeOperation = "source-over";
    }
    tickAurora();
  }
}

bindTabs();
parseClaimAuthFromUrl();
parseProjectInviteFromUrl();
parseProjectDeepLinkFromUrl();
bindAuthMethods();
bindActions();
syncChatContextControls();
updateChatProjectName();
sessionToken = readStoredSessionToken();
setView("auth");
loadOAuthProviders().catch(() => {});
const oauthError = readOauthErrorFromUrl();
if (oauthError) {
  setMessage("auth_msg", oauthError, "error");
  clearOauthErrorParamFromUrl();
}
(async () => {
  if (claimAuthContext.active) {
    const savedClaim = readClaimSocialContext();
    if (savedClaim) applyClaimConnectionFromContext(savedClaim);
    await loadClaimSessionState().catch(() => {});
    const claimed = await tryAutoCompleteClaimFromSocialSession().catch(() => false);
    if (!claimed && claimAuthContext.active) {
      await loadClaimSessionState().catch(() => {});
      applyClaimAuthUI();
      if (claimSessionState.connected) {
        setMessage("auth_msg", "OAuth login successful. Enter OpenClaw Base URL + API key, then click Claim Environment.", "ok");
      }
      setView("auth");
      return;
    }
  }

  if (projectInviteContext.active) {
    setView("auth");
    initializeProjectInviteContext({ refreshConnections: true, autoAccept: false })
      .catch((e) => {
        setMessage("project_invite_msg", detailToText(e?.message || e), "error");
      });
    return;
  }

  fetchInitial({ preferredProjectId: projectDeepLinkContext.projectId || null })
    .catch(() => {
      setView("auth");
    });
})();




