let sessionToken = null;
let activeConnectionId = null;
let selectedProjectId = null;
let selectedProjectData = null;
let selectedProjectPlan = null;
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
const DEFAULT_OWNER_FILES_PATH = "";
const SESSION_TOKEN_KEY = "hivee_session_token_v2";
const AGENT_SETUP_DOC_PATH = "/new-user/NEW-ACCOUNT-SETUP.MD";
const AGENT_SECURITY_DOC_PATH = "/new-user/AGENT-SECURITY-RULES.MD";
const CLAIM_ENV_PARAM = "claim_env_id";
const CLAIM_CODE_PARAM = "claim_code";
const OAUTH_ERROR_PARAM = "oauth_error";
const PASSWORD_POLICY_MIN_LENGTH = 10;
let claimAuthContext = {
  active: false,
  environmentId: "",
  code: "",
};

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

function $(id) { return document.getElementById(id); }

function authHeaders() {
  if (!sessionToken) sessionToken = readStoredSessionToken();
  if (!sessionToken) return {};
  return { Authorization: `Bearer ${sessionToken}` };
}

function clearAuthSession() {
  persistSessionToken(null);
  sessionToken = null;
  activeConnectionId = null;
  selectedProjectId = null;
  selectedProjectData = null;
  selectedProjectPlan = null;
  selectedPrimaryAgentId = null;
  selectedAssignedAgents = [];
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

function applyClaimAuthUI() {
  const notice = $("claim_notice");
  const methodAgent = $("method_agent");
  const btnLogin = $("btn_login");
  const btnSignup = $("btn_signup");
  const socialAuthBlock = $("social_auth_block");
  const claimOnlyBlocks = document.querySelectorAll(".claim-only");
  if (!claimAuthContext.active) {
    if (notice) {
      notice.classList.add("hidden");
      notice.textContent = "";
    }
    if (methodAgent) {
      methodAgent.disabled = false;
      methodAgent.classList.remove("hidden");
    }
    if (btnLogin) btnLogin.textContent = "Continue";
    if (btnSignup) btnSignup.textContent = "Create Account";
    if (socialAuthBlock) socialAuthBlock.classList.remove("hidden");
    claimOnlyBlocks.forEach((el) => el.classList.add("hidden"));
    return;
  }
  activeAuthMethod = "hooman";
  if (methodAgent) {
    methodAgent.disabled = true;
    methodAgent.classList.add("hidden");
  }
  if (notice) {
    const shortEnv = claimAuthContext.environmentId.length > 18
      ? claimAuthContext.environmentId.slice(0, 18) + "..."
      : claimAuthContext.environmentId;
    notice.textContent = `Claim link detected for environment ${shortEnv}. Login or sign up to claim it.`;
    notice.classList.remove("hidden");
  }
  if (btnLogin) btnLogin.textContent = "Login and Claim";
  if (btnSignup) btnSignup.textContent = "Create Account and Claim";
  if (socialAuthBlock) socialAuthBlock.classList.add("hidden");
  claimOnlyBlocks.forEach((el) => el.classList.remove("hidden"));
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

function randomChar(source) {
  if (!source || !source.length) return "";
  let idx = Math.floor(Math.random() * source.length);
  if (window.crypto?.getRandomValues) {
    const bytes = new Uint32Array(1);
    window.crypto.getRandomValues(bytes);
    idx = bytes[0] % source.length;
  }
  return source[idx];
}

function shuffleString(input) {
  const arr = String(input || "").split("");
  for (let i = arr.length - 1; i > 0; i -= 1) {
    let j = Math.floor(Math.random() * (i + 1));
    if (window.crypto?.getRandomValues) {
      const bytes = new Uint32Array(1);
      window.crypto.getRandomValues(bytes);
      j = bytes[0] % (i + 1);
    }
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr.join("");
}

function buildGeneratedPassword(length = 18) {
  const targetLength = Math.max(PASSWORD_POLICY_MIN_LENGTH, Number(length || 18));
  const lower = "abcdefghjkmnpqrstuvwxyz";
  const upper = "ABCDEFGHJKMNPQRSTUVWXYZ";
  const nums = "23456789";
  const symbols = "!@#$%^&*-_=+?";
  const all = lower + upper + nums + symbols;
  let raw = randomChar(lower) + randomChar(upper) + randomChar(nums) + randomChar(symbols);
  while (raw.length < targetLength) raw += randomChar(all);
  return shuffleString(raw);
}

function fillGeneratedSignupPassword() {
  const generated = buildGeneratedPassword(18);
  const passEl = $("su_pass");
  const confirmEl = $("su_pass_confirm");
  if (passEl) passEl.value = generated;
  if (confirmEl) confirmEl.value = generated;
  setMessage("auth_msg", "Secure password generated. You can still edit it before sign up.", "ok");
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
    bootstrap_status: "unknown",
    workspace_tree: "",
    ...policy,
  };
  workspaceTreeText = String(workspacePolicy?.workspace_tree || "").trim();
  renderFolderBrowsers();
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
    connectionHealthy = false;
    renderConfigConnectionDetails();
    return;
  }

  if (!activeConnectionId || !connectionsCache.some((c) => c.id === activeConnectionId)) {
    activeConnectionId = connectionsCache[0].id;
  }
  if (sel) sel.value = activeConnectionId;
  renderConfigConnectionDetails();
}

function renderProjects(projects) {
  projectsCache = projects || [];
  const box = $("projects_list");
  if (!box) return;
  box.innerHTML = "";

  if (!projectsCache.length) {
    box.innerHTML = '<p class="helper">No projects yet. Create your first project.</p>';
    return;
  }

  for (const p of projectsCache) {
    const el = document.createElement("button");
    el.type = "button";
    el.className = "project-item" + (p.id === selectedProjectId ? " active" : "");
    el.innerHTML = `<strong>${p.title}</strong><div class="meta">${formatTs(p.created_at)}</div>`;
    el.onclick = () => {
      if (activeNavTab !== "projects") setNavTab("projects");
      selectProject(p.id).catch((e) => showUiError("chat_hint", e));
    };
    box.appendChild(el);
  }
}

function showEmptyProject() {
  const empty = $("project_empty");
  const details = $("project_details");
  if (empty) empty.classList.remove("hidden");
  if (details) details.classList.add("hidden");
  selectedProjectId = null;
  selectedProjectData = null;
  selectedProjectPlan = null;
  selectedPrimaryAgentId = null;
  selectedAssignedAgents = [];
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
  updateChatProjectName();
  renderFolderBrowsers();
  renderProjectUsage();
  renderProjectPlanInfo();
  renderProjectFiles(null);
  renderWorkspaceFiles(null);
  renderLiveStatus();
  syncProjectHeadbar();
}

function showProjectDetails() {
  const empty = $("project_empty");
  const details = $("project_details");
  if (empty) empty.classList.add("hidden");
  if (details) details.classList.remove("hidden");
  syncProjectHeadbar();
}

function getActiveNavLabel() {
  const activeBtn = document.querySelector("[data-nav-tab].active");
  const text = activeBtn?.querySelector("span")?.textContent || activeBtn?.textContent || "Workspace";
  return String(text).trim();
}

function syncWorkspaceSectionTitle() {
  const title = $("workspace_section_title");
  if (!title) return;
  title.textContent = getActiveNavLabel();
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

function syncProjectHeadbar() {
  const bar = $("project_headbar");
  const title = $("detail_title");
  const subline = $("detail_subline");
  const runBtn = $("btn_run");
  const deleteBtn = $("btn_delete_project");
  if (!bar || !title || !subline || !runBtn || !deleteBtn) return;

  if (activeNavTab !== "projects") {
    bar.classList.add("hidden");
    return;
  }

  if (selectedProjectData) {
    title.textContent = selectedProjectData.title;
    const createdAt = selectedProjectData.created_at ? formatTs(selectedProjectData.created_at) : "-";
    subline.textContent = `${shortText(selectedProjectData.brief)} - Created ${createdAt}`;
    const approved = String(selectedProjectData.plan_status || selectedProjectPlan?.status || "").toLowerCase() === "approved";
    runBtn.classList.toggle("hidden", !approved);
    deleteBtn.classList.remove("hidden");
  } else {
    bar.classList.add("hidden");
    runBtn.classList.add("hidden");
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
  const base = String(agentId || "agent");
  let hash = 0;
  for (let i = 0; i < base.length; i++) {
    hash = ((hash << 5) - hash + base.charCodeAt(i)) | 0;
  }
  const hue = Math.abs(hash) % 360;
  return {
    bg: `hsla(${hue}, 65%, 22%, 0.45)`,
    border: `hsla(${hue}, 75%, 58%, 0.8)`,
  };
}

function parseMessageLinks(text) {
  const raw = String(text || "");
  const re = /((?:https?:\/\/|\/api\/)[^\s<>"']+)/g;
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

function renderChatBubbleContent(bubble, text) {
  bubble.textContent = "";
  const parts = parseMessageLinks(text);
  for (const part of parts) {
    if (part.type !== "link") {
      bubble.appendChild(document.createTextNode(part.value));
      continue;
    }
    const a = document.createElement("a");
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

function updateChatProjectName() {
  const el = $("chat_project_name");
  if (!el) return;
  if (!selectedProjectData) {
    el.textContent = "Project: none";
    return;
  }
  el.textContent = `Project: ${selectedProjectData.title}`;
}

function planStatusLabel(status) {
  const s = String(status || "").toLowerCase();
  if (s === "approved") return "Approved";
  if (s === "awaiting_approval") return "Waiting Approval";
  if (s === "generating") return "Generating";
  if (s === "failed") return "Failed";
  return "Pending";
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
  frame.sandbox = "allow-same-origin";
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
    frame.sandbox = "allow-same-origin";
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
    frame.sandbox = "allow-same-origin";
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

function renderProjectPlanInfo() {
  const statusEl = $("detail_plan_status");
  const updatedEl = $("detail_plan_updated");
  const textEl = $("detail_plan_text");
  const approveBtn = $("btn_approve_plan");
  if (!statusEl || !updatedEl || !textEl || !approveBtn) return;

  if (!selectedProjectData) {
    statusEl.textContent = "Status: -";
    updatedEl.textContent = "";
    textEl.textContent = "No project selected.";
    approveBtn.disabled = true;
    renderProjectExecutionInfo();
    renderLiveStatus();
    return;
  }

  const status = selectedProjectPlan?.status || selectedProjectData.plan_status || "pending";
  const text = selectedProjectPlan?.text || selectedProjectData.plan_text || "";
  const updatedAt = selectedProjectPlan?.updated_at || selectedProjectData.plan_updated_at || null;
  statusEl.textContent = `Status: ${planStatusLabel(status)}`;
  updatedEl.textContent = updatedAt ? `Updated: ${formatTs(updatedAt)}` : "";
  textEl.textContent = text || "Primary agent has not published a plan yet.";
  approveBtn.disabled = status === "approved" || status === "generating";
  approveBtn.textContent = status === "approved" ? "Plan Approved" : "Approve Plan";
  if (selectedProjectData) selectedProjectData.plan_status = status;
  renderProjectExecutionInfo();
  renderLiveStatus();
  syncProjectHeadbar();
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
  renderProjectPlanInfo();
  setMessage("chat_hint", "Plan approved. Delegation task started.", "ok");
  addEvent("project.plan.approved", { project_id: selectedProjectId });
}

async function regenerateProjectPlan() {
  if (!selectedProjectId) throw new Error("Select project first");
  const plan = await api(`/api/projects/${selectedProjectId}/plan/regenerate`, "POST");
  selectedProjectPlan = plan;
  await refreshSelectedProjectData().catch(() => {});
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
      chip.textContent = `@${aliases[0] || normalizeAlias(agent.id)}`;
      chip.title = `${agent.name} (${agent.id})`;
      chip.onclick = () => applyMention(aliases[0] || normalizeAlias(agent.id));
      list.appendChild(chip);
    }
  }

  hideMentionAutocomplete();
  setMessage("chat_hint", agents.length ? `Loaded ${agents.length} agents.` : "No agents available.");
}

async function loadChatAgents() {
  if (selectedProjectId) {
    const scoped = (selectedAssignedAgents || []).map((a) => ({ id: a.id, name: a.name, role: a.role || "", is_primary: Boolean(a.is_primary) }));
    chatAgents = scoped;
    renderChatAgents(scoped);
    connectionHealthy = Boolean(activeConnectionId);
    applyConnectionStatus();
    if (scoped.length) {
      setMessage("chat_hint", `Project scope: ${scoped.length} invited agents available for mention.`, "ok");
    } else {
      setMessage("chat_hint", "No invited agents yet for this project.", "error");
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

  try {
    const res = await api(`/api/openclaw/${activeConnectionId}/agents`);
    chatAgents = res.agents || [];
    renderChatAgents(chatAgents);
    connectionHealthy = true;
    applyConnectionStatus();
  } catch (e) {
    chatAgents = [];
    renderChatAgents([]);
    connectionHealthy = false;
    applyConnectionStatus();
    throw e;
  }
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
  if (!chosen && selectedPrimaryAgentId) chosen = chatById.get(selectedPrimaryAgentId) || null;
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

  const resolved = resolveChatTarget(raw);
  if (resolved.unknownAlias) {
    const msg = `Unknown mention @${resolved.unknownAlias}.`;
    setMessage("chat_hint", msg, "error");
    if (selectedProjectId) throw new Error(msg);
  }

  const targetName = resolved.agent ? `${resolved.agent.name} (${resolved.agent.id})` : "auto route";
  appendChatMessage("user", resolved.message, `you -> ${targetName}`);

  const payload = {
    message: resolved.message,
    agent_id: resolved.agent ? resolved.agent.id : null,
    session_key: selectedProjectId || "main",
    timeout_sec: 25,
  };

  input.value = "";
  hideMentionAutocomplete();
  $("btn_chat_send").disabled = true;

  try {
    const res = await api(`/api/openclaw/${activeConnectionId}/ws-chat`, "POST", payload);
    const shown = res.text || detailToText(res.frames) || "(no text response yet)";
    const resolvedAgentId = String(res.resolved_agent_id || resolved.agent?.id || selectedPrimaryAgentId || "").trim();
    const resolvedAgent = chatById.get(resolvedAgentId);
    const canInlineReply = !selectedProjectId || !projectStreamConnected;
    if (canInlineReply) {
      const role = selectedProjectId ? "agent" : "assistant";
      const meta = selectedProjectId
        ? (resolvedAgent ? resolvedAgent.name : (resolvedAgentId || "agent"))
        : `${res.transport || "ws"} via ${res.path || "gateway"}`;
      appendChatMessage(role, shown, meta, { agentId: resolvedAgentId });
    }
    setMessage("chat_hint", "Message delivered.", "ok");
    addEvent("chat.reply", { path: res.path, text: shown });
    if (selectedProjectId) {
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
  hint.textContent = "Browse owner workspace folder (scoped to your account only).";
  crumbs.innerHTML = "";
  const ownerRootLabel = String(payload.workspace_root || "")
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

  const context = $("context_sidebar");
  if (context) context.classList.toggle("hidden", tab !== "projects");
  syncPrimaryNavState();
  syncWorkspaceSectionTitle();
  syncProjectHeadbar();

  if (tab === "files") {
    renderFolderBrowsers();
    loadWorkspaceFiles(workspaceFilesCurrentPath || DEFAULT_OWNER_FILES_PATH).catch((e) => setMessage("chat_hint", detailToText(e), "error"));
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
  showProjectDetails();
  syncPrimaryNavState();
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
      chip.className = "chip" + (a.is_primary ? " primary" : "");
      const roleText = a.role ? ` - ${a.role}` : "";
      chip.textContent = a.is_primary ? `${a.name} (Primary)${roleText}` : `${a.name}${roleText}`;
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
  currentAgents = res.agents || [];

  const box = $("wizard_agents");
  box.innerHTML = "";
  let autoPrimarySet = false;
  for (const a of currentAgents) {
    const suggestedRole = resolveSuggestedRole(a);
    const row = document.createElement("label");
    row.className = "agent-pick";
    row.innerHTML = `
      <input type="checkbox" data-agent-check="${a.id}">
      <div>
        <strong>${a.name}</strong>
        <span class="small">${a.id}</span>
      </div>
      <label><input type="radio" name="primary_agent" value="${a.id}"> Primary</label>
      <input type="text" data-agent-role="${a.id}" placeholder="Role (optional)" value="${suggestedRole}">
    `;
    box.appendChild(row);
    if (suggestedRole) {
      const ck = row.querySelector(`[data-agent-check="${a.id}"]`);
      const pr = row.querySelector(`input[name="primary_agent"][value="${a.id}"]`);
      if (ck) ck.checked = true;
      if (pr && !autoPrimarySet) {
        pr.checked = true;
        autoPrimarySet = true;
      }
    }
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
    $("form_project").reset();
    wizardMode = "chat";
    wizardChatBooted = false;
    wizardChatPending = false;
    wizardSetupSessionKey = `new-project-${Date.now().toString(36)}`;
    wizardTranscript = [];
    wizardDraft = null;
    wizardSuggestedRoles = new Map();
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
    loadAgentsForWizard().catch((e) => setMessage("wizard_msg", detailToText(e), "error"));
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
  const checks = [...document.querySelectorAll("[data-agent-check]")].filter((el) => el.checked);
  const ids = checks.map((c) => c.getAttribute("data-agent-check"));
  const names = ids.map((id) => {
    const found = currentAgents.find((a) => a.id === id);
    return found ? found.name : id;
  });
  const roles = ids.map((id) => {
    const input = document.querySelector(`[data-agent-role="${id}"]`);
    return input ? String(input.value || "").trim() : "";
  });
  const primary = document.querySelector("input[name='primary_agent']:checked")?.value || ids[0] || null;
  return { ids, names, roles, primary };
}

async function saveAgentSetup() {
  if (!selectedProjectId) throw new Error("Choose project first");
  const picked = getSelectedAgents();
  if (!picked.ids.length) throw new Error("Pick at least one agent");

  await api(`/api/projects/${selectedProjectId}/agents`, "POST", {
    agent_ids: picked.ids,
    agent_names: picked.names,
    agent_roles: picked.roles,
    primary_agent_id: picked.primary,
  });

  setMessage("wizard_msg", "Agents saved. Primary agent is building project info and plan.", "ok");
  await selectProject(selectedProjectId);
  await loadChatAgents().catch(() => {});
  setTimeout(closeWizard, 450);
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

  await loadAgentsForWizard();
  for (const a of currentAgents) {
    const preset = resolveSuggestedRole(a);
    if (!preset) continue;
    const roleInput = document.querySelector(`[data-agent-role="${a.id}"]`);
    if (roleInput && !String(roleInput.value || "").trim()) roleInput.value = preset;
  }
  await fetchInitial();
  await selectProject(created.id);
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
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
  } else {
    setMessage("auth_msg", "Login success", "ok");
  }
  await fetchInitial();
}

async function signup(ev) {
  ev.preventDefault();
  setMessage("auth_msg", "");
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
    clearClaimParamsFromUrl();
    claimAuthContext = { active: false, environmentId: "", code: "" };
    applyClaimAuthUI();
  } else {
    setMessage("auth_msg", "Account created", "ok");
  }
  await fetchInitial();
}

async function startOAuth(provider) {
  setMessage("auth_msg", "");
  if (claimAuthContext.active) {
    throw new Error("Social login is disabled in claim mode. Use email and password for this claim link.");
  }
  const providerKey = String(provider || "").trim().toLowerCase();
  if (!providerKey) throw new Error("Invalid OAuth provider.");
  const res = await api(`/api/oauth/${encodeURIComponent(providerKey)}/start`, "POST", {
    next_path: window.location.pathname || "/",
  });
  const authUrl = String(res?.auth_url || "").trim();
  if (!authUrl) throw new Error("OAuth URL was not generated.");
  window.location.assign(authUrl);
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
  refreshAgentGuideUrls();
  applyClaimAuthUI();
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
  await api(`/api/projects/${selectedProjectId}/run`, "POST");
  addEvent("ui.run", "Run started");
  await refreshSelectedProjectData().catch(() => {});
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

async function fetchInitial() {
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
    const pick = selectedProjectId && projects.some((p) => p.id === selectedProjectId) ? selectedProjectId : projects[0].id;
    await selectProject(pick);
  } else {
    showEmptyProject();
    await loadWorkspaceFiles(DEFAULT_OWNER_FILES_PATH).catch(() => {});
    await loadChatAgents().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
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
  $("btn_generate_password")?.addEventListener("click", () => fillGeneratedSignupPassword());
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
  $("btn_approve_plan").onclick = () => approveProjectPlan().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
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
  $("btn_save_agents").onclick = () => saveAgentSetup().catch((e) => showUiError("wizard_msg", e));

  $("btn_subscribe").onclick = () => subscribeEvents().catch((e) => addEvent("error", detailToText(e)));
  $("btn_run").onclick = () => runProject().catch((e) => addEvent("error", detailToText(e)));
  $("btn_delete_project").onclick = () => deleteSelectedProject().catch((e) => setMessage("chat_hint", detailToText(e), "error"));
  $("btn_clear_events").onclick = () => {
    const evs = $("events");
    if (evs) evs.innerHTML = "";
  };

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
}

bindTabs();
parseClaimAuthFromUrl();
bindAuthMethods();
bindActions();
sessionToken = readStoredSessionToken();
setView("auth");
const oauthError = readOauthErrorFromUrl();
if (oauthError) {
  setMessage("auth_msg", oauthError, "error");
  clearOauthErrorParamFromUrl();
}
fetchInitial()
  .catch(() => {
    setView("auth");
  });
