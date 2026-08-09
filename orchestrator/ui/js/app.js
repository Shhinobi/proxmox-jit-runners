/* ============================================================================
   Runner Orchestrator dashboard — Luminous Slate.

   Replaces ~1300 lines of hand-rolled innerHTML string building with Alpine
   components over one SSE snapshot. Structure mirrors rework-task/ so the
   approved mockup and the real UI stay comparable.

   Everything mutating goes through `apiRequest`, which is the single place the
   admin token is attached.
   ========================================================================= */

(function () {
"use strict";

const GiB = 1024 ** 3;
const TOKEN_KEY = "ro-admin-token";

/* ------------------------------------------------------------ formatters -- */

const fmt = {
  age(seconds) {
    if (seconds === null || seconds === undefined) return "—";
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
    return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
  },
  ageShort(seconds) {
    if (seconds === null || seconds === undefined) return "—";
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.round(s / 60)}m`;
    if (s < 86400) return `${Math.round(s / 3600)}h`;
    return `${Math.round(s / 86400)}d`;
  },
  bytes(n) {
    if (!n) return "0 B";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v < 10 && i > 0 ? 2 : v < 100 ? 1 : 0)} ${units[i]}`;
  },
  gib(n) { return n === null || n === undefined ? "—" : `${(n / GiB).toFixed(1)} GiB`; },
  pct(ratio, digits = 1) {
    return ratio === null || ratio === undefined ? "—" : `${(ratio * 100).toFixed(digits)}%`;
  },
  time(epoch) {
    return new Date(epoch * 1000).toLocaleTimeString(undefined, {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  },
  dateTime(epoch) {
    return new Date(epoch * 1000).toLocaleString(undefined, {
      month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
    });
  },
};

/* Status carries a glyph as well as a colour, so it survives grayscale,
   colour-blindness, and a screenshot pasted into a postmortem. */
const STATUS = {
  ready:          { cls: "s-ready",        glyph: "●", label: "ready" },
  running:        { cls: "s-running",      glyph: "▶", label: "running" },
  provisioning:   { cls: "s-provisioning", glyph: "◐", label: "provisioning" },
  reserved:       { cls: "s-provisioning", glyph: "◐", label: "reserved" },
  delete_pending: { cls: "s-pending",      glyph: "▽", label: "delete pending" },
  deleting:       { cls: "s-pending",      glyph: "▽", label: "deleting" },
  delete_failed:  { cls: "s-failed",       glyph: "✕", label: "delete failed" },
  completed:      { cls: "s-done",         glyph: "●", label: "completed" },
  failed:         { cls: "s-failed",       glyph: "✕", label: "failed" },
  queued:         { cls: "s-idle",         glyph: "○", label: "queued" },
  waiting_for_runner: { cls: "s-provisioning", glyph: "◐", label: "waiting" },
  rejected:       { cls: "s-idle",         glyph: "○", label: "rejected" },
};
const statusMeta = (s) => STATUS[s] || { cls: "s-idle", glyph: "○", label: s || "unknown" };

/* --------------------------------------------------------------- fetch ---- */

let activeDashboard = null;

async function apiRequest(path, options) {
  const opts = { ...(options || {}) };
  opts.headers = { ...(opts.headers || {}) };
  const token = sessionStorage.getItem(TOKEN_KEY);
  if (token) opts.headers["X-Admin-Token"] = token;
  if (opts.body && !opts.headers["Content-Type"]) {
    opts.headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, opts);
  const text = await response.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch { body = { detail: text }; }
  if (!response.ok) {
    if (response.status === 401 && activeDashboard) activeDashboard.promptForToken();
    const detail = (body && (body.detail || body.message)) || `HTTP ${response.status}`;
    throw new Error(detail);
  }
  return body;
}

/* The policies store (ported unchanged) calls these as free functions. */
function showToast(kind, title, body, holdMs) {
  if (activeDashboard) activeDashboard.toast(kind, title, body, holdMs);
}
function confirmAction(opts) {
  return activeDashboard
    ? activeDashboard.ask(opts)
    : Promise.resolve(window.confirm(opts.title || "Are you sure?"));
}
/* Shared re-entrancy guard between the dashboard and the policies store. */
const state = { actionInFlight: false };

/* --------------------------------------------------------- svg helpers ---- */

/* A gap in the data is drawn as a gap. Interpolating across an outage would
   hide exactly the thing this chart exists to show. */
function sparkPaths(values, width = 220, height = 32) {
  const nums = values.filter((v) => v !== null && v !== undefined);
  if (!nums.length) return { runs: [], gaps: [] };
  const max = Math.max(...nums) * 1.15 || 1;
  const step = values.length > 1 ? width / (values.length - 1) : width;
  const x = (i) => i * step;
  const y = (v) => height - (v / max) * height;

  const runs = [];
  const gaps = [];
  let cur = [];
  let gapStart = null;
  values.forEach((v, i) => {
    if (v === null || v === undefined) {
      if (cur.length) { runs.push(cur); cur = []; }
      if (gapStart === null) gapStart = i;
    } else {
      if (gapStart !== null) { gaps.push([gapStart, i]); gapStart = null; }
      cur.push([x(i), y(v)]);
    }
  });
  if (cur.length) runs.push(cur);
  if (gapStart !== null) gaps.push([gapStart, values.length - 1]);

  return {
    runs: runs.filter((r) => r.length > 1).map((r) => ({
      line: r.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" "),
      area:
        `M${r[0][0].toFixed(1)},${height} ` +
        r.map((p) => `L${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ") +
        ` L${r[r.length - 1][0].toFixed(1)},${height} Z`,
    })),
    gaps: gaps.map(([a, b]) => ({ x: x(a), w: Math.max(2, x(b) - x(a)) })),
  };
}

const ROUTES = [
  "overview", "runners", "jobs", "webhooks",
  "capacity", "dependencies", "drift", "logs", "cache", "policies",
];

function parseHash() {
  const raw = (location.hash || "#/overview").slice(2);
  const [path] = raw.split("?");
  return ROUTES.includes(path) ? path : "overview";
}

/* ------------------------------------------------------------ component -- */

function dashboard() {
  return {
    snap: null,
    route: "overview",
    theme: localStorage.getItem("ro-theme") || "auto",
    connection: "connecting",
    lastSnapshotAt: null,
    tick: 0,
    source: null,
    retryDelay: 1000,

    setup: { claimed: true, can_claim: false, claim_window_seconds: 0 },
    setupForm: { password: "", confirm: "" },
    filters: { repository: "", status: "", mode: "", q: "" },
    logFilters: { level: "all", category: "all" },
    openLog: null,
    drawer: null,
    confirmBox: null,
    palette: { open: false, query: "", index: 0 },
    toasts: [],
    busy: "",
    drift: null,
    driftLoading: false,
    driftSelection: [],
    cache: null,
    cacheLoading: false,

    init() {
      activeDashboard = this;
      this.applyTheme();
      this.route = parseHash();
      window.addEventListener("hashchange", () => {
        this.route = parseHash();
        this.drawer = null;
        this.onRouteEnter();
      });
      this.onRouteEnter();
      window.addEventListener("keydown", (e) => this.onKeydown(e));
      /* Relative timestamps must keep moving between snapshots, or "14s ago"
         becomes a lie the moment the stream goes quiet. */
      setInterval(() => { this.tick++; }, 1000);
      /* A hidden tab does not need a snapshot every 3 seconds. */
      document.addEventListener("visibilitychange", () => {
        if (document.hidden) this.closeStream();
        else this.connect();
      });
      this.connect();
      this.refreshSetup();
    },

    onRouteEnter() {
      if (this.route === "policies") {
        const store = window.Alpine?.store("policies");
        if (store && !store.hydrated) store.load();
      }
      if (this.route === "cache" && !this.cache) this.loadCache();
    },

    /* --- stream --- */
    closeStream() {
      if (this.source) { this.source.close(); this.source = null; }
    },
    connect() {
      this.closeStream();
      const source = new EventSource("/api/v1/events?interval=3&limit=50");
      this.source = source;
      source.addEventListener("snapshot", (event) => {
        try {
          this.snap = JSON.parse(event.data);
          this.lastSnapshotAt = Date.now();
          this.connection = "live";
          this.retryDelay = 1000;
        } catch {
          /* A malformed frame must not take the stream down. */
        }
      });
      source.onerror = () => {
        this.connection = "reconnecting";
        this.closeStream();
        /* The browser's own retry is immediate and unbounded; back off. */
        setTimeout(() => this.connect(), this.retryDelay);
        this.retryDelay = Math.min(this.retryDelay * 2, 30000);
      };
    },
    get staleSeconds() {
      void this.tick;
      if (!this.lastSnapshotAt) return null;
      return Math.floor((Date.now() - this.lastSnapshotAt) / 1000);
    },
    get isStale() {
      const age = this.staleSeconds;
      return this.connection !== "live" || (age !== null && age > 15);
    },

    /* --- theme --- */
    applyTheme() {
      const root = document.documentElement;
      if (this.theme === "auto") root.removeAttribute("data-theme");
      else root.setAttribute("data-theme", this.theme);
    },
    cycleTheme() {
      this.theme = { auto: "dark", dark: "light", light: "auto" }[this.theme];
      localStorage.setItem("ro-theme", this.theme);
      this.applyTheme();
    },
    get themeGlyph() { return { auto: "◐", dark: "☾", light: "☀" }[this.theme]; },

    /* --- first-run setup --- */
    get needsSetup() {
      const auth = this.overview.admin_auth ?? this.setup;
      return auth.claimed === false;
    },
    async refreshSetup() {
      try {
        this.setup = await apiRequest("/api/v1/setup");
      } catch {
        /* Non-fatal: the snapshot carries the same state. */
      }
    },
    async claimInstance() {
      const password = this.setupForm.password;
      if (password.length < 12 || password !== this.setupForm.confirm) return;
      try {
        const body = await apiRequest("/api/v1/setup/claim", {
          method: "POST",
          body: JSON.stringify({ password }),
        });
        // Store it for this tab so the very next admin action just works.
        sessionStorage.setItem(TOKEN_KEY, password);
        this.setupForm = { password: "", confirm: "" };
        this.toast("ok", "Instance secured", body.message);
        await this.refreshSetup();
        await this.refreshOnce();
      } catch (error) {
        this.toast("bad", "Could not set the password", String(error.message || error), 12000);
      }
    },

    /* --- derived --- */
    get overview() { return this.snap?.overview ?? {}; },
    get cap() { return this.overview.capacity ?? {}; },
    get storage() { return this.overview.storage ?? {}; },
    get alerts() { return this.snap?.alerts ?? []; },
    get deps() {
      const dependencies = this.overview.dependencies ?? {};
      const github = dependencies.github_api ?? {};
      const proxmox = dependencies.proxmox_api ?? {};
      const broker = dependencies.broker_ssh ?? {};
      return [
        {
          name: "github_api",
          label: "GitHub API",
          up: github.state === "closed" && github.up !== false,
          detail: github.up === false
            ? "down"
            : github.latency_seconds != null
              ? `${Math.round(github.latency_seconds * 1000)}ms`
              : String(github.state ?? "unknown"),
          error: github.last_error,
          failures: github.consecutive_failures,
          openFor: github.opened_for_seconds,
        },
        {
          name: "proxmox_api",
          label: "Proxmox",
          up: proxmox.up !== false,
          detail: proxmox.latency_seconds != null
            ? `${Math.round(proxmox.latency_seconds * 1000)}ms`
            : (proxmox.up === false ? "down" : "—"),
          error: proxmox.last_error,
        },
        {
          name: "broker_ssh",
          label: "Cache broker",
          up: broker.up !== false,
          detail: broker.latency_seconds != null
            ? `${Math.round(broker.latency_seconds * 1000)}ms`
            : (broker.up === false ? "down" : "—"),
          error: broker.last_error,
        },
        {
          name: "storage",
          label: "Storage",
          // Three states, not two. A reading we could not take is neither
          // healthy nor failing: the guard deliberately fails open, so nothing
          // is blocked — but claiming "healthy" would be a lie, and it is
          // exactly the state in which the admission floor is not armed.
          up: this.storage.admits !== false,
          unknown: this.storage.used_ratio == null,
          detail: this.storage.used_ratio != null ? fmt.pct(this.storage.used_ratio) : "no reading",
          error: this.storage.admits === false
            ? this.storage.reason
            : (this.storage.used_ratio == null
                ? "No storage reading, so the admission floor is not armed."
                : null),
        },
      ];
    },

    get meters() {
      const c = this.cap;
      const st = this.storage;
      const m = (label, value, of, ratio, tone, note) =>
        ({ label, value, of, ratio: Math.min(1, Math.max(0, ratio || 0)), tone, note });
      const usedRatio = st.used_ratio ?? 0;
      return [
        m("Queue", c.queued ?? 0, null, (c.queued ?? 0) / 8,
          (c.queued ?? 0) > 0 ? "warn" : "idle",
          c.oldest_queued_job_age_seconds
            ? `oldest ${fmt.age(c.oldest_queued_job_age_seconds)}`
            : "nothing waiting"),
        m("Active", c.active ?? 0, c.max_active, (c.active ?? 0) / (c.max_active || 1),
          "", `ceiling ${c.max_active ?? "—"}`),
        m("Warm ready", c.warm_ready ?? 0, c.warm_target,
          (c.warm_ready ?? 0) / (c.warm_target || 1),
          (c.warm_target ?? 0) === 0 ? "idle"
            : (c.warm_ready ?? 0) === 0 ? "bad"
            : (c.warm_ready < c.warm_target ? "warn" : "ok"),
          c.warm_provisioning ? `${c.warm_provisioning} provisioning` : `target ${c.warm_target ?? 0}`),
        m("Pool ready", c.pool_ready ?? 0, c.pool_size, (c.pool_ready ?? 0) / (c.pool_size || 1),
          (c.pool_size ?? 0) === 0 ? "idle" : (c.pool_ready ?? 0) === 0 ? "warn" : "ok",
          "shared fallback"),
        m("Teardown", c.delete_failed ?? 0, null, (c.delete_failed ?? 0) / 5,
          (c.delete_failed ?? 0) > 0 ? "bad" : "ok",
          (c.delete_failed ?? 0) > 0 ? "containers holding disk" : "nothing stuck"),
        m("Thin pool", st.used_ratio != null ? fmt.pct(usedRatio) : "—", null, usedRatio,
          usedRatio > 0.9 ? "bad" : usedRatio > (st.floor_ratio ?? 0.8) ? "warn" : "ok",
          st.avail_bytes != null ? `${fmt.gib(st.avail_bytes)} free` : "unavailable"),
      ];
    },

    get gauge() {
      const st = this.storage;
      const total = st.total_bytes || 0;
      if (!total) return null;
      const used = st.used_bytes || 0;
      return {
        usedPct: (used / total) * 100,
        floorPct: (st.floor_ratio ?? 0.8) * 100,
        critical: (st.used_ratio ?? 0) > 0.9,
        free: total - used,
        projectedPct: ((st.projected_demand_bytes || 0) / total) * 100,
      };
    },

    get instances() {
      const f = this.filters;
      const q = f.q.toLowerCase();
      return (this.snap?.instances ?? []).filter((r) =>
        (!f.repository || (r.repository || "").toLowerCase().includes(f.repository.toLowerCase())) &&
        (!f.status || r.status === f.status) &&
        (!f.mode || r.mode === f.mode) &&
        (!q || JSON.stringify(r).toLowerCase().includes(q))
      );
    },
    get jobs() {
      const f = this.filters;
      return (this.snap?.jobs ?? []).filter((r) =>
        (!f.repository || (r.repository || "").toLowerCase().includes(f.repository.toLowerCase())) &&
        (!f.status || r.status === f.status) &&
        (!f.mode || r.mode === f.mode)
      );
    },
    get logs() {
      const f = this.logFilters;
      return (this.snap?.logs ?? []).filter((r) => {
        if (f.level === "warn" && !["warn", "error"].includes(r.level)) return false;
        if (f.level === "error" && r.level !== "error") return false;
        if (f.category !== "all" && r.category !== f.category) return false;
        return true;
      });
    },
    get logCategories() {
      return [...new Set((this.snap?.logs ?? []).map((r) => r.category))].sort();
    },
    get repoOptions() {
      return [...new Set((this.snap?.instances ?? []).map((r) => r.repository).filter(Boolean))];
    },
    get failedCount() {
      return (this.snap?.instances ?? []).filter((r) => r.status === "delete_failed").length;
    },
    get driftCount() {
      const d = this.snap?.drift_counts ?? {};
      return (d.rows_without_container || 0) + (d.containers_without_row || 0);
    },

    timelineRows(limit) {
      const jobs = (this.snap?.jobs ?? []).slice(0, limit || 12).filter((j) => j.phases);
      const total = (j) => Object.values(j.phases).reduce((a, b) => a + b, 0) || 1;
      const max = Math.max(...jobs.map(total), 1);
      return jobs.map((j) => ({
        job: j,
        width: (total(j) / max) * 100,
        total: total(j),
        segs: Object.entries(j.phases).map(([phase, v]) => ({
          phase, seconds: v, pct: (v / total(j)) * 100,
        })),
      }));
    },

    /* --- actions --- */
    go(route) { location.hash = `#/${route}`; },

    toast(kind, title, body, holdMs) {
      const id = Math.random().toString(36).slice(2);
      this.toasts.push({ id, kind: kind === "info" ? "" : kind, title, body });
      const hold = holdMs === 0 ? 0 : (holdMs || 6000);
      if (hold) setTimeout(() => { this.toasts = this.toasts.filter((t) => t.id !== id); }, hold);
    },

    ask(opts) {
      return new Promise((resolve) => {
        this.confirmBox = { ...opts, resolve };
      });
    },
    resolveConfirm(ok) {
      const box = this.confirmBox;
      this.confirmBox = null;
      if (box && box.resolve) box.resolve(ok);
    },

    promptForToken() {
      const token = window.prompt(
        "Admin actions require a token (api.admin_token_file on the server)."
      );
      if (token) sessionStorage.setItem(TOKEN_KEY, token.trim());
    },
    signOut() {
      sessionStorage.removeItem(TOKEN_KEY);
      this.toast("ok", "Signed out", "The admin token was cleared from this tab.");
    },
    get hasToken() { return Boolean(sessionStorage.getItem(TOKEN_KEY)); },

    async adminAction(name, path, opts) {
      if (state.actionInFlight) {
        this.toast("warn", "Please wait", "Another action is already in progress.");
        return null;
      }
      state.actionInFlight = true;
      this.busy = name;
      try {
        const body = await apiRequest(path, { method: "POST", ...(opts || {}) });
        this.toast("ok", name, (body && body.message) || "Done.");
        await this.refreshOnce();
        return body;
      } catch (error) {
        this.toast("bad", `${name} failed`, String(error.message || error), 12000);
        return null;
      } finally {
        state.actionInFlight = false;
        this.busy = "";
      }
    },

    async refreshOnce() {
      try {
        this.snap = await apiRequest("/api/v1/snapshot");
        this.lastSnapshotAt = Date.now();
      } catch {
        /* The stream will catch up; a failed manual refresh is not fatal. */
      }
    },

    async destroyInstance(row, force) {
      const ok = await this.ask({
        kind: "danger",
        title: force ? `Force-destroy CT ${row.vmid}?` : `Destroy CT ${row.vmid}?`,
        body: force
          ? "Force skips the GitHub deregistration probe. The container is removed now and "
            + "the registration is queued for cleanup. Use this when GitHub is unreachable."
          : "The runner is deregistered from GitHub if it is reachable, then the container "
            + "is destroyed.",
        label: force ? "Force destroy" : "Destroy",
      });
      if (!ok) return;
      await this.adminAction(
        `Destroy ${row.vmid}`,
        `/api/v1/admin/instances/${row.vmid}/destroy`,
        { body: JSON.stringify({ force: Boolean(force) }) }
      );
      this.drawer = null;
    },

    async loadDrift() {
      this.driftLoading = true;
      try {
        this.drift = await apiRequest("/api/v1/drift");
        this.driftSelection = [];
      } catch (error) {
        this.toast("bad", "Drift scan failed", String(error.message || error), 12000);
      } finally {
        this.driftLoading = false;
      }
    },
    driftItems() {
      if (!this.drift) return [];
      return [
        ...this.drift.rows_without_container.map((r) => ({ ...r, kind: "row" })),
        ...this.drift.containers_without_row.map((r) => ({ ...r, kind: "container" })),
        ...this.drift.stuck_deleting.map((r) => ({ ...r, kind: "stuck" })),
      ];
    },
    toggleDrift(vmid) {
      this.driftSelection = this.driftSelection.includes(vmid)
        ? this.driftSelection.filter((v) => v !== vmid)
        : [...this.driftSelection, vmid];
    },
    async applyDrift() {
      const n = this.driftSelection.length;
      const ok = await this.ask({
        kind: "danger",
        title: `Repair ${n} drift item${n === 1 ? "" : "s"}?`,
        body: "Rows whose container no longer exists are deleted from the database, and "
          + "orphan containers are destroyed on the node. This is confirmed against the "
          + "exact set from the last scan; anything that changed since is rejected.",
        label: "Apply repair",
      });
      if (!ok) return;
      const body = await this.adminAction("Drift repair", "/api/v1/admin/drift/apply", {
        body: JSON.stringify({ confirm_vmids: this.driftSelection }),
      });
      if (body) await this.loadDrift();
    },

    async loadCache() {
      this.cacheLoading = true;
      try {
        this.cache = await apiRequest("/api/v1/cache");
      } catch (error) {
        this.toast("bad", "Cache inventory failed", String(error.message || error), 12000);
      } finally {
        this.cacheLoading = false;
      }
    },
    async purgeCache(repository) {
      const ok = await this.ask({
        kind: "danger",
        title: `Purge the cache for ${repository}?`,
        body: "The next build for this repository starts from an empty cache.",
        label: "Purge",
      });
      if (!ok) return;
      await this.adminAction("Purge cache", "/api/v1/admin/cache/purge", {
        body: JSON.stringify({ repository }),
      });
      await this.loadCache();
    },

    /* --- palette --- */
    get paletteItems() {
      const nav = ROUTES.map((r) => ({
        label: `Go to ${r[0].toUpperCase()}${r.slice(1)}`,
        hint: "page",
        run: () => this.go(r),
      }));
      const actions = [
        { label: "Reconcile now", hint: "action",
          run: () => this.adminAction("Reconcile", "/api/v1/admin/reconcile") },
        { label: "Drain — stop provisioning", hint: "action",
          run: () => this.adminAction("Drain", "/api/v1/admin/drain") },
        { label: "Resume provisioning", hint: "action",
          run: () => this.adminAction("Resume", "/api/v1/admin/resume") },
        { label: "Recycle ready runners", hint: "action",
          run: () => this.adminAction("Recycle ready", "/api/v1/admin/recycle-ready") },
        { label: "Scan for drift", hint: "action", run: () => { this.go("drift"); this.loadDrift(); } },
        { label: "Toggle theme", hint: "action", run: () => this.cycleTheme() },
        { label: "Clear admin token", hint: "action", run: () => this.signOut() },
      ];
      const items = [...actions, ...nav];
      const q = this.palette.query.toLowerCase().trim();
      return q ? items.filter((i) => i.label.toLowerCase().includes(q)) : items;
    },
    openPalette() {
      this.palette = { open: true, query: "", index: 0 };
      this.$nextTick(() => document.getElementById("palette-input")?.focus());
    },
    runPaletteItem(i) {
      const item = this.paletteItems[i];
      this.palette.open = false;
      if (item) item.run();
    },
    onKeydown(e) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        this.openPalette();
        return;
      }
      if (e.key === "Escape") {
        if (this.palette.open) { this.palette.open = false; return; }
        if (this.confirmBox) { this.resolveConfirm(false); return; }
        if (this.drawer) { this.drawer = null; return; }
      }
      if (this.palette.open) {
        const n = this.paletteItems.length;
        if (!n) return;
        if (e.key === "ArrowDown") { e.preventDefault(); this.palette.index = (this.palette.index + 1) % n; }
        if (e.key === "ArrowUp") { e.preventDefault(); this.palette.index = (this.palette.index - 1 + n) % n; }
        if (e.key === "Enter") { e.preventDefault(); this.runPaletteItem(this.palette.index); }
      }
    },

    /* --- template helpers --- */
    fmt,
    statusMeta,
    sparkMarkup(values, down) {
      const { runs, gaps } = sparkPaths(values || []);
      const bad = down ? " is-bad" : "";
      return (
        gaps.map((g) => `<rect class="spark__gap" x="${g.x.toFixed(1)}" y="0" width="${g.w.toFixed(1)}" height="32"/>`).join("") +
        runs.map((r) => `<path class="spark__area${bad}" d="${r.area}"/><path class="spark__line${bad}" d="${r.line}"/>`).join("")
      );
    },
    relAge(epoch) {
      void this.tick;
      if (!epoch) return "—";
      return fmt.ageShort(Math.floor(Date.now() / 1000) - epoch);
    },
  };
}

document.addEventListener("alpine:init", () => {
  window.Alpine.data("dashboard", dashboard);
});

/* ==========================================================================
   Policies store — ported from the previous dashboard unchanged.

   This was already the strongest, most carefully validated part of the old UI
   and the one tab that had been converted to Alpine. It is re-skinned by the
   new tokens rather than rewritten; the only external contract it needs is
   `apiRequest`, `showToast`, `confirmAction`, and `state.actionInFlight`,
   all provided above.
   ========================================================================== */

  let nextDraftId = 1;

  function cloneInstallation(key, inst) {
    inst = inst || {};
    return {
      _id: nextDraftId++,
      key,
      account: inst.account || "",
      installation_id: Number(inst.installation_id || 0),
      scope: inst.scope || "repository",
      runner_group_id: Number(inst.runner_group_id ?? 1),
    };
  }

  function clonePolicy(repo, policy) {
    policy = policy || {};
    return {
      _id: nextDraftId++,
      repository: repo,
      github_installation: policy.github_installation || "",
      default_mode: policy.default_mode || "ephemeral",
      warm_capacity: Number(policy.warm_capacity ?? 0),
      max_concurrent: Number(policy.max_concurrent ?? 0),
      warm_idle_capacity: Number(policy.warm_idle_capacity ?? -1),
      warm_idle_after_minutes: Math.round(Number(policy.warm_idle_after_seconds ?? 0) / 60),
      profile: policy.profile || "crossbuild-small",
      allowed_modes: [...(policy.allowed_modes || [])],
      allowed_events: [...(policy.allowed_events || [])],
      required_labels: [...(policy.required_labels || [])],
    };
  }

  document.addEventListener("alpine:init", () => {
    Alpine.store("policies", {
      capacity: { max_active: 2, pool_size: 1 },
      installations: [],
      repositories: [],
      meta: {
        mode_options: ["ephemeral", "pool", "persistent"],
        event_options: ["push", "workflow_dispatch", "schedule", "pull_request", "release"],
        label_options: ["self-hosted", "proxmox-lxc", "crossbuild"],
        installations: {},
        profiles: {},
        broker_note: "",
        warm_target: 0,
      },
      hydrated: false,
      dirty: false,
      saving: false,
      statusKind: "muted",
      statusText: "",

      get installationNames() {
        return this.installations.map((inst) => inst.key).filter(Boolean);
      },
      get profileNames() {
        return Object.keys(this.meta.profiles || {});
      },
      get warmTarget() {
        return this.repositories.reduce((sum, policy) => sum + Number(policy.warm_capacity || 0), 0);
      },
      get warmBudgetOver() {
        return this.warmTarget + Number(this.capacity.pool_size || 0) > Number(this.capacity.max_active || 0);
      },
      get warmBudgetHint() {
        const budget = this.warmTarget + Number(this.capacity.pool_size || 0);
        const max = Number(this.capacity.max_active || 0);
        return this.warmBudgetOver
          ? `⚠ warm target ${this.warmTarget} + pool_size ${this.capacity.pool_size} > max_active ${max}`
          : `warm target ${this.warmTarget} + pool_size ${this.capacity.pool_size} / max_active ${max}`;
      },

      installationLabel(name) {
        const inst = this.installations.find((i) => i.key === name);
        return inst ? `${name} · ${inst.account || "?"}` : name;
      },
      profileLabel(name) {
        const spec = this.meta.profiles[name] || {};
        return `${name} · ${spec.cores ?? "?"}c/${spec.memory_mb ?? "?"}MB`;
      },

      setStatus(kind, text) {
        this.statusKind = kind || "muted";
        this.statusText = text || "";
      },

      markDirty() {
        this.dirty = true;
        this.setStatus("warn", "Unsaved changes");
      },

      addInstallation() {
        let name = "personal";
        let i = 1;
        const existing = new Set(this.installations.map((inst) => inst.key));
        while (existing.has(name)) name = `personal-${i++}`;
        this.installations.push(cloneInstallation(name, {}));
        this.markDirty();
      },
      removeInstallation(index) {
        this.installations.splice(index, 1);
        this.markDirty();
      },
      addRepository() {
        const firstAccount = this.installations[0]?.account || "Owner";
        let name = `${firstAccount}/NewRepo`;
        let i = 1;
        const existing = new Set(this.repositories.map((p) => p.repository));
        while (existing.has(name)) name = `${firstAccount}/NewRepo-${i++}`;
        this.repositories.push(
          clonePolicy(name, {
            github_installation: this.installations[0]?.key || "",
            default_mode: "ephemeral",
            profile: this.profileNames[0] || "crossbuild-small",
            allowed_modes: ["ephemeral", "pool", "persistent"],
            allowed_events: ["push", "workflow_dispatch", "schedule"],
            required_labels: ["self-hosted", "proxmox-lxc", "crossbuild"],
          })
        );
        this.markDirty();
      },
      removeRepository(index) {
        this.repositories.splice(index, 1);
        this.markDirty();
      },

      applyMeta(policies) {
        if (!policies || typeof policies !== "object") return;
        this.meta.installations = policies.installations || this.meta.installations || {};
        this.meta.profiles = policies.profiles || this.meta.profiles || {};
        this.meta.mode_options = policies.mode_options || this.meta.mode_options;
        this.meta.event_options = policies.event_options || this.meta.event_options;
        this.meta.label_options = policies.label_options || this.meta.label_options;
        this.meta.broker_note = policies.broker_note || this.meta.broker_note || "";
        if (policies.warm_target != null) this.meta.warm_target = Number(policies.warm_target) || 0;
        // The old UI mirrored repository names into a <datalist> for a free-text
        // filter input. The filter is a <select> built from live data now, so
        // there is nothing to populate here.
      },

      hydrate(policies, { force = false } = {}) {
        this.applyMeta(policies || {});
        if (this.hydrated && !force) return;
        this.installations = Object.entries((policies && policies.installations) || {}).map(
          ([key, inst]) => cloneInstallation(key, inst)
        );
        this.repositories = Object.entries((policies && policies.repositories) || {}).map(
          ([repo, policy]) => clonePolicy(repo, policy)
        );
        const cap = (policies && policies.capacity) || {};
        this.capacity = {
          max_active: Number(cap.max_active ?? this.capacity.max_active ?? 2),
          pool_size: Number(cap.pool_size ?? this.capacity.pool_size ?? 1),
        };
        this.hydrated = true;
        this.dirty = false;
        this.setStatus("muted", "");
      },

      toPayload() {
        const installations = {};
        for (const inst of this.installations) {
          const key = (inst.key || "").trim();
          if (!key) continue;
          installations[key] = {
            account: (inst.account || "").trim(),
            installation_id: Number(inst.installation_id || 0),
            scope: inst.scope || "repository",
            runner_group_id: Number(inst.runner_group_id || 1),
          };
        }
        const repositories = {};
        for (const policy of this.repositories) {
          const repo = (policy.repository || "").trim();
          if (!repo) continue;
          repositories[repo] = {
            github_installation: policy.github_installation || "",
            default_mode: policy.default_mode || "ephemeral",
            warm_capacity: Number(policy.warm_capacity || 0),
            max_concurrent: Number(policy.max_concurrent || 0),
            warm_idle_capacity: Number(policy.warm_idle_capacity ?? -1),
            warm_idle_after_seconds: Math.round(Number(policy.warm_idle_after_minutes || 0) * 60),
            profile: policy.profile || "crossbuild-small",
            allowed_modes: [...policy.allowed_modes],
            allowed_events: [...policy.allowed_events],
            required_labels: [...policy.required_labels],
          };
        }
        return {
          capacity: {
            max_active: Number(this.capacity.max_active),
            pool_size: Number(this.capacity.pool_size),
          },
          installations,
          repositories,
        };
      },

      async load() {
        this.setStatus("warn", "Loading policies…");
        try {
          const body = await apiRequest("/api/v1/policies");
          this.hydrate(body, { force: true });
          this.setStatus("ok", "Policies loaded");
          showToast(
            "ok",
            "Policies loaded",
            `${Object.keys(body.installations || {}).length} installation(s), ${Object.keys(body.repositories || {}).length} repo(s)`,
            4000
          );
        } catch (error) {
          this.setStatus("bad", "Load failed");
          showToast("bad", "Failed to load policies", String(error.message || error), 12000);
          // Still show empty editors so the user can create from scratch.
          if (!this.hydrated) this.hydrate({ installations: {}, repositories: {} }, { force: true });
        }
      },

      async reset() {
        const ok = await confirmAction({
          kind: "warn",
          kicker: "Policies",
          title: "Reload from server?",
          body: "Discards the local draft and fetches the current installations/repositories from the API.",
          confirmLabel: "Reload",
          cancelLabel: "Keep editing",
        });
        if (!ok) {
          showToast("info", "Reset cancelled", "", 3000);
          return;
        }
        this.dirty = false;
        await this.load();
      },

      async save() {
        if (state.actionInFlight) {
          showToast("warn", "Please wait", "Another action is already in progress.", 4000);
          return;
        }
        const { capacity, installations, repositories } = this.toPayload();
        if (!Object.keys(installations).length) {
          showToast(
            "bad",
            "Invalid policies",
            "Keep at least one installation (Key + Account + ID).",
            10000
          );
          return;
        }
        if (!Object.keys(repositories).length) {
          showToast("bad", "Invalid policies", "Keep at least one Owner/Name repository.", 10000);
          return;
        }
        if (!Number.isFinite(capacity.max_active) || capacity.max_active < 1) {
          showToast("bad", "Invalid capacity", "max_active must be an integer >= 1.", 10000);
          return;
        }
        if (!Number.isFinite(capacity.pool_size) || capacity.pool_size < 0) {
          showToast("bad", "Invalid capacity", "pool_size must be an integer >= 0.", 10000);
          return;
        }
        const warmTarget = Object.values(repositories).reduce(
          (sum, r) => sum + Number(r.warm_capacity || 0),
          0
        );
        const budget = warmTarget + Number(capacity.pool_size || 0);
        if (budget > capacity.max_active) {
          showToast(
            "bad",
            "Insufficient capacity",
            `max_active (${capacity.max_active}) must be >= sum of warm_capacity (${warmTarget}) + pool_size (${capacity.pool_size}).`,
            12000
          );
          return;
        }
        const instNames = Object.keys(installations);
        const names = Object.keys(repositories);
        const warmLines = names
          .map((n) => {
            const repo = repositories[n];
            const bits = [
              `warm=${repo.warm_capacity ?? 0}`,
              `profile=${repo.profile || "crossbuild-small"}`,
            ];
            if (Number(repo.max_concurrent || 0) > 0) bits.push(`max_concurrent=${repo.max_concurrent}`);
            if (Number(repo.warm_idle_capacity ?? -1) >= 0) {
              bits.push(
                `idle→${repo.warm_idle_capacity} after ${Math.round((repo.warm_idle_after_seconds || 0) / 60)}min`
              );
            }
            return `· ${n} ${bits.join(" ")}`;
          })
          .join("\n");
        const ok = await confirmAction({
          kind: "ok",
          kicker: "Policies",
          title: "Save and apply policies?",
          body: `Capacity: max_active=${capacity.max_active}, pool_size=${capacity.pool_size} (warm target=${warmTarget})\n\n${instNames.length} installation(s):\n${instNames
            .map((n) => `· ${n} → ${installations[n].account} (id ${installations[n].installation_id})`)
            .join("\n")}\n\n${names.length} repository(ies):\n${warmLines}\n\nWrites the override file and syncs the broker.`,
          confirmLabel: "Save and apply",
          cancelLabel: "Cancel",
        });
        if (!ok) {
          showToast("info", "Save cancelled", "Nothing was written.", 4000);
          this.setStatus("warn", "Save cancelled");
          return;
        }
        state.actionInFlight = true;
        this.saving = true;
        showToast("warn", "Saving policies…", "Sending to the orchestrator", 0);
        this.setStatus("warn", "Saving…");
        try {
          const body = await apiRequest("/api/v1/admin/policies", {
            method: "PUT",
            body: JSON.stringify({ capacity, repositories, installations }),
          });
          this.hydrate(body.policies || { capacity, repositories, installations }, { force: true });
          const cacheNote =
            Array.isArray(body.cache_ops) && body.cache_ops.length
              ? `\ncache: ${body.cache_ops.join("; ")}`
              : "";
          showToast(
            "ok",
            "Policies saved",
            `${body.message || "OK"}\n${body.broker_note || ""}${cacheNote}`,
            15000
          );
          this.setStatus("ok", "Saved");
        } catch (error) {
          showToast("bad", "Failed to save policies", String(error.message || error), 15000);
          this.setStatus("bad", "Save error");
        } finally {
          state.actionInFlight = false;
          this.saving = false;
        }
      },
    });

    /* Deliberately NOT loaded here. The old UI fetched policies on every page
       load and toasted about it; it is only needed when the tab is opened. */
  });

  /* Dirty tracking lives outside Alpine so an SSE snapshot never clobbers a
     half-edited draft (see the `!store.dirty` guard where policies are applied). */
  document.addEventListener("input", (event) => {
    if (event.target.closest?.("[data-policies]")) {
      window.Alpine?.store("policies")?.markDirty();
    }
  });
  document.addEventListener("change", (event) => {
    if (event.target.closest?.("[data-policies]")) {
      window.Alpine?.store("policies")?.markDirty();
    }
  });

})();
