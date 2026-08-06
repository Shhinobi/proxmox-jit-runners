(() => {
  const state = {
    source: null,
    filters: { repository: "", status: "", mode: "" },
    logFilters: { level: "", category: "" },
    lastSnapshot: null,
    toastTimer: null,
    actionInFlight: false,
    confirmResolver: null,
    confirmReturnFocus: null,
    openLogIds: new Set(),
  };

  const $ = (id) => document.getElementById(id);

  function closeConfirm(result) {
    // Always resolve the pending promise — even if the modal is already hidden.
    const modal = $("confirm-modal");
    if (modal) {
      modal.classList.add("hidden");
      modal.classList.remove("kind-warn", "kind-bad", "kind-ok", "kind-info");
    }
    document.body.classList.remove("modal-open");
    document.removeEventListener("keydown", onConfirmKeydown);
    const resolve = state.confirmResolver;
    state.confirmResolver = null;
    const returnFocus = state.confirmReturnFocus;
    state.confirmReturnFocus = null;
    if (returnFocus && typeof returnFocus.focus === "function") returnFocus.focus();
    if (resolve) resolve(Boolean(result));
  }

  function confirmFocusables() {
    return [$("confirm-cancel"), $("confirm-ok")].filter(Boolean);
  }

  function onConfirmKeydown(event) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeConfirm(false);
      return;
    }
    if (event.key === "Tab") {
      // Minimal focus trap: cycle only between the two modal buttons.
      const focusables = confirmFocusables();
      if (!focusables.length) return;
      event.preventDefault();
      const currentIndex = focusables.indexOf(document.activeElement);
      const lastIndex = focusables.length - 1;
      const nextIndex = event.shiftKey
        ? currentIndex <= 0
          ? lastIndex
          : currentIndex - 1
        : currentIndex === -1 || currentIndex === lastIndex
          ? 0
          : currentIndex + 1;
      focusables[nextIndex].focus();
      return;
    }
    if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.metaKey) {
      const active = document.activeElement;
      if (active && (active.id === "confirm-cancel" || active.tagName === "TEXTAREA")) return;
      // Ignore Enter that still targets the control which opened the dialog.
      if (active && active.id && active.id.startsWith("btn-")) return;
      event.preventDefault();
      closeConfirm(true);
    }
  }

  /** In-dashboard confirmation dialog. @returns {Promise<boolean>} */
  function confirmAction({
    title = "Confirm action",
    body = "",
    confirmLabel = "Confirm",
    cancelLabel = "Cancel",
    kind = "warn",
    kicker = "Confirm",
  } = {}) {
    return new Promise((resolve) => {
      if (state.confirmResolver) {
        const prev = state.confirmResolver;
        state.confirmResolver = null;
        prev(false);
      }
      const modal = $("confirm-modal");
      if (!modal || !$("confirm-ok") || !$("confirm-cancel")) {
        // Fallback if modal markup is missing (stale HTML cache).
        const ok = window.confirm(`${title}\n\n${body}`);
        resolve(ok);
        return;
      }
      modal.classList.remove("hidden", "kind-warn", "kind-bad", "kind-ok", "kind-info");
      modal.classList.add(`kind-${kind || "warn"}`);
      $("confirm-kicker").textContent = kicker;
      $("confirm-title").textContent = title;
      $("confirm-body").textContent = body;
      $("confirm-ok").textContent = confirmLabel;
      $("confirm-cancel").textContent = cancelLabel;
      state.confirmResolver = resolve;
      state.confirmReturnFocus = document.activeElement;
      document.body.classList.add("modal-open");
      document.addEventListener("keydown", onConfirmKeydown);
      // Delay focus so the activating keyup/click cannot instantly confirm.
      setTimeout(() => {
        const okBtn = $("confirm-ok");
        if (okBtn && state.confirmResolver) okBtn.focus();
      }, 50);
    });
  }

  function setupConfirmModal() {
    const modal = $("confirm-modal");
    if (!modal) return;
    modal.querySelectorAll("[data-confirm-dismiss]").forEach((el) => {
      el.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeConfirm(false);
      });
    });
    const ok = $("confirm-ok");
    if (ok) {
      ok.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeConfirm(true);
      });
    }
  }

  function setPolicyStatus(kind, text) {
    const el = $("policy-status");
    if (!el) return;
    el.className = `policy-status ${kind || "muted"}`;
    el.textContent = text || "";
  }

  function showToast(kind, title, body, holdMs = 8000) {
    const toast = $("toast");
    if (!toast) {
      // Last-resort feedback if toast node is missing.
      console[kind === "bad" ? "error" : "log"](title, body || "");
      setPolicyStatus(kind === "bad" ? "bad" : kind === "warn" ? "warn" : "ok", `${title}${body ? `: ${body}` : ""}`);
      return;
    }
    toast.className = `toast ${kind || "ok"}`;
    toast.innerHTML = `
      <div>
        <div class="toast-title">${escapeHtml(title)}</div>
        <div class="toast-body">${escapeHtml(body || "")}</div>
      </div>
      <button type="button" class="toast-close" aria-label="Dismiss">×</button>
    `;
    toast.querySelector(".toast-close").addEventListener("click", hideToast);
    if (state.toastTimer) clearTimeout(state.toastTimer);
    if (holdMs > 0) {
      state.toastTimer = setTimeout(hideToast, holdMs);
    }
    setPolicyStatus(
      kind === "bad" ? "bad" : kind === "warn" ? "warn" : kind === "ok" ? "ok" : "muted",
      title
    );
  }

  function hideToast() {
    const toast = $("toast");
    toast.className = "toast hidden";
    toast.innerHTML = "";
    if (state.toastTimer) {
      clearTimeout(state.toastTimer);
      state.toastTimer = null;
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function fmtTs(epoch) {
    if (epoch == null || epoch === "") return "—";
    const n = Number(epoch);
    if (!Number.isFinite(n) || n <= 0) return "—";
    return new Date(n * 1000).toLocaleString();
  }

  function fmtAge(seconds) {
    const n = Number(seconds) || 0;
    if (n < 60) return `${n}s`;
    if (n < 3600) return `${Math.floor(n / 60)}m ${n % 60}s`;
    return `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}m`;
  }

  function statusClass(value) {
    return `status ${String(value || "").toLowerCase()}`;
  }

  function badge(text, kind) {
    return `<span class="badge ${kind || ""}">${escapeHtml(text)}</span>`;
  }

  function card(label, value) {
    return `<div class="card"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`;
  }

  function summaryCardsHtml(caps) {
    return [
      card("Queue", caps.queued ?? 0),
      card("Active", `${caps.active ?? 0}/${caps.max_active ?? "?"}`),
      card(
        "Warm ready",
        caps.warm_target != null
          ? `${caps.warm_ready ?? 0}/${caps.warm_target}`
          : (caps.warm_ready ?? 0)
      ),
      card("Pool ready", `${caps.pool_ready ?? 0}/${caps.pool_size ?? "?"}`),
      card("Delete failed", caps.delete_failed ?? 0),
      card("Oldest queued", fmtAge(caps.oldest_queued_job_age_seconds)),
    ].join("");
  }

  function renderTable(el, headers, rowsHtml) {
    if (!rowsHtml) {
      el.innerHTML = `<tbody><tr><td class="empty" colspan="${headers.length}">No records</td></tr></tbody>`;
      return;
    }
    el.innerHTML = `
      <thead><tr>${headers.map((h) => `<th scope="col">${escapeHtml(h)}</th>`).join("")}</tr></thead>
      <tbody>${rowsHtml}</tbody>
    `;
  }

  function queryString() {
    const params = new URLSearchParams();
    if (state.filters.repository) params.set("repository", state.filters.repository);
    if (state.filters.status) params.set("status", state.filters.status);
    if (state.filters.mode) params.set("mode", state.filters.mode);
    params.set("limit", "50");
    params.set("interval", "3");
    return params.toString();
  }

  function link(url, label) {
    if (!url) return escapeHtml(label || "—");
    return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label || "open")}</a>`;
  }

  function formatApiError(body, status) {
    const detail = body?.detail ?? body?.message ?? body?.error;
    if (detail == null || detail === "") return `HTTP ${status}`;
    if (typeof detail === "string") return detail;
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
    }
  }

  /** Shared fetch + JSON parse + error-normalization for every API call. */
  async function apiRequest(path, options = {}) {
    const { body: requestBody, headers, ...rest } = options;
    const response = await fetch(path, {
      ...rest,
      headers: {
        Accept: "application/json",
        ...(requestBody != null ? { "Content-Type": "application/json" } : {}),
        ...(headers || {}),
      },
      body: requestBody,
    });
    const text = await response.text();
    let parsed = {};
    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      parsed = { detail: text };
    }
    if (!response.ok) throw new Error(formatApiError(parsed, response.status));
    return parsed;
  }

  function operationalBadge(overview) {
    // Prefer explicit API status; fall back for older orchestrator builds.
    const status =
      overview.status ||
      (!overview.ready
        ? "not_ready"
        : overview.drain
          ? "draining"
          : overview.shadow_mode
            ? "shadow"
            : "ready");
    switch (status) {
      case "not_ready":
        return badge("not ready", "bad");
      case "draining":
        return badge("draining", "warn");
      case "shadow":
        return badge("shadow", "warn");
      default:
        return badge("ready", "ok");
    }
  }

  function renderBadges(overview) {
    const items = [operationalBadge(overview)];
    // Secondary context — avoid duplicating the primary status label.
    if (overview.drain && overview.status !== "draining" && overview.status) {
      items.push(badge("drain", "warn"));
    } else if (!overview.drain) {
      items.push(badge("accepting", "ok"));
    } else {
      items.push(badge("jobs paused", "warn"));
    }
    if (overview.shadow_mode && overview.status !== "shadow") {
      items.push(badge("shadow", "warn"));
    } else if (!overview.shadow_mode) {
      items.push(badge("live", "ok"));
    }
    items.push(badge(`node ${overview.proxmox?.node || "?"}`, "info"));
    // Health probe still green while draining — surface that explicitly.
    if (overview.drain && overview.ready) {
      items.push(badge("health ok", "info"));
    } else if (!overview.ready) {
      items.push(badge("health fail", "bad"));
    }
    $("badges").innerHTML = items.join("");
  }

  function renderOverview(overview) {
    const caps = overview.capacity || {};
    renderBadges(overview);
    $("cards").innerHTML = summaryCardsHtml(caps);
    $("btn-drain").disabled = !!overview.drain;
    $("btn-resume").disabled = !overview.drain;
  }

  function renderAlerts(alerts) {
    const items = alerts || [];
    if (!items.length) {
      $("alerts").innerHTML = "";
      return;
    }
    $("alerts").innerHTML = items
      .map(
        (a) =>
          `<div class="alert ${escapeHtml(a.severity || "info")}"><span class="code">${escapeHtml(a.code)}</span>${escapeHtml(a.message)}</div>`
      )
      .join("");
  }

  function renderInstances(items) {
    const rows = (items || [])
      .map((item) => {
        const bad = ["delete_failed", "failed"].includes(String(item.status));
        const err = item.error ? String(item.error) : "";
        return `<tr class="${bad ? "row-bad" : ""}" title="${escapeHtml(err)}">
        <td class="mono">${escapeHtml(item.vmid)}</td>
        <td class="mono">${escapeHtml(item.ip_address)}</td>
        <td>${escapeHtml(item.mode)}</td>
        <td><span class="${statusClass(item.status)}">${escapeHtml(item.status)}</span></td>
        <td class="mono">${escapeHtml(item.repository || "—")}</td>
        <td class="mono">${escapeHtml(item.runner_name || "—")}</td>
        <td class="mono">${link(item.github_job_url || item.github_run_url, item.job_id)}</td>
        <td class="mono" title="${escapeHtml(err)}">${escapeHtml(err ? err.slice(0, 80) : "—")}</td>
        <td class="muted">${escapeHtml(fmtTs(item.updated_at))}</td>
      </tr>`;
      })
      .join("");
    renderTable(
      $("table-instances"),
      ["VMID", "IP", "Mode", "Status", "Repository", "Runner", "Job", "Error", "Updated"],
      rows
    );
  }

  function formatContext(ctx) {
    if (!ctx || typeof ctx !== "object") return "";
    return Object.entries(ctx)
      .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`)
      .join(" · ");
  }

  function renderLogs(items) {
    let rows = items || [];
    if (state.logFilters.level === "error") {
      rows = rows.filter((e) => e.level === "error");
    } else if (state.logFilters.level === "warn") {
      rows = rows.filter((e) => e.level === "warn" || e.level === "error");
    } else if (state.logFilters.level === "info") {
      rows = rows.filter((e) => e.level === "info");
    }
    if (state.logFilters.category) {
      rows = rows.filter(
        (e) => String(e.category || "").toLowerCase() === state.logFilters.category
      );
    }
    const list = $("logs-list");
    const empty = $("logs-empty");
    $("logs-meta").textContent = rows.length
      ? `${rows.length} event(s) · ring buffer (process memory)`
      : "no matching events";
    if (!rows.length) {
      list.innerHTML = "";
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";
    list.innerHTML = rows
      .map((event) => {
        const level = String(event.level || "info");
        const open = state.openLogIds.has(event.id) ? "open" : "";
        const hasDetail = Boolean(event.detail);
        const ctx = formatContext(event.context);
        const rowClass = [
          "log-row",
          level === "error" ? "error-row" : "",
          level === "warn" ? "warn-row" : "",
          hasDetail ? "has-detail" : "",
          open,
        ]
          .filter(Boolean)
          .join(" ");
        return `<div class="${rowClass}" data-log-id="${escapeHtml(event.id)}">
          <div class="ts">${escapeHtml(fmtTs(event.ts))}</div>
          <div class="lvl ${escapeHtml(level)}">${escapeHtml(level)}</div>
          <div class="cat">${escapeHtml(event.category || "—")}</div>
          <div class="msg">${escapeHtml(event.message || "")}${
            ctx ? `<div class="ctx">${escapeHtml(ctx)}</div>` : ""
          }</div>
          ${
            hasDetail
              ? `<pre class="detail">${escapeHtml(event.detail)}</pre>`
              : ""
          }
        </div>`;
      })
      .join("");
    list.querySelectorAll(".log-row.has-detail").forEach((row) => {
      row.addEventListener("click", () => {
        const id = Number(row.getAttribute("data-log-id"));
        if (state.openLogIds.has(id)) state.openLogIds.delete(id);
        else state.openLogIds.add(id);
        row.classList.toggle("open");
      });
    });
    if ($("log-auto-scroll")?.checked) {
      // Newest is at top; keep viewport at top when following.
      $("logs-wrap").scrollTop = 0;
    }
  }

  function renderJobs(items) {
    const rows = (items || [])
      .map((item) => {
        const bad = ["failed", "rejected"].includes(String(item.status)) || item.error;
        const labels = Array.isArray(item.labels) ? item.labels.join(", ") : "";
        return `<tr class="${bad ? "row-bad" : ""}">
        <td class="mono">${link(item.github_job_url, item.job_id)}</td>
        <td class="mono">${link(item.github_run_url, item.repository)}</td>
        <td>${escapeHtml(item.mode || "—")}</td>
        <td><span class="${statusClass(item.status)}">${escapeHtml(item.status)}</span></td>
        <td class="mono">${escapeHtml(item.runner_name || "—")}</td>
        <td>${escapeHtml(item.attempts ?? 0)}</td>
        <td>${escapeHtml(item.duration_seconds != null ? fmtAge(item.duration_seconds) : "—")}</td>
        <td class="mono" title="${escapeHtml(labels)}">${escapeHtml(labels || "—")}</td>
        <td class="mono">${escapeHtml(item.error || "—")}</td>
        <td class="muted">${escapeHtml(fmtTs(item.created_at))}</td>
      </tr>`;
      })
      .join("");
    renderTable(
      $("table-jobs"),
      ["Job", "Repository", "Mode", "Status", "Runner", "Attempts", "Duration", "Labels", "Error", "Created"],
      rows
    );
  }

  function renderDeliveries(items) {
    const rows = (items || [])
      .map((item) => {
        const accepted = Number(item.accepted) === 1 || item.accepted === true;
        const bad = !accepted && String(item.reason || "").includes("not_allowed");
        return `<tr class="${bad ? "row-bad" : ""}">
        <td class="muted">${escapeHtml(fmtTs(item.received_at))}</td>
        <td>${escapeHtml(item.action || "—")}</td>
        <td><span class="status ${accepted ? "accepted" : "rejected"}">${accepted ? "accepted" : "rejected"}</span></td>
        <td class="mono">${escapeHtml(item.reason)}</td>
        <td class="mono">${escapeHtml(item.delivery_id)}</td>
      </tr>`;
      })
      .join("");
    renderTable(
      $("table-deliveries"),
      ["Received", "Action", "Result", "Reason", "Delivery"],
      rows
    );
  }

  // --- Policies (Alpine.js store; see #panel-policies in index.html) -------

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
        const list = $("repo-list");
        if (list) {
          list.innerHTML = Object.keys(policies.repositories || {})
            .map((name) => `<option value="${escapeHtml(name)}"></option>`)
            .join("");
        }
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

    Alpine.store("policies").load();
  });

  function setupPolicyDirtyTracking() {
    const panel = $("panel-policies");
    if (!panel) return;
    const markDirty = (event) => {
      if (event.target && event.target.matches("input, select, textarea")) {
        window.Alpine?.store("policies")?.markDirty();
      }
    };
    panel.addEventListener("input", markDirty);
    panel.addEventListener("change", markDirty);
  }

  // --- Metrics / chart -------------------------------------------------------

  function renderMetrics(metrics) {
    const prov = metrics?.provision_seconds_avg || {};
    const wait = metrics?.queue_wait_seconds_avg || {};
    const parts = [];
    Object.entries(prov).forEach(([mode, avg]) => {
      if (avg != null) parts.push(`provision ${mode}=${avg.toFixed(1)}s`);
    });
    Object.entries(wait).forEach(([mode, avg]) => {
      if (avg != null) parts.push(`queue-wait ${mode}=${avg.toFixed(1)}s`);
    });
    $("metrics-line").textContent = parts.length ? parts.join(" · ") : "no provision samples yet";
  }

  function renderDurationChart(points) {
    const canvas = $("duration-chart");
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    const cssWidth = canvas.clientWidth || 960;
    const cssHeight = 160;
    canvas.width = Math.floor(cssWidth * dpr);
    canvas.height = Math.floor(cssHeight * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);

    const data = points || [];
    if (!data.length) {
      ctx.fillStyle = "#93a4c3";
      ctx.font = "13px sans-serif";
      ctx.fillText("No completed jobs with duration yet", 12, 28);
      return;
    }

    const values = data.map((p) => Number(p.duration_seconds) || 0);
    const max = Math.max(...values, 1);
    const pad = { top: 12, right: 12, bottom: 22, left: 36 };
    const w = cssWidth - pad.left - pad.right;
    const h = cssHeight - pad.top - pad.bottom;
    const barW = Math.max(2, w / values.length - 2);

    ctx.strokeStyle = "#243049";
    ctx.beginPath();
    ctx.moveTo(pad.left, pad.top);
    ctx.lineTo(pad.left, pad.top + h);
    ctx.lineTo(pad.left + w, pad.top + h);
    ctx.stroke();

    ctx.fillStyle = "#93a4c3";
    ctx.font = "11px monospace";
    ctx.fillText(`${max}s`, 4, pad.top + 8);
    ctx.fillText("0", 12, pad.top + h);

    values.forEach((value, index) => {
      const x = pad.left + index * (w / values.length) + 1;
      const bh = (value / max) * h;
      const y = pad.top + h - bh;
      ctx.fillStyle = "#5b9dff";
      ctx.fillRect(x, y, barW, bh);
    });
  }

  function applySnapshot(snapshot) {
    state.lastSnapshot = snapshot;
    // Avoid clobbering button state while an admin action is running.
    if (!state.actionInFlight) {
      renderOverview(snapshot.overview || {});
    } else {
      renderBadges(snapshot.overview || {});
      $("cards").innerHTML = summaryCardsHtml(snapshot.overview?.capacity || {});
    }
    renderAlerts(snapshot.alerts || []);
    renderInstances(snapshot.instances || []);
    renderJobs(snapshot.jobs || []);
    renderDeliveries(snapshot.deliveries || []);
    renderLogs(snapshot.logs || []);
    // Policies are loaded via GET /api/v1/policies (not SSE). Keep the server
    // snapshot for reference only — never clobber a dirty local draft.
    if (snapshot.policies) {
      const store = window.Alpine?.store("policies");
      if (store && !store.dirty) store.applyMeta(snapshot.policies);
    }
    renderMetrics(snapshot.metrics || {});
    renderDurationChart(snapshot.job_durations || []);
    if (!state.actionInFlight) {
      $("refresh-meta").textContent = `SSE live · ${new Date().toLocaleTimeString()}`;
    }
  }

  function connectSSE() {
    if (state.source) {
      state.source.close();
      state.source = null;
    }
    const url = `/api/v1/events?${queryString()}`;
    const source = new EventSource(url);
    state.source = source;
    source.addEventListener("snapshot", (event) => {
      try {
        applySnapshot(JSON.parse(event.data));
      } catch (error) {
        $("refresh-meta").textContent = `parse error: ${error.message}`;
      }
    });
    source.onerror = () => {
      $("refresh-meta").textContent = "SSE reconnecting…";
    };
  }

  function summarizeReconcile(body) {
    const beforeJobs = Object.fromEntries(
      ((body.before || {}).jobs || []).map((r) => [r.status, r.total])
    );
    const afterJobs = Object.fromEntries(
      ((body.after || {}).jobs || []).map((r) => [r.status, r.total])
    );
    const beforeInst = ((body.before || {}).instances || [])
      .map((r) => `${r.mode}/${r.status}=${r.total}`)
      .join(", ");
    const afterInst = ((body.after || {}).instances || [])
      .map((r) => `${r.mode}/${r.status}=${r.total}`)
      .join(", ");
    return [
      `jobs before: ${JSON.stringify(beforeJobs)}`,
      `jobs after:  ${JSON.stringify(afterJobs)}`,
      `instances before: ${beforeInst || "—"}`,
      `instances after:  ${afterInst || "—"}`,
    ].join("\n");
  }

  async function postAdmin(path, label) {
    const buttons = [
      $("btn-drain"),
      $("btn-resume"),
      $("btn-reconcile"),
      $("btn-recycle-ready"),
    ];
    const actionKey = path.split("/").pop(); // drain|resume|reconcile|recycle-ready
    const active = buttons.find((b) => b.id === `btn-${actionKey}`);
    state.actionInFlight = true;
    buttons.forEach((b) => {
      b.disabled = true;
      b.classList.remove("busy");
    });
    if (active) active.classList.add("busy");
    showToast("warn", `${label}…`, "Waiting for the orchestrator's response.", 0);
    $("refresh-meta").textContent = `${label} in progress…`;
    try {
      const body = await apiRequest(path, { method: "POST" });
      let detail = body.message || JSON.stringify(body);
      if (body.action === "reconcile" || body.action === "recycle-ready") {
        const extra =
          body.action === "recycle-ready" && body.scheduled != null
            ? `scheduled=${body.scheduled}\n`
            : "";
        detail = `${body.message || "Done."}\n${extra}${summarizeReconcile(body)}`;
      } else if (body.action === "drain" || body.action === "resume") {
        detail = `${body.message || ""}\ndrain=${body.drain} · marker=${body.marker || "—"}`;
      }
      showToast("ok", `${label} done`, detail, 12000);
      $("refresh-meta").textContent = `${label} ok · ${new Date().toLocaleTimeString()}`;

      // Pull a fresh snapshot once so badges reflect the new state immediately.
      const snapshot = await apiRequest(`/api/v1/snapshot?${queryString()}`);
      applySnapshot(snapshot);
    } catch (error) {
      showToast("bad", `${label} failed`, String(error.message || error), 15000);
      $("refresh-meta").textContent = `${label} error: ${error.message}`;
    } finally {
      state.actionInFlight = false;
      buttons.forEach((b) => b.classList.remove("busy"));
      if (state.lastSnapshot?.overview) renderOverview(state.lastSnapshot.overview);
      // Keep SSE alive; reconnect only if it died.
      if (!state.source || state.source.readyState === EventSource.CLOSED) {
        connectSSE();
      }
    }
  }

  function fmtBytes(n) {
    const v = Number(n) || 0;
    if (v < 1024) return `${v} B`;
    if (v < 1024 ** 2) return `${(v / 1024).toFixed(1)} KiB`;
    if (v < 1024 ** 3) return `${(v / 1024 ** 2).toFixed(1)} MiB`;
    return `${(v / 1024 ** 3).toFixed(2)} GiB`;
  }

  function setCacheStatus(kind, text) {
    const el = $("cache-status");
    if (!el) return;
    el.className = `policy-status ${kind || "muted"}`;
    el.textContent = text || "";
  }

  function renderCacheInventory(data) {
    const repos = data.repositories || [];
    const summary = $("cache-summary");
    if (summary) {
      summary.innerHTML = `Volume <code>${escapeHtml(data.root || "/srv/runner-cache")}</code> · used ${escapeHtml(fmtBytes(data.used_bytes))} / ${escapeHtml(fmtBytes(data.total_bytes))} (${escapeHtml(data.usage_percent)}%, max ${escapeHtml(data.max_usage_percent)}%) · soft quota ${escapeHtml(data.quota_gb)} GiB/repo · auto-GC orphans &gt; ${escapeHtml(data.gc_orphan_days ?? "?")}d`;
    }
    const rows = repos
      .map((item) => {
        const orphan = item.orphan || !item.allowed;
        return `<tr class="${orphan ? "row-bad" : ""}">
          <td class="mono">${escapeHtml(item.repository)}</td>
          <td>${escapeHtml(fmtBytes(item.size_bytes))}</td>
          <td class="muted">${escapeHtml(fmtTs(item.mtime))}</td>
          <td><span class="status ${orphan ? "failed" : "ready"}">${orphan ? "orphan" : "allowed"}</span></td>
          <td>
            <button type="button" class="btn warn btn-cache-purge" data-repo="${escapeHtml(item.repository)}">Purge</button>
          </td>
        </tr>`;
      })
      .join("");
    renderTable(
      $("table-cache"),
      ["Repository", "Size", "Modified", "Status", "Actions"],
      rows
    );
    document.querySelectorAll(".btn-cache-purge").forEach((button) => {
      button.addEventListener("click", async () => {
        const repo = button.getAttribute("data-repo") || "";
        const ok = await confirmAction({
          kind: "bad",
          kicker: "Cache · destructive",
          title: `Purge cache ${repo}?`,
          body: `Deletes /srv/runner-cache/${repo} on pve.\nRefuses if any JIT CT still has the path mounted.\nCache rebuilds on the next job.`,
          confirmLabel: "Purge",
          cancelLabel: "Cancel",
        });
        if (!ok) return;
        setCacheStatus("warn", `Purging ${repo}…`);
        try {
          const body = await apiRequest("/api/v1/admin/cache/purge", {
            method: "POST",
            body: JSON.stringify({ repository: repo }),
          });
          showToast("ok", "Cache purged", body.message || repo, 10000);
          setCacheStatus("ok", `Purged ${repo}`);
          await loadCacheInventory();
        } catch (error) {
          showToast("bad", "Purge failed", String(error.message || error), 12000);
          setCacheStatus("bad", "Purge error");
        }
      });
    });
  }

  async function loadCacheInventory() {
    setCacheStatus("warn", "Loading…");
    try {
      const body = await apiRequest("/api/v1/cache");
      renderCacheInventory(body);
      const orphans = (body.repositories || []).filter((r) => r.orphan).length;
      setCacheStatus(
        orphans ? "warn" : "ok",
        `${(body.repositories || []).length} repo cache(s)${orphans ? ` · ${orphans} orphan` : ""}`
      );
    } catch (error) {
      showToast("bad", "Failed to list cache", String(error.message || error), 12000);
      setCacheStatus("bad", "Load failed");
    }
  }

  function setupCacheTab() {
    const refresh = $("btn-cache-refresh");
    const gcBtn = $("btn-cache-gc");
    const renameBtn = $("btn-cache-rename");
    if (refresh) refresh.addEventListener("click", () => loadCacheInventory());
    if (gcBtn) {
      gcBtn.addEventListener("click", async () => {
        const ok = await confirmAction({
          kind: "warn",
          kicker: "Cache GC",
          title: "Garbage-collect orphan caches?",
          body: "Deletes directories under /srv/runner-cache that aren't on the broker's allowlist.\nUses config's gc_orphan_days as the min age (default 7; 0 = all).\nNever touches caches for active policies.",
          confirmLabel: "Run GC",
          cancelLabel: "Cancel",
        });
        if (!ok) return;
        setCacheStatus("warn", "GC running…");
        try {
          const body = await apiRequest("/api/v1/admin/cache/gc", {
            method: "POST",
            body: JSON.stringify({}),
          });
          showToast("ok", "Cache GC", body.message || "done", 12000);
          setCacheStatus("ok", body.message || "GC done");
          await loadCacheInventory();
        } catch (error) {
          showToast("bad", "GC failed", String(error.message || error), 12000);
          setCacheStatus("bad", "GC error");
        }
      });
    }
    if (renameBtn) {
      renameBtn.addEventListener("click", async () => {
        const from = ($("cache-rename-from")?.value || "").trim();
        const to = ($("cache-rename-to")?.value || "").trim();
        if (!from || !to) {
          showToast("bad", "Invalid rename", "Fill in both From and To (Owner/Repo).", 8000);
          return;
        }
        const ok = await confirmAction({
          kind: "ok",
          kicker: "Cache rename",
          title: "Rename cache?",
          body: `${from}\n→ ${to}\n\nMoves the directory on pve. Destination must be empty/nearly empty. Source must not be mounted in a CT.`,
          confirmLabel: "Rename",
          cancelLabel: "Cancel",
        });
        if (!ok) return;
        setCacheStatus("warn", "Renaming…");
        try {
          const body = await apiRequest("/api/v1/admin/cache/rename", {
            method: "POST",
            body: JSON.stringify({ from, to }),
          });
          showToast("ok", "Cache renamed", body.message || `${from} → ${to}`, 12000);
          setCacheStatus("ok", body.message || "renamed");
          await loadCacheInventory();
        } catch (error) {
          showToast("bad", "Rename failed", String(error.message || error), 12000);
          setCacheStatus("bad", "Rename error");
        }
      });
    }
  }

  function setupTabs() {
    document.querySelectorAll(".tab").forEach((button) => {
      button.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach((el) => {
          el.classList.remove("active");
          el.setAttribute("aria-selected", "false");
        });
        document.querySelectorAll(".panel").forEach((el) => el.classList.remove("active"));
        button.classList.add("active");
        button.setAttribute("aria-selected", "true");
        $(`panel-${button.dataset.tab}`).classList.add("active");
        if (button.dataset.tab === "cache") {
          loadCacheInventory();
        }
      });
    });
  }

  function setupFilters() {
    $("btn-apply-filters").addEventListener("click", () => {
      state.filters.repository = $("filter-repo").value.trim();
      state.filters.status = $("filter-status").value;
      state.filters.mode = $("filter-mode").value;
      connectSSE();
    });
    $("btn-clear-filters").addEventListener("click", () => {
      $("filter-repo").value = "";
      $("filter-status").value = "";
      $("filter-mode").value = "";
      state.filters = { repository: "", status: "", mode: "" };
      connectSSE();
    });
  }

  function setupLogFilters() {
    const applyLocal = () => {
      state.logFilters.level = $("log-filter-level").value;
      state.logFilters.category = $("log-filter-category").value;
      if (state.lastSnapshot) renderLogs(state.lastSnapshot.logs || []);
    };
    $("log-filter-level").addEventListener("change", applyLocal);
    $("log-filter-category").addEventListener("change", applyLocal);
    $("btn-log-clear-filters").addEventListener("click", () => {
      $("log-filter-level").value = "";
      $("log-filter-category").value = "";
      applyLocal();
    });
  }

  function setupAdmin() {
    $("btn-drain").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "warn",
        kicker: "Admin",
        title: "Enable drain?",
        body: "New jobs stay queued until you click Resume. Jobs already running are not interrupted.",
        confirmLabel: "Enable drain",
      });
      if (ok) postAdmin("/api/v1/admin/drain", "Drain");
    });
    $("btn-resume").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "ok",
        kicker: "Admin",
        title: "Disable drain?",
        body: "The orchestrator resumes accepting and provisioning queued jobs.",
        confirmLabel: "Resume",
      });
      if (ok) postAdmin("/api/v1/admin/resume", "Resume");
    });
    $("btn-reconcile").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "info",
        kicker: "Admin",
        title: "Run reconcile now?",
        body: "Cleans up pending instances and replenishes pool/warm supply per policy.",
        confirmLabel: "Reconcile",
      });
      if (ok) postAdmin("/api/v1/admin/reconcile", "Reconcile");
    });
    $("btn-recycle-ready").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "bad",
        kicker: "Admin · destructive",
        title: "Recycle ready pool/warm?",
        body: "Only works if no instances are provisioning/running.\n\nReady clones are destroyed and rebuilt from the template. Use after updating the golden image or cache.",
        confirmLabel: "Recycle ready",
      });
      if (ok) postAdmin("/api/v1/admin/recycle-ready", "Recycle ready");
    });
  }

  setupConfirmModal();
  setupTabs();
  setupFilters();
  setupLogFilters();
  setupAdmin();
  setupPolicyDirtyTracking();
  setupCacheTab();
  connectSSE();
  window.addEventListener("resize", () => {
    if (state.lastSnapshot) renderDurationChart(state.lastSnapshot.job_durations || []);
  });
})();
