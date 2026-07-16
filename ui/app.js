(() => {
  const state = {
    source: null,
    filters: { repository: "", status: "", mode: "" },
    logFilters: { level: "", category: "" },
    lastSnapshot: null,
    toastTimer: null,
    actionInFlight: false,
    confirmResolver: null,
    openLogIds: new Set(),
    policyDraft: null,
    installationDraft: null,
    policiesHydrated: false,
    policiesDirty: false,
    policyMeta: {
      mode_options: ["ephemeral", "pool", "persistent"],
      event_options: ["push", "workflow_dispatch", "schedule", "pull_request", "release"],
      label_options: ["self-hosted", "proxmox-lxc", "crossbuild"],
      installations: {},
      broker_note: "",
    },
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
    if (resolve) resolve(Boolean(result));
  }

  function onConfirmKeydown(event) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeConfirm(false);
    } else if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.metaKey) {
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
    title = "Confirmar ação",
    body = "",
    confirmLabel = "Confirmar",
    cancelLabel = "Cancelar",
    kind = "warn",
    kicker = "Confirmar",
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

  function renderTable(el, headers, rowsHtml) {
    if (!rowsHtml) {
      el.innerHTML = `<tbody><tr><td class="empty" colspan="${headers.length}">Nenhum registro</td></tr></tbody>`;
      return;
    }
    el.innerHTML = `
      <thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>
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
    $("cards").innerHTML = [
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

  function defaultPolicy(installationName) {
    const names = Object.keys(state.installationDraft || {});
    return {
      github_installation: installationName || names[0] || "",
      default_mode: "ephemeral",
      warm_capacity: 0,
      allowed_modes: ["ephemeral", "pool", "persistent"],
      allowed_events: ["push", "workflow_dispatch", "schedule"],
      required_labels: ["self-hosted", "proxmox-lxc", "crossbuild"],
    };
  }

  function defaultInstallation() {
    return {
      account: "",
      installation_id: 0,
      scope: "repository",
      runner_group_id: 1,
    };
  }

  function fieldValue(root, selector, fallback = "") {
    const el = root.querySelector(selector);
    if (!el) return fallback;
    return String(el.value ?? fallback);
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

  /** Read current form values into state (source of truth before any re-render). */
  function pullPoliciesFromDom() {
    const installations = {};
    document.querySelectorAll("#install-editor .install-card").forEach((card) => {
      const key = fieldValue(card, "[data-field='install_key']").trim();
      if (!key) return;
      installations[key] = {
        account: fieldValue(card, "[data-field='account']").trim(),
        installation_id: Number(fieldValue(card, "[data-field='installation_id']", "0") || 0),
        scope: fieldValue(card, "[data-field='scope']", "repository") || "repository",
        runner_group_id: Number(fieldValue(card, "[data-field='runner_group_id']", "1") || 1),
      };
    });
    const repositories = {};
    document.querySelectorAll("#policy-editor .repo-card").forEach((card) => {
      const repo = fieldValue(card, "[data-field='repository']").trim();
      if (!repo) return;
      repositories[repo] = {
        github_installation: fieldValue(card, "[data-field='github_installation']"),
        default_mode: fieldValue(card, "[data-field='default_mode']", "ephemeral") || "ephemeral",
        warm_capacity: Number(fieldValue(card, "[data-field='warm_capacity']", "0") || 0),
        allowed_modes: [...card.querySelectorAll("[data-field='allowed_modes']:checked")].map((el) => el.value),
        allowed_events: [...card.querySelectorAll("[data-field='allowed_events']:checked")].map((el) => el.value),
        required_labels: [...card.querySelectorAll("[data-field='required_labels']:checked")].map((el) => el.value),
      };
    });
    state.installationDraft = installations;
    state.policyDraft = repositories;
    state.policiesDirty = true;
    return { installations, repositories };
  }

  function applyPoliciesMeta(policies) {
    if (!policies || typeof policies !== "object") return;
    state.policyMeta.installations = policies.installations || state.policyMeta.installations || {};
    state.policyMeta.mode_options = policies.mode_options || state.policyMeta.mode_options;
    state.policyMeta.event_options = policies.event_options || state.policyMeta.event_options;
    state.policyMeta.label_options = policies.label_options || state.policyMeta.label_options;
    state.policyMeta.broker_note = policies.broker_note || state.policyMeta.broker_note || "";
    const list = $("repo-list");
    if (list) {
      list.innerHTML = Object.keys(policies.repositories || {})
        .map((name) => `<option value="${escapeHtml(name)}"></option>`)
        .join("");
    }
    const note = $("policy-note");
    if (note && !state.policiesDirty) {
      note.textContent = state.policyMeta.broker_note || "";
    }
  }

  function hydratePolicies(policies, { force = false } = {}) {
    applyPoliciesMeta(policies || {});
    if (state.policiesHydrated && !force) return;
    state.installationDraft = JSON.parse(JSON.stringify((policies && policies.installations) || {}));
    state.policyDraft = JSON.parse(JSON.stringify((policies && policies.repositories) || {}));
    state.policiesHydrated = true;
    state.policiesDirty = false;
    const note = $("policy-note");
    if (note) note.textContent = state.policyMeta.broker_note || "";
    renderPolicies();
  }

  function renderPolicies() {
    renderInstallationEditor();
    renderPolicyEditor();
  }

  function renderInstallationEditor() {
    const editor = $("install-editor");
    if (!editor) return;
    const draft = state.installationDraft || {};
    const entries = Object.entries(draft);
    if (!entries.length) {
      editor.innerHTML =
        `<div class="empty">Nenhuma installation. Clique em <strong>Add installation</strong>.</div>`;
      return;
    }
    editor.innerHTML = entries
      .map(([key, inst], index) => {
        const scope = inst.scope || "repository";
        return `<article class="policy-card install-card" data-install-key="${escapeHtml(key)}">
          <div class="policy-card-head">
            <strong class="mono">Installation #${index + 1}</strong>
            <button type="button" class="btn ghost" data-action="install-delete" data-key="${escapeHtml(key)}">Remove</button>
          </div>
          <div class="policy-grid">
            <label>Key
              <input data-field="install_key" value="${escapeHtml(key)}" placeholder="personal-or-org" />
            </label>
            <label>Account (handle/org)
              <input data-field="account" value="${escapeHtml(inst.account || "")}" placeholder="acme" />
            </label>
            <label>Installation ID
              <input data-field="installation_id" type="number" min="1" step="1" value="${escapeHtml(inst.installation_id || "")}" placeholder="1234567" />
            </label>
            <label>Scope
              <select data-field="scope">
                <option value="repository" ${scope === "repository" ? "selected" : ""}>repository</option>
                <option value="organization" ${scope === "organization" ? "selected" : ""}>organization</option>
              </select>
            </label>
            <label>Runner group ID
              <input data-field="runner_group_id" type="number" min="1" step="1" value="${escapeHtml(inst.runner_group_id ?? 1)}" />
            </label>
          </div>
        </article>`;
      })
      .join("");
  }

  function renderPolicyEditor() {
    const editor = $("policy-editor");
    if (!editor) return;
    const draft = state.policyDraft || {};
    const liveInstalls = state.installationDraft || {};
    const installationNames = Object.keys(liveInstalls);
    const entries = Object.entries(draft);
    if (!entries.length) {
      editor.innerHTML =
        `<div class="empty">Nenhuma policy. Clique em <strong>Add repository</strong>.</div>`;
      return;
    }
    editor.innerHTML = entries
      .map(([repo, policy], index) => {
        const modes = (state.policyMeta.mode_options || [])
          .map(
            (mode) =>
              `<label class="inline"><input type="checkbox" data-field="allowed_modes" value="${escapeHtml(mode)}" ${(policy.allowed_modes || []).includes(mode) ? "checked" : ""}/> ${escapeHtml(mode)}</label>`
          )
          .join("");
        const events = (state.policyMeta.event_options || [])
          .map(
            (event) =>
              `<label class="inline"><input type="checkbox" data-field="allowed_events" value="${escapeHtml(event)}" ${(policy.allowed_events || []).includes(event) ? "checked" : ""}/> ${escapeHtml(event)}</label>`
          )
          .join("");
        const labels = (state.policyMeta.label_options || [])
          .map(
            (label) =>
              `<label class="inline"><input type="checkbox" data-field="required_labels" value="${escapeHtml(label)}" ${(policy.required_labels || []).includes(label) ? "checked" : ""}/> ${escapeHtml(label)}</label>`
          )
          .join("");
        const selectedInstall = policy.github_installation || installationNames[0] || "";
        const installOptions = installationNames.length
          ? installationNames
              .map((name) => {
                const account = liveInstalls[name]?.account || "?";
                return `<option value="${escapeHtml(name)}" ${selectedInstall === name ? "selected" : ""}>${escapeHtml(name)} · ${escapeHtml(account)}</option>`;
              })
              .join("")
          : `<option value="">(adicione uma installation)</option>`;
        const modeOptions = (state.policyMeta.mode_options || [])
          .map(
            (mode) =>
              `<option value="${escapeHtml(mode)}" ${policy.default_mode === mode ? "selected" : ""}>${escapeHtml(mode)}</option>`
          )
          .join("");
        return `<article class="policy-card repo-card" data-repo="${escapeHtml(repo)}">
          <div class="policy-card-head">
            <strong class="mono">Policy #${index + 1}</strong>
            <button type="button" class="btn ghost" data-action="repo-delete" data-repo="${escapeHtml(repo)}">Remove</button>
          </div>
          <div class="policy-grid">
            <label>Repository
              <input data-field="repository" value="${escapeHtml(repo)}" placeholder="Owner/Name" />
            </label>
            <label>Installation
              <select data-field="github_installation">${installOptions}</select>
            </label>
            <label>Default mode
              <select data-field="default_mode">${modeOptions}</select>
            </label>
            <label>Warm capacity
              <input data-field="warm_capacity" type="number" min="0" step="1" value="${escapeHtml(policy.warm_capacity ?? 0)}" />
            </label>
          </div>
          <div class="policy-checks">Allowed modes<div class="check-row">${modes}</div></div>
          <div class="policy-checks">Allowed events<div class="check-row">${events}</div></div>
          <div class="policy-checks">Required labels<div class="check-row">${labels}</div></div>
        </article>`;
      })
      .join("");
  }

  async function loadPoliciesFromApi() {
    setPolicyStatus("warn", "Carregando policies…");
    try {
      const response = await fetch("/api/v1/policies", { headers: { Accept: "application/json" } });
      const text = await response.text();
      let body = {};
      try {
        body = text ? JSON.parse(text) : {};
      } catch {
        body = { detail: text };
      }
      if (!response.ok) throw new Error(formatApiError(body, response.status));
      hydratePolicies(body, { force: true });
      setPolicyStatus("ok", "Policies carregadas");
      showToast(
        "ok",
        "Policies carregadas",
        `${Object.keys(body.installations || {}).length} installation(s), ${Object.keys(body.repositories || {}).length} repo(s)`,
        4000
      );
    } catch (error) {
      setPolicyStatus("bad", "Falha ao carregar");
      showToast("bad", "Falha ao carregar policies", String(error.message || error), 12000);
      // Still show empty editors so the user can create from scratch.
      if (!state.policiesHydrated) {
        hydratePolicies({ installations: {}, repositories: {} }, { force: true });
      }
    }
  }

  function setupPolicyEditor() {
    const panel = $("panel-policies");
    if (!panel) {
      console.error("panel-policies missing");
      return;
    }

    // Mark dirty on any edit without re-rendering.
    panel.addEventListener("input", (event) => {
      if (event.target && event.target.matches("input, select, textarea")) {
        state.policiesDirty = true;
        setPolicyStatus("warn", "Alterações não salvas");
      }
    });
    panel.addEventListener("change", (event) => {
      if (event.target && event.target.matches("input, select, textarea")) {
        state.policiesDirty = true;
        setPolicyStatus("warn", "Alterações não salvas");
      }
    });

    // Event delegation for all policy actions.
    panel.addEventListener("click", async (event) => {
      const btn = event.target.closest("button");
      if (!btn || !panel.contains(btn)) return;
      const id = btn.id;
      const action = btn.getAttribute("data-action");

      try {
        if (id === "btn-install-add") {
          event.preventDefault();
          pullPoliciesFromDom();
          let name = "personal";
          let i = 1;
          while (state.installationDraft[name]) name = `personal-${i++}`;
          state.installationDraft[name] = defaultInstallation();
          renderPolicies();
          setPolicyStatus("warn", "Installation adicionada (não salva)");
          return;
        }

        if (id === "btn-policy-add") {
          event.preventDefault();
          pullPoliciesFromDom();
          const installs = state.installationDraft || {};
          const firstAccount = Object.values(installs)[0]?.account || "Owner";
          let name = `${firstAccount}/NewRepo`;
          let i = 1;
          while (state.policyDraft[name]) name = `${firstAccount}/NewRepo-${i++}`;
          state.policyDraft[name] = defaultPolicy();
          renderPolicies();
          setPolicyStatus("warn", "Repository adicionado (não salvo)");
          return;
        }

        if (id === "btn-policy-reset") {
          event.preventDefault();
          const ok = await confirmAction({
            kind: "warn",
            kicker: "Policies",
            title: "Recarregar do servidor?",
            body: "Descarta o draft local e busca installations/repositórios atuais da API.",
            confirmLabel: "Recarregar",
            cancelLabel: "Continuar editando",
          });
          if (!ok) {
            showToast("info", "Reset cancelado", "", 3000);
            return;
          }
          state.policiesDirty = false;
          await loadPoliciesFromApi();
          return;
        }

        if (id === "btn-policy-save") {
          event.preventDefault();
          await savePolicies();
          return;
        }

        if (action === "install-delete") {
          event.preventDefault();
          const key = btn.getAttribute("data-key") || "";
          const ok = await confirmAction({
            kind: "bad",
            kicker: "Installations",
            title: "Remover installation?",
            body: key
              ? `Remove "${key}" do draft. Salve depois para aplicar.`
              : "Remove a installation do draft.",
            confirmLabel: "Remover",
            cancelLabel: "Manter",
          });
          if (!ok) return;
          pullPoliciesFromDom();
          // Prefer current key field if renamed.
          const card = btn.closest(".install-card");
          const liveKey = card ? fieldValue(card, "[data-field='install_key']").trim() : key;
          delete state.installationDraft[liveKey];
          if (key && key !== liveKey) delete state.installationDraft[key];
          renderPolicies();
          setPolicyStatus("warn", "Installation removida do draft");
          return;
        }

        if (action === "repo-delete") {
          event.preventDefault();
          const repo = btn.getAttribute("data-repo") || "";
          const ok = await confirmAction({
            kind: "bad",
            kicker: "Policies",
            title: "Remover policy?",
            body: repo
              ? `Remove ${repo} do draft. Salve depois para aplicar.`
              : "Remove a policy do draft.",
            confirmLabel: "Remover",
            cancelLabel: "Manter",
          });
          if (!ok) return;
          pullPoliciesFromDom();
          const card = btn.closest(".repo-card");
          const liveRepo = card ? fieldValue(card, "[data-field='repository']").trim() : repo;
          delete state.policyDraft[liveRepo];
          if (repo && repo !== liveRepo) delete state.policyDraft[repo];
          renderPolicies();
          setPolicyStatus("warn", "Policy removida do draft");
        }
      } catch (error) {
        showToast("bad", "Erro na aba Policies", String(error.message || error), 12000);
        console.error(error);
      }
    });

    // Initial load independent of SSE.
    loadPoliciesFromApi();
  }

  async function savePolicies() {
    if (state.actionInFlight) {
      showToast("warn", "Aguarde", "Já existe uma ação em andamento.", 4000);
      return;
    }
    const saveBtn = $("btn-policy-save");
    let installations;
    let repositories;
    try {
      ({ installations, repositories } = pullPoliciesFromDom());
    } catch (error) {
      showToast("bad", "Falha ao ler o formulário", String(error.message || error), 12000);
      return;
    }
    if (!Object.keys(installations).length) {
      showToast("bad", "Policies inválidas", "Mantenha ao menos uma installation (Key + Account + ID).", 10000);
      return;
    }
    if (!Object.keys(repositories).length) {
      showToast("bad", "Policies inválidas", "Mantenha ao menos um repositório Owner/Name.", 10000);
      return;
    }
    const instNames = Object.keys(installations);
    const names = Object.keys(repositories);
    showToast("info", "Confirme o save", "Use o diálogo para aplicar.", 4000);
    const ok = await confirmAction({
      kind: "ok",
      kicker: "Policies",
      title: "Salvar e aplicar policies?",
      body: `${instNames.length} installation(s):\n${instNames
        .map((n) => `· ${n} → ${installations[n].account} (id ${installations[n].installation_id})`)
        .join("\n")}\n\n${names.length} repositório(s):\n${names.map((n) => `· ${n}`).join("\n")}\n\nGrava override e sincroniza broker.`,
      confirmLabel: "Salvar e aplicar",
      cancelLabel: "Cancelar",
    });
    if (!ok) {
      showToast("info", "Save cancelado", "Nada foi gravado.", 4000);
      setPolicyStatus("warn", "Save cancelado");
      return;
    }
    state.actionInFlight = true;
    if (saveBtn) {
      saveBtn.disabled = true;
      saveBtn.classList.add("busy");
    }
    showToast("warn", "Saving policies…", "Enviando para o orchestrator", 0);
    setPolicyStatus("warn", "Salvando…");
    try {
      const response = await fetch("/api/v1/admin/policies", {
        method: "PUT",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ repositories, installations }),
      });
      const text = await response.text();
      let body = {};
      try {
        body = text ? JSON.parse(text) : {};
      } catch {
        body = { detail: text };
      }
      if (!response.ok) throw new Error(formatApiError(body, response.status));
      hydratePolicies(body.policies || { repositories, installations }, { force: true });
      showToast("ok", "Policies salvas", `${body.message || "OK"}\n${body.broker_note || ""}`, 15000);
      setPolicyStatus("ok", "Salvo");
    } catch (error) {
      showToast("bad", "Falha ao salvar policies", String(error.message || error), 15000);
      setPolicyStatus("bad", "Erro ao salvar");
    } finally {
      state.actionInFlight = false;
      if (saveBtn) {
        saveBtn.disabled = false;
        saveBtn.classList.remove("busy");
      }
    }
  }

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
      const caps = snapshot.overview?.capacity || {};
      renderBadges(snapshot.overview || {});
      $("cards").innerHTML = [
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
    renderAlerts(snapshot.alerts || []);
    renderInstances(snapshot.instances || []);
    renderJobs(snapshot.jobs || []);
    renderDeliveries(snapshot.deliveries || []);
    renderLogs(snapshot.logs || []);
    // Policies are loaded via GET /api/v1/policies (not SSE). Keep a server
    // snapshot for reference only — never re-render the editors from SSE.
    if (snapshot.policies && !state.policiesDirty) {
      applyPoliciesMeta(snapshot.policies);
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
    showToast("warn", `${label}…`, "Aguardando resposta do orchestrator.", 0);
    $("refresh-meta").textContent = `${label} in progress…`;
    try {
      const response = await fetch(path, {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      const text = await response.text();
      let body = {};
      try {
        body = text ? JSON.parse(text) : {};
      } catch {
        body = { raw: text };
      }
      if (!response.ok) {
        throw new Error(body.message || body.detail || `HTTP ${response.status}`);
      }

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
      showToast("ok", `${label} concluído`, detail, 12000);
      $("refresh-meta").textContent = `${label} ok · ${new Date().toLocaleTimeString()}`;

      // Pull a fresh snapshot once so badges reflect the new state immediately.
      const snap = await fetch(`/api/v1/snapshot?${queryString()}`, {
        headers: { Accept: "application/json" },
      });
      if (snap.ok) applySnapshot(await snap.json());
    } catch (error) {
      showToast("bad", `${label} falhou`, String(error.message || error), 15000);
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

  function setupTabs() {
    document.querySelectorAll(".tab").forEach((button) => {
      button.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach((el) => el.classList.remove("active"));
        document.querySelectorAll(".panel").forEach((el) => el.classList.remove("active"));
        button.classList.add("active");
        $(`panel-${button.dataset.tab}`).classList.add("active");
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
        title: "Ativar drain?",
        body: "Novos jobs ficam na fila até você clicar em Resume. Jobs em andamento não são interrompidos.",
        confirmLabel: "Ativar drain",
      });
      if (ok) postAdmin("/api/v1/admin/drain", "Drain");
    });
    $("btn-resume").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "ok",
        kicker: "Admin",
        title: "Desativar drain?",
        body: "O orchestrator volta a aceitar e provisionar jobs da fila.",
        confirmLabel: "Resume",
      });
      if (ok) postAdmin("/api/v1/admin/resume", "Resume");
    });
    $("btn-reconcile").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "info",
        kicker: "Admin",
        title: "Rodar reconcile agora?",
        body: "Executa cleanup de instances pendentes e repõe pool/warm conforme a policy.",
        confirmLabel: "Reconcile",
      });
      if (ok) postAdmin("/api/v1/admin/reconcile", "Reconcile");
    });
    $("btn-recycle-ready").addEventListener("click", async () => {
      const ok = await confirmAction({
        kind: "bad",
        kicker: "Admin · destrutivo",
        title: "Reciclar pool/warm ready?",
        body: "Só funciona se não houver instances em provisioning/running.\n\nClones ready serão destruídos e repostos do template. Use após update de golden image ou cache.",
        confirmLabel: "Reciclar ready",
      });
      if (ok) postAdmin("/api/v1/admin/recycle-ready", "Recycle ready");
    });
  }

  setupConfirmModal();
  setupTabs();
  setupFilters();
  setupLogFilters();
  setupAdmin();
  setupPolicyEditor();
  connectSSE();
  window.addEventListener("resize", () => {
    if (state.lastSnapshot) renderDurationChart(state.lastSnapshot.job_durations || []);
  });
})();
