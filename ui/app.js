(() => {
  const state = {
    source: null,
    filters: { repository: "", status: "", mode: "" },
    lastSnapshot: null,
    toastTimer: null,
    actionInFlight: false,
    policyDraft: null,
    policyMeta: {
      mode_options: ["ephemeral", "pool", "persistent"],
      event_options: ["push", "workflow_dispatch", "schedule", "pull_request", "release"],
      label_options: ["self-hosted", "proxmox-lxc", "crossbuild"],
      installations: {},
      broker_note: "",
    },
  };

  const $ = (id) => document.getElementById(id);

  function showToast(kind, title, body, holdMs = 8000) {
    const toast = $("toast");
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

  function renderOverview(overview) {
    const caps = overview.capacity || {};
    $("badges").innerHTML = [
      badge(overview.ready ? "ready" : "not ready", overview.ready ? "ok" : "bad"),
      badge(overview.shadow_mode ? "shadow" : "live", overview.shadow_mode ? "warn" : "ok"),
      badge(overview.drain ? "drain" : "accepting", overview.drain ? "warn" : "ok"),
      badge(`node ${overview.proxmox?.node || "?"}`, "info"),
    ].join("");
    $("cards").innerHTML = [
      card("Queue", caps.queued ?? 0),
      card("Active", `${caps.active ?? 0}/${caps.max_active ?? "?"}`),
      card("Warm ready", caps.warm_ready ?? 0),
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
        return `<tr class="${bad ? "row-bad" : ""}">
        <td class="mono">${escapeHtml(item.vmid)}</td>
        <td class="mono">${escapeHtml(item.ip_address)}</td>
        <td>${escapeHtml(item.mode)}</td>
        <td><span class="${statusClass(item.status)}">${escapeHtml(item.status)}</span></td>
        <td class="mono">${escapeHtml(item.repository || "—")}</td>
        <td class="mono">${escapeHtml(item.runner_name || "—")}</td>
        <td class="mono">${escapeHtml(item.job_id ?? "—")}</td>
        <td class="muted">${escapeHtml(fmtTs(item.updated_at))}</td>
      </tr>`;
      })
      .join("");
    renderTable(
      $("table-instances"),
      ["VMID", "IP", "Mode", "Status", "Repository", "Runner", "Job", "Updated"],
      rows
    );
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
    return {
      github_installation: installationName || Object.keys(state.policyMeta.installations)[0] || "",
      default_mode: "ephemeral",
      warm_capacity: 0,
      allowed_modes: ["ephemeral", "pool", "persistent"],
      allowed_events: ["push", "workflow_dispatch", "schedule"],
      required_labels: ["self-hosted", "proxmox-lxc", "crossbuild"],
    };
  }

  function syncPolicyDraftFromServer(policies) {
    state.policyMeta.installations = policies?.installations || {};
    state.policyMeta.mode_options = policies?.mode_options || state.policyMeta.mode_options;
    state.policyMeta.event_options = policies?.event_options || state.policyMeta.event_options;
    state.policyMeta.label_options = policies?.label_options || state.policyMeta.label_options;
    state.policyMeta.broker_note = policies?.broker_note || "";
    // Keep local edits if the user is mid-edit; only bootstrap once.
    if (!state.policyDraft) {
      state.policyDraft = JSON.parse(JSON.stringify(policies?.repositories || {}));
    }
    const list = $("repo-list");
    list.innerHTML = Object.keys(policies?.repositories || {})
      .map((name) => `<option value="${escapeHtml(name)}"></option>`)
      .join("");
    $("policy-note").textContent = state.policyMeta.broker_note || "";
    renderPolicyEditor();
  }

  function collectPolicyDraftFromDom() {
    const cards = [...document.querySelectorAll(".policy-card")];
    const repositories = {};
    for (const card of cards) {
      const repo = card.querySelector('[data-field="repository"]').value.trim();
      if (!repo) continue;
      const modes = [...card.querySelectorAll('[data-field="allowed_modes"]:checked')].map((el) => el.value);
      const events = [...card.querySelectorAll('[data-field="allowed_events"]:checked')].map((el) => el.value);
      const labels = [...card.querySelectorAll('[data-field="required_labels"]:checked')].map((el) => el.value);
      repositories[repo] = {
        github_installation: card.querySelector('[data-field="github_installation"]').value,
        default_mode: card.querySelector('[data-field="default_mode"]').value,
        warm_capacity: Number(card.querySelector('[data-field="warm_capacity"]').value || 0),
        allowed_modes: modes,
        allowed_events: events,
        required_labels: labels,
      };
    }
    return repositories;
  }

  function renderPolicyEditor() {
    const editor = $("policy-editor");
    const draft = state.policyDraft || {};
    const installationNames = Object.keys(state.policyMeta.installations);
    const cards = Object.entries(draft)
      .map(([repo, policy], index) => {
        const modes = state.policyMeta.mode_options
          .map(
            (mode) =>
              `<label class="inline"><input type="checkbox" data-field="allowed_modes" value="${escapeHtml(mode)}" ${(policy.allowed_modes || []).includes(mode) ? "checked" : ""}/> ${escapeHtml(mode)}</label>`
          )
          .join("");
        const events = state.policyMeta.event_options
          .map(
            (event) =>
              `<label class="inline"><input type="checkbox" data-field="allowed_events" value="${escapeHtml(event)}" ${(policy.allowed_events || []).includes(event) ? "checked" : ""}/> ${escapeHtml(event)}</label>`
          )
          .join("");
        const labels = state.policyMeta.label_options
          .map(
            (label) =>
              `<label class="inline"><input type="checkbox" data-field="required_labels" value="${escapeHtml(label)}" ${(policy.required_labels || []).includes(label) ? "checked" : ""}/> ${escapeHtml(label)}</label>`
          )
          .join("");
        const installations = installationNames
          .map(
            (name) =>
              `<option value="${escapeHtml(name)}" ${policy.github_installation === name ? "selected" : ""}>${escapeHtml(name)} (${escapeHtml(state.policyMeta.installations[name].account)})</option>`
          )
          .join("");
        const modeOptions = state.policyMeta.mode_options
          .map(
            (mode) =>
              `<option value="${escapeHtml(mode)}" ${policy.default_mode === mode ? "selected" : ""}>${escapeHtml(mode)}</option>`
          )
          .join("");
        return `<article class="policy-card" data-index="${index}">
          <div class="policy-card-head">
            <strong class="mono">Policy #${index + 1}</strong>
            <button type="button" class="btn ghost btn-policy-delete" data-repo="${escapeHtml(repo)}">Remove</button>
          </div>
          <div class="policy-grid">
            <label>Repository
              <input data-field="repository" value="${escapeHtml(repo)}" placeholder="Owner/Name" />
            </label>
            <label>Installation
              <select data-field="github_installation">${installations}</select>
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
    editor.innerHTML = cards || `<div class="empty">Nenhuma policy configurada. Clique em Add repository.</div>`;
    editor.querySelectorAll(".btn-policy-delete").forEach((button) => {
      button.addEventListener("click", () => {
        const card = button.closest(".policy-card");
        const repoInput = card?.querySelector('[data-field="repository"]');
        const repo = (repoInput?.value || button.getAttribute("data-repo") || "").trim();
        if (!confirm(`Remover policy ${repo || "selecionada"}?`)) return;
        card?.remove();
        state.policyDraft = collectPolicyDraftFromDom();
        renderPolicyEditor();
      });
    });
  }

  function setupPolicyEditor() {
    $("btn-policy-add").addEventListener("click", () => {
      state.policyDraft = collectPolicyDraftFromDom();
      let name = "Owner/NewRepo";
      let i = 1;
      while (state.policyDraft[name]) {
        name = `Owner/NewRepo-${i++}`;
      }
      state.policyDraft[name] = defaultPolicy();
      renderPolicyEditor();
    });
    $("btn-policy-reset").addEventListener("click", () => {
      const server = state.lastSnapshot?.policies?.repositories || {};
      state.policyDraft = JSON.parse(JSON.stringify(server));
      renderPolicyEditor();
      showToast("info", "Policies reset", "Draft restaurado a partir do servidor.", 5000);
    });
    $("btn-policy-save").addEventListener("click", async () => {
      const repositories = collectPolicyDraftFromDom();
      if (!Object.keys(repositories).length) {
        showToast("bad", "Policies inválidas", "Mantenha ao menos um repositório.", 8000);
        return;
      }
      if (!confirm(`Salvar ${Object.keys(repositories).length} policy(ies) e aplicar agora?`)) return;
      state.actionInFlight = true;
      showToast("warn", "Saving policies…", "Validando e gravando config.yaml", 0);
      try {
        const response = await fetch("/api/v1/admin/policies", {
          method: "PUT",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ repositories }),
        });
        const text = await response.text();
        let body = {};
        try {
          body = text ? JSON.parse(text) : {};
        } catch {
          body = { detail: text };
        }
        if (!response.ok) throw new Error(body.detail || body.message || `HTTP ${response.status}`);
        state.policyDraft = JSON.parse(JSON.stringify(body.policies?.repositories || repositories));
        syncPolicyDraftFromServer(body.policies || { repositories: state.policyDraft });
        showToast(
          "ok",
          "Policies salvas",
          `${body.message || "OK"}\n${body.broker_note || ""}`,
          15000
        );
        const snap = await fetch(`/api/v1/snapshot?${queryString()}`, {
          headers: { Accept: "application/json" },
        });
        if (snap.ok) {
          const snapshot = await snap.json();
          // force policy draft sync from saved server state
          state.policyDraft = null;
          applySnapshot(snapshot);
        }
      } catch (error) {
        showToast("bad", "Falha ao salvar policies", String(error.message || error), 15000);
      } finally {
        state.actionInFlight = false;
      }
    });
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
      $("badges").innerHTML = [
        badge(snapshot.overview?.ready ? "ready" : "not ready", snapshot.overview?.ready ? "ok" : "bad"),
        badge(snapshot.overview?.shadow_mode ? "shadow" : "live", snapshot.overview?.shadow_mode ? "warn" : "ok"),
        badge(snapshot.overview?.drain ? "drain" : "accepting", snapshot.overview?.drain ? "warn" : "ok"),
        badge(`node ${snapshot.overview?.proxmox?.node || "?"}`, "info"),
      ].join("");
      $("cards").innerHTML = [
        card("Queue", caps.queued ?? 0),
        card("Active", `${caps.active ?? 0}/${caps.max_active ?? "?"}`),
        card("Warm ready", caps.warm_ready ?? 0),
        card("Pool ready", `${caps.pool_ready ?? 0}/${caps.pool_size ?? "?"}`),
        card("Delete failed", caps.delete_failed ?? 0),
        card("Oldest queued", fmtAge(caps.oldest_queued_job_age_seconds)),
      ].join("");
    }
    renderAlerts(snapshot.alerts || []);
    renderInstances(snapshot.instances || []);
    renderJobs(snapshot.jobs || []);
    renderDeliveries(snapshot.deliveries || []);
    // Do not clobber in-progress policy edits on every SSE tick.
    if (!state.actionInFlight) {
      if (!state.policyDraft) {
        syncPolicyDraftFromServer(snapshot.policies || {});
      } else {
        // Keep meta/options fresh, preserve draft.
        state.policyMeta.installations = snapshot.policies?.installations || state.policyMeta.installations;
        state.policyMeta.broker_note = snapshot.policies?.broker_note || state.policyMeta.broker_note;
        $("policy-note").textContent = state.policyMeta.broker_note || "";
      }
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

  function setupAdmin() {
    $("btn-drain").addEventListener("click", () => {
      if (confirm("Ativar drain? Novos jobs ficam na fila até Resume.")) {
        postAdmin("/api/v1/admin/drain", "Drain");
      }
    });
    $("btn-resume").addEventListener("click", () => {
      if (confirm("Desativar drain e voltar a aceitar jobs?")) {
        postAdmin("/api/v1/admin/resume", "Resume");
      }
    });
    $("btn-reconcile").addEventListener("click", () => {
      if (confirm("Rodar reconcile agora (cleanup + reposição pool/warm)?")) {
        postAdmin("/api/v1/admin/reconcile", "Reconcile");
      }
    });
    $("btn-recycle-ready").addEventListener("click", () => {
      if (
        confirm(
          "Reciclar pool/warm ready agora?\nSó funciona se não houver instances em provisioning/running.\nOs clones ready serão destruídos e repostos."
        )
      ) {
        postAdmin("/api/v1/admin/recycle-ready", "Recycle ready");
      }
    });
  }

  setupTabs();
  setupFilters();
  setupAdmin();
  setupPolicyEditor();
  connectSSE();
  window.addEventListener("resize", () => {
    if (state.lastSnapshot) renderDurationChart(state.lastSnapshot.job_durations || []);
  });
})();
