(() => {
  const state = {
    source: null,
    filters: { repository: "", status: "", mode: "" },
    lastSnapshot: null,
    toastTimer: null,
    actionInFlight: false,
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

  function renderPolicies(policies) {
    const repos = policies?.repositories || {};
    const list = $("repo-list");
    list.innerHTML = Object.keys(repos)
      .map((name) => `<option value="${escapeHtml(name)}"></option>`)
      .join("");
    const rows = Object.entries(repos)
      .map(
        ([name, policy]) => `<tr>
      <td class="mono">${escapeHtml(name)}</td>
      <td>${escapeHtml(policy.default_mode)}</td>
      <td>${escapeHtml(policy.warm_capacity)}</td>
      <td class="mono">${escapeHtml((policy.allowed_modes || []).join(", "))}</td>
      <td class="mono">${escapeHtml((policy.allowed_events || []).join(", "))}</td>
      <td class="mono">${escapeHtml(policy.github_installation)}</td>
    </tr>`
      )
      .join("");
    renderTable(
      $("table-policies"),
      ["Repository", "Default mode", "Warm", "Modes", "Events", "Installation"],
      rows
    );
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
    renderPolicies(snapshot.policies || {});
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
  connectSSE();
  window.addEventListener("resize", () => {
    if (state.lastSnapshot) renderDurationChart(state.lastSnapshot.job_durations || []);
  });
})();
