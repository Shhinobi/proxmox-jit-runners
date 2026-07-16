(() => {
  const REFRESH_MS = 5000;
  const state = { timer: null };

  const $ = (id) => document.getElementById(id);

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
    const v = String(value || "").toLowerCase();
    return `status ${v}`;
  }

  function badge(text, kind) {
    return `<span class="badge ${kind || ""}">${escapeHtml(text)}</span>`;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
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

  async function fetchJson(path) {
    const response = await fetch(path, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`${path} → HTTP ${response.status}`);
    return response.json();
  }

  function renderOverview(overview) {
    const caps = overview.capacity || {};
    const readyKind = overview.ready ? "ok" : "bad";
    const badges = [
      badge(overview.ready ? "ready" : "not ready", readyKind),
      badge(overview.shadow_mode ? "shadow" : "live", overview.shadow_mode ? "warn" : "ok"),
      badge(overview.drain ? "drain" : "accepting", overview.drain ? "warn" : "ok"),
      badge(`node ${overview.proxmox?.node || "?"}`, ""),
    ];
    $("badges").innerHTML = badges.join("");
    $("cards").innerHTML = [
      card("Queue", caps.queued ?? 0),
      card("Active", `${caps.active ?? 0}/${caps.max_active ?? "?"}`),
      card("Warm ready", caps.warm_ready ?? 0),
      card("Pool ready", `${caps.pool_ready ?? 0}/${caps.pool_size ?? "?"}`),
      card("Oldest queued", fmtAge(caps.oldest_queued_job_age_seconds)),
    ].join("");
  }

  function renderInstances(items) {
    const rows = (items || []).map((item) => {
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
    }).join("");
    renderTable($("table-instances"), ["VMID", "IP", "Mode", "Status", "Repository", "Runner", "Job", "Updated"], rows);
  }

  function renderJobs(items) {
    const rows = (items || []).map((item) => {
      const bad = ["failed", "rejected"].includes(String(item.status)) || item.error;
      const labels = Array.isArray(item.labels) ? item.labels.join(", ") : "";
      return `<tr class="${bad ? "row-bad" : ""}">
        <td class="mono">${escapeHtml(item.job_id)}</td>
        <td class="mono">${escapeHtml(item.repository)}</td>
        <td>${escapeHtml(item.mode || "—")}</td>
        <td><span class="${statusClass(item.status)}">${escapeHtml(item.status)}</span></td>
        <td class="mono">${escapeHtml(item.runner_name || "—")}</td>
        <td>${escapeHtml(item.attempts ?? 0)}</td>
        <td class="mono" title="${escapeHtml(labels)}">${escapeHtml(labels || "—")}</td>
        <td class="mono">${escapeHtml(item.error || "—")}</td>
        <td class="muted">${escapeHtml(fmtTs(item.created_at))}</td>
      </tr>`;
    }).join("");
    renderTable($("table-jobs"), ["Job", "Repository", "Mode", "Status", "Runner", "Attempts", "Labels", "Error", "Created"], rows);
  }

  function renderDeliveries(items) {
    const rows = (items || []).map((item) => {
      const accepted = Number(item.accepted) === 1 || item.accepted === true;
      const bad = !accepted && String(item.reason || "").includes("not_allowed");
      return `<tr class="${bad ? "row-bad" : ""}">
        <td class="muted">${escapeHtml(fmtTs(item.received_at))}</td>
        <td>${escapeHtml(item.action || "—")}</td>
        <td><span class="status ${accepted ? "accepted" : "rejected"}">${accepted ? "accepted" : "rejected"}</span></td>
        <td class="mono">${escapeHtml(item.reason)}</td>
        <td class="mono">${escapeHtml(item.delivery_id)}</td>
      </tr>`;
    }).join("");
    renderTable($("table-deliveries"), ["Received", "Action", "Result", "Reason", "Delivery"], rows);
  }

  function renderPolicies(policies) {
    const repos = policies?.repositories || {};
    const rows = Object.entries(repos).map(([name, policy]) => `<tr>
      <td class="mono">${escapeHtml(name)}</td>
      <td>${escapeHtml(policy.default_mode)}</td>
      <td>${escapeHtml(policy.warm_capacity)}</td>
      <td class="mono">${escapeHtml((policy.allowed_modes || []).join(", "))}</td>
      <td class="mono">${escapeHtml((policy.allowed_events || []).join(", "))}</td>
      <td class="mono">${escapeHtml(policy.github_installation)}</td>
    </tr>`).join("");
    renderTable($("table-policies"), ["Repository", "Default mode", "Warm", "Modes", "Events", "Installation"], rows);
  }

  async function refresh() {
    const meta = $("refresh-meta");
    try {
      const [overview, instances, jobs, deliveries, policies] = await Promise.all([
        fetchJson("/api/v1/overview"),
        fetchJson("/api/v1/instances"),
        fetchJson("/api/v1/jobs?limit=50"),
        fetchJson("/api/v1/deliveries?limit=50"),
        fetchJson("/api/v1/policies"),
      ]);
      renderOverview(overview);
      renderInstances(instances);
      renderJobs(jobs);
      renderDeliveries(deliveries);
      renderPolicies(policies);
      meta.textContent = `atualizado ${new Date().toLocaleTimeString()} · refresh ${REFRESH_MS / 1000}s`;
    } catch (error) {
      meta.textContent = `erro: ${error.message}`;
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

  setupTabs();
  refresh();
  state.timer = setInterval(refresh, REFRESH_MS);
})();
