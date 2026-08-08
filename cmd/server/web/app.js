document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initSSE();
  initTopologyCanvas();
  initFailoverSimulator();
  initJobConsole();

  // Initial fetch and 2-second polling loop
  fetchTelemetry();
  setInterval(fetchTelemetry, 2000);
});

// Tab Switching
function initTabs() {
  const buttons = document.querySelectorAll('.tab-btn');
  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));

      btn.classList.add('active');
      const tabId = `tab-${btn.dataset.tab}`;
      const pane = document.getElementById(tabId);
      if (pane) pane.classList.add('active');

      if (btn.dataset.tab === 'topology') {
        drawTopology();
      }
    });
  });
}

// Global State Cache
let state = {
  status: {},
  raft: {},
  jobs: [],
  leases: {},
  nodes: [],
  crons: [],
};

// Periodic Telemetry Fetching
async function fetchTelemetry() {
  try {
    const [statusRes, raftRes, jobsRes, leasesRes, nodesRes, cronsRes, metricsRes] = await Promise.allSettled([
      fetch('/cluster/status').then(r => r.json()),
      fetch('/cluster/raft').then(r => r.json()),
      fetch('/jobs').then(r => r.json()),
      fetch('/jobs/leases').then(r => r.json()),
      fetch('/cluster/nodes').then(r => r.json()),
      fetch('/cron').then(r => r.json()),
      fetch('/metrics').then(r => r.text()),
    ]);

    if (statusRes.status === 'fulfilled') state.status = statusRes.value;
    if (raftRes.status === 'fulfilled') state.raft = raftRes.value;
    if (jobsRes.status === 'fulfilled') state.jobs = jobsRes.value || [];
    if (leasesRes.status === 'fulfilled') state.leases = leasesRes.value || {};
    if (nodesRes.status === 'fulfilled') state.nodes = nodesRes.value || [];
    if (cronsRes.status === 'fulfilled') state.crons = cronsRes.value || [];

    updateOverviewUI();
    updateLeasesUI();
    updateNodesUI();
    updateCronUI();
    renderJobConsole();

    if (metricsRes.status === 'fulfilled') {
      const el = document.getElementById('prometheus-raw');
      if (el) el.textContent = metricsRes.value;
    }
  } catch (err) {
    console.error('Telemetry fetch error:', err);
  }
}

// UI Updates
function updateOverviewUI() {
  const leaderId = state.raft.leader_addr || state.status.leader_node || 'node-1';
  const term = state.raft.term || state.status.raft_term || 1;

  document.getElementById('nav-leader-id').textContent = `Leader: ${leaderId} (Term ${term})`;
  document.getElementById('ov-leader-node').textContent = leaderId;
  document.getElementById('ov-raft-term').textContent = term;

  document.getElementById('ov-active-workers').textContent = state.status.active_nodes || 0;
  document.getElementById('ov-dead-workers').textContent = state.status.dead_nodes || 0;

  const leasesCount = Object.keys(state.leases.leases || state.leases || {}).length;
  document.getElementById('ov-active-leases').textContent = leasesCount;

  let pending = 0, completed = 0, failed = 0, cancelled = 0;
  state.jobs.forEach(j => {
    if (j.status === 'PENDING' || j.status === 'SCHEDULED') pending++;
    else if (j.status === 'COMPLETED') completed++;
    else if (j.status === 'FAILED') failed++;
    else if (j.status === 'CANCELLED') cancelled++;
  });

  document.getElementById('ov-pending-jobs').textContent = pending;
  document.getElementById('ov-completed-jobs').textContent = completed;
  document.getElementById('ov-failed-jobs').textContent = failed;
  document.getElementById('ov-cancelled-jobs').textContent = cancelled;

  document.getElementById('reconstruct-pending-val').textContent = `${pending} Pending / Scheduled Jobs`;
}

function updateLeasesUI() {
  const container = document.getElementById('leases-table');
  if (!container) return;

  const leasesMap = state.leases.leases || state.leases || {};
  const leaseKeys = Object.keys(leasesMap);

  if (leaseKeys.length === 0) {
    container.innerHTML = `<div class="text-muted p-3">No active execution leases held on active Raft leader</div>`;
    return;
  }

  let html = `<table><thead><tr><th>Job ID</th><th>Worker ID</th><th>Raft Term</th><th>Attempt</th><th>Started At</th></tr></thead><tbody>`;
  leaseKeys.forEach(k => {
    const l = leasesMap[k];
    html += `<tr>
      <td>#${l.job_id}</td>
      <td>Worker-${l.worker_id}</td>
      <td>Term ${l.term}</td>
      <td>Attempt ${l.attempt}</td>
      <td>${new Date(l.started_at).toLocaleTimeString()}</td>
    </tr>`;
  });
  html += `</tbody></table>`;
  container.innerHTML = html;
}

function updateNodesUI() {
  const container = document.getElementById('nodes-list-table');
  if (!container) return;

  if (state.nodes.length === 0) {
    container.innerHTML = `<div class="text-muted p-3">No active worker nodes registered</div>`;
    return;
  }

  let html = `<table><thead><tr><th>ID</th><th>Address</th><th>Status</th><th>Topics</th><th>Last Heartbeat</th></tr></thead><tbody>`;
  state.nodes.forEach(n => {
    html += `<tr>
      <td>${n.id}</td>
      <td>${n.address}</td>
      <td><span class="${n.alive ? 'text-success' : 'text-danger'}">${n.alive ? 'ALIVE' : 'DEAD'}</span></td>
      <td>${(n.topics || []).join(', ')}</td>
      <td>${new Date(n.last_heartbeat).toLocaleTimeString()}</td>
    </tr>`;
  });
  html += `</tbody></table>`;
  container.innerHTML = html;
}

function updateCronUI() {
  const container = document.getElementById('cron-list-table');
  if (!container) return;

  if (state.crons.length === 0) {
    container.innerHTML = `<div class="text-muted p-3">No recurring cron schedules registered</div>`;
    return;
  }

  let html = `<table><thead><tr><th>ID</th><th>Schedule</th><th>Job Type</th><th>Priority</th><th>Next Run</th></tr></thead><tbody>`;
  state.crons.forEach(c => {
    html += `<tr>
      <td>${c.id}</td>
      <td><code>${c.schedule}</code></td>
      <td>${c.type}</td>
      <td>${c.priority}</td>
      <td>${new Date(c.next_run).toLocaleTimeString()}</td>
    </tr>`;
  });
  html += `</tbody></table>`;
  container.innerHTML = html;
}

// Job Console Filtering
function initJobConsole() {
  const searchInput = document.getElementById('console-search');
  const filterSelect = document.getElementById('console-filter-status');

  if (searchInput) searchInput.addEventListener('input', renderJobConsole);
  if (filterSelect) filterSelect.addEventListener('change', renderJobConsole);
}

function renderJobConsole() {
  const container = document.getElementById('job-console-table');
  if (!container) return;

  const query = (document.getElementById('console-search')?.value || '').toLowerCase();
  const filter = document.getElementById('console-filter-status')?.value || 'ALL';

  const filtered = state.jobs.filter(j => {
    const matchesQuery = String(j.id).includes(query) || (j.type || '').toLowerCase().includes(query);
    const matchesFilter = filter === 'ALL' || j.status === filter;
    return matchesQuery && matchesFilter;
  });

  if (filtered.length === 0) {
    container.innerHTML = `<div class="text-muted p-3">No jobs matching current query criteria</div>`;
    return;
  }

  let html = `<table><thead><tr><th>ID</th><th>Type</th><th>Priority</th><th>Status</th><th>Retries</th><th>Next Run</th></tr></thead><tbody>`;
  filtered.forEach(j => {
    let statusClass = 'text-muted';
    if (j.status === 'COMPLETED') statusClass = 'text-success';
    else if (j.status === 'PENDING' || j.status === 'SCHEDULED') statusClass = 'text-warning';
    else if (j.status === 'FAILED') statusClass = 'text-danger';

    html += `<tr>
      <td>#${j.id}</td>
      <td>${j.type}</td>
      <td>${j.priority}</td>
      <td><span class="${statusClass}">${j.status}</span></td>
      <td>${j.retry_count || 0} / ${j.max_retries || 3}</td>
      <td>${j.next_run_at ? new Date(j.next_run_at).toLocaleTimeString() : 'N/A'}</td>
    </tr>`;
  });
  html += `</tbody></table>`;
  container.innerHTML = html;
}

// Server-Sent Events (SSE) Live Log Stream
function initSSE() {
  const logEl = document.getElementById('event-stream-log');
  if (!logEl) return;

  try {
    const sse = new EventSource('/events');
    sse.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        const line = document.createElement('div');
        line.className = 'log-line';

        let color = 'text-info';
        if (data.type === 'FAILOVER_TRIGGERED' || data.type === 'WORKER_EVICTED') color = 'text-danger';
        else if (data.type === 'DISPATCH_RESUMED' || data.type === 'WORKER_REGISTERED') color = 'text-success';
        else if (data.type === 'LEADER_CHANGED' || data.type === 'QUEUE_REBUILT') color = 'text-warning';

        const timeStr = new Date(data.timestamp || Date.now()).toLocaleTimeString();
        line.innerHTML = `<span class="text-muted">[${timeStr}]</span> <span class="${color}">${data.type}:</span> ${data.message}`;

        logEl.appendChild(line);
        logEl.scrollTop = logEl.scrollHeight;
      } catch (err) {}
    };
  } catch (err) {
    console.error('SSE initialization error:', err);
  }
}

// Failover Simulator
function initFailoverSimulator() {
  const btn = document.getElementById('btn-kill-leader');
  if (!btn) return;

  btn.addEventListener('click', async () => {
    btn.disabled = true;
    btn.textContent = '⚡ Simulating Leader Crash & Recovery...';

    // Step animations
    const steps = ['f-step-1', 'f-step-2', 'f-step-3', 'f-step-4', 'f-step-5', 'f-step-6'];
    steps.forEach(s => document.getElementById(s)?.classList.remove('active'));

    try {
      await fetch('/cluster/failover-simulate', { method: 'POST' });
    } catch (err) {}

    steps.forEach((stepId, index) => {
      setTimeout(() => {
        document.getElementById(stepId)?.classList.add('active');
        if (index === steps.length - 1) {
          btn.disabled = false;
          btn.textContent = '⚡ Kill Leader (Simulate Failover)';
        }
      }, (index + 1) * 200);
    });
  });
}

// Topology Canvas Rendering
function initTopologyCanvas() {
  drawTopology();
  window.addEventListener('resize', drawTopology);
}

function drawTopology() {
  const canvas = document.getElementById('topology-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  ctx.clearRect(0, 0, canvas.width, canvas.height);

  // Center Leader
  const leaderX = canvas.width / 2;
  const leaderY = 70;

  // Followers
  const followerY = 180;
  const followers = [
    { name: 'node-1 (Follower)', x: leaderX - 220, y: followerY },
    { name: 'node-3 (Follower)', x: leaderX + 220, y: followerY },
  ];

  // Draw Leader Node
  drawNodeCircle(ctx, leaderX, leaderY, 28, '#3b82f6', 'node-2 (Leader)');

  // Draw Lines & Followers
  followers.forEach(f => {
    ctx.beginPath();
    ctx.moveTo(leaderX, leaderY);
    ctx.lineTo(f.x, f.y);
    ctx.strokeStyle = 'rgba(59, 130, 246, 0.4)';
    ctx.lineWidth = 2;
    ctx.setLineDash([5, 5]);
    ctx.stroke();
    ctx.setLineDash([]);

    drawNodeCircle(ctx, f.x, f.y, 20, '#64748b', f.name);
  });

  // Draw Worker Nodes
  const workerY = 280;
  const workerCount = Math.max(state.nodes.length, 3);
  const startX = 150;
  const spacing = (canvas.width - 300) / Math.max(workerCount - 1, 1);

  for (let i = 0; i < workerCount; i++) {
    const wx = startX + i * spacing;
    const isAlive = state.nodes[i] ? state.nodes[i].alive : true;

    ctx.beginPath();
    ctx.moveTo(leaderX, leaderY);
    ctx.lineTo(wx, workerY);
    ctx.strokeStyle = isAlive ? 'rgba(16, 185, 129, 0.3)' : 'rgba(239, 68, 68, 0.3)';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    drawNodeCircle(ctx, wx, workerY, 16, isAlive ? '#10b981' : '#ef4444', `Worker-${i + 1}`);
  }
}

function drawNodeCircle(ctx, x, y, r, color, label) {
  ctx.beginPath();
  ctx.arc(x, y, r, 0, Math.PI * 2);
  ctx.fillStyle = color;
  ctx.fill();

  ctx.strokeStyle = '#fff';
  ctx.lineWidth = 2;
  ctx.stroke();

  ctx.fillStyle = '#f0f4f8';
  ctx.font = '12px Inter, sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText(label, x, y + r + 18);
}
