export const API_BASE = "https://shariahcompliance.onrender.com"; // or Render later

export async function fetchDashboardData() {
  const res = await fetch(`${API_BASE}/dashboard/overview`);
  return res.json();
}

export async function fetchAuditLogs() {
  const res = await fetch(`${API_BASE}/dashboard/audit-logs`);
  return res.json();
}