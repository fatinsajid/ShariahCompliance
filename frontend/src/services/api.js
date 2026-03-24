export async function getDashboardOverview() {
  const res = await fetch("http://localhost:8000/dashboard/overview");

  if (!res.ok) throw new Error("Failed to fetch overview");

  return await res.json();
}

export async function getAuditLogs() {
  const res = await fetch("http://localhost:8000/dashboard/audit-logs");

  if (!res.ok) throw new Error("Failed to fetch logs");

  return await res.json();
}