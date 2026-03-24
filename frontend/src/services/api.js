// src/api.js

export const API_BASE = "https://shariahcompliance.onrender.com"; // replace with your Render URL

// Helper function to add auth headers
const authHeaders = (token) => ({
  "Content-Type": "application/json",
  "Authorization": `Bearer ${token}`,
});

// ----------------------------
// Dashboard Overview
// ----------------------------
export async function getDashboardOverview(token) {
  const res = await fetch(`${API_BASE}/dashboard/overview`, {
    headers: authHeaders(token),
  });
  return res.json();
}

// ----------------------------
// Audit Logs
// ----------------------------
export async function getAuditLogs(token) {
  const res = await fetch(`${API_BASE}/dashboard/audit-logs`, {
    headers: authHeaders(token),
  });
  return res.json();
}

// ----------------------------
// Compliance Check for Single Company
// ----------------------------
export async function runCompliance(companyId, token) {
  const res = await fetch(`${API_BASE}/compliance/${companyId}`, {
    headers: authHeaders(token),
  });
  return res.json();
}

// ----------------------------
// Risk Prediction
// ----------------------------
export async function getRiskPrediction(companyId, token) {
  const res = await fetch(`${API_BASE}/predict/${companyId}`, {
    headers: authHeaders(token),
  });
  return res.json();
}

// ----------------------------
// Scholar Review Assignment
// ----------------------------
export async function assignScholarReview(companyId, token) {
  const res = await fetch(`${API_BASE}/assign_review/${companyId}`, {
    method: "POST",
    headers: authHeaders(token),
  });
  return res.json();
}

// ----------------------------
// Bulk Upload for Screening
// ----------------------------
export async function bulkScreen(file, token) {
  const formData = new FormData();
  formData.append("file", file);

  const res = await fetch(`${API_BASE}/screen/bulk`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${token}`,
    },
    body: formData,
  });

  return res.json();
}

// ----------------------------
// CSV Download
// ----------------------------
export async function downloadCSV(results, token) {
  const res = await fetch(`${API_BASE}/download/csv`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify(results),
  });

  const blob = await res.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "results.csv";
  a.click();
  window.URL.revokeObjectURL(url);
}