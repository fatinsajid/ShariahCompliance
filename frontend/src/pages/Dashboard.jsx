// src/pages/Dashboard.jsx
import { useEffect, useState } from "react";
import { supabase, getJWT } from "../lib/supabaseClient";
import KPIs from "../components/dashboard/KPIs";
import ComplianceChart from "../components/dashboard/ComplianceChart";
import RiskDistribution from "../components/dashboard/RiskDistribution";
import RecentAuditLogs from "../components/dashboard/RecentAuditLogs";
import Sidebar from "../components/dashboard/Sidebar";
import TopBar from "../components/dashboard/TopBar";

const DashboardPage = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchDashboard = async () => {
    setLoading(true);
    setError(null);
    try {
      const token = await getJWT();

      if (!token) {
        console.warn("No auth token found, using mock dashboard data");
        setData({
          total_companies: 10,
          compliance_pct: 80,
          non_compliance_pct: 20,
          avg_violations: 2,
          risk_distribution: [10, 5, 3, 7, 2],
          recent_audit_logs: [
            { company: "ABC Corp", status: "Compliant", date: "2026-03-26" },
            { company: "XYZ Ltd", status: "Non-Compliant", date: "2026-03-25" },
          ],
        });
        return;
      }

      const API_URL = import.meta.env.VITE_API_URL;
      if (!API_URL) throw new Error("VITE_API_URL is undefined");

      const res = await fetch(`${API_URL}/dashboard/overview`, {
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error("Dashboard fetch error:", err);
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDashboard();
  }, []);

  return (
    <div className="flex min-h-screen bg-gray-50">
      {/* Sidebar */}
      <Sidebar />

      {/* Main content */}
      <div className="flex-1 flex flex-col">
        {/* Top Bar */}
        <TopBar username="John Doe" />

        {/* Page content */}
        <div className="flex-1 overflow-auto p-6 space-y-6">
          {loading && (
            <div className="animate-pulse grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
              {[...Array(4)].map((_, i) => (
                <div key={i} className="h-24 bg-gray-200 rounded shadow"></div>
              ))}
              <div className="animate-pulse grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
                <div className="h-64 bg-gray-200 rounded shadow"></div>
                <div className="h-64 bg-gray-200 rounded shadow"></div>
              </div>
              <div className="animate-pulse bg-gray-200 h-48 rounded shadow mt-6"></div>
            </div>
          )}

          {error && (
            <div className="text-red-500">
              <p>Error: {error}</p>
              <button
                onClick={fetchDashboard}
                className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 transition"
              >
                Retry
              </button>
            </div>
          )}

          {!loading && !error && data && (
            <>
              {/* Page title */}
              <h1 className="text-2xl font-bold text-gray-800 mb-4">
                Dashboard
              </h1>

              {/* KPIs */}
              <KPIs
                totalCompanies={data?.total_companies ?? 0}
                compliancePct={data?.compliance_pct ?? 0}
                nonCompliancePct={data?.non_compliance_pct ?? 0}
                avgViolations={data?.avg_violations ?? 0}
              />

              {/* Charts */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ComplianceChart
                  compliancePct={data?.compliance_pct ?? 0}
                  nonCompliancePct={data?.non_compliance_pct ?? 0}
                />
                <RiskDistribution
                  riskScores={Array.isArray(data?.risk_distribution) ? data.risk_distribution : []}
                />
              </div>

              {/* Recent Audit Logs */}
              <RecentAuditLogs
                logs={Array.isArray(data?.recent_audit_logs) ? data.recent_audit_logs : []}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;