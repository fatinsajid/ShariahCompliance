import React, { useEffect, useState } from "react";
import KPIs from "../components/dashboard/KPIs";
import ComplianceChart from "../components/dashboard/ComplianceChart";
import RiskDistribution from "../components/dashboard/RiskDistribution";
import RecentAuditLogs from "../components/dashboard/RecentAuditLogs";

export default function Dashboard() {
  const [loading, setLoading] = useState(true);
  const [kpiData, setKpiData] = useState({
    totalCompanies: 0,
    compliancePercent: 0,
    nonCompliancePercent: 0,
    avgViolations: 0
  });
  const [complianceData, setComplianceData] = useState([]);
  const [riskData, setRiskData] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);

  useEffect(() => {
    async function fetchDashboard() {
      try {
        // Example API calls
        const resOverview = await fetch("/dashboard/overview", {
          headers: { Authorization: `Bearer ${localStorage.getItem("supabase_token")}` }
        });
        const overview = await resOverview.json();

        setKpiData({
          totalCompanies: overview.totalCompanies,
          compliancePercent: overview.compliancePercent,
          nonCompliancePercent: overview.nonCompliancePercent,
          avgViolations: overview.avgViolations
        });

        setComplianceData([
          { name: "Compliant", value: overview.compliantCount },
          { name: "Non-Compliant", value: overview.nonCompliantCount }
        ]);

        setRiskData(overview.riskDistribution); // [{riskLevel: "Low", count: 5}, ...]
        setAuditLogs(overview.recentAuditLogs); // [{company, status, violations, date}, ...]

      } catch (err) {
        console.error("Dashboard fetch error:", err);
      } finally {
        setLoading(false);
      }
    }

    fetchDashboard();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center items-center h-screen text-gray-600 font-semibold">
        Loading Dashboard...
      </div>
    );
  }

  return (
    <div className="p-6 bg-gray-50 min-h-screen space-y-6">
      
      {/* Row 1: KPIs */}
      <div className="grid grid-cols-4 gap-6">
        <KPIs data={kpiData} />
      </div>

      {/* Row 2: Charts */}
      <div className="grid grid-cols-2 gap-6">
        <ComplianceChart data={complianceData} />
        <RiskDistribution data={riskData} />
      </div>

      {/* Row 3: Recent Audit Logs */}
      <RecentAuditLogs logs={auditLogs} />

    </div>
  );
}