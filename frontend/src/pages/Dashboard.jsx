import { useEffect, useState } from "react";
import KPIs from "../components/Dashboard/KPIs";
import ComplianceChart from "../components/Dashboard/ComplianceChart";
import RiskDistribution from "../components/Dashboard/RiskDistribution";
import RecentAuditLogs from "../components/Dashboard/RecentAuditLogs";
import { fetchDashboardData, fetchAuditLogs } from "../api/api";

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    async function load() {
      try {
        const dashboard = await fetchDashboardData();
        const audit = await fetchAuditLogs();

        setData(dashboard);
        setLogs(audit.logs || []);
      } catch (err) {
        console.error("Dashboard error:", err);
      }
    }

    load();
  }, []);

  if (!data) {
    return (
      <div className="h-screen flex items-center justify-center text-xl font-semibold">
        Loading Dashboard...
      </div>
    );
  }

  return (
    <div className="bg-[#F9FAFB] min-h-screen p-6 space-y-6">

      <KPIs data={data.kpis} />

      <div className="grid grid-cols-2 gap-6">
        <ComplianceChart data={data.complianceChart} />
        <RiskDistribution data={data.riskChart} />
      </div>

      <RecentAuditLogs logs={logs} />
    </div>
  );
}