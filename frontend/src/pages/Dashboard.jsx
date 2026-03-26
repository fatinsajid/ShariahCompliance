import { useEffect, useState } from "react";
import { supabase, getJWT } from "../lib/supabaseClient";
import KPIs from "../components/dashboard/KPIs";
import ComplianceChart from "../components/dashboard/ComplianceChart";
import RiskDistribution from "../components/dashboard/RiskDistribution";
import RecentAuditLogs from "../components/dashboard/RecentAuditLogs";

const Dashboard = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchDashboard = async () => {
      const token = await getJWT();
      if (!token) {
        setLoading(false);
        return;
      }

      try {
        const res = await fetch(`${import.meta.env.VITE_API_URL}/dashboard/overview`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const json = await res.json();
        setData(json);
      } catch (err) {
        console.error("Error fetching dashboard:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchDashboard();
  }, []);

  if (loading) return <div className="p-6 text-gray-500">Loading Dashboard...</div>;
  if (!data) return <div className="p-6 text-red-500">Unable to load dashboard data</div>;

  return (
    <div className="p-6 space-y-6">
      {/* Row 1: KPIs */}
      <KPIs 
        totalCompanies={data.total_companies}
        compliancePct={data.compliance_pct}
        nonCompliancePct={data.non_compliance_pct}
        avgViolations={data.avg_violations}
      />

      {/* Row 2: Charts */}
      <div className="grid grid-cols-2 gap-6">
        <ComplianceChart compliancePct={data.compliance_pct} nonCompliancePct={data.non_compliance_pct} />
        <RiskDistribution riskScores={data.risk_distribution} />
      </div>

      {/* Row 3: Recent Audit Logs */}
      <RecentAuditLogs />
    </div>
  );
};

export default Dashboard;