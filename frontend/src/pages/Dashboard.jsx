import { useEffect, useState } from "react";
import TopBar from "../components/dashboard/TopBar";
import Sidebar from "../components/dashboard/Sidebar";
import { supabase } from "../lib/supabaseClient";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";

const Dashboard = () => {
  const [data, setData] = useState({
    totalCompanies: 0,
    compliancePercent: 0,
    nonCompliancePercent: 0,
    avgViolations: 0,
    riskDistribution: [],
    recentAuditLogs: [],
  });

  const [loading, setLoading] = useState(true);

  const username = localStorage.getItem("username") || "John Doe";

  // 🔥 CORE FETCH FUNCTION (USED BY BOTH INITIAL LOAD + REALTIME)
  const fetchDashboard = async () => {
    try {
      setLoading(true);

      const { data: result, error } = await supabase.rpc(
        "get_dashboard_overview"
      );

      if (error) {
        console.error("RPC fetch error:", error);
        return;
      }

      if (!result) {
        console.warn("No dashboard data returned");
        return;
      }

      setData({
        totalCompanies: result.totalCompanies || 0,
        compliancePercent: result.compliancePercent || 0,
        nonCompliancePercent: result.nonCompliancePercent || 0,
        avgViolations: result.avgViolations || 0,
        riskDistribution: result.riskDistribution || [],
        recentAuditLogs: result.recentAuditLogs || [],
      });
    } catch (err) {
      console.error("Dashboard fetch error:", err);
    } finally {
      setLoading(false);
    }
  };

  // 🔁 INITIAL LOAD
  useEffect(() => {
    fetchDashboard();
  }, []);

  // ⚡ REALTIME SUBSCRIPTION
  useEffect(() => {
    const channel = supabase
      .channel("dashboard-live")
      .on(
        "postgres_changes",
        {
          event: "*",
          schema: "public",
          table: "compliance_audit_log",
        },
        () => {
          console.log("Realtime update triggered");
          fetchDashboard();
        }
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, []);

  // 📊 Transform risk data for chart
  const riskData = (data.riskDistribution || []).map((val, index) => {
    const levels = ["Low", "Moderate", "High", "Critical"];
    const colors = ["#22c55e", "#eab308", "#f97316", "#ef4444"];

    return {
      name: levels[index] || `Level ${index + 1}`,
      value: (val || 0) * 100,
      color: colors[index] || "#6366f1",
    };
  });

  // ⏳ LOADING STATE
  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <p className="text-gray-500 text-lg">Loading Dashboard...</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex bg-gray-100">
      {/* Sidebar */}
      <Sidebar username={username} />

      {/* Main Content */}
      <div className="flex-1 flex flex-col">
        <TopBar username={username} />

        <div className="p-6 grid grid-cols-2 gap-6">
          {/* KPI Cards */}
          <div className="bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-2">Total Companies</h3>
            <p className="text-2xl font-bold">
              {data.totalCompanies}
            </p>
          </div>

          <div className="bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-2">Compliance %</h3>
            <p className="text-2xl font-bold">
              {data.compliancePercent}%
            </p>
          </div>

          <div className="bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-2">
              Non-Compliance %
            </h3>
            <p className="text-2xl font-bold">
              {data.nonCompliancePercent}%
            </p>
          </div>

          <div className="bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-2">
              Average Violations
            </h3>
            <p className="text-2xl font-bold">
              {data.avgViolations}
            </p>
          </div>

          {/* Risk Distribution */}
          <div className="col-span-2 bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-4">
              Risk Distribution
            </h3>

            {riskData.length > 0 ? (
              <div className="w-full h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={riskData}>
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip
                      formatter={(value) => [`${value}%`, "Risk"]}
                    />
                    <Bar dataKey="value">
                      {riskData.map((entry, idx) => (
                        <Cell key={idx} fill={entry.color} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="text-gray-400 text-center py-20">
                No risk data available
              </p>
            )}
          </div>

          {/* Recent Audit Logs */}
          <div className="col-span-2 bg-white rounded-2xl shadow p-5">
            <h3 className="text-sm text-gray-500 mb-4">
              Recent Audit Logs
            </h3>

            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="text-gray-600 border-b">
                    <th className="py-2 px-3">Company</th>
                    <th className="py-2 px-3">Status</th>
                    <th className="py-2 px-3">Violations</th>
                    <th className="py-2 px-3">Date</th>
                  </tr>
                </thead>

                <tbody>
                  {data.recentAuditLogs?.length > 0 ? (
                    data.recentAuditLogs.map((log, idx) => (
                      <tr key={idx} className="border-b">
                        <td className="py-2 px-3">
                          {log.company}
                        </td>
                        <td className="py-2 px-3">
                          {log.status}
                        </td>
                        <td className="py-2 px-3">
                          {log.violations}
                        </td>
                        <td className="py-2 px-3">
                          {new Date(log.date).toLocaleDateString()}
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td
                        colSpan={4}
                        className="text-center text-gray-400 py-4"
                      >
                        No audit logs available
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;