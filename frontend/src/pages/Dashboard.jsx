import React, { useEffect, useState } from "react";
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from "recharts";
import axios from "axios";

const COLORS = ["#22c55e", "#ef4444", "#fbbf24"]; // green, red, yellow

export default function Dashboard() {
  const [kpiData, setKpiData] = useState({
    totalCompanies: 0,
    compliant: 0,
    nonCompliant: 0,
    avgViolations: 0
  });
  const [complianceChart, setComplianceChart] = useState([]);
  const [riskChart, setRiskChart] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);

  useEffect(() => {
    async function fetchData() {
      try {
        // Replace these with your backend endpoints
        const kpiRes = await axios.get("/dashboard/overview");
        setKpiData(kpiRes.data.kpis);
        setComplianceChart(kpiRes.data.complianceChart);
        setRiskChart(kpiRes.data.riskChart);

        const logsRes = await axios.get("/dashboard/audit-logs");
        setAuditLogs(logsRes.data.logs);
      } catch (error) {
        console.error("Dashboard fetch error:", error);
      }
    }
    fetchData();
  }, []);

  return (
    <div className="container mx-auto p-4">
      {/* ---------------- Row 1: KPI cards ---------------- */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
        <div className="card flex flex-col items-center justify-center">
          <div className="text-3xl font-bold">{kpiData.totalCompanies}</div>
          <div className="text-gray-500 mt-2">Total Companies</div>
        </div>
        <div className="card flex flex-col items-center justify-center">
          <div className="text-3xl font-bold">{kpiData.compliant}</div>
          <div className="text-green-600 mt-2">Compliant</div>
        </div>
        <div className="card flex flex-col items-center justify-center">
          <div className="text-3xl font-bold">{kpiData.nonCompliant}</div>
          <div className="text-red-600 mt-2">Non-Compliant</div>
        </div>
        <div className="card flex flex-col items-center justify-center">
          <div className="text-3xl font-bold">{kpiData.avgViolations}</div>
          <div className="text-yellow-600 mt-2">Avg Violations</div>
        </div>
      </div>

      {/* ---------------- Row 2: Charts ---------------- */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        {/* Compliance Pie Chart */}
        <div className="card p-6">
          <div className="section-title">Compliance Distribution</div>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie
                data={complianceChart}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={80}
                fill="#8884d8"
                label
              >
                {complianceChart.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Risk Distribution Bar Chart */}
        <div className="card p-6">
          <div className="section-title">Risk Distribution</div>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={riskChart}>
              <XAxis dataKey="riskLevel" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ---------------- Row 3: Audit Logs ---------------- */}
      <div className="card p-6">
        <div className="section-title">Recent Audit Logs</div>
        <div className="overflow-x-auto">
          <table className="table w-full">
            <thead>
              <tr>
                <th>Company</th>
                <th>Status</th>
                <th>Violations</th>
                <th>Last Reviewed</th>
              </tr>
            </thead>
            <tbody>
              {auditLogs.map((log, index) => (
                <tr key={index}>
                  <td>{log.company}</td>
                  <td>
                    <span
                      className={`${
                        log.status === "Compliant" ? "badge-compliant" : "badge-noncompliant"
                      }`}
                    >
                      {log.status}
                    </span>
                  </td>
                  <td>{log.violations.join(", ")}</td>
                  <td>{log.lastReviewed}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}