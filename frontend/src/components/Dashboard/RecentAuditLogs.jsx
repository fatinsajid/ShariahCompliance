// src/components/Dashboard/RecentAuditLogs.jsx
import React from "react";

const RecentAuditLogs = ({ logs }) => {
  return (
    <div className="bg-white p-6 rounded-xl shadow">
      <h2 className="text-lg font-semibold mb-4">Recent Audit Logs</h2>
      <table className="table-auto w-full text-sm">
        <thead>
          <tr className="bg-gray-100">
            <th className="px-4 py-2">Company ID</th>
            <th className="px-4 py-2">Rule</th>
            <th className="px-4 py-2">Status</th>
            <th className="px-4 py-2">Date</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log, idx) => (
            <tr key={idx} className="even:bg-gray-50">
              <td className="px-4 py-2">{log.company_id}</td>
              <td className="px-4 py-2">{log.rule_code}</td>
              <td className="px-4 py-2">{log.status}</td>
              <td className="px-4 py-2">{new Date(log.timestamp).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default RecentAuditLogs;