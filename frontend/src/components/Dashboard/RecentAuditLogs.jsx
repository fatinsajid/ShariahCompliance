const RecentAuditLogs = ({ logs = [] }) => {
  if (logs.length === 0)
    return (
      <div className="p-4 bg-white rounded shadow text-gray-400">
        No recent audit logs
      </div>
    );

  return (
    <div className="p-4 bg-white rounded shadow overflow-x-auto">
      <p className="text-gray-500 font-bold text-lg mb-5">Recent Audit Logs</p>
      <table className="w-full table-auto text-left">
        <thead>
          <tr className="border-b">
            <th className="p-2">Company</th>
            <th className="p-2">Status</th>
            <th className="p-2">Date</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log, i) => (
            <tr key={i} className="border-b hover:bg-gray-50">
              <td className="p-2">{log.company ?? "N/A"}</td>
              <td className="p-2">{log.status ?? "N/A"}</td>
              <td className="p-2">{log.date ?? "N/A"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default RecentAuditLogs;