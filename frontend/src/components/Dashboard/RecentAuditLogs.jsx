export default function RecentAuditLogs({ logs }) {
  return (
    <div className="bg-white p-6 rounded-2xl shadow-sm">
      <h2 className="text-lg font-semibold mb-4">Recent Audit Logs</h2>

      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-gray-500 border-b">
            <th>Company</th>
            <th>Status</th>
            <th>Violations</th>
            <th>Date</th>
          </tr>
        </thead>

        <tbody>
          {logs.map((log, i) => (
            <tr key={i} className="border-b">
              <td>{log.company_id}</td>
              <td className={log.status === "PASS" ? "text-green-600" : "text-red-600"}>
                {log.status}
              </td>
              <td>{log.violations?.length || 0}</td>
              <td>{new Date(log.timestamp).toLocaleDateString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}