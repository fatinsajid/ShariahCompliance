export default function RecentAuditLogs({ logs }) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-gray-700 font-semibold mb-4">Recent Audit Logs</h3>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-4 py-2 text-left text-gray-500 text-sm">Company</th>
              <th className="px-4 py-2 text-left text-gray-500 text-sm">Status</th>
              <th className="px-4 py-2 text-left text-gray-500 text-sm">Violations</th>
              <th className="px-4 py-2 text-left text-gray-500 text-sm">Date</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {logs.map((log, idx) => (
              <tr key={idx}>
                <td className="px-4 py-2">{log.company}</td>
                <td className="px-4 py-2">{log.status}</td>
                <td className="px-4 py-2">{log.violations.join(", ")}</td>
                <td className="px-4 py-2">{new Date(log.date).toLocaleDateString()}</td>
              </tr>
            ))}
            {logs.length === 0 && (
              <tr>
                <td colSpan="4" className="px-4 py-2 text-center text-gray-400">
                  No logs available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}