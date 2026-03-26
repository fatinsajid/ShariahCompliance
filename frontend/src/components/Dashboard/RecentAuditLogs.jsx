const RecentAuditLogs = () => {
  // Placeholder static logs, replace with API call if needed
  const logs = [
    { company: "ABC Ltd", date: "2026-03-26", status: "Compliant" },
    { company: "XYZ Corp", date: "2026-03-25", status: "Non-Compliant" },
  ];

  return (
    <div className="p-4 bg-white rounded shadow">
      <p className="text-gray-500 text-sm mb-2">Recent Audit Logs</p>
      <table className="w-full text-left">
        <thead>
          <tr>
            <th className="border-b p-2">Company</th>
            <th className="border-b p-2">Date</th>
            <th className="border-b p-2">Status</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log, i) => (
            <tr key={i}>
              <td className="p-2">{log.company}</td>
              <td className="p-2">{log.date}</td>
              <td className={`p-2 font-semibold ${log.status === "Compliant" ? "text-green-600" : "text-red-600"}`}>{log.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default RecentAuditLogs;