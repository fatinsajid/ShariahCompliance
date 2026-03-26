export default function KPIs({ data }) {
  return (
    <>
      <div className="bg-white p-6 rounded-lg shadow flex flex-col justify-center items-center">
        <div className="text-gray-500 text-sm">Total Companies</div>
        <div className="text-2xl font-bold">{data.totalCompanies}</div>
      </div>
      <div className="bg-white p-6 rounded-lg shadow flex flex-col justify-center items-center">
        <div className="text-gray-500 text-sm">Compliance %</div>
        <div className="text-2xl font-bold">{data.compliancePercent}%</div>
      </div>
      <div className="bg-white p-6 rounded-lg shadow flex flex-col justify-center items-center">
        <div className="text-gray-500 text-sm">Non-Compliance %</div>
        <div className="text-2xl font-bold">{data.nonCompliancePercent}%</div>
      </div>
      <div className="bg-white p-6 rounded-lg shadow flex flex-col justify-center items-center">
        <div className="text-gray-500 text-sm">Avg Violations</div>
        <div className="text-2xl font-bold">{data.avgViolations}</div>
      </div>
    </>
  );
}