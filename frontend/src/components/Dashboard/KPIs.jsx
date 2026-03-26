const KPIs = ({ totalCompanies, compliancePct, nonCompliancePct, avgViolations }) => {
  return (
    <div className="grid grid-cols-4 gap-6">
      <div className="p-4 bg-white rounded shadow text-center">
        <p className="text-gray-500 text-sm">Total Companies</p>
        <p className="text-2xl font-semibold">{totalCompanies}</p>
      </div>
      <div className="p-4 bg-white rounded shadow text-center">
        <p className="text-gray-500 text-sm">Compliance %</p>
        <p className="text-2xl font-semibold">{compliancePct}%</p>
      </div>
      <div className="p-4 bg-white rounded shadow text-center">
        <p className="text-gray-500 text-sm">Non-Compliance %</p>
        <p className="text-2xl font-semibold">{nonCompliancePct}%</p>
      </div>
      <div className="p-4 bg-white rounded shadow text-center">
        <p className="text-gray-500 text-sm">Average Violations</p>
        <p className="text-2xl font-semibold">{avgViolations}</p>
      </div>
    </div>
  );
};

export default KPIs;