export default function KPIs({ data }) {
  return (
    <div className="grid grid-cols-4 gap-6">

      <Card title="Total Companies" value={data.totalCompanies} />
      <Card title="Compliance %" value={`${data.compliancePercent}%`} green />
      <Card title="Non-Compliance %" value={`${data.nonCompliancePercent}%`} red />
      <Card title="Avg Violations" value={data.avgViolations} yellow />

    </div>
  );
}

function Card({ title, value, green, red, yellow }) {
  const color =
    green ? "text-green-600" :
    red ? "text-red-600" :
    yellow ? "text-yellow-600" :
    "text-gray-900";

  return (
    <div className="bg-white rounded-2xl p-6 shadow-sm">
      <p className="text-gray-500 text-sm">{title}</p>
      <p className={`text-2xl font-semibold mt-2 ${color}`}>{value}</p>
    </div>
  );
}