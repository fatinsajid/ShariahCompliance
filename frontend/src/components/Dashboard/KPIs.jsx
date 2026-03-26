const KPIs = ({
  totalCompanies = 0,
  compliancePct = 0,
  nonCompliancePct = 0,
  avgViolations = 0,
}) => {
  const cards = [
    { label: "Total Companies", value: totalCompanies },
    { label: "Compliance %", value: `${compliancePct}%` },
    { label: "Non-Compliance %", value: `${nonCompliancePct}%` },
    { label: "Average Violations", value: avgViolations },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
      {cards.map((card, i) => (
        <div
          key={i}
          className="p-5 bg-white rounded-xl shadow-md hover:shadow-lg transition"
        >
          <p className="text-gray-500 text-sm">{card.label}</p>
          <p className="text-2xl font-semibold mt-2">{card.value}</p>
        </div>
      ))}
    </div>
  );
};

export default KPIs;