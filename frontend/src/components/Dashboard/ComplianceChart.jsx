import { PieChart, Pie, Cell, Tooltip, Legend } from "recharts";

const ComplianceChart = ({ compliancePct, nonCompliancePct }) => {
  const data = [
    { name: "Compliant", value: compliancePct },
    { name: "Non-Compliant", value: nonCompliancePct },
  ];

  const COLORS = ["#22C55E", "#EF4444"];

  return (
    <div className="p-4 bg-white rounded shadow">
      <p className="text-gray-500 text-sm mb-2">Compliance Chart</p>
      <PieChart width={250} height={250}>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          outerRadius={80}
          fill="#8884d8"
          label
        >
          {data.map((entry, index) => (
            <Cell key={`cell-${index}`} fill={COLORS[index]} />
          ))}
        </Pie>
        <Tooltip />
        <Legend />
      </PieChart>
    </div>
  );
};

export default ComplianceChart;