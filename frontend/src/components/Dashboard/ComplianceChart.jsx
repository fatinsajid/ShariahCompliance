import { PieChart, Pie, Tooltip, Cell } from "recharts";

const ComplianceChart = ({
  compliancePct = 0,
  nonCompliancePct = 0,
}) => {
  const data = [
    { name: "Compliant", value: compliancePct },
    { name: "Non-Compliant", value: nonCompliancePct },
  ];

  const COLORS = ["#10B981", "#EF4444"];

  return (
    <div className="p-4 bg-white rounded shadow text-center">
      <p className="text-gray-500 text-lg mb-5">Compliance Overview</p>
      <PieChart width={350} height={250}>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          outerRadius={80}
          label
        >
          {data.map((entry, index) => (
            <Cell key={index} fill={COLORS[index]} />
          ))}
        </Pie>
        <Tooltip />
      </PieChart>
    </div>
  );
};

export default ComplianceChart;