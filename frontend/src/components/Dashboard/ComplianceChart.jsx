import { PieChart, Pie, Cell, Tooltip } from "recharts";

const COLORS = ["#22C55E", "#EF4444"];

export default function ComplianceChart({ data }) {
  return (
    <div className="bg-white p-6 rounded-2xl shadow-sm">
      <h2 className="text-lg font-semibold mb-4">Compliance</h2>

      <PieChart width={250} height={250}>
        <Pie data={data} dataKey="value" outerRadius={90}>
          {data.map((_, i) => (
            <Cell key={i} fill={COLORS[i]} />
          ))}
        </Pie>
        <Tooltip />
      </PieChart>
    </div>
  );
}