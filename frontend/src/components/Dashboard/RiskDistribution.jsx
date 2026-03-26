import { BarChart, Bar, XAxis, YAxis, Tooltip } from "recharts";

export default function RiskDistribution({ data }) {
  return (
    <div className="bg-white p-6 rounded-2xl shadow-sm">
      <h2 className="text-lg font-semibold mb-4">Risk Distribution</h2>

      <BarChart width={350} height={250} data={data}>
        <XAxis dataKey="riskLevel" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="count" fill="#3B82F6" />
      </BarChart>
    </div>
  );
}