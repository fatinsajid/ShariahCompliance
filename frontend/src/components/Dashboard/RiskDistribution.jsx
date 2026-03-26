import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

export default function RiskDistribution({ data }) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-gray-700 font-semibold mb-4">Risk Distribution</h3>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={data}>
          <XAxis dataKey="riskLevel" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="count" fill="#60A5FA" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}