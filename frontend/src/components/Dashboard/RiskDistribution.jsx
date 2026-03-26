import { BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

const RiskDistribution = ({ riskScores }) => {
  const data = riskScores.map((score, index) => ({ name: `C${index + 1}`, score }));

  return (
    <div className="p-4 bg-white rounded shadow">
      <p className="text-gray-500 text-sm mb-2">Risk Distribution</p>
      <BarChart width={350} height={250} data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="name" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="score" fill="#4F46E5" />
      </BarChart>
    </div>
  );
};

export default RiskDistribution;