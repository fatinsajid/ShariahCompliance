import { BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

const RiskDistribution = ({ riskScores = [] }) => {
  if (riskScores.length === 0)
    return (
      <div className="p-4 bg-white rounded shadow text-gray-400">
        No risk distribution data available
      </div>
    );

  const data = riskScores.map((score, index) => ({
    name: `C${index + 1}`,
    score,
  }));

  return (
    <div className="p-4 bg-white rounded shadow text-center">
      <p className="text-gray-500 text-lg mb-5">Risk Distribution</p>
      <BarChart width={350} height={250} data={data} >
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