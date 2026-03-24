// src/components/Dashboard/KPIs.jsx
import React from "react";

const KPIs = ({ data }) => {
  const { totalCompanies, compliant, nonCompliant, avgViolations } = data;

  return (
    <div className="grid grid-cols-4 gap-6">
      <div className="bg-white p-6 rounded-xl shadow flex flex-col items-center justify-center">
        <span className="text-gray-500">Total Companies</span>
        <span className="text-2xl font-bold">{totalCompanies}</span>
      </div>
      <div className="bg-white p-6 rounded-xl shadow flex flex-col items-center justify-center">
        <span className="text-gray-500">Compliant</span>
        <span className="text-2xl font-bold">{compliant}</span>
      </div>
      <div className="bg-white p-6 rounded-xl shadow flex flex-col items-center justify-center">
        <span className="text-gray-500">Non-Compliant</span>
        <span className="text-2xl font-bold">{nonCompliant}</span>
      </div>
      <div className="bg-white p-6 rounded-xl shadow flex flex-col items-center justify-center">
        <span className="text-gray-500">Avg Violations</span>
        <span className="text-2xl font-bold">{avgViolations}</span>
      </div>
    </div>
  );
};

export default KPIs;