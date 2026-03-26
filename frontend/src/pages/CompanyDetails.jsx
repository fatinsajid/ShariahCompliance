import { useEffect, useState } from "react";
import Sidebar from "../components/dashboard/Sidebar";
import TopBar from "../components/dashboard/Topbar";
import { getJWT } from "../lib/supabaseClient";

const dummyCompanies = [
  { id: 1, name: "Alpha Corp" },
  { id: 2, name: "Beta Ltd." },
  { id: 3, name: "Gamma Inc." },
];

const CompanyDetails = () => {
  const [selectedCompany, setSelectedCompany] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const API_URL = import.meta.env.VITE_API_URL;

  const fetchCompanyDetails = async (companyId) => {
    setLoading(true);
    try {
      const token = await getJWT();

      // Replace with real API later
      const res = await fetch(`${API_URL}/companies/${companyId}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });

      if (!res.ok) throw new Error("API error");

      const json = await res.json();
      setData(json);
    } catch (err) {
      console.warn("Using fallback data:", err);
      setData({
        risk_score: Math.random().toFixed(2),
        compliance_status: Math.random() > 0.5 ? "Compliant" : "Non-Compliant",
        explanation:
          "The company maintains compliance with Shariah screening standards. Minor deviations exist but are within acceptable limits.",
        violations: [
          { type: "Interest Exposure", severity: "High", date: "2026-03-20" },
          { type: "Late Disclosure", severity: "Medium", date: "2026-03-18" },
        ],
        scholar_reviews: [
          { text: "Aligned with Shariah principles.", author: "Scholar A" },
          { text: "Liquidity ratio needs monitoring.", author: "Scholar B" },
        ],
        anomaly_flag: Math.random() > 0.5 ? "Stable" : "Alert",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleSelectCompany = (e) => {
    const companyId = e.target.value;
    setSelectedCompany(companyId);
    if (companyId) fetchCompanyDetails(companyId);
    else setData(null);
  };

  const handleReset = () => {
    setSelectedCompany(null);
    setData(null);
  };

  const handleDownloadPDF = () => {
    alert("PDF download feature coming soon!");
  };

  const handleDownloadCSV = () => {
    alert("CSV download feature coming soon!");
  };

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <TopBar user={{ name: "Admin User" }} />

        <div className="p-6">
          <h1 className="text-2xl font-bold text-gray-800 mb-4">
            Company Details
          </h1>

          <div className="grid grid-cols-4 grid-rows-4 gap-6 h-[calc(100vh-160px)]">

            {/* Select Company */}
            <div className="bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-2">Select Company</h3>
              <select
                className="w-full border border-gray-300 rounded px-3 py-2"
                value={selectedCompany || ""}
                onChange={handleSelectCompany}
              >
                <option value="">-- Select --</option>
                {dummyCompanies.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name}
                  </option>
                ))}
              </select>
            </div>

            {/* Risk Score */}
            <div className="bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-2">Risk Score</h3>
              <p className="text-3xl font-bold text-indigo-600">
                {data?.risk_score ?? "-"}
              </p>
            </div>

            {/* Compliance */}
            <div className="bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-2">Compliance</h3>
              <span
                className={`px-3 py-1 rounded-full text-sm font-medium ${
                  data?.compliance_status === "Compliant"
                    ? "bg-green-100 text-green-700"
                    : "bg-red-100 text-red-700"
                }`}
              >
                {data?.compliance_status ?? "-"}
              </span>
            </div>

            {/* Explanation */}
            <div className="bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-2">Explanation</h3>
              <p className="text-sm text-gray-700 leading-relaxed">
                {data?.explanation ?? "No explanation available"}
              </p>
            </div>

            {/* Violations */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5 overflow-auto">
              <h3 className="text-sm text-gray-500 mb-4">Violations</h3>
              {data?.violations?.length > 0 ? (
                <table className="w-full text-sm">
                  <thead className="text-gray-500 border-b">
                    <tr>
                      <th className="text-left py-2">Type</th>
                      <th className="text-left py-2">Severity</th>
                      <th className="text-left py-2">Date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data?.violations?.map((v, i) => (
                      <tr key={i} className="border-b">
                        <td className="py-2">{v.type}</td>
                        <td
                          className={`py-2 ${
                            v.severity === "High"
                              ? "text-red-500"
                              : "text-yellow-500"
                          }`}
                        >
                          {v.severity}
                        </td>
                        <td className="py-2">{v.date}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p>No violation data</p>
              )}
            </div>

            {/* Scholar Reviews */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-4">Scholar Reviews</h3>
              {data?.scholar_reviews?.length > 0 ? (
                <div className="space-y-3">
                  {data.scholar_reviews.map((r, i) => (
                    <div key={i} className="p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-700">{r.text}</p>
                      <p className="text-xs text-gray-400 mt-1">– {r.author}</p>
                    </div>
                  ))}
                </div>
              ) : (
                <p>No scholar reviews</p>
              )}
            </div>

            {/* Anomaly */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-4">Anomaly Detection</h3>
              <div className="flex items-center justify-between">
                <p className="text-gray-700 text-sm">
                  {data?.anomaly_flag === "Stable"
                    ? "No anomalies detected"
                    : "Anomaly detected"}
                </p>
                <span
                  className={`px-3 py-1 rounded-full text-sm ${
                    data?.anomaly_flag === "Stable"
                      ? "bg-green-100 text-green-700"
                      : "bg-red-100 text-red-700"
                  }`}
                >
                  {data?.anomaly_flag ?? "-"}
                </span>
              </div>
            </div>

            {/* Buttons */}
            <div className="col-span-1 row-span-1 rounded-2xl p-5 flex flex-col gap-2 items-start">
              <button
                className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300"
                onClick={handleReset}
              >
                Reset
              </button>
              <button
                className="px-4 py-2 bg-blue-700 text-white rounded hover:bg-indigo-700"
                onClick={handleDownloadPDF}
              >
                Download PDF
              </button>
              <button
                className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700"
                onClick={handleDownloadCSV}
              >
                Download CSV
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CompanyDetails;