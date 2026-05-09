import { useEffect, useState } from "react";
import Sidebar from "../components/Dashboard/sidebar";
import TopBar from "../components/dashboard/Topbar";
import { supabase } from "../lib/supabaseClient";

const CompanyDetails = () => {
  const [companies, setCompanies] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const username = localStorage.getItem("username") || "John Doe";

  // Fetch companies
  const fetchCompanies = async () => {
    const { data: companyData, error } = await supabase
      .from("compliance_audit_log")
      .select("company_id, company_name")
      .order("company_name", { ascending: true });

    if (error) {
      console.error("Error fetching companies:", error);
      setCompanies([]);
    } else {
      setCompanies(companyData);
    }
  };

  // Fetch company details
  const fetchCompanyDetails = async (companyId) => {
    if (!companyId) return;

    setLoading(true);
    try {
      const { data: companyDetail, error } = await supabase
        .from("compliance_audit_log")
        .select("*")
        .eq("company_id", companyId)
        .single();

      if (error || !companyDetail) {
        console.warn("No company details found:", error);
        setData({
          company_name: "-",
          risk_score: "-",
          compliance_status: "-",
          explanation: "No data available",
          violations_count: 0,
          scholar_reviews: [],
          anomaly_flag: "-",
        });
      } else {
        // Parse scholar_reviews safely
        let reviews = [];
        try {
          if (companyDetail.scholar_reviews) {
            reviews = Array.isArray(companyDetail.scholar_reviews)
              ? companyDetail.scholar_reviews
              : JSON.parse(companyDetail.scholar_reviews);
          }
        } catch {
          reviews = [];
        }

        setData({
          company_name: companyDetail.company_name ?? "-",
          risk_score: companyDetail.risk_score ?? "-",
          compliance_status: companyDetail.compliance_status ?? "-",
          explanation: companyDetail.explanation ?? "No explanation available",
          violations_count: companyDetail.violations_count ?? 0,
          scholar_reviews: reviews,
          anomaly_flag: companyDetail.anomaly_flag ?? "-",
        });
      }
    } catch (err) {
      console.error("Error fetching company details:", err);
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  const handleSelectCompany = (e) => {
    const companyId = e.target.value;
    setSelectedCompany(companyId);
    fetchCompanyDetails(companyId);
  };

  const handleReset = () => {
    setSelectedCompany(null);
    setData(null);
  };

  useEffect(() => {
    fetchCompanies();
  }, []);

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <TopBar username={username} />

        <div className="p-6">
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
                {companies.map((c) => (
                  <option key={c.company_id} value={c.company_id}>
                    {c.company_name}
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
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5 flex flex-col justify-center items-center">
              <h3 className="text-sm text-gray-500 mb-4">Violations</h3>
              <p className="text-xl font-bold text-red-500">
                {data?.violations_count ?? 0} violation(s)
              </p>
            </div>

            {/* Scholar Reviews */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5">
              <h3 className="text-sm text-gray-500 mb-4">Scholar Reviews</h3>
              {data?.scholar_reviews?.length > 0 ? (
                <div className="space-y-3">
                  {data.scholar_reviews.map((r, i) => (
                    <div key={i} className="p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-700">{r.text ?? "No text"}</p>
                      <p className="text-xs text-gray-400 mt-1">
                        – {r.author ?? "Unknown"}
                      </p>
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
            <div className="col-span-1 row-span-1 rounded-2xl p-5 flex gap-2 items-end justify-start">
              <button
                className="bg-gray-200 rounded px-4 py-2 hover:bg-gray-300"
                onClick={handleReset}
              >
                Reset
              </button>
              <button
                className="bg-blue-700 text-white rounded px-4 py-2 hover:bg-indigo-700"
                onClick={() => alert("PDF download coming soon")}
              >
                Download PDF
              </button>
              <button
                className="bg-green-600 text-white rounded px-4 py-2 hover:bg-green-700"
                onClick={() => alert("CSV download coming soon")}
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