// DataAnalysis.jsx
import { useState, useEffect } from "react";
import { supabase } from "../lib/supabaseClient"; // single instance
import axios from "axios";
import Sidebar from "../components/Dashboard/sidebar";
import Topbar from "../components/Dashboard/TopBar";
import jsPDF from "jspdf";

const DataAnalysis = () => {
  const [singleData, setSingleData] = useState({
    companyName: "",
    companyIndustry: "",
    totalAsset: "",
    totalDebt: "",
    totalIncome: "",
    nonHalalIncome: "",
    cashInterestSecurities: "",
  });
  const username = localStorage.getItem("username") || "John Doe";
  const [submitted, setSubmitted] = useState(false);
  const [bulkFile, setBulkFile] = useState(null);
  const [industries, setIndustries] = useState([]);
  const [analysisResult, setAnalysisResult] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [bulkResults, setBulkResults] = useState([]);
  const [errors, setErrors] = useState({});
  const downloadSingleCSV = (data) => {
  if (!data) return;

  const headers = [
    "company_id",
    "risk_score",
    "status",
    "violations",
    "debt_ratio",
    "liquidity_ratio",
    "non_halal_income_ratio"
  ];

  const f = data.result.features || {};

  const row = [
    data.company_id,
    data.result.risk_score,
    data.result.status,
    data.result.violations.join("; "),
    f.debt_ratio,
    f.liquidity_ratio,
    f.non_halal_income_ratio
  ];

  const csv = headers.join(",") + "\n" + row.join(",");

  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = `${data.company_id}.csv`;
  a.click();
};
  useEffect(() => {
    const fetchIndustries = async () => {
      const { data, error } = await supabase
        .from("compliance_audit_log")
        .select("company_industry")
        .neq("company_industry", null)
        .order("company_industry", { ascending: true });

      if (error) console.error(error);
      else {
  const uniqueIndustries = [
    ...new Set(
      data
        .map((i) => i.company_industry)
        .filter(Boolean)
        .map((i) => i.trim())
    ),
  ];
  setIndustries(uniqueIndustries);
}
    };
    fetchIndustries();
  }, []);
  const downloadBulkCSV = () => {
  if (!bulkResults.length) return;

  const headers = [
    "company_id",
    "risk_score",
    "status",
    "debt_ratio",
    "liquidity_ratio",
    "non_halal_income_ratio"
  ];

  const rows = bulkResults.map((item, idx) => {
    const f = item.result.features || {};
  
    return [
      item.company_id,
      item.result.risk_score,
      item.result.status,
      f.debt_ratio,
      f.liquidity_ratio,
      f.non_halal_income_ratio
    ].join(",");
  });

  const csv = headers.join(",") + "\n" + rows.join("\n");

  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = "bulk_analysis.csv";
  a.click();
};
const downloadBulkPDF = () => {
  if (!bulkResults.length) return;

  const doc = new jsPDF();

  doc.text("Bulk Shariah Compliance Report", 10, 10);

  let y = 20;

  bulkResults.forEach((item, i) => {
    const f = item.result.features || {};

    doc.text(`Company ${i + 1}: ${item.company_id}`, 10, y);
    y += 10;

    doc.text(`Score: ${item.result.risk_score}`, 10, y);
    y += 10;

    doc.text(`Status: ${item.result.status}`, 10, y);
    y += 10;

    doc.text(`Debt: ${f.debt_ratio}`, 10, y);
    y += 10;

    if (y > 270) {
      doc.addPage();
      y = 10;
    }
  });

  doc.save("bulk_report.pdf");
};

  const handleSingleChange = (e) => {
    const { name, value } = e.target;
    setSingleData({ ...singleData, [name]: value });
    setErrors({ ...errors, [name]: "" });
  };

  const handleSingleSubmit = async () => {
  if (!singleData.companyName.trim()) {
    alert("Company name is required");
    return;
  }

  try {
    setUploading(true);

    // ✅ Get logged in user session
    const {
      data: { session },
    } = await supabase.auth.getSession();

    if (!session) {
      alert("User session not found");
      return;
    }

    // ✅ Backend payload
    const payload = {
      company_name: singleData.companyName.trim(),
      company_industry: singleData.companyIndustry || "Unknown",

      total_assets: Number(singleData.totalAsset) || 0,
      total_debt: Number(singleData.totalDebt) || 0,
      total_income: Number(singleData.totalIncome) || 0,
      non_halal_income: Number(singleData.nonHalalIncome) || 0,
      cash_and_interest_securities:
        Number(singleData.cashInterestSecurities) || 0,
    };

    console.log("Sending payload:", payload);

    // ✅ Send authenticated request
    const res = await axios.post(
      `${import.meta.env.VITE_BACKEND_URL}/api/analyze/single`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${session.access_token}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("Backend response:", res.data);

    // ✅ Save result
    setAnalysisResult(res.data);

    // ✅ Success state
    setSubmitted(true);

    // ✅ Reset form
    setSingleData({
      companyName: "",
      companyIndustry: "",
      totalAsset: "",
      totalDebt: "",
      totalIncome: "",
      nonHalalIncome: "",
      cashInterestSecurities: "",
    });

    alert("Analysis completed successfully!");

  } catch (err) {
    console.error("Single analysis error:", err);

    console.error(
      "Backend response:",
      err.response?.data
    );

    alert(
      err.response?.data?.detail ||
      "Failed to analyze company"
    );
  } finally {
    setUploading(false);
  }
};
  const handleBulkUpload = (e) => {
  const file = e.target.files[0];

  if (!file) return;

  if (!file.name.endsWith(".csv")) {
    alert("Only CSV files are allowed!");
    return;
  }

  // Reset previous results
  setBulkResults([]);
  setAnalysisResult(null);

  // IMPORTANT: create fresh file reference (fixes ERR_UPLOAD_FILE_CHANGED)
  const newFile = new File([file], file.name, { type: file.type });

  setBulkFile(newFile);
};

  const handleBulkSubmit = async () => {
  if (!bulkFile) return;

  const formData = new FormData();
  formData.append("file", bulkFile);

  try {
    const res = await axios.post(
      `${import.meta.env.VITE_BACKEND_URL}/api/analyze/bulk`,
      formData,
      { headers: { "Content-Type": "multipart/form-data" } }
    );

    console.log("Bulk Response:", res.data);

    // Guard against null or unexpected structure
    if (!res.data || !Array.isArray(res.data.results)) {
      alert("Backend returned an invalid response for bulk upload.");
      return;
    }

    setBulkResults(res.data.results);
    alert("Bulk analysis completed successfully!");
  } catch (err) {
    console.error("Bulk Upload Error:", err);
    alert("Failed to submit bulk CSV. Check console for details.");
  }
};
  const downloadSinglePDF = (data) => {
  if (!data) return;

  const doc = new jsPDF();

  doc.setFontSize(16);
  doc.text("Shariah Compliance Report", 10, 10);

  doc.setFontSize(12);
  doc.text(`Company ID: ${data.company_id}`, 10, 20);
  doc.text(`Risk Score: ${data.result.risk_score}`, 10, 30);
  doc.text(`Status: ${data.result.status}`, 10, 40);

  const violations =
    data.result.violations.length > 0
      ? data.result.violations.join(", ")
      : "None";

  doc.text(`Violations: ${violations}`, 10, 50);

  const f = data.result.features || {};

  doc.text(`Debt Ratio: ${f.debt_ratio}`, 10, 60);
  doc.text(`Liquidity Ratio: ${f.liquidity_ratio}`, 10, 70);
  doc.text(`Non-Halal Income Ratio: ${f.non_halal_income_ratio}`, 10, 80);

  doc.save(`${data.company_id}.pdf`);
};

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Topbar username={username}/>
        <div className="p-6">
          
          <div className="grid grid-cols-4 grid-rows-3 gap-6">
            {/* Single Company */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5">
              <h3 className="text-lg font-semibold text-gray-700 mb-4">Single Company Analysis</h3>
              <div className="grid grid-cols-2 gap-4">
                {/* Industry */}
                <label className="flex flex-col text-sm text-gray-600">
                  Industry
                  <select
                    name="companyIndustry"
                    value={singleData.companyIndustry}
                    onChange={handleSingleChange}
                    className={`mt-1 px-3 py-2 border rounded ${
                      errors.companyIndustry ? "border-red-500" : ""
                    }`}
                  >
                    <option value="">Select Industry</option>
                    {industries.map((ind) => (
                      <option key={ind} value={ind}>{ind}</option>
                    ))}
                  </select>
                  {errors.companyIndustry && (
                    <span className="text-red-500 text-xs mt-1">{errors.companyIndustry}</span>
                  )}
                </label>

                {/* Company Name */}
                <label className="flex flex-col text-sm text-gray-600">
                  Company Name
                  <input
                    type="text"
                    name="companyName"
                    value={singleData.companyName}
                    onChange={handleSingleChange}
                    className={`mt-1 px-3 py-2 border rounded ${
                      errors.companyName ? "border-red-500" : ""
                    }`}
                  />
                  {errors.companyName && (
                    <span className="text-red-500 text-xs mt-1">{errors.companyName}</span>
                  )}
                </label>

                {/* Numeric fields */}
                {[
                  { label: "Total Asset", name: "totalAsset" },
                  { label: "Total Debt", name: "totalDebt" },
                  { label: "Total Income", name: "totalIncome" },
                  { label: "Non-Halal Income", name: "nonHalalIncome" },
                  { label: "Cash & Interest Securities", name: "cashInterestSecurities" },
                ].map((f) => (
                  <label key={f.name} className="flex flex-col text-sm text-gray-600">
                    {f.label}
                    <input
                      type="number"
                      name={f.name}
                      value={singleData[f.name]}
                      onChange={handleSingleChange}
                      className={`mt-1 px-3 py-2 border rounded ${
                        errors[f.name] ? "border-red-500" : ""
                      }`}
                    />
                    {errors[f.name] && (
                      <span className="text-red-500 text-xs mt-1">{errors[f.name]}</span>
                    )}
                  </label>
                ))}
              </div>

              <div className="mt-4 flex gap-3">
                <button
                  onClick={handleSingleSubmit}
                  className="px-5 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700"
                >
                  Submit
                </button>
              </div>

              {submitted && analysisResult && (
                <div className="mt-4 flex gap-3">
                  <button
                    onClick={() => downloadSingleCSV(analysisResult)}
                    className="px-5 py-2 bg-green-600 text-white rounded hover:bg-green-700"
                  >
                    Download CSV
                  </button>
                  <button
                    onClick={() => downloadSinglePDF(analysisResult)}
                    className="px-7 py-2 bg-blue-700 text-white rounded hover:bg-indigo-700"
                  >
                    Download PDF
                  </button>
                </div>
              )}
            </div>

            {/* Bulk Analysis */}
            <div className="col-span-2 row-span-3 bg-white rounded-2xl shadow p-5 flex flex-col items-center justify-center">
              <h3 className="text-lg font-semibold text-gray-700 mb-4">Bulk Analysis (CSV)</h3>
              <label className="w-full border-2 border-dashed border-gray-300 rounded-xl p-10 flex flex-col items-center justify-center cursor-pointer hover:border-indigo-600 hover:bg-indigo-50">
                <svg
                  className="w-12 h-12 text-gray-400 mb-3"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M7 16v4h10v-4M12 12v8M5 12l7-8 7 8"
                  ></path>
                </svg>
                <span className="text-gray-500 mb-2">Drag & drop a CSV file here</span>
                <span className="text-gray-400 text-sm">or click to upload</span>
                <input
                  type="file"
                  accept=".csv"
                  className="hidden"
                  onChange={handleBulkUpload}
                />
              </label>
              {bulkFile && <p className="mt-2 text-gray-700">Selected file: {bulkFile.name}</p>}
              <button
                onClick={handleBulkSubmit}
                className="mt-4 px-5 py-2 bg-green-600 text-white rounded hover:bg-green-700"
              >
                Submit Bulk
              </button>
              
              {bulkResults.length > 0 && (
  <div className="mt-4 flex gap-3">
    <button
      onClick={downloadBulkCSV}
      className="px-5 py-2 bg-green-600 text-white rounded"
    >
      Download Bulk CSV
    </button>

    <button
      onClick={downloadBulkPDF}
      className="px-5 py-2 bg-blue-700 text-white rounded"
    >
      Download Bulk PDF
    </button>
  </div>
)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataAnalysis;