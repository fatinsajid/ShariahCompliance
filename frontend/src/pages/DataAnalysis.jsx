// DataAnalysis.jsx
import { useState, useEffect } from "react";
import { supabase } from "../lib/supabaseClient"; // single instance
import axios from "axios";
import Sidebar from "../components/dashboard/Sidebar";
import Topbar from "../components/dashboard/Topbar";

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
  const [submitted, setSubmitted] = useState(false);
  const [bulkFile, setBulkFile] = useState(null);
  const [industries, setIndustries] = useState([]);
  const [errors, setErrors] = useState({});

  useEffect(() => {
    const fetchIndustries = async () => {
      const { data, error } = await supabase
        .from("compliance_audit_log")
        .select("company_industry")
        .neq("company_industry", null)
        .order("company_industry", { ascending: true });

      if (error) console.error(error);
      else setIndustries([...new Set(data.map((i) => i.company_industry))]);
    };
    fetchIndustries();
  }, []);

  const handleSingleChange = (e) => {
    const { name, value } = e.target;
    setSingleData({ ...singleData, [name]: value });
    setErrors({ ...errors, [name]: "" });
  };

  const handleSingleSubmit = async () => {
    if (!singleData.companyName.trim() || !singleData.companyIndustry) {
      alert("Company Name and Industry are required.");
      return;
    }

    // Prepare numeric fields with fallback to 0
    const payload = {
      company_name: singleData.companyName.trim(),
      company_industry: singleData.companyIndustry,
      total_assets: parseFloat(singleData.totalAsset) || 0,
      total_debt: parseFloat(singleData.totalDebt) || 0,
      total_income: parseFloat(singleData.totalIncome) || 0,
      non_halal_income: parseFloat(singleData.nonHalalIncome) || 0,
      cash_and_interest_securities: parseFloat(singleData.cashInterestSecurities) || 0,
    };

    console.log("Validated payload:", payload);

    try {
      const res = await axios.post(
        `${import.meta.env.VITE_BACKEND_URL}/api/analyze/single`,
        payload
      );
      console.log(res.data);
      setSubmitted(true);
      alert("Single company analysis submitted successfully!");
    } catch (err) {
      console.error("Error submitting single company:", err);
      alert("Failed to submit single company. Check backend logs for details.");
    }
  };

  const handleBulkUpload = (e) => {
    const file = e.target.files[0];
    if (file?.type !== "text/csv") return alert("Only CSV files are allowed!");
    setBulkFile(file);
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
      console.log(res.data);
      alert("Bulk analysis completed successfully!");
    } catch (err) {
      console.error(err);
      alert("Failed to submit bulk CSV.");
    }
  };

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Topbar />
        <div className="p-6">
          <h1 className="text-xl font-semibold text-gray-800 mb-6">Data Analysis</h1>
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

              {submitted && (
                <div className="mt-4 flex gap-3">
                  <button
                    onClick={downloadSingleCSV}
                    className="px-5 py-2 bg-green-600 text-white rounded hover:bg-green-700"
                  >
                    Download CSV
                  </button>
                  <button
                    onClick={downloadSinglePDF}
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
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataAnalysis;