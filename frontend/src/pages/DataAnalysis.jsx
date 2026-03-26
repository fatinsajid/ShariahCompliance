import { useState } from "react";
import Sidebar from "../components/dashboard/Sidebar";
import Topbar from "../components/dashboard/Topbar";

const DataAnalysis = () => {
  const [singleData, setSingleData] = useState({
    companyId: "",
    totalAsset: "",
    totalDebt: "",
    totalIncome: "",
    nonHalalIncome: "",
    cashInterestSecurities: "",
  });
  const [submitted, setSubmitted] = useState(false);
  const [bulkFile, setBulkFile] = useState(null);

  const handleSingleChange = (e) => {
    const { name, value } = e.target;
    setSingleData({ ...singleData, [name]: value });
  };

  const handleBulkUpload = (e) => {
    const file = e.target.files[0];
    if (file && file.type === "text/csv") {
      setBulkFile(file);
    } else {
      alert("Only CSV files are allowed!");
    }
  };

  const handleSingleSubmit = () => {
    console.log("Single company data submitted:", singleData);
    setSubmitted(true); // show download buttons
  };

  const downloadSingleCSV = () => {
    const headers = [
      "Company ID",
      "Total Asset",
      "Total Debt",
      "Total Income",
      "Non-Halal Income",
      "Cash & Interest Securities",
    ];
    const values = [
      singleData.companyId,
      singleData.totalAsset,
      singleData.totalDebt,
      singleData.totalIncome,
      singleData.nonHalalIncome,
      singleData.cashInterestSecurities,
    ];
    const csvContent = [headers.join(","), values.join(",")].join("\n");
    const blob = new Blob([csvContent], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "single_company_analysis.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadSinglePDF = () => {
    const docContent = `
      Company ID: ${singleData.companyId}
      Total Asset: ${singleData.totalAsset}
      Total Debt: ${singleData.totalDebt}
      Total Income: ${singleData.totalIncome}
      Non-Halal Income: ${singleData.nonHalalIncome}
      Cash & Interest Securities: ${singleData.cashInterestSecurities}
    `;
    const blob = new Blob([docContent], { type: "application/pdf" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "single_company_analysis.pdf";
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Topbar />

        <div className="p-6">
          <h1 className="text-xl font-semibold text-gray-800 mb-6">
            Data Analysis
          </h1>

          <div className="grid grid-cols-4 grid-rows-3 gap-6">

            {/* Single Company Analysis */}
            <div className="col-span-2 row-span-2 bg-white rounded-2xl shadow p-5">
              <h3 className="text-lg font-semibold text-gray-700 mb-4">
                Single Company Analysis
              </h3>
              <div className="grid grid-cols-2 gap-4">
                <label className="flex flex-col text-sm text-gray-600">
                  Company ID
                  <input
                    type="text"
                    name="companyId"
                    value={singleData.companyId}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
                <label className="flex flex-col text-sm text-gray-600">
                  Total Asset
                  <input
                    type="text"
                    name="totalAsset"
                    value={singleData.totalAsset}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
                <label className="flex flex-col text-sm text-gray-600">
                  Total Debt
                  <input
                    type="text"
                    name="totalDebt"
                    value={singleData.totalDebt}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
                <label className="flex flex-col text-sm text-gray-600">
                  Total Income
                  <input
                    type="text"
                    name="totalIncome"
                    value={singleData.totalIncome}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
                <label className="flex flex-col text-sm text-gray-600">
                  Non-Halal Income
                  <input
                    type="text"
                    name="nonHalalIncome"
                    value={singleData.nonHalalIncome}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
                <label className="flex flex-col text-sm text-gray-600">
                  Cash & Interest Securities
                  <input
                    type="text"
                    name="cashInterestSecurities"
                    value={singleData.cashInterestSecurities}
                    onChange={handleSingleChange}
                    className="mt-1 px-3 py-2 border rounded"
                  />
                </label>
              </div>

              <div className="mt-4 flex gap-3">
                <button
                  onClick={handleSingleSubmit}
                  className="px-5 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700"
                >
                  Submit
                </button>
              </div>
            </div>

            {/* Download Buttons - Row 3, Col 1 only */}
            {submitted && (
              <div className="col-end-2 row-start-3 gap-3">
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

            {/* Bulk Analysis */}
            <div className="col-span-2 row-span-3 bg-white rounded-2xl shadow p-5 flex flex-col items-center justify-center">
              <h3 className="text-lg font-semibold text-gray-700 mb-4">
                Bulk Analysis (CSV)
              </h3>
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
                onClick={() => console.log("Bulk CSV submitted:", bulkFile)}
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