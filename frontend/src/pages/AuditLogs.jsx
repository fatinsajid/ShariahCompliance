import React, { useEffect, useState } from "react";
import Sidebar from "../components/Dashboard/sidebar";
import Topbar from "../components/Dashboard/TopBar";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { supabase } from "../lib/supabaseClient";

export default function AuditLogs() {
  const [industries, setIndustries] = useState([]);
  const username = localStorage.getItem("username") || "John Doe";
  const [industry, setIndustry] = useState("");
  const [compliance, setCompliance] = useState("");
  const [dateRange, setDateRange] = useState([null, null]);
  const [startDate, endDate] = dateRange;
  const isFormValid = compliance && startDate && endDate;
  const [auditData, setAuditData] = useState([]);
  const [selectedAudit, setSelectedAudit] = useState(null);

  // ✅ Fetch unique industries
  const fetchIndustries = async () => {
    const { data, error } = await supabase
      .from("compliance_audit_log")
      .select("company_industry");

    if (error) {
      console.error("Industry fetch error:", error);
      return;
    }

    const unique = [...new Set(data.map(d => d.company_industry).filter(Boolean))];
    setIndustries(unique);
  };

  // ✅ Fetch audit records with filters
    const handleSubmit = async () => {
  if (!compliance || !startDate || !endDate) {
    console.warn("Compliance and date range are required");
    return;
  }

  let query = supabase
    .from("compliance_audit_log")
    .select("*");

  // ✅ Apply ONLY if selected
  if (industry) {
    query = query.eq("company_industry", industry);
  }

  if (compliance !== "All") {
    query = query.eq("compliance_status", compliance);
  }

  query = query
    .gte("created_at", startDate.toISOString())
    .lte("created_at", endDate.toISOString());

  const { data, error } = await query;

  if (error) {
    console.error("Audit fetch error:", error);
    setAuditData([]);
    return;
  }

  console.log("Fetched audit data:", data); // 🔍 DEBUG

  setAuditData(data || []);
  setSelectedAudit(null);
};
    const handleReset = () => {
      setIndustry("");
      setCompliance("");
      setDateRange([null, null]);
      setAuditData([]);
      setSelectedAudit(null);
    };

  useEffect(() => {
    fetchIndustries();
  }, []);

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar active="audit-logs" />

      <div className="flex-1 flex flex-col">
        <Topbar username={username}/>

        <div className="p-6">
          

          <div className="grid grid-cols-4 grid-rows-4 gap-5">

            {/* Industry */}
            <div className="bg-white rounded-2xl shadow p-5 flex flex-col gap-3">
              <label>Industry</label>
              <select
                value={industry}
                onChange={(e) => setIndustry(e.target.value)}
                className="border p-2 rounded"
              >
                <option value="">All Industries</option>
                {industries.map((ind, i) => (
                  <option key={i} value={ind}>
                    {ind}
                  </option>
                ))}
              </select>
            </div>

            {/* Compliance */}
            <div className="bg-white rounded-2xl shadow p-5 flex flex-col gap-3">
              <label>Compliance Status</label>
              <select
                value={compliance}
                onChange={(e) => setCompliance(e.target.value)}
                className="border p-2 rounded"
              >
                <option value="All">All</option>
                <option value="Compliant">Compliant</option>
                <option value="Non-Compliant">Non-Compliant</option>
              </select>
            </div>

            {/* Date + Buttons */}
            <div className="bg-white rounded-2xl shadow p-5 flex flex-col gap-3">
              <label>Date Range</label>
              <DatePicker
                selectsRange
                startDate={startDate}
                endDate={endDate}
                onChange={(update) => setDateRange(update)}
                isClearable
                className="border p-2 rounded"
              />

              <div className="flex gap-3 mt-3">
                <button
                  onClick={handleSubmit}
                  className="bg-blue-700 text-white px-4 py-2 rounded hover:bg-indigo-700"
                >
                  Submit
                </button>
                <button
                  onClick={handleReset}
                  className="bg-gray-300 px-4 py-2 rounded hover:bg-gray-400"
                >
                  Reset
                </button>
              </div>
            </div>

            {/* Audit Details */}
            <div className="col-span-1 row-span-4 bg-white rounded-2xl shadow p-5 overflow-auto">
              <h2 className="font-bold text-lg mb-3">Audit Details</h2>

              {selectedAudit ? (
                <div>
                  <p><strong>Company:</strong> {selectedAudit.company_name}</p>
                  <p><strong>Compliance:</strong> {selectedAudit.compliance_status}</p>
                  <p><strong>Date:</strong> {new Date(selectedAudit.created_at).toLocaleDateString()}</p>

                  <p className="mt-2 font-semibold">Details:</p>

                  {Array.isArray(selectedAudit.audit_details) ? (
                    selectedAudit.audit_details.map((d, i) => (
                      <div key={i} className="mb-2 p-2 bg-gray-50 rounded">
                        <p><strong>{d.rule}</strong> ({d.status})</p>
                        <p className="text-sm">{d.description}</p>
                      </div>
                    ))
                  ) : (
                    <p>No details available</p>
                  )}

                  <p className="mt-2">
                    <strong>Fatwa Version:</strong> {selectedAudit.fatwa_version || "-"}
                  </p>
                </div>
              ) : (
                <p>Select a record to view details</p>
              )}
            </div>

            {/* Audit Table */}
            <div className="col-span-3 row-span-3 bg-white rounded-2xl shadow p-5 overflow-auto">
              <h2 className="font-bold text-lg mb-3">Audit Records</h2>

              {auditData.length === 0 ? (
                <p>No data found</p>
              ) : (
                <table className="w-full border">
                  <thead>
                    <tr>
                      <th className="border px-3 py-2">Company Name</th>
                      <th className="border px-3 py-2">Compliance</th>
                      <th className="border px-3 py-2">Date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {auditData.map((row) => (
                      <tr
                        key={row.audit_id}
                        className="cursor-pointer hover:bg-gray-100"
                        onClick={() => setSelectedAudit(row)}
                      >
                        <td className="border px-3 py-2">{row.company_name}</td>
                        <td className="border px-3 py-2">{row.compliance_status}</td>
                        <td className="border px-3 py-2">
                          {new Date(row.created_at).toLocaleDateString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

          </div>
        </div>
      </div>
    </div>
  );
}