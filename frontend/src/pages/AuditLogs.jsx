import React, { useState } from "react";
import Sidebar from "../components/dashboard/Sidebar";
import Topbar from "../components/dashboard/Topbar";
import { DatePicker } from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

const dummyAuditData = [
  { id: 1, companyId: "C001", compliance: "Compliant", date: "2026-03-25", details: "All checks passed" },
  { id: 2, companyId: "C002", compliance: "Non Compliant", date: "2026-03-24", details: "Missing documentation" },
  { id: 3, companyId: "C003", compliance: "Compliant", date: "2026-03-23", details: "Minor issues resolved" },
];

export default function AuditLogs() {
  const [companyId, setCompanyId] = useState("");
  const [compliance, setCompliance] = useState("");
  const [dateRange, setDateRange] = useState([null, null]);
  const [startDate, endDate] = dateRange;
  const [auditData, setAuditData] = useState([]);
  const [selectedAudit, setSelectedAudit] = useState(null);

  const handleSubmit = () => {
    const filtered = dummyAuditData.filter((item) => {
      const matchCompany = companyId ? item.companyId === companyId : true;
      const matchCompliance = compliance ? item.compliance === compliance : true;
      const matchDate =
        (!startDate || !endDate) ||
        (new Date(item.date) >= startDate && new Date(item.date) <= endDate);
      return matchCompany && matchCompliance && matchDate;
    });
    setAuditData(filtered);
    setSelectedAudit(null);
  };

  const handleReset = () => {
    setCompanyId("");
    setCompliance("");
    setDateRange([null, null]);
    setAuditData([]);
    setSelectedAudit(null);
  };

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      {/* Sidebar */}
      <Sidebar active="audit-logs" />

      {/* Main content */}
      <div className="flex-1 flex flex-col">
        {/* Topbar */}
        <Topbar />
        <div className="p-6">
        {/* Page title */}
        <h1 className="text-2xl font-bold text-gray-800 mb-4">Audit Logs</h1>

        {/* Filters + Table + Details */}
        <div className="grid grid-cols-4 grid-rows-4 gap-5 flex-1">
          {/* Company ID */}
          <div className="col-span-1 row-span-1 bg-white rounded-2xl shadow p-5 flex flex-col gap-3">
            <label>Company ID</label>
            <select
              value={companyId}
              onChange={(e) => setCompanyId(e.target.value)}
              className="border p-2 rounded"
            >
              <option value="">Select Company</option>
              <option value="C001">C001</option>
              <option value="C002">C002</option>
              <option value="C003">C003</option>
            </select>
          </div>

          {/* Compliance */}
          <div className="col-span-1 row-span-1 bg-white rounded-2xl shadow p-5 flex flex-col gap-3">
            <label>Compliance Status</label>
            <select
              value={compliance}
              onChange={(e) => setCompliance(e.target.value)}
              className="border p-2 rounded"
            >
              <option value="">Select Status</option>
              <option value="Compliant">Compliant</option>
              <option value="Non Compliant">Non Compliant</option>
            </select>
          </div>

          {/* Date range + buttons */}
          <div className="col-span-1 row-span-1 bg-white rounded-2xl shadow p-5 flex flex-col gap-3 ">
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
                className="bg-gray-300 text-black px-4 py-2 rounded hover:bg-gray-400"
              >
                Reset
              </button>
            </div>
          </div>

          {/* Audit Details spanning all rows */}
          <div className="col-span-1 row-span-4 bg-white rounded-2xl shadow p-5 overflow-auto">
            <h2 className="font-bold text-lg mb-3">Audit Details</h2>
            {selectedAudit ? (
              <div>
                <p><strong>Company ID:</strong> {selectedAudit.companyId}</p>
                <p><strong>Compliance:</strong> {selectedAudit.compliance}</p>
                <p><strong>Date:</strong> {selectedAudit.date}</p>
                <p><strong>Details:</strong> {selectedAudit.details}</p>
              </div>
            ) : (
              <p>Select a record from the table to see details</p>
            )}
          </div>

          {/* Audit Table spanning 3 columns and 3 rows */}
          <div className="col-span-3 row-span-3 bg-white rounded-2xl shadow p-5 overflow-auto">
            <h2 className="font-bold text-lg mb-3">Audit Records</h2>
            {auditData.length === 0 ? (
              <p>No data to display</p>
            ) : (
              <table className="w-full table-auto border-collapse border">
                <thead>
                  <tr>
                    <th className="border px-3 py-2">Company ID</th>
                    <th className="border px-3 py-2">Compliance</th>
                    <th className="border px-3 py-2">Date</th>
                  </tr>
                </thead>
                <tbody>
                  {auditData.map((record) => (
                    <tr
                      key={record.id}
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => setSelectedAudit(record)}
                    >
                      <td className="border px-3 py-2">{record.companyId}</td>
                      <td className="border px-3 py-2">{record.compliance}</td>
                      <td className="border px-3 py-2">{record.date}</td>
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