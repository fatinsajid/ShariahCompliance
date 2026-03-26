import React, { useState } from "react";
import Sidebar from "../components/dashboard/Sidebar";
import Topbar from "../components/dashboard/Topbar";

const dummyCompanies = [
  { id: "C001", name: "Company A" },
  { id: "C002", name: "Company B" },
];

const dummyReviews = [
  {
    id: 1,
    companyId: "C001",
    status: "pending",
    parameters: {
      "Parameter 1": false,
      "Parameter 2": false,
      "Parameter 3": false,
    },
    comment: "",
  },
  {
    id: 2,
    companyId: "C002",
    status: "rejected",
    parameters: {
      "Parameter 1": true,
      "Parameter 2": false,
      "Parameter 3": true,
    },
    comment: "",
  },
];

const ScholarReviews = () => {
  const [companyId, setCompanyId] = useState("");
  const [status, setStatus] = useState("pending");
  const [reviewQueue, setReviewQueue] = useState([]);
  const [selectedReview, setSelectedReview] = useState(null);

  const handleSubmit = () => {
    const filtered = dummyReviews.filter((r) => {
      return (
        (!companyId || r.companyId === companyId) &&
        (status === "all" || r.status === status)
      );
    });
    setReviewQueue(filtered);
    setSelectedReview(null);
  };

  const handleReset = () => {
    setCompanyId("");
    setStatus("pending");
    setReviewQueue([]);
    setSelectedReview(null);
  };

  const handleSelectReview = (review) => {
    setSelectedReview({ ...review }); // clone to allow editing
  };

  const handleSubmitReview = () => {
  if (!selectedReview) return;

  // Determine new status based on parameters
  const allObtained = Object.values(selectedReview.parameters).every(
    (v) => v === true
  );

  const newStatus = allObtained ? "approved" : "rejected";

  const updatedReview = { ...selectedReview, status: newStatus };

  setReviewQueue((prev) =>
    prev.map((r) => (r.id === updatedReview.id ? updatedReview : r))
  );

  alert(
    `Review for ${updatedReview.companyId} submitted with status "${updatedReview.status}"!`
  );
  setSelectedReview(null);
};

  return (
    <div className="flex min-h-screen bg-[#F9FAFB]">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Topbar />
        <div className="p-6">
          <h1 className="text-2xl font-bold text-gray-800 mb-4">Scholar Reviews</h1>

          {/* Filter Row */}
          <div className="grid grid-cols-2 gap-6 mb-6">
            <div>
              <label className="block font-semibold mb-1">Company ID</label>
              <select
                className="w-full border p-2 rounded"
                value={companyId}
                onChange={(e) => setCompanyId(e.target.value)}
              >
                <option value="">Select Company</option>
                {dummyCompanies.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.id} - {c.name}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block font-semibold mb-1">Status</label>
              <div className="flex items-center gap-2">
                <select
                  className="w-full border p-2 rounded"
                  value={status}
                  onChange={(e) => setStatus(e.target.value)}
                >
                  <option value="pending">Pending</option>
                  <option value="rejected">Rejected</option>
                  <option value="approved">Approved</option>
                  <option value="all">All</option>
                </select>
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
          </div>

          {/* Review Queue Table */}
          <div className="overflow-x-auto mb-6">
            <table className="w-full border">
              <thead>
                <tr className="bg-gray-100">
                  <th className="border p-2">Select</th>
                  <th className="border p-2">Company ID</th>
                  <th className="border p-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {reviewQueue.length === 0 && (
                  <tr>
                    <td
                      colSpan={3}
                      className="text-center p-4 text-gray-500"
                    >
                      No reviews to show. Press Submit to fetch.
                    </td>
                  </tr>
                )}
                {reviewQueue.map((review) => (
                  <tr key={review.id} className="hover:bg-gray-50">
                    <td className="border p-2 text-center">
                      <input
                        type="radio"
                        name="selectedReview"
                        checked={selectedReview?.id === review.id}
                        onChange={() => handleSelectReview(review)}
                      />
                    </td>
                    <td className="border p-2">{review.companyId}</td>
                    <td className="border p-2">{review.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Selected Review Panel */}
          {selectedReview && (
            <div className="border p-4 rounded bg-gray-50">
              <h2 className="font-bold text-lg mb-3">
                Selected Review: {selectedReview.companyId}
              </h2>
              {Object.entries(selectedReview.parameters).map(
                ([key, value]) => (
                  <div
                    key={key}
                    className="flex items-center gap-3 mb-2"
                  >
                    <span className="w-48">{key}</span>
                    <label className="flex items-center gap-1">
                      <input
                        type="radio"
                        name={`${key}-${selectedReview.id}`}
                        checked={value === true}
                        onChange={() =>
                          setSelectedReview({
                            ...selectedReview,
                            parameters: {
                              ...selectedReview.parameters,
                              [key]: true,
                            },
                          })
                        }
                      />
                      Obtained
                    </label>
                    <label className="flex items-center gap-1">
                      <input
                        type="radio"
                        name={`${key}-${selectedReview.id}`}
                        checked={value === false}
                        onChange={() =>
                          setSelectedReview({
                            ...selectedReview,
                            parameters: {
                              ...selectedReview.parameters,
                              [key]: false,
                            },
                          })
                        }
                      />
                      Not Obtained
                    </label>
                  </div>
                )
              )}

              {/* Comments Section */}
              <div className="mt-3">
                <label className="block font-semibold mb-1">Comments</label>
                <textarea
                  className="w-full border p-2 rounded"
                  rows={3}
                  placeholder="Add your comments here..."
                  value={selectedReview.comment || ""}
                  onChange={(e) =>
                    setSelectedReview({
                      ...selectedReview,
                      comment: e.target.value,
                    })
                  }
                />
              </div>

              <button
                onClick={handleSubmitReview}
                className="mt-3 bg-green-500 text-white px-4 py-2 rounded"
              >
                Submit Review
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ScholarReviews;