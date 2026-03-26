// src/components/dashboard/TopBar.jsx
import { FiUser } from "react-icons/fi";

const TopBar = ({ username }) => {
  return (
    <div className="flex items-center justify-end bg-white shadow px-6 py-4">
      <span className="mr-3 font-medium text-gray-700">{username ?? "Guest User"}</span>
      <FiUser className="text-gray-700 w-6 h-6" />
    </div>
  );
};

export default TopBar;