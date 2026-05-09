// src/components/dashboard/Sidebar.jsx

import {
  FiHome,
  FiLogOut,
  FiFolder,
  FiClipboard,
  FiBarChart2,
  FiInfo,
} from "react-icons/fi";
import logo from "../../assets/logo.png";
import { useNavigate, useLocation } from "react-router-dom";
import { supabase } from "../../lib/supabaseClient";

const Sidebar = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const handleLogout = async () => {
  await supabase.auth.signOut();
  navigate("/login");
};

  const links = [
    { name: "Dashboard", icon: <FiHome />, path: "/dashboard" },
    { name: "Data Analysis", icon: <FiBarChart2 />, path: "/data-analysis" },
    { name: "Company Details", icon: <FiInfo />, path: "/companies" },
    { name: "Audit Log", icon: <FiFolder />, path: "/audit-logs" },
  ];

  return (
    <div className="w-64 min-h-screen bg-gray-900 text-white flex flex-col justify-between ">

      {/* Logo */}
      <div>
        <div className="p-4">
          <img
            src={logo}
            alt="Logo"
            className="w-full h-24 object-cover"/>
        </div>

        {/* Navigation */}
        <nav className="px-4 mt-4">
          {links.map((link) => {
            const isActive = location.pathname === link.path;

            return (
              <div
                key={link.name}
                onClick={() => navigate(link.path)}
                className={`flex items-center px-4 py-2 mb-2 rounded cursor-pointer transition
                  ${isActive ? "bg-purple-700" : "hover:bg-gray-700"}
                `}
              >
                {link.icon}
                <span className="ml-3">{link.name}</span>
              </div>
            );
          })}
        </nav>
      </div>

      {/* Logout (Bottom) */}
      <div className="px-4 pb-6">
        <div
          onClick={handleLogout}
          className="flex items-center px-4 py-2 rounded cursor-pointer hover:bg-red-600 transition"
        >
          <FiLogOut />
          <span className="ml-3">Log Out</span>
        </div>
      </div>
    </div>
  );
};
 

export default Sidebar;
