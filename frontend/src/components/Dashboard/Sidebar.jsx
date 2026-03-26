// src/components/dashboard/Sidebar.jsx
import { FiHome, FiLogOut, FiFolder, FiClipboard, FiBarChart2, FiInfo } from "react-icons/fi"; // ensure you run: npm install react-icons
import logo from "../../components/dashboard/logo.png"; // add your logo in /assets

const Sidebar = () => {
  const links = [
  { name: "Dashboard",icon: <FiHome />, href: "/dashboard" },
  { name: "Data Analysis",icon: <FiBarChart2 />, href: "/data-analysis" },
  { name: "Company Details",icon: <FiInfo />, href: "/company-details" },
  { name: "Audit Log",icon: <FiFolder />, href: "/audit-log" },
  { name: "Scholar Reviews",icon: <FiClipboard />, href: "/scholar-reviews" },
  { name: "Log Out",icon: <FiLogOut />, href: "/log-out" },
];

  return (
    <div className="w-64 min-h-screen bg-gray-900 text-white flex flex-col">
      <div className="p-6 flex items-center space-x-2">
        <img src={logo} alt="Logo" className="h-20 w-full object-cover rounded" />
    
      </div>
      <nav className="flex-1 px-4 py-2">
        {links.map((link) => (
          <a
            key={link.name}
            href={link.href}
            className="flex items-center px-4 py-2 mb-2 rounded hover:bg-purple-700 transition"
          >
            {link.icon}
            <span className="ml-3">{link.name}</span>
          </a>
        ))}
      </nav>
    </div>
  );
};

export default Sidebar;