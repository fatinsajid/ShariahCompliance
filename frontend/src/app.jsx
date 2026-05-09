import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { supabase } from "./lib/supabaseClient";

// Pages
import Login from "./pages/login";
import Register from "./pages/register";
import ForgotPassword from "./pages/ForgotPassword";
import ResetPassword from "./pages/ResetPassword";

import Dashboard from "./pages/dashboard";
import CompanyDetails from "./pages/CompanyDetails";
import AuditLogs from "./pages/AuditLogs";
import DataAnalysis from "./pages/DataAnalysis";

function App() {
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      setSession(data.session);
      setLoading(false);
    });

    const { data: listener } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        setSession(session);
      }
    );

    return () => listener.subscription.unsubscribe();
  }, []);

  if (loading) return <div className="p-6">Loading...</div>;

  return (
    <BrowserRouter>
      <Routes>

        {/* Public */}
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/reset-password" element={<ResetPassword />} />

        {/* Protected */}
        <Route
          path="/dashboard"
          element={session ? <Dashboard /> : <Navigate to="/login" />}
        />
        <Route
          path="/companies"
          element={session ? <CompanyDetails /> : <Navigate to="/login" />}
        />
        <Route
          path="/audit-logs"
          element={session ? <AuditLogs /> : <Navigate to="/login" />}
        />
        <Route
          path="/data-analysis"
          element={session ? <DataAnalysis /> : <Navigate to="/login" />}
        />

        {/* Default */}
        <Route
          path="*"
          element={<Navigate to={session ? "/dashboard" : "/login"} />}
        />

      </Routes>
    </BrowserRouter>
  );
}

export default App;
