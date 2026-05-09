import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "../lib/supabaseClient";
import loginBg from "../components/dashboard/loginbg.jpg"; // your background image
import logo from "../components/dashboard/Logo.png";

const Login = () => {
  const navigate = useNavigate();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);

    const { error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    setLoading(false);

    if (error) {
      alert(error.message);
      return;
    }

    navigate("/dashboard");
  };

  return (
    <div
      className="min-h-screen flex items-center justify-center bg-cover bg-center"
      style={{ backgroundImage: `url(${loginBg})` }}
    >
      {/* Overlay */}
      <div className="absolute inset-0 bg-black/60"></div>

      {/* Login Container */}
      <div className="relative z-10 w-full max-w-md px-6">
        
        {/* Logo */}
        <img
          src={logo}
          alt="Logo"
          className="w-full mb-4 object-contain"
        />

        {/* Glass Panel */}
        <form
          onSubmit={handleLogin}
          className="backdrop-blur-lg bg-white/10 border border-white/20 p-8 rounded-2xl shadow-xl"
        >
          <h2 className="text-white text-2xl font-semibold mb-6 text-center">
            Login
          </h2>

          {/* Email */}
          <input
            type="email"
            placeholder="Email"
            className="w-full p-3 mb-4 rounded-lg bg-white/20 text-white placeholder-gray-300 focus:outline-none focus:ring-2 focus:ring-purple-500"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />

          {/* Password */}
          <input
            type="password"
            placeholder="Password"
            className="w-full p-3 mb-2 rounded-lg bg-white/20 text-white placeholder-gray-300 focus:outline-none focus:ring-2 focus:ring-purple-500"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />

          {/* Forgot Password */}
          <div className="text-right mb-4">
            <button
              type="button"
              onClick={() => navigate("/forgot-password")}
              className="text-sm text-purple-300 hover:underline"
            >
              Forgot Password?
            </button>
          </div>

          {/* Button */}
          <button
            type="submit"
            disabled={loading}
            className="w-full bg-purple-600 hover:bg-purple-700 transition text-white py-3 rounded-lg font-medium"
          >
            {loading ? "Logging in..." : "Login"}
          </button>

          {/* Register */}
          <p className="text-sm text-gray-300 text-center mt-4">
            Don’t have an account?{" "}
            <button
              type="button"
              onClick={() => navigate("/register")}
              className="text-purple-400 hover:underline"
            >
              Register
            </button>
          </p>
        </form>
      </div>
    </div>
  );
};

export default Login;
