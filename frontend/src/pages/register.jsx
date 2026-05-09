import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "../lib/supabaseClient";
import loginBg from "../components/dashboard/loginbg.jpg"; // your background image
import logo from "../components/dashboard/logo.png";

const Register = () => {
  const navigate = useNavigate();

  const [form, setForm] = useState({
    email: "",
    password: "",
    confirmPassword: "",
  });

  const [loading, setLoading] = useState(false);

  const handleRegister = async (e) => {
    e.preventDefault();

    if (form.password !== form.confirmPassword) {
      alert("Passwords do not match");
      return;
    }

    setLoading(true);

    const { error } = await supabase.auth.signUp({
      email: form.email,
      password: form.password,
    });

    setLoading(false);

    if (error) {
      alert(error.message);
      return;
    }

    alert("Registration successful! Check your email.");
    navigate("/login");
  };

  return (
    <div
      className="min-h-screen flex items-center justify-center bg-cover bg-center"
      style={{ backgroundImage: `url(${loginBg})` }}
    >
      <div className="absolute inset-0 bg-black/60"></div>

      <div className="relative z-10 w-full max-w-md px-6">
        
        <img src={logo} alt="Logo" className="w-full mb-4 object-contain" />

        <form
          onSubmit={handleRegister}
          className="backdrop-blur-lg bg-white/10 border border-white/20 p-8 rounded-2xl"
        >
          <h2 className="text-white text-2xl mb-6 text-center">Register</h2>

          <input
            type="email"
            placeholder="Email"
            className="input"
            onChange={(e) => setForm({ ...form, email: e.target.value })}
          />

          <input
            type="password"
            placeholder="Password"
            className="input"
            onChange={(e) => setForm({ ...form, password: e.target.value })}
          />

          <input
            type="password"
            placeholder="Confirm Password"
            className="input mb-4"
            onChange={(e) =>
              setForm({ ...form, confirmPassword: e.target.value })
            }
          />

          <button className="btn w-full">
            {loading ? "Registering..." : "Register"}
          </button>

          <p className="text-gray-300 text-sm mt-4 text-center">
            Already have an account?{" "}
            <button
              type="button"
              onClick={() => navigate("/login")}
              className="text-purple-400 hover:underline"
            >
              Login
            </button>
          </p>
        </form>
      </div>
    </div>
  );
};

export default Register;
