import { useState } from "react";
import { supabase } from "../lib/supabaseClient";
import { useNavigate } from "react-router-dom";
import loginBg from "../components/dashboard/loginBg.jpg"; // your background image
import logo from "../components/dashboard/logo.png";

const ResetPassword = () => {
  const navigate = useNavigate();

  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");

  const handleUpdate = async (e) => {
    e.preventDefault();

    if (password !== confirm) {
      alert("Passwords do not match");
      return;
    }

    const { error } = await supabase.auth.updateUser({
      password,
    });

    if (error) {
      alert(error.message);
    } else {
      alert("Password updated!");
      navigate("/login");
    }
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
          onSubmit={handleUpdate}
          className="backdrop-blur-lg bg-white/10 border border-white/20 p-8 rounded-2xl"
        >
          <h2 className="text-white text-2xl mb-6 text-center">
            New Password
          </h2>

          <input
            type="password"
            placeholder="New password"
            className="input"
            onChange={(e) => setPassword(e.target.value)}
          />

          <input
            type="password"
            placeholder="Confirm password"
            className="input mb-4"
            onChange={(e) => setConfirm(e.target.value)}
          />

          <button className="btn w-full">Update Password</button>
        </form>
      </div>
    </div>
  );
};

export default ResetPassword;