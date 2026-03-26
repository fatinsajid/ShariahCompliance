import { useState } from "react";
import { supabase } from "../lib/supabaseClient";
import loginBg from "../components/dashboard/loginbg.jpg"; // your background image
import logo from "../components/dashboard/Logo.png";


const ForgotPassword = () => {
  const [email, setEmail] = useState("");

  const handleReset = async (e) => {
    e.preventDefault();

    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: "http://localhost:5173/reset-password",
    });

    if (error) {
      alert(error.message);
    } else {
      alert("Reset email sent!");
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
          onSubmit={handleReset}
          className="backdrop-blur-lg bg-white/10 border border-white/20 p-8 rounded-2xl"
        >
          <h2 className="text-white text-2xl mb-6 text-center">
            Reset Password
          </h2>

          <input
            type="email"
            placeholder="Enter your email"
            className="input mb-4"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />

          <button className="btn w-full">Send Reset Link</button>
        </form>
      </div>
    </div>
  );
};

export default ForgotPassword;