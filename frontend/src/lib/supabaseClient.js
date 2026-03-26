// src/lib/supabaseClient.js
import { createClient } from "@supabase/supabase-js";

// Pull from environment variables
const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = import.meta.env.VITE_SUPABASE_KEY;

// Defensive: warn if missing
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Supabase environment variables missing!");
}

// Initialize Supabase client
export const supabase = createClient(SUPABASE_URL ?? "", SUPABASE_KEY ?? "");

// Export getJWT safely
export const getJWT = async () => {
  try {
    const session = await supabase.auth.getSession();
    return session?.data?.session?.access_token ?? null;
  } catch (err) {
    console.error("Error getting JWT:", err);
    return null;
  }
};