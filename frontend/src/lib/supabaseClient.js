import { createClient } from "@supabase/supabase-js";

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

export const supabase = createClient(supabaseUrl, supabaseKey);

// Get JWT token
export const getJWT = async () => {
  const user = supabase.auth.user();
  if (!user) return null;

  const session = supabase.auth.session();
  return session?.access_token || null;
};