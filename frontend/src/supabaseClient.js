// src/supabaseClient.js

import { createClient } from '@supabase/supabase-js';

const supabaseUrl = "https://vspgwdndwzrxmkbofyrt.supabase.co";
const supabaseAnonKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZzcGd3ZG5kd3pyeG1rYm9meXJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MzM5MzQsImV4cCI6MjA4NzAwOTkzNH0.ItNYgORHAsgnboFJUh2ijq0sdLLFzxLp24KSeBjdw4g"; // keep this ONLY

export const supabase = createClient(supabaseUrl, supabaseAnonKey);