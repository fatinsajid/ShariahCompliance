/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx,vue}"
  ],
  theme: {
    extend: {
      colors: {
        primary: "#4F46E5",       // Indigo for buttons/headers
        secondary: "#6366F1",     // Lighter indigo
        accent: "#10B981",        // Green for success/positive metrics
        danger: "#EF4444",        // Red for violations/errors
        warning: "#F59E0B",       // Amber for warnings/anomalies
        neutral: {
          100: "#F3F4F6",
          200: "#E5E7EB",
          300: "#D1D5DB",
          400: "#9CA3AF",
          500: "#6B7280",
          600: "#4B5563",
          700: "#374151",
          800: "#1F2937",
          900: "#111827"
        }
      },
      spacing: {
        18: "4.5rem",
        22: "5.5rem",
        26: "6.5rem",
        30: "7.5rem"
      },
      borderRadius: {
        xl: "1rem",
        "2xl": "1.5rem"
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui"],
        mono: ["Fira Code", "monospace"]
      },
      boxShadow: {
        dashboard: "0 4px 6px rgba(0, 0, 0, 0.1)",
        card: "0 2px 4px rgba(0, 0, 0, 0.06)"
      }
    }
  },
  plugins: [
    require('@tailwindcss/forms'),
    require('@tailwindcss/typography')
  ]
}