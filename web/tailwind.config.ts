import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        paper: "#F5F1E8",
        "paper-deep": "#EBE4D4",
        ink: "#1A1412",
        rule: "#B8A989",
        accent: "#B91C2F",
        muted: "#6B6056",
      },
      fontFamily: {
        display: ["var(--font-instrument)", "ui-serif", "Georgia", "serif"],
        sans: ["var(--font-plex-sans)", "ui-sans-serif", "system-ui"],
        mono: ["var(--font-plex-mono)", "ui-monospace", "monospace"],
      },
      letterSpacing: {
        widish: "0.08em",
        widest: "0.24em",
      },
    },
  },
  plugins: [],
};

export default config;
