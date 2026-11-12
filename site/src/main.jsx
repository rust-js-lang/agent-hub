import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../website/src/App.jsx";
import "./style.css";

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
