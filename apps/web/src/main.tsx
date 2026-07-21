import React from "react";
import ReactDOM from "react-dom/client";
import { Cockpit } from "./Cockpit";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Cockpit />
  </React.StrictMode>,
);
