import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter as Router } from "react-router-dom";
import { Provider } from "react-redux";
import store from "./redux/store";
//import { HashRouter as Router } from "react-router-dom";
import App from "./App";
import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider, createTheme } from "@mui/material/styles";

const rootElement = document.getElementById("root");
const root = createRoot(rootElement);
const theme = createTheme();

// strict mode is disabled so that findDOMNode warning is suppressed
root.render(
  <Provider store={store}>
    <ThemeProvider theme={theme}>
      <Router>
        <CssBaseline />
        <App />
      </Router>
    </ThemeProvider>
  </Provider>
);

//use this for strict mode, however it always throws the findDOMNode warning
/*root.render(
  <React.StrictMode>
    <Router>
      <CssBaseline />
      <App />
    </Router>
  </React.StrictMode>,
);
*/
