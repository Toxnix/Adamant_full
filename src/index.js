import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter as Router } from "react-router-dom";
import { Provider } from 'react-redux';
import store from './redux/store';
//import { HashRouter as Router } from "react-router-dom";
import App from "./App";
import CssBaseline from "@material-ui/core/CssBaseline";

const rootElement = document.getElementById("root");
const root = createRoot(rootElement);

// strict mode is disabled so that findDOMNode warning is suppressed
root.render(
  <Provider store={store}>
    <Router>
      <CssBaseline />
      <App />
    </Router>
  </Provider>,
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
