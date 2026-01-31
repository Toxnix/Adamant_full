import React, { useEffect } from "react";
import "./styles.css";
import { Route, Switch, Redirect} from "react-router-dom";
import { useDispatch, useSelector } from 'react-redux';
import { loginSuccess, logoutSuccess } from './redux/actions/authActions';
import AdamantMain from "./pages/AdamantMain";
import "cors";
import packageJson from "../package.json";
import { ToastContainer } from "react-toastify";
import Login from "../src/components/Login";

export default function App() {
  // const [loggedIn, setLoggedIn] = useState(false); // State to track login status

  // check if adamant endpoint exists in the homepage
  const homepage = packageJson["homepage"];
  const adamantEndpoint = homepage.includes("/adamant");
  const loggedIn = useSelector(state => state.auth.loggedIn);
  const dispatch = useDispatch();

  // useEffect(() => {
  //     console.log('refreshed ');
  //     setLoggedIn(localStorage.getItem('sessionToken') != undefined);
  // }, []);

  useEffect(() => {
    const sessionToken = localStorage.getItem('sessionToken');
    if (sessionToken) {
      dispatch(loginSuccess());
    }
  }, [dispatch]);

  const handleLoginSuccess = () => {
    console.log('loginSuccessCalled');
    dispatch(loginSuccess());
    // fetchProtectedData();
  };

  const handleLogout = (e) => {
    // Clear session state
    dispatch(logoutSuccess());
    // Remove session token from localStorage
    localStorage.removeItem('sessionToken');
  };

  if (adamantEndpoint) {
    console.log("/adamant endpoint is detected")
    return (
      /** Use this for if homepage has /adamant endpoint, this is only for deploying on github-page */
      <>
        <div className="the_app">
          <Switch>
            <Route exact path="/">
              {loggedIn ? (
                <Redirect to="/adamant" />
              ) : (
                <Login onLoginSuccess={handleLoginSuccess} />
              )}
            </Route>
            <Route exact path="/adamant">
              {loggedIn ? (
                <AdamantMain onLogout={handleLogout} />
              ) : (
                <Redirect to="/" />
              )}
            </Route>
          </Switch>
        </div>
        <ToastContainer
          position="top-right"
          autoClose={5000}
          hideProgressBar={false}
          closeOnClick={true}
          pauseOnHover={true}
          draggable={false}
          progress={undefined} />
      </>
    );
  } else {
    return (
      <>
        <div className="the_app">
          <Switch>
              <Route exact path="/">
                {loggedIn ? (
                  <Redirect to="/adamant" />
                ) : (
                  <Login onLoginSuccess={handleLoginSuccess} />
                )}
              </Route>
              <Route exact path="/adamant" component={AdamantMain}></Route>
          </Switch>
        </div>
        <ToastContainer
          position="top-right"
          autoClose={5000}
          hideProgressBar={false}
          closeOnClick={true}
          pauseOnHover={true}
          draggable={false}
          progress={undefined} />
      </>
    );
  };
};
