import React, { useState } from 'react';
import TextField from '@mui/material/TextField';
import Button from '@mui/material/Button';
import QPTDATLogo from "../assets/adamant-header-5.svg";
import EMPIRFLogo from "../assets/EMPI_Logo_reactive-fluids_Color_Black.png"

const Login = ({ onLoginSuccess }) => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [loginError, setLoginError] = useState(false); // State to track login error
  
    // const handleLogin = () => {
    //   if (username === 'admin' && password === 'admin!') {
    //     console.log('Login successful');
    //     onLoginSuccess(); // Call the callback function provided by the parent component
    //   } else {
    //     console.log('Login failed');
    //     setLoginError(true); // Set login error state to true
    //   }
    // };
    const handleLogin = async () => {
    try {
      // Send a login request to the backend API
      const response = await fetch('/api/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      });

      if (response.ok) {
        // If login is successful, handle it in the parent component
        const data = await response.json();
        const token = data.token;

        localStorage.setItem('sessionToken', token);

        console.log('Login successful');
        onLoginSuccess();
        // Redirect or perform any other action on successful login
      } else {
        // If login fails, display an error message to the user
        console.log('Login failed');
        setLoginError(true);
      }
    } catch (error) {
      // Handle network errors or other exceptions
      console.error('Error during login:', error);
      // Display an error message to the user
    }
    };
    
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
      <img
            style={{ height: "100px", borderRadius: "2px"}}
            alt="header"
            src={QPTDATLogo}
          />
          <img
            style={{ height: "70px", borderRadius: "5px", marginTop: "20px"}}
            alt="empi-rf"
            src={EMPIRFLogo}
          />
      <h2>Login</h2>
      <TextField
        label="Username"
        value={username}
        onChange={(e) => setUsername(e.target.value)}
        variant="outlined"
        margin="normal"
      />
      <TextField
        label="Password"
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        variant="outlined"
        margin="normal"
      />
      {loginError && <p style={{ color: 'red', marginBottom: '10px' }}>Invalid username or password. Please try again.</p>}
      <Button variant="contained" color="primary" onClick={handleLogin}>
        Login
      </Button>
    </div>
  );
};

export default Login;