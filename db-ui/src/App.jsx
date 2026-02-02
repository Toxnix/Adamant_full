import React, { useEffect, useState } from "react";
import { AppBar, Toolbar, Typography, Select, MenuItem, Box } from "@mui/material";
import TablePage from "./TablePage"; // Seite für Tabellen
import DataGridPage from "./DataGridPage"; // Die Seite mit dem ursprünglichen Skript

const App = () => {
  const [currentPage, setCurrentPage] = useState(() => {
    try {
      return localStorage.getItem("adamant_dbui_page") || "Page1";
    } catch (e) {
      return "Page1";
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem("adamant_dbui_page", currentPage);
    } catch (e) {
      // ignore storage errors
    }
  }, [currentPage]);

  // Render-Funktion für Seiten
  const renderPage = () => {
    switch (currentPage) {
      case "Page1":
        return <DataGridPage />;
      case "Page2":
        return <TablePage />;
      default:
        return <Typography variant="h6">Seite nicht gefunden.</Typography>;
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      {/* Titelleiste */}
      <AppBar position="static" sx={{ backgroundColor: '#3f51b5' }}>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Adamant SQL Database
          </Typography>
          {/* Seitenauswahl */}
          <Select
            value={currentPage}
            onChange={(e) => setCurrentPage(e.target.value)}
            sx={{ color: "black", borderBottom: "2px solid white", variant: "outlined", backgroundColor: "white", width: 320 }} 
          >
            <MenuItem value="Page1">Investigate Tables</MenuItem>
            <MenuItem value="Page2">Join Tables</MenuItem>
          </Select>
        </Toolbar>
      </AppBar>

      {/* Seiteninhalt */}
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          overflow: "hidden",
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          minWidth: 0,
        }}
      >
        {renderPage()}
      </Box>
    </div>
  );
};

export default App;
