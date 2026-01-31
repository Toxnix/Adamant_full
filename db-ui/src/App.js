import React, { useState } from "react";
import { AppBar, Toolbar, Typography, Select, MenuItem, Box } from "@mui/material";
import TablePage from "./TablePage"; // Seite für Tabellen
import DataGridPage from "./DataGridPage"; // Die Seite mit dem ursprünglichen Skript

const App = () => {
  const [currentPage, setCurrentPage] = useState("Page1");

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
            sx={{ color: "black", borderBottom: "2px solid white", variant: "outlined", backgroundColor: "white" }} 
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
          overflow: "auto",
        }}
      >
        {renderPage()}
      </Box>
    </div>
  );
};

export default App;
