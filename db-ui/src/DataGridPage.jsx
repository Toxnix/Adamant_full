import React, { useEffect, useMemo, useState } from "react";
import { MaterialReactTable } from "material-react-table";
import { TextField, MenuItem, Button, FormControl } from "@mui/material";
import { mkConfig, generateCsv, download } from "export-to-csv";
import * as XLSX from "@e965/xlsx";
import { toast, ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

function DataGridPage() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState("");
  const [rows, setRows] = useState([]);
  const [columns, setColumns] = useState([]);
  const [density, setDensity] = useState("compact");

  // Fetch table list from the backend
  useEffect(() => {
    fetch("/api/tables")
      .then((response) => response.json())
      .then((data) => {
        if (data.length > 0) {
          setTables(data);
          setSelectedTable(data[0]);
        } else {
          setTables([]);
          setSelectedTable("");
          toast.warning("Database is offline. No tables available.");
        }
      })
      .catch((err) => {
        console.error("Error fetching table list:", err);
        setTables([]);
        setSelectedTable("");
        toast.error("Unable to connect to the database. Please try again later.");
      });
  }, []);

  // Fetch data and column information of the selected table
  useEffect(() => {
    if (!selectedTable) {
      setRows([]);
      setColumns([]);
      return;
    }

    fetch(`/api/data/${selectedTable}`)
      .then((response) => response.json())
      .then((data) => {
        setRows(Array.isArray(data) ? data : []);
      })
      .catch((err) => {
        console.error("Error fetching table data:", err);
        setRows([]);
        toast.error("Failed to fetch data for the selected table.");
      });

    fetch(`/api/columns/${selectedTable}`)
      .then((response) => response.json())
      .then((columnData) => {
        const formattedColumns = columnData.map((column) => ({
          accessorKey: column.name,
          header: column.name.charAt(0).toUpperCase() + column.name.slice(1),
        }));
        setColumns(formattedColumns);
      })
      .catch((err) => {
        console.error("Error fetching column information:", err);
        setColumns([]);
        toast.error("Failed to fetch column information for the selected table.");
      });
  }, [selectedTable]);

  const columnHeaders = useMemo(
    () =>
      columns.map((col) => ({
        key: col.accessorKey,
        displayLabel: col.header,
      })),
    [columns]
  );

  const csvConfig = useMemo(
    () =>
      mkConfig({
        filename: selectedTable || "table",
        useKeysAsHeaders: false,
        columnHeaders,
      }),
    [selectedTable, columnHeaders]
  );

  const handleExportCsv = () => {
    if (!rows.length) {
      toast.info("No data to export.");
      return;
    }
    const csv = generateCsv(csvConfig)(rows);
    download(csvConfig)(csv);
  };

  const handleExportExcel = () => {
    if (!rows.length) {
      toast.info("No data to export.");
      return;
    }
    const headers = columns.map((col) => col.accessorKey);
    const worksheet = XLSX.utils.json_to_sheet(rows, { header: headers });
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, selectedTable || "Data");
    XLSX.writeFile(workbook, `${selectedTable || "data"}.xlsx`);
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        height: "100%",
        overflowX: "auto",
      }}
    >
      <ToastContainer />
      <div style={{ display: "flex", gap: "10px", marginBottom: "30px", padding: "5px" }}>
        <FormControl style={{ minWidth: 500 }}>
          <TextField
            select
            label="Select Table"
            value={selectedTable}
            onChange={(e) => setSelectedTable(e.target.value)}
            disabled={tables.length === 0}
          >
            {tables.map((table) => (
              <MenuItem key={table} value={table}>
                {table}
              </MenuItem>
            ))}
          </TextField>
        </FormControl>

        <Button variant="contained" onClick={handleExportCsv}>
          Export as CSV
        </Button>
        <Button variant="contained" onClick={handleExportExcel}>
          Export as Excel
        </Button>

        <FormControl style={{ minWidth: 150 }}>
          <TextField
            select
            label="Grid Density"
            value={density}
            onChange={(e) => setDensity(e.target.value)}
          >
            <MenuItem value="compact">Compact</MenuItem>
            <MenuItem value="comfortable">Comfortable</MenuItem>
            <MenuItem value="spacious">Spacious</MenuItem>
          </TextField>
        </FormControl>
      </div>

      <div style={{ height: "700px", overflow: "auto", overflowX: "auto" }}>
        <MaterialReactTable
          columns={columns}
          data={rows}
          enableRowSelection
          enableDensityToggle={false}
          enableStickyHeader
          onDensityChange={setDensity}
          state={{ density }}
          getRowId={(_, index) => index.toString()}
          initialState={{ pagination: { pageSize: 10, pageIndex: 0 } }}
          muiTableContainerProps={{ sx: { maxHeight: "700px" } }}
        />
      </div>
    </div>
  );
}

export default DataGridPage;
