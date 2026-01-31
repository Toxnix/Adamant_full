import React, { useEffect, useMemo, useState } from "react";
import { MaterialReactTable } from "material-react-table";
import { Button, TextField, MenuItem } from "@mui/material";
import { mkConfig, generateCsv, download } from "export-to-csv";
import * as XLSX from "@e965/xlsx";

function JoinTablePage() {
  const [tables, setTables] = useState([]);
  const [table1, setTable1] = useState("");
  const [table2, setTable2] = useState("");
  const [columns1, setColumns1] = useState([]);
  const [columns2, setColumns2] = useState([]);
  const [column1, setColumn1] = useState("");
  const [column2, setColumn2] = useState("");
  const [rows, setRows] = useState([]);
  const [columns, setColumns] = useState([]);
  const [density, setDensity] = useState("compact");

  useEffect(() => {
    fetch("/api/tables")
      .then((response) => response.json())
      .then((data) => setTables(data))
      .catch((err) => console.error("Error fetching tables:", err));
  }, []);

  useEffect(() => {
    if (!table1) return;

    fetch(`/api/columns/${table1}`)
      .then((response) => response.json())
      .then((data) => setColumns1(data.map((col) => col.name)))
      .catch((err) => console.error("Error fetching columns for table 1:", err));
  }, [table1]);

  useEffect(() => {
    if (!table2) return;

    fetch(`/api/columns/${table2}`)
      .then((response) => response.json())
      .then((data) => setColumns2(data.map((col) => col.name)))
      .catch((err) => console.error("Error fetching columns for table 2:", err));
  }, [table2]);

  const fetchJoinData = () => {
    if (!table1 || !table2 || !column1 || !column2) {
      alert("Please fill all fields before performing the join.");
      return;
    }

    fetch(
      `/api/left-join?table1=${table1}&table2=${table2}&column1=${column1}&column2=${column2}`
    )
      .then((response) => response.json())
      .then(({ columns, data }) => {
        if (data.length === 0) {
          alert("No data available for the join.");
        }

        const allColumns = columns.map((field) => ({
          accessorKey: field,
          header: field.charAt(0).toUpperCase() + field.slice(1),
        }));

        setRows(data);
        setColumns(allColumns);
      })
      .catch((err) => console.error("Error fetching join data:", err));
  };

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
        filename: "joined-table",
        useKeysAsHeaders: false,
        columnHeaders,
      }),
    [columnHeaders]
  );

  const handleExportCsv = () => {
    if (!rows.length) {
      alert("No data to export.");
      return;
    }
    const csv = generateCsv(csvConfig)(rows);
    download(csvConfig)(csv);
  };

  const handleExportExcel = () => {
    if (!rows.length) {
      alert("No data to export.");
      return;
    }
    const headers = columns.map((col) => col.accessorKey);
    const worksheet = XLSX.utils.json_to_sheet(rows, { header: headers });
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "JoinedData");
    XLSX.writeFile(workbook, "joined-table.xlsx");
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        height: "100%",
      }}
    >
      <div style={{ display: "flex", gap: "10px", marginBottom: "10px" }}>
        <TextField
          select
          label="Table 1"
          value={table1}
          onChange={(e) => setTable1(e.target.value)}
          style={{ minWidth: 200 }}
        >
          {tables.map((table) => (
            <MenuItem key={table} value={table}>
              {table}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          label="Table 2"
          value={table2}
          onChange={(e) => setTable2(e.target.value)}
          style={{ minWidth: 200 }}
        >
          {tables.map((table) => (
            <MenuItem key={table} value={table}>
              {table}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          label="Column from Table 1"
          value={column1}
          onChange={(e) => setColumn1(e.target.value)}
          style={{ minWidth: 200 }}
          variant="outlined"
          disabled={!columns1.length}
        >
          {columns1.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          label="Column from Table 2"
          value={column2}
          onChange={(e) => setColumn2(e.target.value)}
          style={{ minWidth: 200 }}
          disabled={!columns2.length}
        >
          {columns2.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </TextField>

        <Button
          variant="contained"
          onClick={fetchJoinData}
          style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
        >
          Fetch Data
        </Button>

        {rows.length > 0 && (
          <>
            <Button
              variant="contained"
              onClick={handleExportCsv}
              style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
            >
              Export as CSV
            </Button>
            <Button
              variant="contained"
              onClick={handleExportExcel}
              style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
            >
              Export as Excel
            </Button>
          </>
        )}

        <TextField
          select
          label="Grid Density"
          value={density}
          onChange={(e) => setDensity(e.target.value)}
          style={{ minWidth: 150 }}
        >
          <MenuItem value="compact">Compact</MenuItem>
          <MenuItem value="comfortable">Comfortable</MenuItem>
          <MenuItem value="spacious">Spacious</MenuItem>
        </TextField>
      </div>

      <div style={{ height: "700px", overflow: "auto" }}>
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

export default JoinTablePage;
