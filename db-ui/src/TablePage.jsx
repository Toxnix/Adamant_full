import React, { useEffect, useMemo, useState } from "react";
import { MaterialReactTable, MRT_TablePagination } from "material-react-table";
import { Button, TextField, MenuItem, Switch, FormControlLabel } from "@mui/material";
import { mkConfig, generateCsv, download } from "export-to-csv";
import * as XLSX from "@e965/xlsx";

const DEFAULT_HIDDEN_COLUMN_KEY_MATCH =
  /(filetypeidentifier|schemaid|documentation[_-]?location|document[_-]?location|documatiatin[_-]?location)/i;
const DATE_TIME_COLUMN_KEY_MATCH = /^(date|time)$/i;
const DATE_TIME_DEFAULT_SIZE = 95; // 10% smaller than 105 (previous size)

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
  const [columnVisibility, setColumnVisibility] = useState({});
  const [exportSelectedOnly, setExportSelectedOnly] = useState(false);

  const filterOptionsByKey = useMemo(() => {
    const map = {};
    columns.forEach((col) => {
      const key = col.accessorKey;
      const values = new Set();
      rows.forEach((row) => {
        const value = row?.[key];
        if (value === undefined || value === null) return;
        if (typeof value === "string") values.add(value);
        else values.add(JSON.stringify(value));
      });
      map[key] = Array.from(values);
    });
    return map;
  }, [columns, rows]);

  const displayColumns = useMemo(
    () =>
      columns.map((col) => {
        const next = { ...col };
        const variant = col.filterVariant;
        if (variant === "autocomplete" || variant === "select" || variant === "multi-select") {
          next.filterSelectOptions = filterOptionsByKey[col.accessorKey] || [];
        }
        return next;
      }),
    [columns, filterOptionsByKey]
  );

  const defaultColumn = useMemo(
    () => ({
      minSize: 80,
      maxSize: 500,
      size: 140,
    }),
    []
  );

  useEffect(() => {
    if (!columns.length) return;
    setColumnVisibility((prev) => {
      let updated = false;
      const next = { ...prev };
      columns.forEach((col) => {
        const key = col.accessorKey;
        if (!key || !DEFAULT_HIDDEN_COLUMN_KEY_MATCH.test(key)) return;
        if (key in next) return;
        next[key] = false;
        updated = true;
      });
      return updated ? next : prev;
    });
  }, [columns]);

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

        const inferFilterConfigFromSample = (field, sampleRows) => {
          const maxSample = Math.min(sampleRows.length, 50);
          for (let i = 0; i < maxSample; i += 1) {
            const value = sampleRows[i]?.[field];
            if (value === undefined || value === null || value === "") continue;
            if (typeof value === "number" && Number.isFinite(value)) {
              return { filterVariant: "autocomplete" };
            }
            if (typeof value === "string") {
              const trimmed = value.trim();
              if (trimmed !== "" && /^-?\d+(?:\.\d+)?$/.test(trimmed)) {
                return { filterVariant: "autocomplete" };
              }
            }
            return { filterVariant: "autocomplete" };
          }
          return { filterVariant: "autocomplete" };
        };

        const allColumns = columns.map((field) => {
          const isDateOrTime = DATE_TIME_COLUMN_KEY_MATCH.test(field || "");
          return {
            accessorKey: field,
            header: field.charAt(0).toUpperCase() + field.slice(1),
            ...inferFilterConfigFromSample(field, data),
            ...(isDateOrTime ? { size: DATE_TIME_DEFAULT_SIZE } : null),
          };
        });

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

  const getExportRows = (table) => {
    if (!exportSelectedOnly) return rows;
    return table.getSelectedRowModel().rows.map((row) => row.original);
  };

  const handleExportCsv = (table) => {
    const exportRows = getExportRows(table);
    if (!exportRows.length) {
      alert(exportSelectedOnly ? "No rows selected." : "No data to export.");
      return;
    }
    const csv = generateCsv(csvConfig)(exportRows);
    download(csvConfig)(csv);
  };

  const handleExportExcel = (table) => {
    const exportRows = getExportRows(table);
    if (!exportRows.length) {
      alert(exportSelectedOnly ? "No rows selected." : "No data to export.");
      return;
    }
    const headers = columns.map((col) => col.accessorKey);
    const worksheet = XLSX.utils.json_to_sheet(exportRows, { header: headers });
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
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <div style={{ display: "flex", gap: "8px", marginBottom: "8px", padding: "4px 4px 0 4px", alignItems: "center" }}>
        <TextField
          size="small"
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
          size="small"
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
          size="small"
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
          size="small"
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
          size="small"
          variant="contained"
          onClick={fetchJoinData}
          style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
        >
          Fetch Data
        </Button>
      </div>

      <div style={{ flex: 1, minHeight: 0, minWidth: 0, overflow: "hidden" }}>
        <MaterialReactTable
          columns={displayColumns}
          data={rows}
          defaultColumn={defaultColumn}
          enableColumnFilters
          enableColumnFilterModes
          enableColumnResizing
          columnResizeMode="onChange"
          enableFacetedValues={false}
          columnFilterDisplayMode="popover"
          enableRowSelection
          enableDensityToggle={false}
          enableStickyHeader
          enableTopToolbar
          enableBottomToolbar={false}
          renderTopToolbarCustomActions={({ table }) => (
            <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <MRT_TablePagination table={table} />
              {rows.length > 0 && (
                <>
                  <FormControlLabel
                    label="export only selection"
                    control={
                      <Switch
                        size="small"
                        checked={exportSelectedOnly}
                        onChange={(e) => setExportSelectedOnly(e.target.checked)}
                      />
                    }
                  />
                  <Button
                    size="small"
                    variant="contained"
                    onClick={() => handleExportCsv(table)}
                    style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
                  >
                    Export as CSV
                  </Button>
                  <Button
                    size="small"
                    variant="contained"
                    onClick={() => handleExportExcel(table)}
                    style={{ minWidth: 150, backgroundColor: "#3f51b5", color: "#fff" }}
                  >
                    Export as Excel
                  </Button>
                </>
              )}
            </div>
          )}
          getRowId={(_, index) => index.toString()}
          state={{ columnVisibility }}
          onColumnVisibilityChange={setColumnVisibility}
          initialState={{ pagination: { pageSize: 10, pageIndex: 0 }, density: "compact" }}
          muiPaginationProps={{ rowsPerPageOptions: [10, 25, 50, 100, 250, 500] }}
          muiTableContainerProps={{
            sx: {
              flex: 1,
              maxWidth: "100%",
              minWidth: 0,
              overflow: "auto",
              scrollbarWidth: "auto",
              "&::-webkit-scrollbar": {
                width: 22,
                height: 22,
              },
              "&::-webkit-scrollbar-thumb": {
                backgroundColor: "rgba(0, 0, 0, 0.35)",
                borderRadius: 8,
                border: "5px solid transparent",
                backgroundClip: "content-box",
              },
              "&::-webkit-scrollbar-track": {
                backgroundColor: "rgba(0, 0, 0, 0.06)",
              },
            },
          }}
          muiTableProps={{
            sx: {
              width: "max-content",
              minWidth: "100%",
              tableLayout: "auto",
            },
          }}
          displayColumnDefOptions={{
            "mrt-row-select": { size: 36, minSize: 36, maxSize: 44 },
          }}
          muiTableHeadCellProps={{
            sx: {
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              borderRight: "1px solid rgba(0, 0, 0, 0.12)",
            },
          }}
          muiTableBodyCellProps={{
            sx: {
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              borderRight: "1px solid rgba(0, 0, 0, 0.12)",
            },
          }}
          muiTablePaperProps={{
            sx: {
              boxShadow: "none",
              border: "none",
              height: "100%",
              display: "flex",
              flexDirection: "column",
              minWidth: 0,
            },
          }}
        />
      </div>
    </div>
  );
}

export default JoinTablePage;
