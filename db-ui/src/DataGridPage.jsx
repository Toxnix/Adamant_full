import React, { useEffect, useMemo, useState } from "react";
import { MaterialReactTable, MRT_TablePagination } from "material-react-table";
import { TextField, MenuItem, Button, FormControl, Switch, FormControlLabel } from "@mui/material";
import { mkConfig, generateCsv, download } from "export-to-csv";
import * as XLSX from "@e965/xlsx";
import { toast, ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

const DEFAULT_HIDDEN_COLUMN_KEY_MATCH =
  /(filetypeidentifier|schemaid|documentation[_-]?location|document[_-]?location|documatiatin[_-]?location)/i;
const DATE_TIME_COLUMN_KEY_MATCH = /^(date|time)$/i;
const DATE_TIME_DEFAULT_SIZE = 95; // 10% smaller than 105 (previous size)

function DataGridPage() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState("");
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
        const getFilterConfigFromDbType = (dbType) => {
          const normalized = (dbType || "").toLowerCase();
          if (/(int|decimal|numeric|float|double|real)/.test(normalized)) {
            return { filterVariant: "autocomplete" };
          }
          return { filterVariant: "autocomplete" };
        };

        const formattedColumns = columnData.map((column) => {
          const filterConfig = getFilterConfigFromDbType(column.type);
          const isDateOrTime = DATE_TIME_COLUMN_KEY_MATCH.test(column.name || "");
          return {
            accessorKey: column.name,
            header: column.name.charAt(0).toUpperCase() + column.name.slice(1),
            ...filterConfig,
            ...(isDateOrTime ? { size: DATE_TIME_DEFAULT_SIZE } : null),
          };
        });
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

  const getExportRows = (table) => {
    if (!exportSelectedOnly) return rows;
    return table.getSelectedRowModel().rows.map((row) => row.original);
  };

  const handleExportCsv = (table) => {
    const exportRows = getExportRows(table);
    if (!exportRows.length) {
      toast.info(exportSelectedOnly ? "No rows selected." : "No data to export.");
      return;
    }
    const csv = generateCsv(csvConfig)(exportRows);
    download(csvConfig)(csv);
  };

  const handleExportExcel = (table) => {
    const exportRows = getExportRows(table);
    if (!exportRows.length) {
      toast.info(exportSelectedOnly ? "No rows selected." : "No data to export.");
      return;
    }
    const safeSheetName = (name) => {
      const cleaned = (name || "Data").toString().replace(/[\[\]\:\*\?\/\\]/g, "_");
      return cleaned.length > 31 ? cleaned.slice(0, 31) : cleaned;
    };
    const headers = columns.map((col) => col.accessorKey);
    const worksheet = XLSX.utils.json_to_sheet(exportRows, { header: headers });
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, safeSheetName(selectedTable));
    XLSX.writeFile(workbook, `${selectedTable || "data"}.xlsx`);
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
      <ToastContainer />
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
          muiTopToolbarProps={{
            sx: {
              position: "sticky",
              top: 0,
              zIndex: 2,
              backgroundColor: "#fff",
            },
          }}
          renderTopToolbarCustomActions={({ table }) => (
            <div style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
              <FormControl size="small" style={{ minWidth: 260 }}>
                <TextField
                  select
                  label="Select Table"
                  value={selectedTable}
                  onChange={(e) => setSelectedTable(e.target.value)}
                  disabled={tables.length === 0}
                  size="small"
                  InputLabelProps={{ shrink: true }}
                >
                  {tables.map((table) => (
                    <MenuItem key={table} value={table}>
                      {table}
                    </MenuItem>
                  ))}
                </TextField>
              </FormControl>
              <MRT_TablePagination table={table} />
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
              <Button size="small" variant="contained" onClick={() => handleExportCsv(table)}>
                Export as CSV
              </Button>
              <Button size="small" variant="contained" onClick={() => handleExportExcel(table)}>
                Export as Excel
              </Button>
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

export default DataGridPage;
