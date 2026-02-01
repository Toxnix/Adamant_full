# AGENTS.md

## Purpose
- Adamant is a JSON-Schema-based metadata editor and data intake tool for research workflows (FAIR-style metadata).
- The UI renders interactive forms from JSON Schema, validates input, and produces JSON datasets that can be ingested into MariaDB.

## High-level architecture
- Frontend (Vite + React) in `src/`
  - Loads schemas from the backend (`/api/get_schemas`) or uses bundled examples in `src/schemas`.
  - Supports schema upload, schema creation from scratch, editing, and drag/drop ordering of fields.
  - Create-from-scratch uses JSON Schema draft-07 and injects a required `FileTypeIdentifier` plus core fields (Identifier, Creator, ORCID, Date, Time, Project).
  - Validates form data with AJV (draft-04 + latest) and shows structured error messages.
  - Can upload a JSON dataset to pre-fill the form (drag/drop in the renderer uses `fillForm`).
  - Downloads JSON schema and JSON dataset; dataset download injects `SchemaID` derived from the selected schema file name.
  - Provides login UI; token stored in localStorage. Backend accepts `admin`/`admin` as credentials.
  - "Browse experiments" embeds the DB UI; dev uses `http://localhost:3001/db-ui/`, prod uses `/db-ui/`.
- Backend (Flask) in `backend/api.py`
  - Reads DB config from `backend/conf/db_config.json`.
  - Endpoints: `/api/check_mode`, `/api/get_schemas`, `/api/save_schema`, `/api/tables`, `/api/data/<table>`, `/api/columns/<table>`, `/api/left-join`, `/api/login`.
  - `/api/check_mode` reads `backend/conf/jobrequest-conf.json` (if present) to drive job request workflows.
  - `save_schema` injects a `SchemaID` property (enum = `$id`), writes schema to `backend/schemas/`, and (re)creates the matching DB table.
  - Table creation flattens object properties, maps JSON types to SQL, appends `documentlocation`, and drops the table before recreate (destructive).
- DB UI (Vite + React) in `db-ui/`
  - DataGrid page: lists tables, columns, rows; filters; hides FileTypeIdentifier/SchemaID/documentlocation by default; CSV/XLSX export.
  - Join page: uses `/api/left-join` to merge two tables and export results.

## Data ingestion / Nextcloud flow
- `bin/data_preprocessing.sh`
  - Watches Nextcloud raw data (default `NEXTCLOUD_DATA_DIR/rawData`).
  - Extracts `SchemaID`, copies JSON into `DATA_SORTED_DIR/SchemaID/`, and adds `documentlocation`.
- `bin/syncscript.sh`
  - `rsync` loop from Nextcloud host to local `LOCAL_DATA_DIR` (defaults to `data_sorted`).
- `bin/insert_data2db.sh`
  - Reads JSON in `DATA_SORTED_DIR` (folder name = table, file name = Identifier).
  - Inserts/upserts into MariaDB using `ON DUPLICATE KEY UPDATE`.
  - Uses `inotifywait` to process creates/deletes (delete removes by Identifier).
- `bin/insert_data2db_win.py` + `bin/insert_data2db_win.bat`
  - Windows loop importer; default source `data_inbox` (or `DATA_SOURCE_DIR`).
  - Uses `SchemaID` in JSON to pick the table, Identifier from `Identifier` field (or filename).
  - Tracks state in `.db_ingest_state.json` and can delete missing rows (`--delete-missing`).

## Configuration
- `.env` (see `env.example`) controls DB credentials, Nextcloud host/user/paths, data directories, and SSL settings.
- `backend/conf/db_config.json` is the runtime DB config (overwritten by deployment script).

## Development
- Frontend: `npm install` then `npm run dev` (root).
- Backend: `cd backend`, create venv, `pip install -r requirements.txt`, run `gunicorn -b :5000 api:app` or `flask run`.
- DB UI: `cd db-ui`, `npm install`, `npm run dev`.

## Deployment (Ubuntu 24.04)
- `deployment/deploy_web_server.sh`
  - Installs Node/Python/MariaDB/Nginx, builds frontend + db-ui to `/var/www/html/build`, configures systemd for backend, and sets cron for DB import.
- `deployment/deploy_nextcloud.sh`
  - Sets up `data_preprocessing.sh` and `syncscript.sh` on Nextcloud machine and registers cron jobs.
