# SchemaFlow

SchemaFlow is a JSON-Schema-based workflow for FAIR metadata in a Nextcloud + MariaDB environment.
It lets you:

- define metadata requirements as JSON Schema,
- collect metadata as validated JSON files (via a form UI),
- store those JSON files in Nextcloud,
- ingest them into MariaDB via WebDAV so they become queryable, filterable and exportable.

This project builds on upstream work at `https://github.com/plasma-mds/adamant`.

## Why this makes FAIR metadata easier

FAIR is hard when metadata is collected in ad-hoc spreadsheets or free-text forms. SchemaFlow reduces the friction by turning metadata into a schema-driven workflow:

- Findable: once ingested, metadata lives in MariaDB tables and can be searched, filtered, joined and exported.
- Accessible: datasets are stored as plain JSON files in Nextcloud (WebDAV) and can be processed automatically.
- Interoperable: JSON Schema is an explicit contract (types, required fields, enums) that tools can validate against.
- Reusable: validation and controlled vocabularies reduce ambiguity and ensure comparable metadata across experiments.

In practice this means fewer missing fields, fewer inconsistent spellings/units, and fewer "unknown" values when you later want to aggregate or reuse data.

## What the system does (high level)

SchemaFlow combines four parts:

1. Frontend (schema editor + form renderer)
   - Renders interactive forms from JSON Schema.
   - Validates user input and shows structured error messages.
   - Downloads JSON datasets (including `SchemaID`) and schemas.

2. Backend API (schema storage + MariaDB table management)
   - Stores schemas and injects a `SchemaID` property.
   - (Re)creates the matching MariaDB table from the schema.

3. WebDAV ingester (`bin/webdav_ingest.py`)
   - Scans a Nextcloud folder via WebDAV.
   - Validates JSON files against local schemas.
   - Inserts the datasets into the corresponding MariaDB tables.
   - Tracks state so unchanged files are skipped.

4. DB UI (embedded)
   - Browse tables/rows/columns.
   - Filter and export to CSV/XLSX.
   - Run left-joins for combined exports.


## Typical workflow (FAIR metadata)

1. Define or edit a schema in the web UI (JSON Schema draft-07).
2. Researchers fill in the generated form; the UI validates required fields and types.
3. The resulting metadata is saved as a JSON file in Nextcloud.
4. The ingester reads that folder via WebDAV and writes the metadata into MariaDB.
5. Metadata can now be browsed and exported from the DB UI.

## Features

- Schema-driven metadata entry (forms generated from JSON Schema)
- Create schemas from scratch (JSON Schema draft-07 + project core fields)
- Edit schemas and reorder fields via drag & drop
- Upload JSON datasets to pre-fill forms
- Download JSON schema and JSON dataset (datasets include `SchemaID`)
- WebDAV ingest (`bin/webdav_ingest.py`) into MariaDB with state tracking
- Embedded DB UI for browsing, joins, and export

## Supported JSON Schema Keywords

Draft-07 is used for new and edited schemas.

| Field Type | Implemented Keywords | Notes |
|------------|-----------------------|-------|
| String     | `title`, `id`, `$id`, `description`, `type`, `enum`, `contentEncoding`, `default`, `minLength`, `maxLength` | `contentEncoding` supports "base64" |
| Number     | `title`, `id`, `$id`, `description`, `type`, `enum`, `default`, `minimum`, `maximum` | |
| Integer    | `title`, `id`, `$id`, `description`, `type`, `enum`, `default`, `minimum`, `maximum` | |
| Boolean    | `title`, `id`, `$id`, `description`, `type`, `default` | |



## WebDAV ingest

What the ingester does:

1. PROPFINDs the configured WebDAV folder recursively and collects `.json` files.
2. Skips unchanged files based on `etag`/`last_modified` and the `ingest_state` table.
3. Validates each JSON payload against the matching local schema in `backend/schemas/`.
4. Inserts the dataset into the MariaDB table that corresponds to the file's `SchemaID`.

The ingester keeps state in MariaDB:

- `ingest_state`: per file (path, etag/last_modified, schema_id, identifier, status/error)
- `ingest_folder_state`: per folder (used to skip folders when the WebDAV server provides stable folder metadata)

## Deployment (Ubuntu 24.04)

Use `deployment/deploy_web_server.sh` to install system dependencies, build frontend + db-ui, and configure the backend service.
