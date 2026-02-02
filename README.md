# <img src="https://raw.githubusercontent.com/csihda/adamant/6b2a50dff162b0fc7af0dc6873d7e9d34cfa93aa/src/assets/adamant-header-5.svg" alt="Adamant Logo" style="width:45%;"/> <img src="src/assets/EMPI_Logo_reactive-fluids_Color_Black.png" alt="EMPI-RF Logo" style="width:45%;"/>

# SchemaFlow

SchemaFlow is a JSON-Schema-based workflow for FAIR metadata in a Nextcloud + MariaDB environment. It lets you design schemas, collect metadata as JSON, and ingest those datasets into MariaDB via WebDAV.

This project builds on the upstream work at `https://github.com/plasma-mds/adamant`.

## What is new in this workflow

- **WebDAV to MariaDB ingest** (`bin/webdav_ingest.py`) with state tracking
- **End-to-end Nextcloud workflow** for FAIR metadata
- **Embedded DB Web UI** for browsing and joins
- **One-shot Ubuntu deployment script** with systemd services
- **Nginx Basic Auth** for the entire UI/API

## Features

- Render interactive forms from JSON Schema
- Create schemas from scratch (draft-07, fixed)
- Edit schemas and reorder fields via drag & drop
- Upload JSON datasets to pre-fill forms
- Download JSON schema and datasets (adds `SchemaID`)
- WebDAV ingest (`bin/webdav_ingest.py`) for automated DB upserts
- Embedded DB UI for browsing and joining tables

## Workflow focus (FAIR metadata)

SchemaFlow is designed around a practical workflow for FAIR metadata in a Nextcloud environment:

1. **Schema authoring** in the web UI (JSON Schema draft-07).
2. **Dataset creation** by researchers using the rendered forms.
3. **Storage in Nextcloud** (WebDAV) as JSON files.
4. **Automated ingest** into MariaDB via the WebDAV ingester.
5. **Exploration and export** through the embedded Web UI.

## Supported JSON Schema Keywords

Draft-07 is used for new and edited schemas.

| Field Type | Implemented Keywords | Notes |
|------------|-----------------------|-------|
| String     | `title`, `id`, `$id`, `description`, `type`, `enum`, `contentEncoding`, `default`, `minLength`, `maxLength` | `contentEncoding` supports "base64" |
| Number     | `title`, `id`, `$id`, `description`, `type`, `enum`, `default`, `minimum`, `maximum` | |
| Integer    | `title`, `id`, `$id`, `description`, `type`, `enum`, `default`, `minimum`, `maximum` | |
| Boolean    | `title`, `id`, `$id`, `description`, `type`, `default` | |

## Development

Use the WebDAV ingester during development:

```bash
python bin/webdav_ingest.py --once
```

## WebDAV Ingest

Configure `.env` using `env.example`, then run:

```bash
python bin/webdav_ingest.py --once
```

Use `--watch` (or omit `--once`) for polling mode.

## Deployment (Ubuntu 24.04)

Use `deployment/deploy_web_server.sh` to install system dependencies, build frontend + db-ui, and configure the backend service.
