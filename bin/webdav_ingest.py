import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import pymysql
import requests
from jsonschema import Draft4Validator, Draft7Validator

FILE_TYPE_IDENTIFIER = "This is a EMPI-RF metadata File. Do not change this for crawler identification"
logger = logging.getLogger("webdav_ingest")


def setup_logging(level_name: str) -> None:
    level = logging.getLevelName(level_name.upper())
    if isinstance(level, str):
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_env_file(path: Path) -> Dict[str, str]:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def resolve_path(path: str, base_dir: Path) -> Path:
    path_obj = Path(path)
    if not path_obj.is_absolute():
        path_obj = (base_dir / path_obj).resolve()
    return path_obj


def load_config(repo_root: Path) -> Dict[str, str]:
    env = {}
    env.update(load_env_file(repo_root / ".env"))
    env.update(load_env_file(repo_root / "bin" / ".env"))

    db_cfg_path = repo_root / "backend" / "conf" / "db_config.json"
    db_cfg = {}
    if db_cfg_path.exists():
        db_cfg = json.loads(db_cfg_path.read_text(encoding="utf-8"))

    cfg = {
        "db_host": env.get("DB_HOST", db_cfg.get("DB_HOST", "127.0.0.1")),
        "db_port": int(env.get("DB_PORT", db_cfg.get("DB_PORT", 3306))),
        "db_user": env.get("DB_USER", db_cfg.get("DB_USER", "root")),
        "db_password": env.get("DB_PASSWORD", db_cfg.get("DB_PASSWORD", "password")),
        "db_name": env.get("DB_NAME", db_cfg.get("DB_NAME", "experiment_data")),
        "webdav_url": env.get("WEBDAV_URL", "https://example.nextcloud.com/remote.php/dav/files/demo/"),
        "webdav_user": env.get("WEBDAV_USER", "demo"),
        "webdav_password": env.get("WEBDAV_PASSWORD", "demo_password"),
        "webdav_root": env.get("WEBDAV_ROOT", "EMPI-RF"),
        "schema_dir": env.get("SCHEMA_DIR", str(repo_root / "backend" / "schemas")),
        "poll_interval": int(env.get("POLL_INTERVAL", 10)),
        "allowed_schemaids": env.get("ALLOWED_SCHEMAIDS", "").strip(),
    }

    return cfg


def normalize_webdav_url(url: str) -> str:
    if not url.endswith("/"):
        return url + "/"
    return url


def build_file_url(base_url: str, href: str) -> str:
    parsed = urlparse(base_url)
    if href.startswith(parsed.path):
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return urljoin(base_url, href.lstrip("/"))


def propfind(session: requests.Session, url: str) -> List[Dict[str, str]]:
    headers = {"Depth": "1"}
    body = """
    <d:propfind xmlns:d="DAV:">
      <d:prop>
        <d:getetag />
        <d:getlastmodified />
        <d:getcontentlength />
        <d:resourcetype />
      </d:prop>
    </d:propfind>
    """.strip()
    response = session.request("PROPFIND", url, data=body, headers=headers)
    response.raise_for_status()
    return parse_propfind(response.text)


def parse_propfind(xml_text: str) -> List[Dict[str, str]]:
    ns = {"d": "DAV:"}
    root = ET.fromstring(xml_text)
    items = []
    for resp in root.findall("d:response", ns):
        href = resp.findtext("d:href", default="", namespaces=ns)
        propstat = resp.find("d:propstat", ns)
        if propstat is None:
            continue
        prop = propstat.find("d:prop", ns)
        if prop is None:
            continue
        res_type = prop.find("d:resourcetype", ns)
        is_collection = res_type is not None and res_type.find("d:collection", ns) is not None
        items.append({
            "href": href,
            "etag": prop.findtext("d:getetag", default="", namespaces=ns),
            "last_modified": prop.findtext("d:getlastmodified", default="", namespaces=ns),
            "content_length": prop.findtext("d:getcontentlength", default="", namespaces=ns),
            "is_collection": is_collection,
        })
    return items


def get_state_map(cursor, paths: List[str]) -> Dict[str, Dict[str, str]]:
    if not paths:
        return {}
    state = {}
    chunk_size = 500
    for i in range(0, len(paths), chunk_size):
        chunk = paths[i:i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        query = f"SELECT path, etag, last_modified, status FROM ingest_state WHERE path IN ({placeholders})"
        cursor.execute(query, chunk)
        for path, etag, last_modified, status in cursor.fetchall():
            state[path] = {
                "etag": etag,
                "last_modified": last_modified,
                "status": status,
            }
    return state


def ensure_state_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_state (
            path VARCHAR(512) PRIMARY KEY,
            etag VARCHAR(255),
            last_modified VARCHAR(255),
            schema_id VARCHAR(255),
            identifier VARCHAR(255),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(32),
            error TEXT
        )
        """
    )


def table_exists(cursor, table: str) -> bool:
    cursor.execute("SHOW TABLES LIKE %s", (table,))
    return cursor.fetchone() is not None


def get_columns(cursor, table: str) -> List[str]:
    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    return [row[0] for row in cursor.fetchall()]


def normalize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def get_schema_id(data: Dict) -> Optional[str]:
    for key, value in data.items():
        if key.replace("_", "").lower() == "schemaid":
            return str(value)
    return None


def get_identifier(data: Dict, fallback: str) -> str:
    return str(data.get("Identifier") or data.get("identifier") or fallback)


def build_validator(schema_data: Dict):
    schema_version = schema_data.get("$schema", "")
    if "draft-04" in schema_version:
        return Draft4Validator(schema_data)
    return Draft7Validator(schema_data)


def validate_payload(schema_path: Path, payload: Dict) -> Optional[str]:
    schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = build_validator(schema_data)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        return errors[0].message
    return None


def process_file(
    cursor,
    conn,
    schema_dir: Path,
    url: str,
    data: Dict,
    schema_id: str,
    identifier: str,
    allowed_schemaids: Optional[List[str]],
):
    if allowed_schemaids is not None and schema_id not in allowed_schemaids:
        return False, f"SchemaID '{schema_id}' not in allow-list."

    schema_path = schema_dir / f"{schema_id}.json"
    if not schema_path.exists():
        return False, f"Schema file not found: {schema_path}"

    validation_error = validate_payload(schema_path, data)
    if validation_error:
        return False, f"Schema validation failed: {validation_error}"

    table = schema_id
    if not table_exists(cursor, table):
        return False, f"Table '{table}' does not exist."

    columns = get_columns(cursor, table)
    if not columns:
        return False, f"Table '{table}' has no columns."

    identifier_col = "Identifier" if "Identifier" in columns else "identifier"

    # Transactional check to avoid duplicates without UNIQUE constraint
    cursor.execute(f"SELECT 1 FROM `{table}` WHERE `{identifier_col}` = %s LIMIT 1 FOR UPDATE", (identifier,))
    if cursor.fetchone() is not None:
        conn.commit()
        return False, f"Identifier '{identifier}' already exists in '{table}'."

    data_keys = {str(k).lower(): k for k in data.keys()}
    values = []
    for column in columns:
        key = data_keys.get(column.lower())
        value = data.get(key) if key is not None else None
        if value is None:
            if column.lower() == "identifier":
                value = identifier
            elif column.lower() == "documentlocation":
                value = url
        values.append(normalize_value(value))

    column_list = ", ".join(f"`{col}`" for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO `{table}` ({column_list}) VALUES ({placeholders})"
    cursor.execute(query, values)
    conn.commit()
    return True, "inserted"


def main():
    parser = argparse.ArgumentParser(description="WebDAV JSON -> MariaDB ingester")
    parser.add_argument("--once", action="store_true", help="Run a single scan then exit")
    parser.add_argument("--interval", type=int, help="Seconds between scans")
    parser.add_argument("--webdav-url", help="Base WebDAV URL (files endpoint)")
    parser.add_argument("--webdav-root", help="Remote folder under WebDAV base")
    parser.add_argument("--webdav-user", help="WebDAV user")
    parser.add_argument("--webdav-password", help="WebDAV password")
    parser.add_argument("--schema-dir", help="Path to backend schemas")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    setup_logging(args.log_level)

    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_config(repo_root)
    if args.interval is not None:
        cfg["poll_interval"] = args.interval
    if args.webdav_url:
        cfg["webdav_url"] = args.webdav_url
    if args.webdav_root:
        cfg["webdav_root"] = args.webdav_root
    if args.webdav_user:
        cfg["webdav_user"] = args.webdav_user
    if args.webdav_password:
        cfg["webdav_password"] = args.webdav_password
    if args.schema_dir:
        cfg["schema_dir"] = args.schema_dir

    schema_dir = resolve_path(cfg["schema_dir"], repo_root)
    allowed_schemaids = None
    if cfg["allowed_schemaids"]:
        allowed_schemaids = [s.strip() for s in cfg["allowed_schemaids"].split(",") if s.strip()]

    session = requests.Session()
    session.auth = (cfg["webdav_user"], cfg["webdav_password"])

    base_url = normalize_webdav_url(cfg["webdav_url"])
    webdav_root = cfg["webdav_root"].strip("/")
    target_url = urljoin(base_url, webdav_root + "/")

    logger.info("Starting WebDAV ingest (once=%s, interval=%ss)", args.once, cfg["poll_interval"])
    logger.info("WebDAV target: %s", target_url)
    logger.info("Schema dir: %s", schema_dir)
    if allowed_schemaids:
        logger.info("Allowed SchemaIDs: %s", ", ".join(allowed_schemaids))

    while True:
        try:
            conn = pymysql.connect(
                host=cfg["db_host"],
                port=cfg["db_port"],
                user=cfg["db_user"],
                password=cfg["db_password"],
                database=cfg["db_name"],
                autocommit=False,
            )
        except Exception:
            logger.exception("DB connection failed")
            if args.once:
                raise
            time.sleep(cfg["poll_interval"])
            continue
        try:
            with conn.cursor() as cursor:
                ensure_state_table(cursor)
                conn.commit()

                try:
                    entries = propfind(session, target_url)
                except Exception:
                    logger.exception("WebDAV PROPFIND failed")
                    if args.once:
                        raise
                    time.sleep(cfg["poll_interval"])
                    continue
                files = [e for e in entries if not e["is_collection"] and e["href"].lower().endswith(".json")]
                paths = [f["href"] for f in files]
                state_map = get_state_map(cursor, paths)
                logger.info("Scan: %d entries, %d json files", len(entries), len(files))

                for item in files:
                    href = item["href"]
                    etag = item["etag"]
                    last_modified = item["last_modified"]
                    prev = state_map.get(href)

                    if prev and prev.get("etag") == etag and prev.get("last_modified") == last_modified and prev.get("status") == "ok":
                        logger.debug("Unchanged: %s", href)
                        continue

                    file_url = build_file_url(base_url, href)
                    try:
                        response = session.get(file_url, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                    except Exception as exc:
                        logger.warning("Download/parse error: %s (%s)", href, exc)
                        cursor.execute(
                            "REPLACE INTO ingest_state (path, etag, last_modified, status, error) VALUES (%s, %s, %s, %s, %s)",
                            (href, etag, last_modified, "error", f"download/parse error: {exc}"),
                        )
                        conn.commit()
                        continue

                    if data.get("FileTypeIdentifier") != FILE_TYPE_IDENTIFIER:
                        logger.warning("Skipped (invalid FileTypeIdentifier): %s", href)
                        cursor.execute(
                            "REPLACE INTO ingest_state (path, etag, last_modified, status, error) VALUES (%s, %s, %s, %s, %s)",
                            (href, etag, last_modified, "skipped", "invalid FileTypeIdentifier"),
                        )
                        conn.commit()
                        continue

                    schema_id = get_schema_id(data)
                    if not schema_id:
                        logger.warning("Skipped (missing SchemaID): %s", href)
                        cursor.execute(
                            "REPLACE INTO ingest_state (path, etag, last_modified, status, error) VALUES (%s, %s, %s, %s, %s)",
                            (href, etag, last_modified, "skipped", "missing SchemaID"),
                        )
                        conn.commit()
                        continue

                    identifier = get_identifier(data, Path(href).stem)
                    try:
                        logger.info("Processing: %s (schema=%s, id=%s)", href, schema_id, identifier)
                        ok, msg = process_file(
                            cursor,
                            conn,
                            schema_dir,
                            file_url,
                            data,
                            schema_id,
                            identifier,
                            allowed_schemaids,
                        )
                        status = "ok" if ok else "skipped"
                        cursor.execute(
                            "REPLACE INTO ingest_state (path, etag, last_modified, schema_id, identifier, status, error) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (href, etag, last_modified, schema_id, identifier, status, None if ok else msg),
                        )
                        conn.commit()
                        if ok:
                            logger.info("Inserted: %s", href)
                        else:
                            logger.warning("Skipped: %s (%s)", href, msg)
                    except Exception as exc:
                        conn.rollback()
                        logger.exception("Processing error: %s", href)
                        cursor.execute(
                            "REPLACE INTO ingest_state (path, etag, last_modified, schema_id, identifier, status, error) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (href, etag, last_modified, schema_id, identifier, "error", str(exc)),
                        )
                        conn.commit()
        finally:
            conn.close()

        if args.once:
            break
        time.sleep(cfg["poll_interval"])


if __name__ == "__main__":
    main()
