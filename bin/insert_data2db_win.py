import argparse
import json
import time
from pathlib import Path

import pymysql


def load_env_file(path):
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


def resolve_path(path, base_dir):
    path = Path(path)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def load_config(repo_root):
    env = {}
    env.update(load_env_file(repo_root / ".env"))
    env.update(load_env_file(repo_root / "bin" / ".env"))

    cfg = {}
    db_cfg_path = repo_root / "backend" / "conf" / "db_config.json"
    if db_cfg_path.exists():
        cfg.update(json.loads(db_cfg_path.read_text(encoding="utf-8")))

    db_host = env.get("DB_HOST", cfg.get("DB_HOST", "127.0.0.1"))
    db_port = int(env.get("DB_PORT", cfg.get("DB_PORT", 3306)))
    db_user = env.get("DB_USER", cfg.get("DB_USER", "root"))
    db_password = env.get("DB_PASSWORD", cfg.get("DB_PASSWORD", "password"))
    db_name = env.get("DB_NAME", cfg.get("DB_NAME", "experiment_data"))

    source_dir = env.get("DATA_SOURCE_DIR") or env.get("DATA_SORTED_DIR")
    if source_dir:
        source_dir = resolve_path(source_dir, repo_root)
    else:
        source_dir = repo_root / "data_inbox"

    return {
        "db_host": db_host,
        "db_port": db_port,
        "db_user": db_user,
        "db_password": db_password,
        "db_name": db_name,
        "source_dir": source_dir,
    }


def get_columns(cursor, table):
    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    return [row[0] for row in cursor.fetchall()]

def table_exists(cursor, table):
    cursor.execute("SHOW TABLES LIKE %s", (table,))
    return cursor.fetchone() is not None


def normalize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def get_schema_id(data):
    for key, value in data.items():
        key_norm = key.replace("_", "").lower()
        if key_norm == "schemaid":
            return str(value)
    return None


def process_file(cursor, file_path, table, identifier, data=None):
    if data is None:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    if not table_exists(cursor, table):
        print(f"Skipping {file_path}: table '{table}' does not exist.")
        return
    columns = get_columns(cursor, table)
    if not columns:
        print(f"Warning: table '{table}' has no columns.")
        return

    data_keys = {str(k).lower(): k for k in data.keys()}
    values = []
    for column in columns:
        key = data_keys.get(column.lower())
        value = data.get(key) if key is not None else None

        if value is None:
            if column.lower() == "identifier":
                value = identifier
            elif column.lower() == "documentlocation":
                value = str(file_path)

        values.append(normalize_value(value))

    column_list = ", ".join(f"`{col}`" for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_list = ", ".join(f"`{col}`=VALUES(`{col}`)" for col in columns)
    query = (
        f"INSERT INTO `{table}` ({column_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_list}"
    )
    cursor.execute(query, values)


def delete_row(cursor, table, identifier):
    columns = get_columns(cursor, table)
    if "Identifier" not in columns and "identifier" not in columns:
        return
    column = "Identifier" if "Identifier" in columns else "identifier"
    cursor.execute(
        f"DELETE FROM `{table}` WHERE `{column}` = %s",
        (identifier,),
    )


def run_once(cfg, delete_missing):
    source_dir = Path(cfg["source_dir"])
    source_dir.mkdir(parents=True, exist_ok=True)
    state_path = source_dir / ".db_ingest_state.json"
    try:
        prev_state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        prev_state = {}

    current_state = {}

    conn = pymysql.connect(
        host=cfg["db_host"],
        port=cfg["db_port"],
        user=cfg["db_user"],
        password=cfg["db_password"],
        database=cfg["db_name"],
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            for file_path in source_dir.rglob("*.json"):
                if file_path.name == ".db_ingest_state.json":
                    continue
                try:
                    data = json.loads(file_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    print(f"Error reading {file_path}: {exc}")
                    continue

                schema_id = get_schema_id(data)
                if not schema_id:
                    print(f"Skipping {file_path}: missing SchemaID.")
                    continue

                table = schema_id
                identifier = data.get("Identifier") or data.get("identifier") or file_path.stem
                current_state[str(file_path)] = {
                    "table": table,
                    "identifier": identifier,
                }
                if str(file_path) not in prev_state:
                    print(f"Found new file: {file_path}")
                try:
                    process_file(cursor, file_path, table, identifier, data=data)
                except Exception as exc:
                    print(f"Error processing {file_path}: {exc}")

            if delete_missing:
                for path_str, info in prev_state.items():
                    if path_str not in current_state:
                        try:
                            delete_row(cursor, info["table"], info["identifier"])
                        except Exception as exc:
                            print(f"Error deleting row for {path_str}: {exc}")
    finally:
        conn.close()

    state_path.write_text(json.dumps(current_state, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Windows cron-like JSON->DB ingester")
    parser.add_argument("--source", help="Source directory for JSON files")
    parser.add_argument("--interval", type=int, default=5, help="Seconds between runs")
    parser.add_argument("--watch", action="store_true", help="Run in a loop")
    parser.add_argument("--delete-missing", action="store_true", help="Delete rows for removed files")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_config(repo_root)
    if args.source:
        cfg["source_dir"] = resolve_path(args.source, repo_root)

    if args.watch:
        while True:
            run_once(cfg, args.delete_missing)
            time.sleep(args.interval)
    else:
        run_once(cfg, args.delete_missing)


if __name__ == "__main__":
    main()
