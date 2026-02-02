from flask import Flask, request, jsonify
from flask_restful import Api
import json
from pathlib import Path
import re
import os
import pymysql
import logging
import json as _json
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler


app = Flask(__name__, static_folder='../build',
            static_url_path='/')  # for Gunicorn deployment
# app = Flask(__name__)
api = Api(app)
app.config['EMPIRF_SECRET_KEY'] = 'EPMIRF_SECURITY'

# jwt = JWTManager(app)

# Database configuration
with open(os.path.join(os.path.dirname(__file__), 'conf', 'db_config.json'), 'r') as f:
    db_config = _json.load(f)
DB_HOST = db_config['DB_HOST']
DB_PORT = db_config['DB_PORT']
DB_USER = db_config['DB_USER']
DB_PASSWORD = db_config['DB_PASSWORD']
DB_NAME = db_config['DB_NAME']

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"

# class DataHandler(FileSystemEventHandler):
#     def __init__(self):
#         pass

#     def table_exists(self, connection, table_name):
#         with connection.cursor() as cursor:
#             cursor.execute("SHOW TABLES LIKE %s", (table_name,))
#             result = cursor.fetchone()
#             return result is not None

#     def entry_exists(self, connection, table_name, identifier):
#         with connection.cursor() as cursor:
#             cursor.execute(f"SELECT 1 FROM `{table_name}` WHERE identifier = %s", (identifier,))
#             result = cursor.fetchone()
#             return result is not None

#     def process_json_file(self, file_path, action):
#         with open(file_path, 'r') as file:
#             data = json.load(file)
#             schema_id = data.get('schema_id')
#             identifier = data.get('identifier')

#             if not schema_id or not identifier:
#                 print("Missing schema_id or identifier in the JSON file.")
#                 return

#             conn = pymysql.connect(
#                 host=DB_HOST,
#                 port=DB_PORT,
#                 user=DB_USER,
#                 password=DB_PASSWORD,
#                 database=DB_NAME
#             )

#             try:
#                 if not self.table_exists(conn, schema_id):
#                     print(f"Table `{schema_id}` does not exist.")
#                     return

#                 if action == 'create':
#                     if self.entry_exists(conn, schema_id, identifier):
#                         print(f"Entry with identifier `{identifier}` already exists in table `{schema_id}`.")
#                         return

#                     placeholders = ', '.join(['%s'] * len(data))
#                     columns = ', '.join(data.keys())
#                     sql = f"INSERT INTO `{schema_id}` ({columns}) VALUES ({placeholders})"
#                     with conn.cursor() as cursor:
#                         cursor.execute(sql, tuple(data.values()))

#                 elif action == 'delete':
#                     if not self.entry_exists(conn, schema_id, identifier):
#                         print(f"Entry with identifier `{identifier}` does not exist in table `{schema_id}`.")
#                         return

#                     sql = f"DELETE FROM `{schema_id}` WHERE identifier = %s"
#                     with conn.cursor() as cursor:
#                         cursor.execute(sql, (identifier,))

#                 conn.commit()
#             finally:
#                 conn.close()

#     def on_created(self, event):
#         if not event.is_directory and event.src_path.endswith('.json'):
#             self.process_json_file(event.src_path, 'create')

#     def on_deleted(self, event):
#         if not event.is_directory and event.src_path.endswith('.json'):
#             self.process_json_file(event.src_path, 'delete')

# def start_watcher(path):
#     event_handler = DataHandler()
#     observer = Observer()
#     observer.schedule(event_handler, path, recursive=True)
#     observer.start()
#     return observer

def map_json_type_to_sql(json_type):
    type_mapping = {
        "string": "VARCHAR(255)",
        "number": "FLOAT",
        "integer": "INT",
        "boolean": "BOOLEAN",
        "file upload(string)": "VARCHAR(255)",
        "object": "JSON",
        "array": "JSON"
    }
    return type_mapping.get(json_type, "VARCHAR(255)")

def normalize_json_type(json_type):
    if isinstance(json_type, list):
        for item in json_type:
            if item != "null":
                return item
        return json_type[0] if json_type else "string"
    if not json_type:
        return "string"
    return json_type

def extract_properties(properties, parent_key=''):
    items = {}
    for key, value in properties.items():
        # Adding parent property before the parameters under while separating with a _ 
        # full_key = f"{parent_key}_{key}" if parent_key else key 
        full_key = f"{key}" if parent_key else key
        if value['type'] == 'object' and 'properties' in value:
            items.update(extract_properties(value['properties'], full_key))
        else:
            items[full_key] = value
    return items

def create_table_from_schema(schema_name, schema_content):
    # Parse the schema content to extract properties and types
    # print('@@@@@@@@@@@@@@@ INSIDE CREATE @@@@@@@@@@@@@@@@@')
    schema = json.loads(schema_content)
    # print('@@@@@@@@@@@@@@@ Schema @@@@@@@@@@@@@@@@@',schema)
    properties = schema.get("properties", {})
    # print('@@@@@@@@@@@@@@@ PROPERTIES @@@@@@@@@@@@@@@@@',properties)
    
    # Extract all properties, including nested ones
    flattened_properties = extract_properties(properties)
    # print('@@@@@@@@@@@@@@@ Flattened Properties @@@@@@@@@@@@@@@@@', flattened_properties)

    columns = []
    for prop, details in flattened_properties.items():
        column_type = map_json_type_to_sql(normalize_json_type(details.get("type", "string")))
        columns.append(f"`{prop}` {column_type}")
        
    # Append documentLocation per default
    columns.append("`documentlocation` VARCHAR(255)")

    columns_str = ", ".join(columns)
    drop_table_query = f"DROP TABLE IF EXISTS `{schema_name}`;"
    logger.debug(f'DB QUERY: {drop_table_query}')
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{schema_name}` ({columns_str});"
    logger.debug(f'DB QUERY: {create_table_query}')
    

    # Connect to the database and execute the query
    connection = None
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info('CONNECTION TO DB IS SUCCESSFUL')
        with connection.cursor() as cursor:
            cursor.execute(drop_table_query)
            logger.info('DROP TABLE EXECUTED')
            cursor.execute(create_table_query)
        connection.commit()
        logger.info('COMMIT SUCCESSFUL')
    except pymysql.Error as e:
        logger.error(f'Error connecting to MariaDB: {e}')
        raise
    finally:
        if connection:
            connection.close()
            logger.info('CONNECTION IS CLOSED')


# @app.route('/')
# def index():
#    return app.send_static_file('index.html')

@app.route('/api/check_mode', methods=["GET"])
def check_mode():
    # check if online, and get list of schemas used in job request workflows
    listSchemas = []
    listSubmitText = []
    try:
        with open("./conf/jobrequest-conf.json", "r") as fi:
            f = fi.read()
            f = json.loads(f)
            emailconf_list = f["confList"]
            for element in emailconf_list:
                listSchemas.append(element["completeSchemaTitle"])
                listSchemas.append(element["requestSchemaTitle"])
                listSubmitText.append(element["submitButtonText"])
                listSubmitText.append(element["submitButtonText"])
        return {"message": "connection is a success", "jobRequestSchemaList": listSchemas, "submitButtonText": listSubmitText}
    except Exception as e:
        return {"message": "connection is a success", "jobRequestSchemaList": listSchemas, "submitButtonText": listSubmitText}


# get schemas from backend
@app.route('/api/get_schemas', methods=["GET"])
def get_schemas():
    list_of_schemas = {"schemaName": [""], "schema": [None]}
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    filelist = list(SCHEMA_DIR.glob('**/*.json'))
    for i in range(0, len(filelist)):
        file = filelist[i]
        content = Path(file).read_text(encoding='utf-8')
        filename = Path(file).relative_to(SCHEMA_DIR).as_posix()
        list_of_schemas["schema"].append(content)
        list_of_schemas["schemaName"].append(filename)
    return list_of_schemas

@app.route('/api/save_schema', methods=["POST"])
def save_schema():
    data = request.json
    logger.info(f"Received data: {data}")
    schema_name = data.get("schemaName")
    schema_content = data.get("schema")

    if schema_name and schema_content:
        try:
            schema_dict = json.loads(schema_content)
            if not isinstance(schema_dict.get("properties"), dict):
                return {"error": "Schema properties missing or invalid"}, 400
            schema_id = schema_dict.get("$id")
            if not schema_id:
                return {"error": "$id is missing in the schema"}, 400
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$", schema_name):
                return {"error": "Invalid schema/table name. Use letters, numbers, and underscores only."}, 400

            schema_dict["properties"]["SchemaID"] = {
                "title": "SchemaID",
                "description": "Unique identifier for the schema",
                "type": "string",
                "enum": [schema_id]
            }
            
            # Convert the updated schema dictionary back to JSON
            updated_schema_content = json.dumps(schema_dict, indent=2)
            SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
            schema_path = SCHEMA_DIR / f"{schema_name}.json"
            with open(schema_path, "w", encoding="utf-8") as file:
                file.write(updated_schema_content)
                # Create the corresponding table in the database
                logger.info('CREATE IS CALLED')
                create_table_from_schema(schema_name, updated_schema_content)
            return {"message": f"Schema '{schema_name}' saved successfully"}, 200
        except Exception as e:
            return {"error": str(e)}, 500
    else:
        return {"error": "Schema name or content not provided"}, 400

# API Endpoint: Get list of tables
@app.route("/api/tables", methods=["GET"])
def get_tables():
    connection = None
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info('CONNECTION TO DB IS SUCCESSFUL')
    except pymysql.Error as e:
        logger.error(f'Error connecting to MariaDB: {e}')
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            return jsonify(tables)
    except Exception as e:
        return jsonify({"error": f"Error retrieving tables: {str(e)}"}), 500

# API Endpoint: Get data from a specific table
@app.route("/api/data/<string:table>", methods=["GET"])
def get_table_data(table):
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info('CONNECTION TO DB IS SUCCESSFUL')
    except pymysql.Error as e:
        logger.error(f'Error connecting to MariaDB: {e}')
    try:
        with connection.cursor() as cursor:
            query = f"SELECT * FROM `{table}`"
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in results]
            return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"Error retrieving data from table {table}: {str(e)}"}), 500

# API Endpoint: Get column information of a table
@app.route("/api/columns/<string:table>", methods=["GET"])
def get_columns(table):
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info('CONNECTION TO DB IS SUCCESSFUL')
    except pymysql.Error as e:
        logger.error(f'Error connecting to MariaDB: {e}')
    try:
        with connection.cursor() as cursor:
            query = f"DESCRIBE `{table}`"
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [
                {"name": col[0], "type": col[1], "nullable": col[2] == "YES",
                 "key": col[3], "default": col[4], "extra": col[5]}
                for col in results
            ]
            return jsonify(columns)
    except Exception as e:
        return jsonify({"error": f"Error retrieving column info for table {table}: {str(e)}"}), 500

# API Endpoint: Perform LEFT JOIN on two tables
@app.route("/api/left-join", methods=["GET"])
def left_join():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info('CONNECTION TO DB IS SUCCESSFUL')
    except pymysql.Error as e:
        logger.error(f'Error connecting to MariaDB: {e}')
    table1 = request.args.get("table1")
    table2 = request.args.get("table2")
    column1 = request.args.get("column1")
    column2 = request.args.get("column2")

    # Validate input parameters
    if not all([table1, table2, column1, column2]):
        return jsonify({"error": "Missing parameters: table1, table2, column1, column2"}), 400

    try:
        with connection.cursor() as cursor:
            # 1. SELECT all from table A (experiments)
            cursor.execute(f"SELECT * FROM `{table1}`")
            table_a_data = cursor.fetchall()

            # 2. SELECT all from table B (conditions)
            cursor.execute(f"SELECT * FROM `{table2}`")
            table_b_data = cursor.fetchall()

            # Build lookup for Table B
            b_lookup = {row[column2]: row for row in table_b_data}

            # 4. Merge Table A with Table B based on sample_id
            merged_data = []
            for a_row in table_a_data:
                sample_id = a_row[column1]
                b_row = b_lookup.get(sample_id, {})

                # Prefix B fields to avoid conflict
                b_row_prefixed = {f"{k}_condition": v for k, v in b_row.items() if k != column2}
                # Merge rows: Table A + Table B (prefixed)
                merged_row = {**a_row, **b_row_prefixed}
                merged_data.append(merged_row)

            if merged_data:
                columns = list(merged_data[0].keys())
            else:
                columns = []
            return jsonify({
                "columns": columns,
                "data": merged_data
            })
            # return jsonify(results)
    except Exception as e:
        return jsonify({"error": f"Error performing LEFT JOIN: {str(e)}"}), 500

# if __name__ == "__main__":
#    app.run(debug=True, host="0.0.0.0", port=5000)
