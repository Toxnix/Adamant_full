from flask import Flask, request, jsonify
from flask_restful import Api
import elabapy
import json
import base64
from pathlib import Path
import os
import smtplib
from email.message import EmailMessage
import mimetypes
import io
import base64
from datetime import date
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

# convert json form data to eLabFTW description list
def findBase64(data, prevKey, emptyArray):

    for key in data:

        if isinstance(data[key], list) and (type(data[key]) is dict):
            findBase64(data[key], prevKey+"-"+key, emptyArray)
        elif isinstance(data[key], list):
            for i in range(0, len(data[key])):
                findBase64(data[key][i], key+"-"+str(i+1), emptyArray)
        else:
            if isinstance(data[key], str):
                if data[key].startswith("data:") and ("base64" in data[key]):
                    logger.info("found data")
                    emptyArray.append({"key": key, "data": data[key]})

    return emptyArray


# find the value of requesterKeyword
def findRequesterEmail(jsdata, requesterKeyword, result):
    for key in jsdata:
        if key == requesterKeyword:
            result = jsdata[key]
        if isinstance(jsdata[key], dict):
            result = findRequesterEmail(jsdata[key], requesterKeyword, result)
    return result


# find the value of operator Keyword
def findOperatorName(jsdata, operatorKeyword, result):
    for key in jsdata:
        if key == operatorKeyword:
            result = jsdata[key]
        if isinstance(jsdata[key], dict):
            result = findOperatorName(jsdata[key], operatorKeyword, result)
    return result


# find the falue of requesterNameKeyword
def findRequesterName(jsdata, requesterNameKeyword, result):
    for key in jsdata:
        if key == requesterNameKeyword:
            result = jsdata[key]
        if isinstance(jsdata[key], dict):
            result = findRequesterName(
                jsdata[key], requesterNameKeyword, result)
    return result

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
        column_type = map_json_type_to_sql(details.get("type", "string"))
        columns.append(f"`{prop}` {column_type}")
        
    # Append documentLocation per default
    columns.append("`documentlocation` VARCHAR(255)")

    columns_str = ", ".join(columns)
    drop_table_query = f"DROP TABLE IF EXISTS `{schema_name}`;"
    logger.debug(f'DB QUERY: {drop_table_query}')
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{schema_name}` ({columns_str});"
    logger.debug(f'DB QUERY: {create_table_query}')
    

    # Connect to the database and execute the query
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
            cursor.execute(drop_table_query)
            logger.info('DROP TABLE EXECUTED')
            cursor.execute(create_table_query)
        connection.commit()
        logger.info('COMMIT SUCCESSFUL')
    finally:
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
    filelist = list(Path('./schemas').glob('**/*.json'))
    for i in range(0, len(filelist)):
        file = filelist[i]
        file = open(str(file), 'r', encoding='utf-8')
        filename = str(file.name).replace("schemas\\", "")
        filename = filename.replace("schemas/", "")  # for linux, maybe
        content = file.read()
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
            schema_id = schema_dict.get("$id")
            if not schema_id:
                return {"error": "$id is missing in the schema"}, 400

            schema_dict["properties"]["SchemaID"] = {
                "title": "SchemaID",
                "description": "Unique identifier for the schema",
                "type": "string",
                "enum": [schema_id]
            }
            
            # Convert the updated schema dictionary back to JSON
            updated_schema_content = json.dumps(schema_dict, indent=2)
            with open(f"./schemas/{schema_name}.json", "w", encoding="utf-8") as file:
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

# API Endpoint: Search for a string in all tables
@app.route("/api/search/<string:search_string>", methods=["GET"])
def search_tables(search_string):
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
            search_results = []

            for table in tables:
                cursor.execute(f"DESCRIBE `{table}`")
                columns = [col[0] for col in cursor.fetchall()]

                if not columns:
                    continue

                query = f"SELECT * FROM `{table}` WHERE CONCAT_WS(' ', {', '.join(columns)}) LIKE %s"
                cursor.execute(query, (f"%{search_string}%",))
                results = cursor.fetchall()

                for row in results:
                    search_results.append({"table": table, "data": dict(zip(columns, row))})

            return jsonify(search_results)
    except Exception as e:
        return jsonify({"error": f"Error searching tables: {str(e)}"}), 500

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

# Login endpoint
@app.route('/api/login', methods=["POST"])
def login():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    # Check if username and password match the hardcoded admin credentials
    if username == 'admin' and password == 'admin':
        # Generate token
        token = 'dummy_token_for_admin'
        # Return token to frontend
        return jsonify({"token": token}), 200
    else:
        # If credentials are incorrect, return error message
        return jsonify({"error": "Invalid username or password"}), 401

#Get protected Data
# @app.route('/api/protected', methods=["GET"])
# @jwt_required()
# def protected():
#     current_user = get_jwt_identity()
#     return jsonify(logged_in_as=current_user), 200


# get available tags from eLabFTW
@app.route('/api/get_tags', methods=['POST'])
def get_tags():
    elabURL = request.form['eLabURL']
    token = request.form['eLabToken']
    # create elab manager
    elabURL = '{}/api/v1/'.format(elabURL)
    elabURL = elabURL.replace('//api', '/api')
    manager = elabapy.Manager(
        endpoint=elabURL, token=token)
    all_tags = manager.get_tags()

    return json.dumps(all_tags)


# create experiment in eLabFTW
@app.route('/api/create_experiment', methods=['POST'])
def create_experiment():
    jsdata = request.form['javascript_data']
    jsschema = request.form['schema']
    elabURL = request.form['eLabURL']
    token = request.form['eLabToken']
    title = request.form['title']
    body = request.form['body']
    tags = request.form['tags']
    tags = json.loads(tags)
    jsdata = json.loads(jsdata)
    jsschema = json.loads(jsschema)
    jsschema_title = jsschema["title"]

    # create experiment in eLabFtw
    elabURL = '{}/api/v1/'.format(elabURL)
    elabURL = elabURL.replace('//api', '/api')
    manager = elabapy.Manager(
        endpoint=elabURL, token=token)
    response = manager.create_experiment()

    logger.info(f"response: {response}")

    # create the experiment body which is the description list attained by converting the jsdata

    # update the experiment
    params = {"title": title, "body": body}
    manager.post_experiment(response['id'], params)

    # upload the schema
    with open('temp-files//json_schema.json', 'w') as outfile:
        outfile.write(json.dumps(jsschema))
    with open('temp-files//json_schema.json') as file:
        file_param = {'file': file}
        manager.upload_to_experiment(response['id'], file_param)

    # upload form data/jsdata
    with open('temp-files//json_data.json', 'w') as outfile:
        outfile.write(json.dumps(jsdata))
    with open('temp-files//json_data.json') as file:
        file_param = {'file': file}
        manager.upload_to_experiment(response['id'], file_param)

    # now if tags is not empty then add tags to this experiment id one by one
    if len(tags) != 0:
        for i in tags:
            params = {'tag': i['tag']}
            manager.add_tag_to_experiment(response['id'], params)

    """ to append the body
    params = {"bodyappend": "appended text<br>"}
    manager.post_experiment(response['id'], params)
    """

    # now check if there are file data in jsdata, if there is then upload it
    collected_data = findBase64(jsdata, "start", [])
    file = open("./mime-types-extensions.json",
                'r', encoding='utf-8')
    mimeExtensions = file.read()
    file.close()
    fileNames = []
    mimeExtensions = json.loads(mimeExtensions)
    for item in collected_data:
        mimeType = item["data"].split(";")[0].replace("data:", "")
        extension = list(mimeExtensions.keys())[
            list(mimeExtensions.values()).index(mimeType)]
        fileNames.append(item["key"]+extension)
        _, encoded = item["data"].split(",", 1)
        binary_data = base64.b64decode(encoded)
        with open("./temp-files/"+item["key"]+extension, "wb") as fh:
            fh.write(binary_data)
        with open("temp-files//"+item["key"]+extension, "r+b") as fh:
            file_param = {'file': fh}
            manager.upload_to_experiment(response['id'], file_param)
    logger.info(f"fileNames: {fileNames}")

    # now delete everything in temp-files directory
    dir = './temp-files'
    for f in os.listdir(dir):
        if f != ".placeholder":
            os.remove(os.path.join(dir, f))

    # check if this process is related to job request workflow, if yes then send an e-mail notif to the requester
    try:
        with open("./conf/jobrequest-conf.json", "r") as fi:
            f = fi.read()
            f = json.loads(f)
            email_conf = ""
            for element in f["confList"]:
                if element["completeSchemaTitle"] == jsschema_title or element["requestSchemaTitle"] == jsschema_title:
                    email_conf = element
            # finish the process if not related to job request workflow
            if email_conf == "":
                return {"responseText": f"Created experiment with id {response['id']}.", "message": "success", "experimentId": response['id']}

            requesterEmail = findRequesterEmail(
                jsdata, email_conf["requesterEmailKeyword"], "")

            # sending the emails
            s = smtplib.SMTP_SSL(email_conf["smtp"])

            # PREPARE msg for APPLICANT
            msg = EmailMessage()
            msg['From'] = email_conf["from"]
            msg['To'] = requesterEmail
            msg['Subject'] = email_conf["requestAcceptedSubject"]

            header = email_conf["requestAcceptedHeaderText"]
            html = header+body

            msg.set_content(html, subtype="html")

            s.send_message(msg)
            logger.info("applicant email is valid")
            del msg
    except Exception as e:
        logger.warning("No job request configuration was found. Skipping.")

    return {"responseText": f"Created experiment with id {response['id']}.", "message": "success", "experimentId": response['id']}


@app.route('/api/submit_job_request', methods=['POST'])
def submit_job_request():
    today = date.today()
    dateToday = today.strftime("%d_%b_%Y")

    jsdata = request.form['javascript_data']
    jsschema = request.form['schema']
    body = request.form['body']
    jsdata = json.loads(jsdata)
    jsschema = json.loads(jsschema)

    try:
        with open("./conf/jobrequest-conf.json", "r") as fi:
            f = fi.read()

            # find the right conf based on the schema title
            f = json.loads(f)
            email_conf = {}
            for element in f["confList"]:
                if element["completeSchemaTitle"] == jsschema["title"] or element["requestSchemaTitle"] == jsschema["title"]:
                    email_conf = element

            requesterEmail = findRequesterEmail(
                jsdata, email_conf["requesterEmailKeyword"], "")
            operatorName = findOperatorName(
                jsdata, email_conf["operatorNameKeyword"], "")
            operatorName_ = operatorName.replace(" ", "_")
            operatorEmail = ""
            responsiblePersonEmail = email_conf["responsibleOperatorEmail"]

            for key in email_conf["operators"]:
                if key == operatorName_:
                    operatorEmail = email_conf["operators"][operatorName_]

            #print("smtp:", email_conf["smtp"])
            #print("operator:", operatorEmail)
            #print("requester:", requesterEmail)
            #print("responsible:", responsiblePersonEmail)

            # sending the emails
            s = smtplib.SMTP_SSL(email_conf["smtp"])

            # PREPARE msg1 for APPLICANT
            msg1 = EmailMessage()
            msg1['From'] = email_conf["from"]
            msg1['To'] = requesterEmail
            msg1['Subject'] = email_conf["confirmationEmailSubject"]

            header1 = email_conf["confirmationHeaderText"]
            html1 = header1+body

            msg1.set_content(html1, subtype="html")

            # PREPARE msg2 for OPERATOR
            msg2 = EmailMessage()
            msg2['From'] = email_conf["from"]
            msg2['To'] = "{0}, {1}".format(
                operatorEmail, responsiblePersonEmail)
            msg2['Subject'] = email_conf["requestReceivedEmailSubject"]

            header2 = email_conf["requestReceivedHeaderText"]
            html2 = header2+body

            msg2.set_content(html2, subtype="html")

            # create json attachments
            for i in range(0, 2):
                data = ""
                if i == 0:
                    data = jsdata
                    fileName = "form_data"
                else:
                    data = jsschema
                    fileName = "schema"

                f = json.dumps(data, indent=2).encode('utf-8')
                f_byte_arr = io.BytesIO()
                f_byte_arr.write(f)
                f_byte_arr.seek(0)
                binary_data = f_byte_arr.read()
                # Guess MIME type or use 'application/octet-stream'
                maintype, _, subtype = (mimetypes.guess_type("{0}_{1}.json".format(
                    fileName, dateToday))[0] or 'application/octet-stream').partition("/")
                # Add as attachment
                msg1.add_attachment(binary_data, maintype=maintype, subtype=subtype,
                                    filename="{0}_{1}.json".format(fileName, dateToday))
                msg2.add_attachment(binary_data, maintype=maintype, subtype=subtype,
                                    filename="{0}_{1}.json".format(fileName, dateToday))

            # now send the emails to both requester and operator (and responsible person)
            try:
                s.send_message(msg1)
                s.send_message(msg2)
                logger.info("applicant email is valid")
                del msg1
                del msg2
                return {"response": 200, "responseText": "Your request has been submitted."}
            except Exception as e:
                del msg1
                del msg2
                logger.error(e)
                return {"response": 500, "responseText": "Something went wrong"}

    except Exception as e:
        logger.error(e)
        return {"response": 500, "responseText": "List of operators are not available in the server."}

# if __name__ == "__main__":
#    app.run(debug=True, host="0.0.0.0", port=5000)
