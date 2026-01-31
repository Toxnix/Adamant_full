#!/bin/bash

set -e

# Load environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
    set -a
    source "$SCRIPT_DIR/../.env"
    set +a
elif [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# Define the source directory
source_dir="${DATA_SORTED_DIR:-./data_sorted}"
echo "$source_dir"

# Database configuration
db_host="${DB_HOST:-127.0.0.1}"
db_port="${DB_PORT:-3306}"
db_user="${DB_USER:-root}"
db_password="${DB_PASSWORD:-password}"
db_name="${DB_NAME:-experiment_data}"

mysql_base=(mysql -h "$db_host" -P "$db_port" -u "$db_user")
if [ -n "$db_password" ]; then
    mysql_base+=(-p"$db_password")
fi

# Function to process JSON files and insert into database
process_file() {
    local file=$1
    local folder=$(dirname "$file")
    local subfolder=$(basename "$folder")
    local filename=$(basename "$file")
    local identifier="${filename%.*}"  # Remove file extension

    echo "Processing file: $file"
    echo "Subfolder (table name): $subfolder"
    echo "Filename (identifier): $filename"

    # Query table schema from the database
    column_query="SHOW COLUMNS FROM \`$subfolder\`"
    columns=$("${mysql_base[@]}" "$db_name" -e "$column_query" | awk '{print $1}' | grep -v 'Field')
    echo "@@@ COLUMNS FIELDS @@@"
    echo $columns
    
    if [[ -n "$columns" ]]; then
        # Read JSON content and dynamically create an associative array
        declare -A file_content=()
        for column in $columns; do
            value=$(jq -r ".$column" "$file")
            file_content["$column"]=$value
        done

        # Build the INSERT statement
        column_list=$(echo ${columns[@]} | sed 's/ /`, `/g')
        column_list="\`${column_list}\`"
        insert_query="INSERT INTO \`$subfolder\` ($column_list) VALUES ("
        for column in $columns; do
            value="${file_content[$column]}"
            if [[ "$value" == "null" ]]; then
                insert_query+="NULL, "
            else
                insert_query+="'$value', "
            fi
        done

        insert_query=${insert_query%, }")"

        # Add ON DUPLICATE KEY UPDATE part to handle the case where the entry already exists
        on_duplicate=" ON DUPLICATE KEY UPDATE "
        for column in $columns; do
            on_duplicate+="\`$column\`=VALUES(\`$column\`), "
        done
        on_duplicate=${on_duplicate%, }

        # Combine the insert query and on_duplicate part
        insert_query+="$on_duplicate;"

        echo "@@@@@ INSERT QUERY @@@@@"
        echo $insert_query
        # Execute the INSERT statement
        "${mysql_base[@]}" "$db_name" -e "$insert_query"
    else
        echo "Warning: No table '$subfolder' found in database '$db_name' or table has no columns."
    fi
}

# Function to remove JSON files
remove_file() {
    local file=$1
    
    # Extract the subfolder name and file name from the file path
    subfolder=$(basename "$(dirname "$file")")
    file_name=$(basename "$file" .json)
    
    echo "Processing Removed file"
    echo "@@@ FILENAME @@@"
    echo $file
    echo "@@@ SUBFOLDER @@@"
    echo $subfolder
    echo "@@@ FILE NAME @@@"
    echo $file_name
    
    # Remove corresponding entry from MariaDB table that matches the folder name and identifier
    delete_query="DELETE FROM \`$subfolder\` WHERE \`Identifier\` = '$file_name';"
    echo "@@@@@ DELETE QUERY "
    echo "$delete_query"
    "${mysql_base[@]}" "$db_name" -e "$delete_query"
}

# Process existing JSON files
find "$source_dir" -type f -name "*.json" | while read -r file; do
    process_file "$file"
done

echo "Initial insertion complete."

# # Monitor the source directory for new files and deletions
# inotifywait -m -r -e create,delete,close_write "$source_dir" | while read -r directory event file; do
#     # Check if the new file is a JSON file
#     if [[ "$file" == *.json ]]; then
#         if [[ "$event" == "CREATE" || "$event" == "CLOSE_WRITE" ]]; then
#             echo "@@@@@@@@@@ EVENT TRIGGERED @@@@@@@@"
#             echo $event
#             process_file "$directory/$file"
#             echo "Processed new file: $directory/$file"
#         elif [[ "$event" == "DELETE" ]]; then
#             remove_file "$directory/$file"
#             echo "Removed file: $directory/$file"
#         fi
#     fi
# done

# Monitor the source directory for new files and deletions
inotifywait -m -r -e create,delete,close_write,moved_to "$source_dir" | while read -r directory event file; do
    # Check if the new file is a JSON file
    echo "Event: $event"
    echo "Directory: $directory"
    echo "File: $file"
    if [[ "$file" == *.json ]]; then
        if [[ "$event" == "CREATE" || "$event" == "MOVED_TO" ]]; then
            echo "CREATE EVENT TRIGGERED!!"
            process_file "$directory/$file"
            echo "Processed new file: $directory/$file"
        elif [[ "$event" == "DELETE" ]]; then
            echo "DELETE EVENT TRIGGERED!!"
            remove_file "$directory/$file"
            echo "Removed file: $directory/$file"
        fi
    fi
done

