#!/bin/bash

# Load environment variables from .env file if it exists
# Try to find .env file in the script's directory or parent directory
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

# Define the source and target directories
# Use NEXTCLOUD_DATA_DIR from .env file, default to ./nextcloud_dir/rawData if not set
source_dir="${NEXTCLOUD_DATA_DIR:-./nextcloud_dir}/rawData"
target_dir="${DATA_SORTED_DIR:-./data_sorted}"
echo $source_dir

# Create the target directory if it does not exist
mkdir -p "$target_dir"

# Function to process JSON files
process_file() {
    local file=$1
    # Extract the schema unique ID from the JSON file
    schema_id=$(jq -r '.SchemaID' "$file")

    # Check if the schema_id is not null or empty
    if [ -n "$schema_id" ] && [ "$schema_id" != "null" ]; then
        # Create a subfolder named after the schema unique ID
        subfolder="$target_dir/$schema_id"
        mkdir -p "$subfolder"
        # Get the absolute path to the JSON file
        absolute_path=$(realpath "$file")

        # Move the JSON file into the appropriate subfolder
        cp "$file" "$subfolder/"

        # Add the "documentlocation" parameter with the absolute path
        jq --arg path "$absolute_path" '. + {documentlocation: $path}' "$file" > "$subfolder/$(basename "$file")"
    else
        echo "Warning: No schema_id found in $file"
    fi
}

# Function to remove JSON files
remove_file() {
    local file=$1
    # Extract the schema unique ID from the JSON file
    find "$target_dir" -type f -name "$file" -exec rm -v {} \;
}

# Process existing JSON files
find "$source_dir" -type f -name "*.json" | while read -r file; do
    process_file "$file"
done

echo "Initial sorting complete."

# Monitor the source directory for new files and deletions
inotifywait -m -r -e create,delete "$source_dir" | while read -r directory event file; do
    # Check if the new file is a JSON file
    if [[ "$file" == *.json ]]; then
        if [[ "$event" == "CREATE" ]]; then
            process_file "$directory/$file"
            echo "Processed new file: $directory/$file"
        elif [[ "$event" == "DELETE" ]]; then
            remove_file "$file"
            echo "Removed file: $directory/$file"
        fi
    fi
done
