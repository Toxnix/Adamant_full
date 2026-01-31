#!/bin/bash

INSERT2DB_SCRIPT_PATH="/home/user/scripts/insert_data2db.sh"
chmod +x "$INSERT2DB_SCRIPT_PATH"

echo "Setting cron job for DB Insertion..."
(crontab -l 2>/dev/null; echo "0 * * * * $INSERT2DB_SCRIPT_PATH") | crontab -
