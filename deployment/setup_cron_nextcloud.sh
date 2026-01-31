#!/bin/bash

DPP_SCRIPT_PATH="/home/user/scripts/data_preprocessing.sh"
chmod +x "$DPP_SCRIPT_PATH"

echo "Setting cron job for Data Preprocessing..."
(crontab -l 2>/dev/null; echo "0 * * * * $DPP_SCRIPT_PATH") | crontab -

CRAWLER_SCRIPT_PATH="/home/user/scripts/syncscript.sh"
chmod +x "$CRAWLER_SCRIPT_PATH"

echo "Setting cron job for Crawler..."
(crontab -l 2>/dev/null; echo "0 * * * * $CRAWLER_SCRIPT_PATH") | crontab -
