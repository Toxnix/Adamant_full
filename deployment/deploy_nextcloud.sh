#!/bin/bash

set -e

echo "Cloning the Adamant repository..."
git clone https://github.com/mohamedatawfik/adamant.git
cd adamant

echo "Installing Nextcloud Scripts Dependencies..."
sudo apt update && sudo apt install -y git jq inotify-tools rsync

echo "Copying Bash scripts to /home/scripts..."
mkdir -p /home/user/scripts
cp bin/data_preprocessing.sh /home/user/scripts/
chmod +x /home/user/scripts/data_preprocessing.sh
cp bin/syncscript.sh /home/user/scripts/
chmod +x /home/user/scripts/syncscript.sh

echo "Copying .env file to /home/user/scripts/..."
if [ -f .env ]; then
    cp .env /home/user/scripts/.env
    echo ".env file copied successfully."
else
    echo "Warning: .env file not found. Please create and configure it manually in /home/user/scripts/.env"
fi
echo "Setting up cron jobs..."
bash ./setup_cron_nextcloud.sh

echo "Nextcloud Machine setup complete."
