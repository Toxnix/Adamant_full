#!/bin/bash

set -e

echo "Cloning the Adamant repository..."
git clone https://github.com/mohamedatawfik/adamant.git
cd adamant

echo "Installing dependencies for Machine 1 (Ubuntu 24.04)..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg git jq inotify-tools rsync \
  python3-venv python3-dev build-essential libjpeg-dev zlib1g-dev \
  mariadb-server mariadb-client nginx certbot python3-certbot-nginx

echo "Installing Node.js LTS (includes current npm)..."
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
echo "node version: $(node -v)"
echo "npm version: $(npm -v)"

echo "Setting up MariaDB..."
# Load environment variables from .env file if it exists
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "Warning: .env file not found. Using default values."
    DB_ROOT_PASSWORD=${DB_ROOT_PASSWORD:-rootpassword}
    DB_NAME=${DB_NAME:-experiment_data}
    DB_USER=${DB_USER:-adamant_user}
    DB_PASSWORD=${DB_PASSWORD:-adamant_password}
fi

# Write DB config for backend
cat > backend/conf/db_config.json <<EOF
{
  "DB_HOST": "127.0.0.1",
  "DB_PORT": 3306,
  "DB_USER": "${DB_USER}",
  "DB_PASSWORD": "${DB_PASSWORD}",
  "DB_NAME": "${DB_NAME}"
}
EOF

# Start and enable MariaDB service
sudo systemctl start mariadb
sudo systemctl enable mariadb

# Wait for MariaDB to be ready
sleep 3

# Try to set root password (works if root doesn't have password or uses unix_socket)
# First try without password (unix_socket auth), then with password if it exists
if sudo mysql -u root -e "SELECT 1;" >/dev/null 2>&1; then
    # Root can login without password (unix_socket authentication)
    echo "Setting root password..."
    sudo mysql -u root <<MYSQL_ROOT_SETUP
ALTER USER 'root'@'localhost' IDENTIFIED BY '${DB_ROOT_PASSWORD}';
FLUSH PRIVILEGES;
MYSQL_ROOT_SETUP
fi

# Create database and user
echo "Creating database ${DB_NAME} and user ${DB_USER}..."
# Try with root password first, fallback to unix_socket auth
if sudo mysql -u root -p"${DB_ROOT_PASSWORD}" -e "SELECT 1;" >/dev/null 2>&1; then
    sudo mysql -u root -p"${DB_ROOT_PASSWORD}" <<MYSQL_SETUP
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
MYSQL_SETUP
else
    sudo mysql -u root <<MYSQL_SETUP
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
MYSQL_SETUP
fi

echo "MariaDB setup complete. Database '${DB_NAME}' and user '${DB_USER}' created."

echo "Setting up Python backend..."
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

sudo tee /etc/systemd/system/adamant-backend.service > /dev/null <<EOF
[Unit]
Description=Adamant Flask Backend via Gunicorn
After=network.target

[Service]
User=$USER
Group=www-data
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
ExecStart=$(pwd)/venv/bin/gunicorn -b 0.0.0.0:5000 api:app

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start backend service
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable adamant-backend
sudo systemctl start adamant-backend


echo "Setting up Node frontend..."
cd ..  # Go back to adamant root directory

npm install && npm run build

echo "Copying adamant build to Nginx root..."
sudo cp -r build /var/www/html/

cd db-ui
npm install && npm run build

echo "Copying db-ui build to Nginx root..."
sudo mkdir -p /var/www/html/build/db-ui
sudo cp -r build/* /var/www/html/build/db-ui/

echo "Setting up Nginx..."
cd ..  # Go back to adamant root directory
sudo cp deployment/nginx.default.prod.conf /etc/nginx/conf.d/adamant.conf
sudo systemctl restart nginx

echo "Copying Bash scripts to /home/user/scripts..."
mkdir -p /home/user/scripts
cp bin/insert_data2db.sh /home/user/scripts/
chmod +x /home/user/scripts/insert_data2db.sh

echo "Copying .env file to /home/user/scripts/..."
if [ -f .env ]; then
    cp .env /home/user/scripts/.env
    echo ".env file copied successfully."
else
    echo "Warning: .env file not found. Please create and configure it manually in /home/user/scripts/.env"
fi

echo "Obtaining SSL with Certbot..."
# Use SSL configuration from .env file
SSL_EMAIL=${SSL_EMAIL:-admin@example.com}
SSL_DOMAIN=${SSL_DOMAIN:-metadata.empi-rf.de}

if [ -z "$SSL_EMAIL" ] || [ -z "$SSL_DOMAIN" ]; then
    echo "Warning: SSL_EMAIL or SSL_DOMAIN not set in .env file. Using defaults."
fi

sudo certbot --nginx \
    --non-interactive \
    --agree-tos \
    --email "${SSL_EMAIL}" \
    -d "${SSL_DOMAIN}"

echo "Setting up cron jobs..."
bash ./setup_cron_web_server.sh

echo "Adamant Web Server Machine setup complete."
