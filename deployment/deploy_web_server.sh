#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_URL=${REPO_URL:-git@github.com:Toxnix/Adamant_full.git}
REPO_DIR=${REPO_DIR:-Adamant_full}
REPO_BRANCH=${REPO_BRANCH:-main}
ROOT_DIR="$SCRIPT_DIR/$REPO_DIR"
SERVICE_USER=${SERVICE_USER:-${SUDO_USER:-$USER}}
if command -v getent >/dev/null 2>&1; then
    SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
fi
SERVICE_HOME=${SERVICE_HOME:-/home/$SERVICE_USER}
SCRIPTS_DIR=${SCRIPTS_DIR:-$SERVICE_HOME/scripts}

echo "Cloning the Adamant repository..."
if [ ! -d "$ROOT_DIR/.git" ]; then
    git clone --branch "$REPO_BRANCH" "$REPO_URL" "$ROOT_DIR"
else
    echo "Repository already exists at $ROOT_DIR"
    UPDATE_REPO=1
    for arg in "$@"; do
        case "$arg" in
            --no-pull|--no-update)
                UPDATE_REPO=0
                ;;
        esac
    done
    if [ "$UPDATE_REPO" = "1" ]; then
        echo "Updating repository..."
        git -C "$ROOT_DIR" fetch --all --prune
        git -C "$ROOT_DIR" checkout "$REPO_BRANCH"
        git -C "$ROOT_DIR" pull --ff-only
    fi
fi

if [ ! -f "$ROOT_DIR/package.json" ] || [ ! -d "$ROOT_DIR/backend" ]; then
    echo "Error: Repository is missing expected files. Check REPO_URL/REPO_DIR."
    exit 1
fi

#+ Optional cleanup flag: pass --clean/--cleanup
CLEAN_BEFORE_INSTALL=0
FORCE_CERTBOT=0
for arg in "$@"; do
    case "$arg" in
        --clean|--cleanup)
            CLEAN_BEFORE_INSTALL=1
            ;;
        --newSSL|--newssl)
            FORCE_CERTBOT=1
            ;;
    esac
done
if [ "$CLEAN_BEFORE_INSTALL" = "1" ]; then
    echo "Preparing system (cleanup to avoid conflicts)..."
    echo "Removing previously installed Adamant services..."
    sudo systemctl disable --now adamant-backend 2>/dev/null || true
    sudo systemctl disable --now adamant-webdav-ingest 2>/dev/null || true
    sudo rm -f /etc/systemd/system/adamant-backend.service
    sudo rm -f /etc/systemd/system/adamant-webdav-ingest.service
    sudo systemctl daemon-reload

    echo "Removing Nginx and Certbot for a clean reinstall..."
    sudo systemctl stop nginx 2>/dev/null || true
    sudo systemctl stop certbot.timer certbot.service 2>/dev/null || true
    sudo apt purge -y nginx nginx-common nginx-core || true
    sudo apt purge -y certbot python3-certbot-nginx || true
    sudo rm -rf /etc/nginx
    if [ "$FORCE_CERTBOT" = "1" ]; then
        sudo rm -rf /etc/letsencrypt /var/lib/letsencrypt /var/log/letsencrypt
    fi

    echo "Removing potentially conflicting Node.js/npm installs..."
    sudo apt remove -y nodejs npm yarn || true
    sudo apt purge -y nodejs npm yarn || true
    sudo apt autoremove -y || true
    sudo rm -f /usr/local/bin/node /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack || true
    sudo rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack || true
    sudo rm -f /etc/apt/sources.list.d/nodesource.list /etc/apt/sources.list.d/nodesource.list.save || true

    echo "Cleaning old frontend installs..."
    rm -rf "$ROOT_DIR/node_modules" "$ROOT_DIR/package-lock.json" || true
    rm -rf "$ROOT_DIR/db-ui/node_modules" "$ROOT_DIR/db-ui/package-lock.json" || true
fi

echo "Installing dependencies for Machine 1 (Ubuntu 24.04)..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg git jq inotify-tools rsync \
  python3-venv python3-dev build-essential libjpeg-dev zlib1g-dev apache2-utils \
  mariadb-server mariadb-client nginx certbot python3-certbot-nginx

echo "Installing Node.js LTS (includes current npm)..."
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
echo "node version: $(node -v)"
echo "npm version: $(npm -v)"

echo "Setting up MariaDB..."
# Load environment variables from .env file if it exists
ENV_FILE=""
if [ -f "$SCRIPT_DIR/.env" ]; then
    ENV_FILE="$SCRIPT_DIR/.env"
elif [ -f "$ROOT_DIR/.env" ]; then
    ENV_FILE="$ROOT_DIR/.env"
fi

if [ -n "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
    if [ "$ENV_FILE" != "$ROOT_DIR/.env" ]; then
        cp "$ENV_FILE" "$ROOT_DIR/.env"
    fi
else
    echo "Warning: .env file not found. Using default values."
    DB_ROOT_PASSWORD=${DB_ROOT_PASSWORD:-rootpassword}
    DB_NAME=${DB_NAME:-experiment_data}
    DB_USER=${DB_USER:-adamant_user}
    DB_PASSWORD=${DB_PASSWORD:-adamant_password}
fi
DB_HOST=${DB_HOST:-127.0.0.1}
DB_PORT=${DB_PORT:-3306}
SSL_EMAIL=${SSL_EMAIL:-admin@example.com}
SSL_DOMAIN=${SSL_DOMAIN:-metadata.empi-rf.de}
BASIC_AUTH_USER=${BASIC_AUTH_USER:-}
BASIC_AUTH_PASSWORD=${BASIC_AUTH_PASSWORD:-}
BASIC_AUTH_REALM=${BASIC_AUTH_REALM:-"ADAMANT"}

if [ -z "$BASIC_AUTH_USER" ] || [ -z "$BASIC_AUTH_PASSWORD" ]; then
    echo "Error: BASIC_AUTH_USER and BASIC_AUTH_PASSWORD must be set in .env"
    exit 1
fi

# Write DB config for backend
cat > "$ROOT_DIR/backend/conf/db_config.json" <<EOF
{
  "DB_HOST": "${DB_HOST}",
  "DB_PORT": ${DB_PORT},
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
CREATE USER IF NOT EXISTS '${DB_USER}'@'127.0.0.1' IDENTIFIED BY '${DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'${DB_HOST}' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'127.0.0.1';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'${DB_HOST}';
FLUSH PRIVILEGES;
MYSQL_SETUP
else
    sudo mysql -u root <<MYSQL_SETUP
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'127.0.0.1' IDENTIFIED BY '${DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'${DB_HOST}' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'127.0.0.1';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'${DB_HOST}';
FLUSH PRIVILEGES;
MYSQL_SETUP
fi

echo "MariaDB setup complete. Database '${DB_NAME}' and user '${DB_USER}' created."

echo "Setting up Python backend..."
cd "$ROOT_DIR/backend"
sudo mkdir -p "$ROOT_DIR/backend/schemas"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$ROOT_DIR/backend/schemas"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r "$ROOT_DIR/bin/webdav_ingest.requirements.txt"

sudo tee /etc/systemd/system/adamant-backend.service > /dev/null <<EOF
[Unit]
Description=Adamant Flask Backend via Gunicorn
After=network.target

[Service]
User=$SERVICE_USER
Group=www-data
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
ExecStart=$(pwd)/venv/bin/gunicorn -b 0.0.0.0:5000 api:app

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/adamant-webdav-ingest.service > /dev/null <<EOF
[Unit]
Description=Adamant WebDAV Ingest Service
After=network.target

[Service]
User=$SERVICE_USER
Group=www-data
WorkingDirectory=$ROOT_DIR
Environment="PATH=$ROOT_DIR/backend/venv/bin"
ExecStart=$ROOT_DIR/backend/venv/bin/python $ROOT_DIR/bin/webdav_ingest.py --log-level INFO
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start backend service
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable adamant-backend
sudo systemctl start adamant-backend
sudo systemctl enable adamant-webdav-ingest
sudo systemctl start adamant-webdav-ingest


echo "Setting up Node frontend..."
cd "$ROOT_DIR"
# Optional: override npm install flags via NPM_INSTALL_FLAGS env var.
NPM_INSTALL_FLAGS=${NPM_INSTALL_FLAGS:-}
npm install $NPM_INSTALL_FLAGS
npm run build

echo "Copying adamant build to Nginx root..."
sudo rm -rf /var/www/html/build
sudo cp -r "$ROOT_DIR/dist" /var/www/html/build

cd "$ROOT_DIR/db-ui"
npm install $NPM_INSTALL_FLAGS
npm run build

echo "Copying db-ui build to Nginx root..."
sudo rm -rf /var/www/html/build/db-ui
sudo mkdir -p /var/www/html/build/db-ui
sudo cp -r "$ROOT_DIR/db-ui/dist/"* /var/www/html/build/db-ui/

echo "Setting up Nginx..."
cd "$ROOT_DIR"
if [ -z "$SSL_DOMAIN" ]; then
    echo "Warning: SSL_DOMAIN is empty; using '_' as a catch-all server_name."
    SSL_DOMAIN="_"
fi
sudo tee /etc/nginx/conf.d/adamant.conf > /dev/null <<EOF
server {
    listen       80;
    listen  [::]:80;

    server_name  ${SSL_DOMAIN};

    root   /var/www/html/build;
    index index.html;
    error_page   500 502 503 504  /50x.html;
    client_max_body_size 64M;
    auth_basic "${BASIC_AUTH_REALM}";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        try_files \$uri \$uri/ /index.html;
        add_header Cache-Control "no-cache";
        client_max_body_size 64M;
    }

    location /static {
        expires 1y;
        add_header Cache-Control "public";
        client_max_body_size 64M;
    }

    location /api {
        proxy_pass http://localhost:5000;
        client_max_body_size 64M;
    }

    location /db-ui/ {
        alias /var/www/html/build/db-ui/;
        index index.html;
        try_files \$uri \$uri/ /db-ui/index.html;
        add_header Cache-Control "no-store, no-cache, must-revalidate, proxy-revalidate";
    }

}
EOF
echo "Nginx config written to /etc/nginx/conf.d/adamant.conf"
sudo htpasswd -bc /etc/nginx/.htpasswd "$BASIC_AUTH_USER" "$BASIC_AUTH_PASSWORD"
sudo systemctl restart nginx

echo "Copying .env file to ${SCRIPTS_DIR}/..."
mkdir -p "$SCRIPTS_DIR"
if [ -f "$ROOT_DIR/.env" ]; then
    cp "$ROOT_DIR/.env" "$SCRIPTS_DIR/.env"
    echo ".env file copied successfully."
else
    echo "Warning: .env file not found. Please create and configure it manually in ${SCRIPTS_DIR}/.env"
fi

echo "Obtaining SSL with Certbot..."
# Use SSL configuration from .env file
if [ -z "$SSL_EMAIL" ] || [ -z "$SSL_DOMAIN" ]; then
    echo "Warning: SSL_EMAIL or SSL_DOMAIN not set in .env file. Using defaults."
fi

if [ "$SSL_DOMAIN" = "_" ]; then
    echo "Skipping certbot: SSL_DOMAIN is empty."
else
    CERT_PATH="/etc/letsencrypt/live/${SSL_DOMAIN}/fullchain.pem"
    KEY_PATH="/etc/letsencrypt/live/${SSL_DOMAIN}/privkey.pem"
    if [ "$FORCE_CERTBOT" = "1" ]; then
        sudo certbot --nginx \
            --non-interactive \
            --agree-tos \
            --email "${SSL_EMAIL}" \
            -d "${SSL_DOMAIN}"
    else
        if [ -f "$CERT_PATH" ] && [ -f "$KEY_PATH" ]; then
            echo "Existing SSL certificate found for ${SSL_DOMAIN}. Skipping certbot."
        else
            sudo certbot --nginx \
                --non-interactive \
                --agree-tos \
                --email "${SSL_EMAIL}" \
                -d "${SSL_DOMAIN}"
        fi
    fi
fi

echo "Adamant Web Server Machine setup complete."
