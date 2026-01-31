#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_URL=${REPO_URL:-git@github.com:Toxnix/Adamant_full.git}
REPO_DIR=${REPO_DIR:-Adamant_full}
REPO_BRANCH=${REPO_BRANCH:-main}
ROOT_DIR="$SCRIPT_DIR/$REPO_DIR"

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
for arg in "$@"; do
    case "$arg" in
        --clean|--cleanup)
            CLEAN_BEFORE_INSTALL=1
            ;;
    esac
done
if [ "$CLEAN_BEFORE_INSTALL" = "1" ]; then
    echo "Preparing system (cleanup to avoid conflicts)..."
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
  python3-venv python3-dev build-essential libjpeg-dev zlib1g-dev \
  mariadb-server mariadb-client nginx certbot python3-certbot-nginx

echo "Installing Node.js LTS (includes current npm)..."
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
echo "node version: $(node -v)"
echo "npm version: $(npm -v)"

echo "Setting up MariaDB..."
# Load environment variables from .env file if it exists
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
else
    echo "Warning: .env file not found. Using default values."
    DB_ROOT_PASSWORD=${DB_ROOT_PASSWORD:-rootpassword}
    DB_NAME=${DB_NAME:-experiment_data}
    DB_USER=${DB_USER:-adamant_user}
    DB_PASSWORD=${DB_PASSWORD:-adamant_password}
fi

# Write DB config for backend
cat > "$ROOT_DIR/backend/conf/db_config.json" <<EOF
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
cd "$ROOT_DIR/backend"
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
cd "$ROOT_DIR"
# Work around React 18 + Material-UI v4 peer dependency conflicts.
NPM_INSTALL_FLAGS=${NPM_INSTALL_FLAGS:---legacy-peer-deps}
npm install $NPM_INSTALL_FLAGS
npm run build

echo "Copying adamant build to Nginx root..."
sudo cp -r "$ROOT_DIR/build" /var/www/html/

cd "$ROOT_DIR/db-ui"
npm install $NPM_INSTALL_FLAGS
npm run build

echo "Copying db-ui build to Nginx root..."
sudo mkdir -p /var/www/html/build/db-ui
sudo cp -r "$ROOT_DIR/db-ui/build/"* /var/www/html/build/db-ui/

echo "Setting up Nginx..."
cd "$ROOT_DIR"
sudo cp "$ROOT_DIR/deployment/nginx.default.prod.conf" /etc/nginx/conf.d/adamant.conf
sudo systemctl restart nginx

echo "Copying Bash scripts to /home/user/scripts..."
mkdir -p /home/user/scripts
cp "$ROOT_DIR/bin/insert_data2db.sh" /home/user/scripts/
chmod +x /home/user/scripts/insert_data2db.sh

echo "Copying .env file to /home/user/scripts/..."
if [ -f "$ROOT_DIR/.env" ]; then
    cp "$ROOT_DIR/.env" /home/user/scripts/.env
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
bash "$ROOT_DIR/deployment/setup_cron_web_server.sh"

echo "Adamant Web Server Machine setup complete."
