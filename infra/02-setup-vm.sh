#!/bin/bash
set -e

echo "=== Setting up Airflow VM ==="
echo ""

# Update system
echo "[1/6] Updating system packages..."
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -qq

# Install Docker
echo "[2/6] Installing Docker..."
sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg 2>/dev/null

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -qq
sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add current user to docker group
sudo usermod -aG docker $USER

# Install Docker Compose standalone
echo "[3/6] Installing Docker Compose..."
sudo curl -sL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Create Airflow directory structure
echo "[4/6] Setting up Airflow directories..."
sudo mkdir -p /opt/airflow/{dags,logs,plugins,config}
sudo chown -R $USER:$USER /opt/airflow

# Copy airflow files if they exist in home directory
if [ -d "$HOME/airflow" ]; then
    echo "Copying Airflow configuration from ~/airflow..."
    cp -r $HOME/airflow/* /opt/airflow/
fi

# Create .env file with secure credentials
echo "[5/6] Generating secure credentials..."
FERNET_KEY=$(openssl rand -base64 32)
ADMIN_PASS=$(openssl rand -base64 12 | tr -d '/+=')
POSTGRES_PASS="airflow_$(openssl rand -hex 8)"

cat > /opt/airflow/.env << EOF
# Airflow Configuration
# Generated on $(date)

AIRFLOW_UID=$(id -u)
AIRFLOW_FERNET_KEY=$FERNET_KEY
POSTGRES_PASSWORD=$POSTGRES_PASS
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$ADMIN_PASS
AIRFLOW_ADMIN_EMAIL=admin@example.com
_PIP_ADDITIONAL_REQUIREMENTS=
EOF

# Configure firewall
echo "[6/6] Configuring firewall..."
sudo ufw allow 22/tcp >/dev/null
sudo ufw allow 8080/tcp >/dev/null
sudo ufw --force enable >/dev/null

# Create systemd service for Airflow
sudo tee /etc/systemd/system/airflow.service > /dev/null <<EOF
[Unit]
Description=Airflow Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/airflow
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
User=$USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable airflow.service >/dev/null

# Get public IP
PUBLIC_IP=$(curl -s https://ipinfo.io/ip)

echo ""
echo "=============================================="
echo "         Setup Complete!                     "
echo "=============================================="
echo ""
echo "Airflow Credentials:"
echo "  Username: admin"
echo "  Password: $ADMIN_PASS"
echo ""
echo "IMPORTANT: Save this password securely!"
echo ""
echo "To start Airflow:"
echo "  cd /opt/airflow"
echo "  docker-compose up -d"
echo ""
echo "Airflow UI will be available at:"
echo "  http://$PUBLIC_IP:8080"
echo ""
echo "NOTE: Run 'newgrp docker' or log out/in for docker permissions."
echo ""
