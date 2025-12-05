#!/bin/bash
set -e

echo "=== Setting up Airflow VM ==="

# Update system
echo "Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install Docker
echo "Installing Docker..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add current user to docker group
sudo usermod -aG docker $USER

# Install Docker Compose standalone (for compatibility)
echo "Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Create Airflow directory structure
echo "Setting up Airflow directories..."
sudo mkdir -p /opt/airflow/{dags,logs,plugins,config}
sudo chown -R $USER:$USER /opt/airflow

# Copy airflow files if they exist in home directory
if [ -d "$HOME/airflow" ]; then
    echo "Copying Airflow configuration..."
    cp -r $HOME/airflow/* /opt/airflow/
fi

# Create .env file from example if it doesn't exist
if [ -f "/opt/airflow/.env.example" ] && [ ! -f "/opt/airflow/.env" ]; then
    echo "Creating .env file..."
    cp /opt/airflow/.env.example /opt/airflow/.env

    # Generate Fernet key
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || openssl rand -base64 32)
    sed -i "s/YOUR_FERNET_KEY_HERE/$FERNET_KEY/" /opt/airflow/.env

    # Generate random admin password
    ADMIN_PASS=$(openssl rand -base64 12)
    sed -i "s/YOUR_ADMIN_PASSWORD/$ADMIN_PASS/" /opt/airflow/.env

    echo "Generated admin password: $ADMIN_PASS"
    echo "Please save this password securely!"
fi

# Configure firewall
echo "Configuring firewall..."
sudo ufw allow 22/tcp
sudo ufw allow 8080/tcp
sudo ufw --force enable

# Create systemd service for Airflow
echo "Creating systemd service..."
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
sudo systemctl enable airflow.service

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To start Airflow:"
echo "  cd /opt/airflow && docker-compose up -d"
echo ""
echo "Or use systemd:"
echo "  sudo systemctl start airflow"
echo ""
echo "Airflow UI will be available at: http://$(curl -s https://ipinfo.io/ip):8080"
echo ""
echo "NOTE: You may need to log out and back in for docker group permissions to take effect."
echo "      Or run: newgrp docker"
