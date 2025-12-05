# Trinity

Airflow deployment on Azure VM with Docker Compose.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Azure (eastus2)                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Resource Group: airflow-rg               │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │         VM: airflow-vm (Standard_D4s_v3)        │  │  │
│  │  │         Ubuntu 22.04 LTS | 128 GB SSD           │  │  │
│  │  │  ┌───────────────────────────────────────────┐  │  │  │
│  │  │  │            Docker Compose                 │  │  │  │
│  │  │  │  ┌─────────────┐  ┌─────────────────────┐ │  │  │  │
│  │  │  │  │  PostgreSQL │  │  Airflow Webserver  │ │  │  │  │
│  │  │  │  │    :5432    │  │       :8080         │ │  │  │  │
│  │  │  │  └─────────────┘  └─────────────────────┘ │  │  │  │
│  │  │  │  ┌─────────────────────────────────────┐  │  │  │  │
│  │  │  │  │        Airflow Scheduler            │  │  │  │  │
│  │  │  │  │        (LocalExecutor)              │  │  │  │  │
│  │  │  │  └─────────────────────────────────────┘  │  │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  │  NSG: SSH (22), Airflow UI (8080)                     │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- macOS or Linux
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- Azure subscription with permissions to create resources

### Install Azure CLI (macOS)

```bash
brew install azure-cli
```

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/gaon-ai/trinity.git
cd trinity
```

Edit `infra/variables.sh` and set your subscription ID:

```bash
export SUBSCRIPTION_ID="your-subscription-id-here"
```

### 2. Login to Azure

```bash
az login
```

### 3. Create the VM

```bash
cd infra
chmod +x *.sh
./01-create-vm.sh
```

This creates:
| Resource | Value |
|----------|-------|
| Resource Group | `airflow-rg` |
| VM Size | `Standard_D4s_v3` (4 vCPU, 16 GB RAM) |
| Region | `eastus2` |
| OS | Ubuntu 22.04 LTS |
| Disk | 128 GB Premium SSD |
| SSH Key | `~/.ssh/airflow_vm_key` |

The script outputs the VM's public IP and SSH command.

### 4. Copy files and setup VM

```bash
# Copy files to VM (replace <VM_IP> with actual IP)
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/

# SSH into the VM
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# Run setup script
chmod +x 02-setup-vm.sh
./02-setup-vm.sh
```

The setup script:
- Installs Docker and Docker Compose
- Creates `/opt/airflow` directory structure
- Configures firewall (ufw)
- Creates systemd service for auto-restart

### 5. Configure and start Airflow

```bash
# Create .env file
cd /opt/airflow
cp .env.example .env

# Generate secure keys
FERNET_KEY=$(openssl rand -base64 32)
ADMIN_PASS=$(openssl rand -base64 12 | tr -d "/+=")

# Update .env with generated values
sed -i "s|YOUR_FERNET_KEY_HERE|$FERNET_KEY|" .env
sed -i "s|YOUR_ADMIN_PASSWORD|$ADMIN_PASS|" .env

# Save your admin password!
echo "Admin password: $ADMIN_PASS"

# Start Airflow
docker-compose up -d
```

### 6. Access Airflow

Open `http://<VM_IP>:8080` in your browser.

Login with:
- **Username**: `admin`
- **Password**: (the password you generated above)

### 7. (Optional) Open firewall for public access

By default, the firewall restricts access to your IP. To open it:

```bash
# From your local machine
az network nsg rule update \
  --resource-group airflow-rg \
  --nsg-name airflow-nsg \
  --name AllowAirflowUI \
  --source-address-prefixes '*'
```

## Project Structure

```
trinity/
├── infra/
│   ├── variables.sh          # Azure configuration (subscription, region, VM size)
│   ├── 01-create-vm.sh       # Creates Azure resources (VM, network, firewall)
│   └── 02-setup-vm.sh        # Configures VM (Docker, directories, systemd)
├── airflow/
│   ├── docker-compose.yaml   # Airflow services (webserver, scheduler, postgres)
│   ├── .env.example          # Environment variables template
│   └── dags/
│       └── example_dag.py    # Sample DAG for testing
└── README.md
```

## Configuration

### VM Settings (`infra/variables.sh`)

| Variable | Default | Description |
|----------|---------|-------------|
| `SUBSCRIPTION_ID` | - | Your Azure subscription ID |
| `RESOURCE_GROUP` | `airflow-rg` | Azure resource group name |
| `LOCATION` | `eastus2` | Azure region |
| `VM_SIZE` | `Standard_D4s_v3` | VM size (4 vCPU, 16 GB RAM) |
| `OS_DISK_SIZE` | `128` | OS disk size in GB |
| `ADMIN_USERNAME` | `azureuser` | SSH username |

### Airflow Settings (`airflow/.env`)

| Variable | Description |
|----------|-------------|
| `AIRFLOW_UID` | User ID for Airflow containers (default: 50000) |
| `AIRFLOW_FERNET_KEY` | Encryption key for sensitive data |
| `POSTGRES_PASSWORD` | PostgreSQL database password |
| `AIRFLOW_ADMIN_USER` | Admin username |
| `AIRFLOW_ADMIN_PASSWORD` | Admin password |
| `_PIP_ADDITIONAL_REQUIREMENTS` | Extra Python packages to install |

## Operations

### SSH into VM

```bash
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
```

### View logs

```bash
cd /opt/airflow
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
```

### Restart Airflow

```bash
cd /opt/airflow
docker-compose restart
```

### Stop Airflow

```bash
cd /opt/airflow
docker-compose down
```

### Update Airflow

```bash
cd /opt/airflow
docker-compose pull
docker-compose up -d
```

### Add DAGs

Copy your DAG files to `/opt/airflow/dags/` on the VM:

```bash
scp -i ~/.ssh/airflow_vm_key my_dag.py azureuser@<VM_IP>:/opt/airflow/dags/
```

## Costs

Estimated monthly cost: **~$163**

| Resource | Cost |
|----------|------|
| VM (Standard_D4s_v3) | ~$140 |
| Storage (128 GB Premium SSD) | ~$20 |
| Static Public IP | ~$3 |

### Cost optimization options

- Use `Standard_D2s_v3` for lighter workloads (~$70/month)
- Use Standard SSD instead of Premium (~$10/month)
- Deallocate VM when not in use

## Cleanup

Delete all Azure resources:

```bash
az group delete --name airflow-rg --yes --no-wait
```

This removes the VM, networking, storage, and all associated resources.

## Troubleshooting

### Cannot access Airflow UI

1. Check VM is running: `az vm show -g airflow-rg -n airflow-vm --query powerState`
2. Check your IP is allowed: `az network nsg rule list -g airflow-rg --nsg-name airflow-nsg -o table`
3. Check Airflow is healthy: `ssh ... 'curl http://localhost:8080/health'`

### Airflow containers not starting

```bash
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
cd /opt/airflow
docker-compose logs
```

### Permission denied errors

```bash
# On the VM, ensure correct ownership
sudo chown -R 50000:0 /opt/airflow/{dags,logs,plugins}
```

## License

MIT
