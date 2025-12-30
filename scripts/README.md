# Scripts

All scripts for deploying and operating the Trinity Data Lakehouse platform.

## Infrastructure Scripts (One-Time Setup)

Run these in order when setting up a new environment.

| Script | Purpose | Output |
|--------|---------|--------|
| `variables.sh` | Configuration variables | Sourced by other scripts |
| `01-create-vm.sh` | Create VM, VNet, NSG | VM IP, SSH key |
| `02-setup-vm.sh` | Install Docker & Airflow on VM | Airflow password |
| `03-create-datalake.sh` | Create ADLS Gen2 Data Lake | Storage account, key |
| `04-create-synapse.sh` | Create Synapse Serverless | Synapse endpoint, password |
| `05-create-azure-sql.sh` | Create Azure SQL *(optional)* | SQL server, password |

### Deployment Order

```bash
# 1. Set environment variables
export SUBSCRIPTION_ID="your-subscription-id"
export RESOURCE_GROUP="trinity-rg"
export LOCATION="eastus2"
export VM_NAME="airflow-vm"
export VNET_NAME="trinity-vnet"
export SUBNET_NAME="trinity-subnet"

# 2. Create infrastructure
./scripts/01-create-vm.sh           # ~5 min
./scripts/03-create-datalake.sh     # ~2 min
./scripts/04-create-synapse.sh      # ~5 min

# 3. Setup VM (run on VM after SSH)
scp -i ~/.ssh/airflow_vm_key -r airflow scripts/02-setup-vm.sh azureuser@<VM_IP>:~/
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh

# 4. Configure Synapse (from local machine)
python3 scripts/synapse_setup.py
```

---

## Operational Scripts (Day-to-Day)

| Script | Purpose | Usage |
|--------|---------|-------|
| `local-dev.sh` | Local Airflow development | `./scripts/local-dev.sh start` |
| `validate_dags.py` | Validate DAG syntax | `python3 scripts/validate_dags.py` |
| `synapse_setup.py` | Configure Synapse credentials | One-time after Synapse creation |

### Local Development

```bash
# Start local Airflow
./scripts/local-dev.sh start

# Stop
./scripts/local-dev.sh stop

# Validate DAGs before committing
./scripts/local-dev.sh test
# or
python3 scripts/validate_dags.py
```

---

## Script Details

### variables.sh

Configuration sourced by infrastructure scripts:

```bash
SUBSCRIPTION_ID     # Azure subscription
RESOURCE_GROUP      # Resource group name
LOCATION            # Azure region (default: eastus2)
VM_NAME             # VM name
VNET_NAME           # Virtual network name
SUBNET_NAME         # Subnet name
STORAGE_ACCOUNT_NAME # Data Lake storage account
SYNAPSE_WORKSPACE_NAME # Synapse workspace
```

### 01-create-vm.sh

Creates:
- Resource Group
- VNet (10.0.0.0/16) + Subnet (10.0.1.0/24)
- NSG with SSH access (port 22)
- Static Public IP
- Ubuntu 22.04 VM (D4s_v3)
- SSH key at `~/.ssh/airflow_vm_key`

### 02-setup-vm.sh

Run on the VM after SSH. Installs:
- Docker & Docker Compose
- Airflow containers (webserver, scheduler, postgres)
- Generates admin password

### 03-create-datalake.sh

Creates:
- ADLS Gen2 storage account with HNS
- Bronze, Silver, Gold containers
- Private Endpoint
- RBAC for VM Managed Identity

### 04-create-synapse.sh

Creates:
- Synapse workspace
- Firewall rules (Azure services)
- Private Endpoint
- Generates SQL admin password

### 05-create-azure-sql.sh

*Optional* - Creates:
- Azure SQL Server
- Trinity database
- Firewall rules
- Private Endpoint

### synapse_setup.py

Configures Synapse Serverless:
- Creates `trinity` database
- Creates credential for Data Lake access
- Creates external data source

Required environment variables:
```bash
export SYNAPSE_SERVER="<workspace>-ondemand.sql.azuresynapse.net"
export SYNAPSE_USER="sqladmin"
export SYNAPSE_PASSWORD="<password>"
export STORAGE_ACCOUNT_NAME="<storage-account>"
export STORAGE_ACCOUNT_KEY="<storage-key>"
```

### local-dev.sh

Local Airflow development helper:

```bash
./scripts/local-dev.sh start          # Start containers
./scripts/local-dev.sh stop           # Stop containers
./scripts/local-dev.sh logs           # Tail scheduler logs
./scripts/local-dev.sh logs webserver # Tail webserver logs
./scripts/local-dev.sh test           # Validate DAGs
./scripts/local-dev.sh shell          # Bash into scheduler
./scripts/local-dev.sh trigger <dag>  # Trigger a DAG
```

### validate_dags.py

Validates DAG files without running Airflow:
- Checks Python syntax
- Verifies DAG imports and structure
- Used by CI/CD pipeline
