# Trinity

A lean data lakehouse platform on Azure for BI and analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Azure (eastus2)                                │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────────┐  │
│  │             │    │             │    │     ADLS Gen2 (Data Lake)       │  │
│  │  Source DB  │───▶│   Airflow   │───▶│  ┌─────────┬────────┬───────┐  │  │
│  │   (ERP)     │    │    (VM)     │    │  │ Bronze  │ Silver │ Gold  │  │  │
│  │             │    │             │    │  │  (raw)  │(clean) │ (agg) │  │  │
│  └─────────────┘    └─────────────┘    │  └─────────┴────────┴───────┘  │  │
│                            │           └─────────────────────────────────┘  │
│                            │                          │                     │
│                            ▼                          ▼                     │
│                     ┌─────────────┐           ┌─────────────┐               │
│                     │  Azure SQL  │◀──────────│  Power BI   │               │
│                     │  (serving)  │           │             │               │
│                     └─────────────┘           └─────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Component | Status |
|-------|-----------|--------|
| 1. Source | Azure SQL DB / ERP | - |
| 2. Orchestration | Airflow on Azure VM | ✅ |
| 3. Lakehouse | ADLS Gen2 (Bronze/Silver/Gold) | ✅ |
| 4. Transform | Airflow + Python/Pandas | - |
| 5. Serving | Azure SQL DB | - |
| 6. BI | Power BI | - |

## Prerequisites

- macOS or Linux
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- Azure subscription

```bash
# Install Azure CLI (macOS)
brew install azure-cli

# Login
az login
```

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/gaon-ai/trinity.git
cd trinity

# Set your subscription ID
export SUBSCRIPTION_ID="your-subscription-id"
```

### 2. Create the VM (Airflow)

```bash
cd infra
chmod +x *.sh
./01-create-vm.sh
```

### 3. Setup the VM

```bash
# Copy files to VM (use the IP from step 2)
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/

# SSH and run setup
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh

# Start Airflow
cd /opt/airflow && docker-compose up -d
```

### 4. Create Data Lake

```bash
./03-create-datalake.sh
```

### 5. Access Airflow

Open `http://<VM_IP>:8080` - credentials are shown during setup.

## Project Structure

```
trinity/
├── infra/
│   ├── variables.sh          # All configuration in one place
│   ├── 01-create-vm.sh       # Creates Azure VM for Airflow
│   ├── 02-setup-vm.sh        # Installs Docker + Airflow on VM
│   └── 03-create-datalake.sh # Creates ADLS Gen2 with medallion layers
├── airflow/
│   ├── docker-compose.yaml   # Airflow services
│   ├── .env.example          # Environment template
│   └── dags/
│       └── example_dag.py    # Sample DAG
├── docs/
│   └── infrastructure-request.md  # Template for infra team
└── README.md
```

## Configuration

All settings are in `infra/variables.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `SUBSCRIPTION_ID` | - | **Required**: Your Azure subscription |
| `RESOURCE_GROUP` | `airflow-rg` | Azure resource group |
| `LOCATION` | `eastus2` | Azure region |
| `VM_SIZE` | `Standard_D4s_v3` | VM size (4 vCPU, 16 GB) |
| `ALLOW_ALL_IPS` | `true` | Open firewall to all IPs |
| `STORAGE_ACCOUNT_NAME` | `trinitylake` | Data lake storage name |

## Current Deployment

| Resource | Value |
|----------|-------|
| VM IP | `172.200.54.159` |
| Airflow URL | http://172.200.54.159:8080 |
| Data Lake | `gaaborotrinity` |
| SSH | `ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159` |

### Data Lake URLs

```
Bronze: abfss://bronze@gaaborotrinity.dfs.core.windows.net/
Silver: abfss://silver@gaaborotrinity.dfs.core.windows.net/
Gold:   abfss://gold@gaaborotrinity.dfs.core.windows.net/
```

## Operations

```bash
# SSH into VM
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# View Airflow logs
cd /opt/airflow && docker-compose logs -f

# Restart Airflow
cd /opt/airflow && docker-compose restart

# Deploy new DAGs
scp -i ~/.ssh/airflow_vm_key my_dag.py azureuser@<VM_IP>:/opt/airflow/dags/
```

## Costs

| Resource | Monthly Cost |
|----------|-------------|
| VM (Standard_D4s_v3) | ~$140 |
| Storage (128 GB SSD) | ~$20 |
| Data Lake (pay per use) | ~$5-20 |
| Static IP | ~$3 |
| **Total** | **~$170** |

## Cleanup

```bash
# Delete everything
az group delete --name airflow-rg --yes --no-wait
```

## Troubleshooting

**Can't access Airflow UI?**
```bash
# Check VM is running
az vm show -g airflow-rg -n airflow-vm --query powerState

# Check firewall allows your IP
az network nsg rule list -g airflow-rg --nsg-name airflow-nsg -o table

# Open to all IPs if needed
az network nsg rule update -g airflow-rg --nsg-name airflow-nsg \
  -n AllowAirflowUI --source-address-prefixes '*'
```

**Provider not registered?**
```bash
az provider register --namespace Microsoft.Storage --wait
az provider register --namespace Microsoft.Network --wait
az provider register --namespace Microsoft.Compute --wait
```

## License

MIT
