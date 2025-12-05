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
| 4. Transform | Airflow + Python/Pandas | ✅ |
| 5. Serving | Azure SQL DB | - |
| 6. BI | Power BI | - |

## Current Deployment

| Resource | Value |
|----------|-------|
| **Airflow URL** | http://172.200.54.159:8080 |
| **Username** | `admin` |
| **Password** | `7YZPlNypphFXiHhq` |
| **SSH** | `ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159` |
| **Data Lake** | `gaaborotrinity` |

### Data Lake URLs (ABFS)

```
Bronze: abfss://bronze@gaaborotrinity.dfs.core.windows.net/
Silver: abfss://silver@gaaborotrinity.dfs.core.windows.net/
Gold:   abfss://gold@gaaborotrinity.dfs.core.windows.net/
```

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

## Quick Start (From Scratch)

### 1. Clone and configure

```bash
git clone https://github.com/gaon-ai/trinity.git
cd trinity

# Set your subscription ID
export SUBSCRIPTION_ID="your-subscription-id"
```

### 2. Create the VM

```bash
cd infra
chmod +x *.sh
./01-create-vm.sh
```

### 3. Setup the VM

```bash
# Copy files to VM (use IP from step 2)
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/

# SSH and run setup
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh

# Build and start Airflow
cd /opt/airflow
docker-compose build
docker-compose up -d
```

### 4. Create Data Lake

```bash
# Back on local machine
cd infra
./03-create-datalake.sh
```

### 5. Configure Data Lake credentials on VM

```bash
# Add storage credentials to Airflow
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# Append to .env (replace with your actual key)
cat >> /opt/airflow/.env << 'EOF'
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_key
EOF

# Restart Airflow
cd /opt/airflow && docker-compose restart
```

### 6. Access Airflow

Open `http://<VM_IP>:8080` - credentials shown during VM setup.

## Project Structure

```
trinity/
├── infra/
│   ├── variables.sh          # All configuration in one place
│   ├── 01-create-vm.sh       # Creates Azure VM for Airflow
│   ├── 02-setup-vm.sh        # Installs Docker + Airflow on VM
│   └── 03-create-datalake.sh # Creates ADLS Gen2 with medallion layers
├── airflow/
│   ├── Dockerfile            # Custom Airflow image with Azure libs
│   ├── docker-compose.yaml   # Airflow services
│   ├── .env.example          # Environment template
│   └── dags/
│       ├── example_dag.py        # Simple test DAG
│       └── datalake_sample_dag.py # Medallion architecture demo
├── docs/
│   └── infrastructure-request.md  # Template for infra team
└── README.md
```

## Sample DAG: Medallion Architecture

The `datalake_sample` DAG demonstrates the Bronze → Silver → Gold pattern:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ write_to_bronze │───▶│transform_to_silver│───▶│aggregate_to_gold│
│   (raw JSON)    │    │  (cleaned CSV)   │    │  (aggregates)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

| Layer | Path | Content |
|-------|------|---------|
| Bronze | `erp/sales/date=YYYY-MM-DD/sales_raw.json` | Raw sales data |
| Silver | `erp/sales/date=YYYY-MM-DD/sales_cleaned.csv` | Cleaned + calculated fields |
| Gold | `analytics/sales_by_region/.../data.csv` | Regional aggregates |
| Gold | `analytics/sales_by_product/.../data.csv` | Product aggregates |
| Gold | `serving/daily_summary/.../summary.json` | Daily summary for BI |

### Trigger the DAG

```bash
# Via CLI
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP> \
  'cd /opt/airflow && docker-compose exec -T airflow-scheduler airflow dags trigger datalake_sample'

# Or via UI at http://<VM_IP>:8080
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

## Operations

### SSH into VM

```bash
ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159
```

### View Airflow logs

```bash
cd /opt/airflow && docker-compose logs -f airflow-scheduler
```

### Restart Airflow

```bash
cd /opt/airflow && docker-compose restart
```

### Rebuild Airflow (after Dockerfile changes)

```bash
cd /opt/airflow && docker-compose down && docker-compose build --no-cache && docker-compose up -d
```

### Deploy new DAGs

```bash
scp -i ~/.ssh/airflow_vm_key my_dag.py azureuser@172.200.54.159:~/
ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159 'sudo cp ~/my_dag.py /opt/airflow/dags/ && sudo chown 50000:0 /opt/airflow/dags/my_dag.py'
```

### List Data Lake files

```bash
STORAGE_KEY=$(az storage account keys list -g airflow-rg -n gaaborotrinity --query '[0].value' -o tsv)
az storage fs file list --file-system bronze --account-name gaaborotrinity --account-key "$STORAGE_KEY" --recursive -o table
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

### Can't access Airflow UI

```bash
# 1. Check VM is running
az vm show -g airflow-rg -n airflow-vm --query powerState

# 2. Check firewall
az network nsg rule list -g airflow-rg --nsg-name airflow-nsg -o table

# 3. Open to all IPs if needed
az network nsg rule update -g airflow-rg --nsg-name airflow-nsg \
  -n AllowAirflowUI --source-address-prefixes '*'
```

### Azure provider not registered

```bash
az provider register --namespace Microsoft.Storage --wait
az provider register --namespace Microsoft.Network --wait
az provider register --namespace Microsoft.Compute --wait
```

### Storage account name taken

Storage account names must be globally unique. The script auto-adds a random suffix if the name is taken. Or set a custom name:

```bash
export STORAGE_ACCOUNT_NAME="myuniquename123"
./03-create-datalake.sh
```

### Airflow init fails with pip error

This happens when using `_PIP_ADDITIONAL_REQUIREMENTS` with the init container. Solution: use a custom Dockerfile (already implemented):

```dockerfile
FROM apache/airflow:2.8.1
USER airflow
RUN pip install --no-cache-dir azure-storage-file-datalake pandas pyarrow
```

### Permission denied when copying DAGs

```bash
# Fix ownership
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP> \
  'sudo chown -R 50000:0 /opt/airflow/dags'
```

### Docker permission denied

```bash
# Add user to docker group (already done in setup, but if needed)
sudo usermod -aG docker $USER
newgrp docker
```

### DAG not appearing in Airflow UI

```bash
# Check for syntax errors
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP> \
  'cd /opt/airflow && docker-compose exec -T airflow-scheduler python /opt/airflow/dags/your_dag.py'

# Check scheduler logs
cd /opt/airflow && docker-compose logs airflow-scheduler | grep -i error
```

## License

MIT
