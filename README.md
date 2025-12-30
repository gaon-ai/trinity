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
│                            │           └───────────────┬────────────────┘  │
│                            │                           │                   │
│                            ▼                           ▼                   │
│                     ┌─────────────┐           ┌─────────────────┐          │
│                     │  Azure SQL  │           │     Synapse     │          │
│                     │  (serving)  │           │   (Serverless)  │◀── SQL   │
│                     └─────────────┘           └────────┬────────┘          │
│                            │                           │                   │
│                            └───────────┬───────────────┘                   │
│                                        ▼                                   │
│                                ┌─────────────┐                             │
│                                │  Power BI   │                             │
│                                └─────────────┘                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Component | Status |
|-------|-----------|--------|
| 1. Source | Azure SQL DB / ERP | - |
| 2. Orchestration | Airflow on Azure VM | ✅ |
| 3. Lakehouse | ADLS Gen2 (Bronze/Silver/Gold) | ✅ |
| 4. Transform | Airflow + Python/Pandas | ✅ |
| 5. Ad-hoc Query | Synapse Serverless SQL | ✅ |
| 6. CI/CD | GitHub Actions | ✅ |
| 7. Serving | Azure SQL DB | ✅ |
| 8. BI | Power BI | - |

## Current Deployment

| Resource | Value |
|----------|-------|
| **Airflow URL** | http://172.200.54.159:8080 |
| **Username** | `admin` |
| **Password** | `7YZPlNypphFXiHhq` |
| **SSH** | `ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159` |
| **Data Lake** | `gaaborotrinity` |
| **Synapse** | `trinitysynapse-ondemand.sql.azuresynapse.net` |
| **Synapse DB** | `trinity` (use this, not `master`) |
| **Azure SQL** | `trinitydb.database.windows.net` |
| **Azure SQL DB** | `trinity` |

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

### 5. Create Synapse (Optional - for ad-hoc SQL queries)

```bash
./04-create-synapse.sh
```

### 6. Create Azure SQL Database (for Power BI serving layer)

```bash
./05-create-azure-sql.sh
```

### 7. Configure Data Lake and SQL credentials on VM

```bash
# Add storage and SQL credentials to Airflow
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# Append to .env (replace with your actual values)
cat >> /opt/airflow/.env << 'EOF'
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_key
AZURE_SQL_SERVER=trinitydb.database.windows.net
AZURE_SQL_DATABASE=trinity
AZURE_SQL_USER=sqladmin
AZURE_SQL_PASSWORD=your_sql_password
EOF

# Restart Airflow
cd /opt/airflow && docker-compose restart
```

### 8. Access Airflow

Open `http://<VM_IP>:8080` - credentials shown during VM setup.

## CI/CD: Automated DAG Deployment

DAGs are automatically deployed to the Airflow VM when changes are pushed to `main`.

### How It Works

```
Push to main (airflow/** changed)
         │
         ▼
   ┌─────────────┐
   │  Validate   │ ─── python syntax check
   │   DAGs      │
   └─────────────┘
         │ (pass)
         ▼
   ┌─────────────┐
   │   Deploy    │ ─── rsync to VM
   │   to VM     │
   └─────────────┘
```

### Setup (One-time)

Add these secrets to GitHub (Settings → Secrets → Actions):

| Secret | Value |
|--------|-------|
| `AIRFLOW_VM_IP` | `172.200.54.159` |
| `AIRFLOW_SSH_USER` | `azureuser` |
| `AIRFLOW_SSH_KEY` | Contents of `~/.ssh/airflow_vm_key` |

### What Gets Deployed

- **DAG changes** (`airflow/dags/*`): Synced to VM, ownership fixed
- **Dockerfile/docker-compose changes**: Containers rebuilt and restarted

### Manual Deployment (if needed)

```bash
scp -i ~/.ssh/airflow_vm_key airflow/dags/*.py azureuser@172.200.54.159:~/
ssh -i ~/.ssh/airflow_vm_key azureuser@172.200.54.159 \
  'sudo cp ~/*.py /opt/airflow/dags/ && sudo chown -R 50000:0 /opt/airflow/dags/'
```

### Test Workflow Locally

```bash
# Install act (GitHub Actions local runner)
brew install act

# Test validate job only
act -j validate --container-architecture linux/amd64

# Test full deploy (requires secrets)
act -j deploy \
  --secret AIRFLOW_VM_IP="172.200.54.159" \
  --secret AIRFLOW_SSH_USER="azureuser" \
  --secret AIRFLOW_SSH_KEY="$(cat ~/.ssh/airflow_vm_key)" \
  --container-architecture linux/amd64
```

## Project Structure

```
trinity/
├── .github/
│   └── workflows/
│       └── deploy-airflow.yml  # CI/CD pipeline
├── infra/
│   ├── variables.sh          # All configuration in one place
│   ├── 01-create-vm.sh       # Creates Azure VM for Airflow
│   ├── 02-setup-vm.sh        # Installs Docker + Airflow on VM
│   ├── 03-create-datalake.sh # Creates ADLS Gen2 with medallion layers
│   ├── 04-create-synapse.sh  # Creates Synapse for ad-hoc SQL queries
│   └── 05-create-azure-sql.sh # Creates Azure SQL for Power BI serving
├── airflow/
│   ├── Dockerfile            # Custom Airflow image with Azure libs
│   ├── docker-compose.yaml   # Airflow services
│   ├── .env.example          # Environment template
│   └── dags/
│       ├── example_dag.py        # Simple test DAG
│       └── datalake_sample_dag.py # Medallion architecture demo
├── scripts/
│   └── synapse_setup.py      # Synapse configuration script
├── docs/
│   ├── infrastructure-request.md  # Template for infra team
│   ├── synapse-setup.md           # Synapse troubleshooting guide
│   ├── azure-sql-setup.md         # Azure SQL setup guide
│   └── replication-guide.md       # Full setup in new Azure account
└── README.md
```

## Sample DAG: Medallion Architecture

The `datalake_sample` DAG demonstrates the full Bronze → Silver → Gold → SQL pattern:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ write_to_bronze │───▶│transform_to_silver│───▶│aggregate_to_gold│───▶│   load_to_sql   │
│   (raw JSON)    │    │  (cleaned CSV)   │    │  (aggregates)   │    │  (Power BI)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

| Layer | Path/Table | Content |
|-------|------------|---------|
| Bronze | `erp/sales/date=YYYY-MM-DD/sales_raw.json` | Raw sales data |
| Silver | `erp/sales/date=YYYY-MM-DD/sales_cleaned.csv` | Cleaned + calculated fields |
| Gold | `analytics/sales_by_region/.../data.csv` | Regional aggregates |
| Gold | `analytics/sales_by_product/.../data.csv` | Product aggregates |
| Gold | `serving/daily_summary/.../summary.json` | Daily summary for BI |
| SQL | `sales_by_region` table | Regional aggregates for Power BI |
| SQL | `sales_by_product` table | Product aggregates for Power BI |

### Trigger the DAG

```bash
# Via CLI
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP> \
  'cd /opt/airflow && docker-compose exec -T airflow-scheduler airflow dags trigger datalake_sample'

# Or via UI at http://<VM_IP>:8080
```

## Synapse: Ad-hoc SQL Queries

Synapse Serverless SQL lets you query Data Lake files directly using SQL - no data loading required.

### Setup

```bash
# 1. Create Synapse workspace
cd infra
./04-create-synapse.sh

# 2. Configure credentials (run once)
python3 scripts/synapse_setup.py
```

Or see `docs/synapse-setup.md` for manual setup steps.

### Sample Queries

**Important:** Use the `trinity` database, not `master`.

Query CSV files in Silver layer:
```sql
SELECT *
FROM OPENROWSET(
    BULK 'silver/erp/sales/date=2025-12-05/sales_cleaned.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product VARCHAR(100),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    order_date VARCHAR(50),
    region VARCHAR(50),
    total_amount VARCHAR(50),
    processed_at VARCHAR(100)
) AS sales
```

Query with wildcard (single level only, no `**`):
```sql
SELECT *
FROM OPENROWSET(
    BULK 'silver/erp/sales/*/sales_cleaned.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product VARCHAR(100),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    order_date VARCHAR(50),
    region VARCHAR(50),
    total_amount VARCHAR(50),
    processed_at VARCHAR(100)
) AS sales
```

### Cost

| Usage | Cost |
|-------|------|
| Data scanned | ~$5 per TB |
| Minimum | $0 (pay only when you query) |

### Common Issues

See `docs/synapse-setup.md` for troubleshooting:

## Azure SQL: Power BI Serving Layer

Azure SQL Database stores Gold layer aggregations for fast Power BI reporting.

### Setup

```bash
cd infra
./05-create-azure-sql.sh
```

### Tables Created by DAG

The `datalake_sample` DAG creates and populates these tables:

| Table | Description |
|-------|-------------|
| `sales_by_region` | Daily sales aggregates by region |
| `sales_by_product` | Daily sales aggregates by product |

### Power BI Connection

| Setting | Value |
|---------|-------|
| Server | `trinitydb.database.windows.net` |
| Database | `trinity` |
| Authentication | SQL Server |
| Username | `sqladmin` |
| Password | (from script output) |

### Sample Query

```sql
SELECT region, SUM(total_revenue) as revenue
FROM sales_by_region
WHERE report_date >= DATEADD(day, -7, GETDATE())
GROUP BY region
ORDER BY revenue DESC
```

### Cost

| SKU | Monthly Cost |
|-----|--------------|
| Basic | ~$5 |
| S0 (default) | ~$15 |
| S1 | ~$30 |

## Synapse Common Issues

- Must use `trinity` database (not `master`)
- Use `FIRSTROW = 2` instead of `HEADER_ROW = TRUE`
- Use single `*` wildcards (not `**`)
- Start with `VARCHAR` for all columns

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
| Azure SQL (S0) | ~$15 |
| Synapse (pay per TB) | ~$5-50 |
| Static IP | ~$3 |
| **Total** | **~$190-250** |

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
az provider register --namespace Microsoft.Synapse --wait
az provider register --namespace Microsoft.Sql --wait
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

### Environment variables not picked up by Airflow

After adding variables to `.env`, you must do a full restart (not just `docker-compose restart`):

```bash
cd /opt/airflow
docker-compose down
docker-compose up -d
```

**Why:** `docker-compose restart` doesn't re-read environment variables from the compose file. Only `down` + `up` does.

### pymssql build fails in Dockerfile

**Error:** `Could not find C compiler` or `setuptools_scm version conflict`

**Solution:** Use pymssql 2.2.x and install build dependencies:

```dockerfile
FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "pymssql>=2.2.0,<2.3.0"
```

### Azure SQL credentials not working in DAG

**Symptom:** Task logs show "Azure SQL credentials not configured. Skipping SQL load."

**Cause:** Environment variables in `.env` must be mapped in `docker-compose.yaml`.

**Solution:** Ensure these lines exist in `docker-compose.yaml` under `environment`:

```yaml
AZURE_SQL_SERVER: ${AZURE_SQL_SERVER:-}
AZURE_SQL_DATABASE: ${AZURE_SQL_DATABASE:-trinity}
AZURE_SQL_USER: ${AZURE_SQL_USER:-sqladmin}
AZURE_SQL_PASSWORD: ${AZURE_SQL_PASSWORD:-}
```

Then restart: `docker-compose down && docker-compose up -d`

### Synapse query fails with "File cannot be opened"

See `docs/synapse-setup.md` for detailed troubleshooting. Common fixes:

1. **Use user database, not `master`** - Create and use `trinity` database
2. **Create database scoped credential** - SAS token or storage key
3. **Set ACLs on files** - `other::r-x` for read access
4. **Use correct OPENROWSET syntax** - `FIRSTROW = 2` not `HEADER_ROW = TRUE`

### CI/CD workflow fails with "rsync not found"

**Solution:** Add rsync installation step to workflow:

```yaml
- name: Install rsync
  run: sudo apt-get update && sudo apt-get install -y rsync
```

### act (local GitHub Actions) fails

**Error:** Architecture mismatch on Apple Silicon

**Solution:** Use `--container-architecture linux/amd64`:

```bash
act -j validate --container-architecture linux/amd64
```

## License

MIT
